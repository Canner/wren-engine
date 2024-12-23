from __future__ import annotations

from app.mdl.rewriter import Rewriter
from app.model import NotFoundError, UnprocessableEntityError
from app.model.connector import Connector
from app.util import base64_to_dict

rules = ["column_is_valid", "relationship_is_valid"]


class Validator:
    def __init__(self, connector: Connector, rewriter: Rewriter):
        self.connector = connector
        self.rewriter = rewriter

    async def validate(self, rule: str, parameters: dict[str, str], manifest_str: str):
        if rule not in rules:
            raise RuleNotFoundError(rule)
        try:
            await getattr(self, f"_validate_{rule}")(parameters, manifest_str)
        except ValidationError as e:
            raise e
        except Exception as e:
            raise ValidationError(f"Unknown exception: {type(e)}, message: {e!s}")

    async def _validate_column_is_valid(
        self, parameters: dict[str, str], manifest_str: str
    ):
        model_name = parameters.get("modelName")
        column_name = parameters.get("columnName")
        if model_name is None:
            raise MissingRequiredParameterError("modelName")
        if column_name is None:
            raise MissingRequiredParameterError("columnName")

        try:
            sql = f'SELECT "{column_name}" FROM "{model_name}" LIMIT 1'
            rewritten_sql = await self.rewriter.rewrite(sql)
            self.connector.dry_run(rewritten_sql)
        except Exception as e:
            raise ValidationError(f"Exception: {type(e)}, message: {e!s}")

    async def _validate_relationship_is_valid(
        self, parameters: dict[str, str], manifest_str: str
    ):
        relationship_name = parameters.get("relationshipName")
        if relationship_name is None:
            raise MissingRequiredParameterError("relationship")

        manifest = base64_to_dict(manifest_str)

        relationship = list(
            filter(lambda r: r["name"] == relationship_name, manifest["relationships"])
        )

        if len(relationship) == 0:
            raise ValidationError(
                f"Relationship {relationship_name} not found in manifest"
            )

        left_model = self._get_model(manifest, relationship[0]["models"][0])
        right_model = self._get_model(manifest, relationship[0]["models"][1])
        relationship_type = relationship[0]["joinType"].lower()
        condition = relationship[0]["condition"]
        columns = condition.split("=")
        left_column = columns[0].strip().split(".")[1]
        right_column = columns[1].strip().split(".")[1]

        def generate_column_is_unique_sql(model_name, column_name):
            return f'SELECT count(*) = count(distinct {column_name}) AS result FROM "{model_name}"'

        def generate_is_exist_join_sql(
            left_model, right_model, left_column, right_column
        ):
            return f'SELECT count(*) > 0 AS result FROM "{left_model}" JOIN "{right_model}" ON "{left_model}"."{left_column}" = "{right_model}"."{right_column}"'

        def generate_sql_from_type(
            relationship_type, left_model, right_model, left_column, right_column
        ):
            if relationship_type == "one_to_one":
                return f"""WITH
                    lefttable AS ({generate_column_is_unique_sql(left_model, left_column)}),
                    righttable AS ({generate_column_is_unique_sql(right_model, right_column)}),
                    joinexist AS ({generate_is_exist_join_sql(left_model, right_model, left_column, right_column)})
                SELECT lefttable.result AND righttable.result AND joinexist.result result,
                    lefttable.result left_table_unique,
                    righttable.result right_table_unique,
                    joinexist.result is_related
                FROM lefttable, righttable, joinexist"""
            elif relationship_type == "many_to_one":
                return f"""WITH 
                    righttable AS ({generate_column_is_unique_sql(right_model, right_column)}),
                    joinexist AS ({generate_is_exist_join_sql(left_model, right_model, left_column, right_column)})
                SELECT righttable.result AND joinexist.result result,
                    righttable.result right_table_unique,
                    joinexist.result is_related
                FROM righttable, joinexist"""
            elif relationship_type == "one_to_many":
                return f"""WITH 
                    lefttable AS ({generate_column_is_unique_sql(left_model, left_column)}),
                    joinexist AS ({generate_is_exist_join_sql(left_model, right_model, left_column, right_column)})
                SELECT lefttable.result AND joinexist.result result,
                    lefttable.result left_table_unique,
                    joinexist.result is_related
                FROM lefttable, joinexist"""
            elif relationship_type == "many_to_many":
                return f"""WITH 
                    joinexist AS ({generate_is_exist_join_sql(left_model, right_model, left_column, right_column)})
                SELECT joinexist.result result,
                    joinexist.result is_related
                FROM joinexist"""
            else:
                raise ValidationError(f"Unknown relationship type: {relationship_type}")

        def format_result(result):
            output = {}
            output["result"] = str(result.get("result").get(0))
            output["is_related"] = str(result.get("is_related").get(0))
            if result.get("left_table_unique") is not None:
                output["left_table_unique"] = str(
                    result.get("left_table_unique").get(0)
                )
            if result.get("right_table_unique") is not None:
                output["right_table_unique"] = str(
                    result.get("right_table_unique").get(0)
                )
            return output

        sql = generate_sql_from_type(
            relationship_type,
            left_model["name"],
            right_model["name"],
            left_column,
            right_column,
        )
        try:
            rewritten_sql = await self.rewriter.rewrite(sql)
            result = self.connector.query(rewritten_sql, limit=1)
            if not result.get("result").get(0):
                raise ValidationError(
                    f"Relationship {relationship_name} is not valid: {format_result(result)}"
                )

        except Exception as e:
            raise ValidationError(f"Exception: {type(e)}, message: {e!s}")

    def _get_model(self, manifest, model_name):
        models = list(filter(lambda m: m["name"] == model_name, manifest["models"]))
        if len(models) == 0:
            raise ValidationError(f"Model {model_name} not found in manifest")
        return models[0]


class ValidationError(UnprocessableEntityError):
    def __init__(self, message: str):
        super().__init__(message)


class RuleNotFoundError(NotFoundError):
    def __init__(self, rule: str):
        super().__init__(f"The rule `{rule}` is not in the rules, rules: {rules}")


class MissingRequiredParameterError(ValidationError):
    def __init__(self, parameter: str):
        super().__init__(f"Missing required parameter: `{parameter}`")
