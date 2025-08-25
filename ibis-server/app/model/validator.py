from __future__ import annotations

from wren_core import (
    RowLevelAccessControl,
    SessionProperty,
    to_manifest,
    validate_rlac_rule,
)

from app.mdl.rewriter import Rewriter
from app.model.connector import Connector
from app.model.error import ErrorCode, ErrorPhase, WrenError
from app.util import base64_to_dict

rules = ["column_is_valid", "relationship_is_valid", "rlac_condition_syntax_is_valid"]


class Validator:
    def __init__(self, connector: Connector, rewriter: Rewriter):
        self.connector = connector
        self.rewriter = rewriter

    async def validate(self, rule: str, parameters: dict, manifest_str: str):
        if rule not in rules:
            raise WrenError(
                ErrorCode.VALIDATION_RULE_NOT_FOUND,
                f"The rule `{rule}` is not in the rules, rules: {rules}",
            )
        try:
            await getattr(self, f"_validate_{rule}")(parameters, manifest_str)
        except WrenError:
            raise
        except Exception as e:
            raise WrenError(
                ErrorCode.GENERIC_USER_ERROR, str(e), phase=ErrorPhase.VALIDATION
            ) from e

    async def _validate_column_is_valid(
        self, parameters: dict[str, str], manifest_str: str
    ):
        model_name = parameters.get("modelName")
        column_name = parameters.get("columnName")
        if model_name is None:
            raise WrenError(
                ErrorCode.VALIDATION_PARAMETER_ERROR,
                "modelName is required",
                phase=ErrorPhase.VALIDATION,
            )
        if column_name is None:
            raise WrenError(
                ErrorCode.VALIDATION_PARAMETER_ERROR,
                "columnName is required",
                phase=ErrorPhase.VALIDATION,
            )

        try:
            sql = f'SELECT "{column_name}" FROM "{model_name}" LIMIT 1'
            rewritten_sql = await self.rewriter.rewrite(sql)
            self.connector.dry_run(rewritten_sql)
        except Exception as e:
            raise WrenError(
                ErrorCode.GENERIC_USER_ERROR, str(e), phase=ErrorPhase.VALIDATION
            ) from e

    async def _validate_relationship_is_valid(
        self, parameters: dict[str, str], manifest_str: str
    ):
        relationship_name = parameters.get("relationshipName")
        if relationship_name is None:
            raise WrenError(
                ErrorCode.VALIDATION_PARAMETER_ERROR,
                "relationshipName is required",
                phase=ErrorPhase.VALIDATION,
            )

        manifest = base64_to_dict(manifest_str)

        relationship = list(
            filter(lambda r: r["name"] == relationship_name, manifest["relationships"])
        )

        if len(relationship) == 0:
            raise WrenError(
                ErrorCode.VALIDATION_PARAMETER_ERROR,
                f"Relationship {relationship_name} not found in manifest",
                phase=ErrorPhase.VALIDATION,
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
                raise WrenError(
                    ErrorCode.VALIDATION_PARAMETER_ERROR,
                    f"Unknown relationship type: {relationship_type}",
                    phase=ErrorPhase.VALIDATION,
                )

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
            result = self.connector.query(rewritten_sql, limit=1).to_pandas()
            if not result.get("result").get(0):
                raise WrenError(
                    ErrorCode.VALIDATION_ERROR,
                    f"Relationship {relationship_name} is not valid: {format_result(result)}",
                    phase=ErrorPhase.VALIDATION,
                )
        except WrenError:
            raise
        except Exception as e:
            raise WrenError(
                ErrorCode.GENERIC_INTERNAL_ERROR, str(e), phase=ErrorPhase.VALIDATION
            ) from e

    async def _validate_rlac_condition_syntax_is_valid(
        self, parameters: dict, manifest_str: str
    ):
        if parameters.get("modelName") is None:
            raise WrenError(
                ErrorCode.VALIDATION_PARAMETER_ERROR,
                "modelName is required",
                phase=ErrorPhase.VALIDATION,
            )
        if parameters.get("requiredProperties") is None:
            raise WrenError(
                ErrorCode.VALIDATION_PARAMETER_ERROR,
                "requiredProperties is required",
                phase=ErrorPhase.VALIDATION,
            )
        if parameters.get("condition") is None:
            raise WrenError(
                ErrorCode.VALIDATION_PARAMETER_ERROR,
                "condition is required",
                phase=ErrorPhase.VALIDATION,
            )

        model_name = parameters.get("modelName")
        required_properties = parameters.get("requiredProperties")
        condition = parameters.get("condition")

        required_properties = [
            SessionProperty(
                name=prop["name"],
                required=bool(prop["required"]),
                default_expr=prop.get("defaultExpr", None),
            )
            for prop in required_properties
        ]

        rlac = RowLevelAccessControl(
            name="rlac_validation",
            required_properties=required_properties,
            condition=condition,
        )

        manifest = to_manifest(manifest_str)
        model = manifest.get_model(model_name)
        if model is None:
            raise WrenError(
                ErrorCode.VALIDATION_PARAMETER_ERROR,
                f"Model {model_name} not found in manifest",
                phase=ErrorPhase.VALIDATION,
            )

        try:
            validate_rlac_rule(rlac, model)
        except Exception as e:
            raise WrenError(
                ErrorCode.VALIDATION_ERROR, str(e), phase=ErrorPhase.VALIDATION
            )

    def _get_model(self, manifest, model_name):
        models = list(filter(lambda m: m["name"] == model_name, manifest["models"]))
        if len(models) == 0:
            raise WrenError(
                ErrorCode.VALIDATION_PARAMETER_ERROR,
                f"Model {model_name} not found in manifest",
                phase=ErrorPhase.VALIDATION,
            )
        return models[0]
