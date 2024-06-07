from __future__ import annotations

from pydantic import BaseModel, Field

from app.mdl.rewriter import rewrite
from app.model.data_source import ConnectionInfo, DataSource

rules = ["column_is_valid"]


class Validator:
    def __init__(
        self,
        data_source: DataSource,
        connection_info: ConnectionInfo,
        manifest_str: str,
    ):
        self.data_source = data_source
        self.connection = self.data_source.get_connection(connection_info)
        self.manifest_str = manifest_str

    def validate(self, rule: str, parameters: dict[str, str]):
        if rule not in rules:
            raise RuleNotFoundError(rule)
        try:
            getattr(self, f"_validate_{rule}")(parameters)
        except ValidationError as e:
            raise e
        except Exception as e:
            raise ValidationError(f"Unknown exception: {type(e)}, message: {str(e)}")

    def _validate_column_is_valid(self, parameters: dict[str, str]):
        model_name = parameters.get("modelName")
        column_name = parameters.get("columnName")
        if model_name is None:
            raise MissingRequiredParameterError("modelName")
        if column_name is None:
            raise MissingRequiredParameterError("columnName")

        sql = f'SELECT "{column_name}" FROM "{model_name}" LIMIT 1'
        rewritten_sql = rewrite(self.manifest_str, sql)
        try:
            self.connection.sql(rewritten_sql, dialect="trino")
        except Exception as e:
            raise ValidationError(f"Exception: {type(e)}, message: {str(e)}")


class ValidateDTO(BaseModel):
    manifest_str: str = Field(alias="manifestStr", description="Base64 manifest")
    parameters: dict[str, str]
    connection_info: ConnectionInfo = Field(alias="connectionInfo")


class ValidationError(Exception):
    pass


class RuleNotFoundError(ValidationError):
    def __init__(self, rule: str):
        super().__init__(f"The rule `{rule}` is not in the rules, rules: {rules}")


class MissingRequiredParameterError(ValidationError):
    def __init__(self, parameter: str):
        super().__init__(f"Missing required parameter: `{parameter}`")
