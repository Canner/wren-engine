from app.model import QueryDTO
from app.model.data_source import DataSource


# Rebuild model to validate the dto is correct via validation of the pydantic
def verify_query_dto(data_source: DataSource, dto: QueryDTO):
    data_source.get_dto_type()(**dto.model_dump(by_alias=True))
