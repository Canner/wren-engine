from pydantic import BaseModel


class Function(BaseModel):
    function_type: str
    name: str
    return_type: str
    param_names: list[str] | None
    param_types: list[str] | None
    description: str
