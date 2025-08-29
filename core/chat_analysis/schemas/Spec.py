from pydantic import BaseModel, Field
from typing import Optional, List

class FieldSpec(BaseModel):
    name: str
    py_type: str
    description: str
    default: Optional[str]
    enum_values: List[str] = Field(default_factory=list)


class SchemaSpec(BaseModel):
    class_name: str
    fields: List[FieldSpec]