from dataclasses import dataclass

from .base import BindparamDbSchema


@dataclass
class CeleryTasksDbSchema(BindparamDbSchema):
    task: str
    params: str
    description: str
    tags: str
