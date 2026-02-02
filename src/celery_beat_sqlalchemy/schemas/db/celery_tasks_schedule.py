from dataclasses import dataclass

from .base import BindparamDbSchema


@dataclass
class CeleryTasksScheduleDbSchema(BindparamDbSchema):
    task_key: str
    task: str
    args: str
    kwargs: str
    schedule: str
    enabled: bool = True
    comment: str | None = None
