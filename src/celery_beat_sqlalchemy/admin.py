from typing import Any

from sqladmin import ModelView
from sqladmin.forms import ModelConverter
from sqlalchemy.orm import RelationshipProperty, sessionmaker

from .models.celery_tasks import CeleryTasksModel
from .models.celery_tasks_schedule import CeleryTasksScheduleModel


class CustomModelConverter(ModelConverter):
    async def _prepare_select_options(
        self,
        prop: RelationshipProperty,
        session_maker: sessionmaker,
    ) -> list[tuple[str, Any]]:
        res = await super()._prepare_select_options(prop=prop, session_maker=session_maker)
        return sorted(res, key=lambda x: x[0])


class CeleryTasksAdmin(ModelView, model=CeleryTasksModel):
    form_converter = CustomModelConverter
    can_delete = False
    can_create = False

    column_list = [
        CeleryTasksModel.task,
        CeleryTasksModel.params,
        CeleryTasksModel.description,
        CeleryTasksModel.tags,
        CeleryTasksModel.scheduled_tasks,
        CeleryTasksModel.created_at,
        CeleryTasksModel.updated_at,
    ]

    column_searchable_list = [
        CeleryTasksModel.task,
        CeleryTasksModel.description,
        CeleryTasksModel.tags,
    ]

    column_default_sort = "task"
    column_sortable_list = [
        CeleryTasksModel.task,
        CeleryTasksModel.created_at,
        CeleryTasksModel.updated_at,
    ]

    form_columns = [
        CeleryTasksModel.tags,
    ]

    icon = "fa-solid fa-desktop"
    human_name_plural = "Celery tasks"
    category = "Celery Tasks"


class CeleryTasksScheduleAdmin(ModelView, model=CeleryTasksScheduleModel):
    form_converter = CustomModelConverter

    column_list = [
        CeleryTasksScheduleModel.task_head,
        CeleryTasksScheduleModel.args,
        CeleryTasksScheduleModel.kwargs,
        CeleryTasksScheduleModel.schedule,
        CeleryTasksScheduleModel.enabled,
        CeleryTasksScheduleModel.comment,
        CeleryTasksScheduleModel.created_at,
        CeleryTasksScheduleModel.updated_at,
    ]

    column_searchable_list = [
        CeleryTasksScheduleModel.task,
        CeleryTasksScheduleModel.args,
        CeleryTasksScheduleModel.kwargs,
    ]

    column_default_sort = "task"
    column_sortable_list = [
        CeleryTasksScheduleModel.enabled,
        CeleryTasksScheduleModel.created_at,
        CeleryTasksScheduleModel.updated_at,
    ]

    form_columns = [
        CeleryTasksScheduleModel.task_head,
        CeleryTasksScheduleModel.args,
        CeleryTasksScheduleModel.kwargs,
        CeleryTasksScheduleModel.schedule,
        CeleryTasksScheduleModel.enabled,
        CeleryTasksScheduleModel.comment,
    ]

    column_details_exclude_list = [
        CeleryTasksScheduleModel.id,
    ]

    icon = "fa-solid fa-desktop"
    human_name_plural = "Celery tasks schedule"
    category = "Celery Tasks"
