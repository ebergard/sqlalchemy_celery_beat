import json
from json import JSONDecodeError

from celery import current_app as celery_app
from celery.beat import debug
from celery.schedules import crontab
from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    String,
    delete,
    event,
    func,
    insert,
    select,
    true,
)
from sqlalchemy.orm import relationship

from ..db import db_sessionmaker
from ..schemas.db.celery_tasks_schedule import CeleryTasksScheduleDbSchema
from ..utils import get_task_key
from .base import CeleryTasksScheduleBase
from .celery_tasks_schedule_meta import CeleryTasksScheduleMetaModel


class CeleryTasksScheduleModel(CeleryTasksScheduleBase):
    """Celery tasks schedule."""

    __tablename__ = "celery_tasks_schedule"

    id = Column(BigInteger, primary_key=True)
    task = Column(
        String,
        ForeignKey("celery_tasks.task", ondelete="CASCADE"),
        nullable=False,
        doc="Task name (Example: my_task)",
    )
    args = Column(
        String,
        nullable=False,
        server_default="[]",
        default="[]",
        doc='Task positional arguments (Example: ["arg1", 2])',
    )
    kwargs = Column(
        String,
        nullable=False,
        server_default="{}",
        default="{}",
        doc='Task keyword arguments (Example: {"kwarg1": true})',
    )
    schedule = Column(
        String,
        nullable=False,
        doc="Crontab schedule [minute hour day_of_month month day_of_week] (Example: */2 * * * *)",
    )
    enabled = Column(
        Boolean,
        nullable=False,
        server_default=true(),
        default="checked",
        doc="Enable/disable task execution",
    )
    created_at = Column(
        DateTime(timezone=True), server_default=func.now(), doc="Datetime this task was created"
    )
    updated_at = Column(
        DateTime(timezone=True), onupdate=func.now(), doc="Datetime this task was last modified"
    )
    comment = Column(String, nullable=True, doc="Any comment")
    task_key = Column(String, nullable=False, unique=True)

    task_head = relationship("CeleryTasksModel", back_populates="scheduled_tasks", lazy="selectin")

    @classmethod
    def bulk_insert(cls, data: list[CeleryTasksScheduleDbSchema]):
        with db_sessionmaker() as session, session.begin():
            existing_entries = set((session.execute(select(cls.task_key))).scalars().all())
            entries_to_insert = {d.task_key for d in data}
            missing_entries = entries_to_insert - existing_entries
            if missing_entries:
                stmt = insert(cls).values(**CeleryTasksScheduleDbSchema.get_bindparams())
                session.execute(
                    stmt, [d.model_dump() for d in data if d.task_key in missing_entries]
                )

    @classmethod
    def select_all_tasks(cls) -> list[str]:
        with db_sessionmaker() as session, session.begin():
            stmt = select(cls.task)
            return (session.execute(stmt)).scalars().all()

    @classmethod
    def select_enabled_entries(cls):
        with db_sessionmaker() as session, session.begin():
            stmt = select(cls).where(cls.enabled)
            return (session.execute(stmt)).scalars().all()

    @classmethod
    def delete(cls, tasks: list[str]):
        with db_sessionmaker() as session, session.begin():
            stmt = delete(cls).where(cls.task.in_(tasks))
            session.execute(stmt)

    def __repr__(self):
        return (
            f"Task: {self.task} Args: {self.args} Kwargs: {self.kwargs} Schedule: {self.schedule}"
        )


@event.listens_for(CeleryTasksScheduleModel, "before_insert")
def before_insert_handler(mapper, connection, target: CeleryTasksScheduleModel):
    """Validate task schedule entry before insert."""
    before_validator(mapper, connection, target)


@event.listens_for(CeleryTasksScheduleModel, "before_update")
def before_update_handler(mapper, connection, target: CeleryTasksScheduleModel):
    """Validate task schedule entry before update."""
    before_validator(mapper, connection, target)


@event.listens_for(CeleryTasksScheduleModel, "after_insert")
def after_insert_handler(mapper, connection, target: CeleryTasksScheduleModel):
    """Update when schedule entry was modified."""
    CeleryTasksScheduleMetaModel.update_last_updated_at()


@event.listens_for(CeleryTasksScheduleModel, "after_update")
def after_update_handler(mapper, connection, target: CeleryTasksScheduleModel):
    """Update when schedule entry was modified."""
    CeleryTasksScheduleMetaModel.update_last_updated_at()


@event.listens_for(CeleryTasksScheduleModel, "after_delete")
def after_delete_handler(mapper, connection, target: CeleryTasksScheduleModel):
    """Update when schedule entry was modified."""
    CeleryTasksScheduleMetaModel.update_last_updated_at()


def before_validator(mapper, connection, target):
    """Validate task schedule entry before insert/update."""
    if target.task not in celery_app.tasks:
        raise RuntimeError(f"Task '{target.task}' does not exist")

    try:
        args = json.loads(target.args)
    except JSONDecodeError as e:
        raise RuntimeError(f"Invalid format for task positional arguments: {e}")

    try:
        kwargs = json.loads(target.kwargs)
    except JSONDecodeError as e:
        raise RuntimeError(f"Invalid format for task keyword arguments: {e}")

    task = celery_app.tasks[target.task]
    try:
        check_arguments = task.__header__
    except AttributeError:  # pragma: no cover
        debug(f"Task '{task}' has no attribute '__header__': cannot validate arguments")
    else:
        check_arguments(*(args or ()), **(kwargs or {}))

    try:
        minute, hour, day_of_month, month_of_year, day_of_week = target.schedule.split()
        crontab(
            minute=minute,
            hour=hour,
            day_of_week=day_of_week,
            day_of_month=day_of_month,
            month_of_year=month_of_year,
        )
    except Exception as e:
        raise RuntimeError(f"Invalid schedule: {e}")

    # Generate unique task key
    target.task_key = get_task_key(target.task, args, kwargs)
