from sqlalchemy import (
    Column,
    DateTime,
    String,
    bindparam,
    delete,
    func,
    insert,
    or_,
    select,
    update,
)
from sqlalchemy.orm import relationship

from ..db import db_sessionmaker
from ..schemas.db.celery_tasks import CeleryTasksDbSchema
from .base import CeleryTasksScheduleBase


class CeleryTasksModel(CeleryTasksScheduleBase):
    """Application celery tasks."""

    __tablename__ = "celery_tasks"

    task = Column(String, primary_key=True, unique=True)
    params = Column(String, nullable=False, server_default="")
    description = Column(String, nullable=False, server_default="")
    tags = Column(String, nullable=False, server_default="")
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    scheduled_tasks = relationship(
        "CeleryTasksScheduleModel",
        back_populates="task_head",
        lazy="selectin",
        cascade="all, delete-orphan",
    )

    @classmethod
    def bulk_insert(cls, data: list[CeleryTasksDbSchema]):
        with db_sessionmaker() as session, session.begin():
            existing_tasks = set((session.execute(select(cls.task))).scalars().all())
            tasks_to_insert = {d.task for d in data}
            missing_tasks = tasks_to_insert - existing_tasks
            if missing_tasks:
                stmt = insert(cls).values(**CeleryTasksDbSchema.get_bindparams())
                session.execute(stmt, [d.model_dump() for d in data if d.task in missing_tasks])

    @classmethod
    def bulk_update(cls, data: list[CeleryTasksDbSchema]):
        with db_sessionmaker() as session, session.begin():
            stmt = (
                update(cls)
                .where(
                    cls.task == bindparam("task_value"),
                    or_(
                        cls.params != bindparam("params_value"),
                        cls.description != bindparam("description_value"),
                    ),
                )
                .values(**CeleryTasksDbSchema.get_bindparams(exclude=["tags"]))
                .execution_options(synchronize_session=None)
            )
            session.connection().execute(stmt, [d.model_dump() for d in data])

    @classmethod
    def select_all_tasks(cls) -> list[str]:
        with db_sessionmaker() as session, session.begin():
            stmt = select(cls.task)
            return (session.execute(stmt)).scalars().all()

    @classmethod
    def delete(cls, tasks: list[str]):
        with db_sessionmaker() as session, session.begin():
            stmt = delete(cls).where(cls.task.in_(tasks))
            session.execute(stmt)

    def __repr__(self):
        return f"{self.task}"
