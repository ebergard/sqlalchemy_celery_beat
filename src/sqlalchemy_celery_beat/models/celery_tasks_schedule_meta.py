import datetime as dt

from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    func,
    insert,
    select,
    update,
)

from ..db import db_sessionmaker
from .base import CeleryTasksScheduleBase


class CeleryTasksScheduleMetaModel(CeleryTasksScheduleBase):
    """Keeps time when celery_tasks_schedule table was last updated."""

    __tablename__ = "celery_tasks_schedule_meta"

    id = Column(BigInteger, primary_key=True)
    last_updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    @classmethod
    def init_last_updated_at(cls):
        with db_sessionmaker() as session, session.begin():
            entry = (session.execute(select(cls).where(cls.id == 1))).scalars().first()
            if not entry:
                stmt = insert(cls).values(id=1, last_updated_at=dt.datetime.now(tz=dt.UTC))
                session.execute(stmt)

    @classmethod
    def update_last_updated_at(cls):
        with db_sessionmaker() as session, session.begin():
            stmt = update(cls).where(cls.id == 1).values(last_updated_at=dt.datetime.now(tz=dt.UTC))
            session.execute(stmt)

    @classmethod
    def get_last_updated_at(cls) -> dt.datetime:
        with db_sessionmaker() as session, session.begin():
            stmt = select(cls.last_updated_at).where(cls.id == 1)
            return (session.execute(stmt)).scalars().first()

    def __repr__(self):
        return f"{self.last_updated_at}"
