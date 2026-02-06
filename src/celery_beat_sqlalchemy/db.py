import contextlib
from collections.abc import Generator
from typing import Any

from celery import current_app as celery_app
from sqlalchemy import NullPool, create_engine
from sqlalchemy.orm import Session

engine = create_engine(url=celery_app.conf.beat_db_scheduler_dsn, future=True, poolclass=NullPool)


@contextlib.contextmanager
def db_sessionmaker() -> Generator[Session, Any, None]:
    """Database session maker."""
    with engine.connect() as conn, Session(
        bind=conn, expire_on_commit=False, autoflush=False
    ) as session:
        yield session
