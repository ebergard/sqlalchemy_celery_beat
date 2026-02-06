import datetime as dt
import enum
import inspect
import json
import time

from celery.beat import ScheduleEntry, Scheduler, debug, error
from celery.schedules import crontab
from celery.utils.time import get_exponential_backoff_interval
from sqlalchemy.exc import DatabaseError

from .db import engine
from .models.base import CeleryTasksScheduleBase
from .models.celery_tasks import CeleryTasksModel
from .models.celery_tasks_schedule import CeleryTasksScheduleModel
from .models.celery_tasks_schedule_meta import CeleryTasksScheduleMetaModel
from .schemas.db.celery_tasks import CeleryTasksDbSchema
from .schemas.db.celery_tasks_schedule import CeleryTasksScheduleDbSchema
from .utils import get_task_category, get_task_key

DEFAULT_MAX_LOOP_INTERVAL = 5  # seconds
MAX_SYNC_SCHEDULE_INTERVAL = 5  # minutes
PREPARE_MODELS_MAX_RETRIES = 10


class DatabaseScheduler(Scheduler):
    """Database-backed Beat Scheduler."""

    ModelTask = CeleryTasksModel
    ModelSchedule = CeleryTasksScheduleModel
    ModelMeta = CeleryTasksScheduleMetaModel

    _schedule = None
    _last_updated_at = None

    def __init__(self, *args, **kwargs):
        """Initialize the database scheduler."""
        Scheduler.__init__(self, *args, **kwargs)
        self.max_interval = (
            kwargs.get("max_interval")
            or self.app.conf.beat_max_loop_interval
            or DEFAULT_MAX_LOOP_INTERVAL
        )

    @property
    def schedule(self):
        self._schedule = self._schedule or {}
        if self._need_update():
            self._update_schedule()
        return self._schedule

    def setup_schedule(self):
        debug("Setup schedule")
        self._prepare_models()
        self._clean_deprecated()
        self._fill_celery_tasks()
        self._fill_celery_tasks_schedule()
        self._update_schedule()

    def _fill_celery_tasks(self):
        """Fill celery_tasks table with application celery tasks."""

        def convert_type(param_type) -> str:
            if param_type is inspect._empty:
                return ""
            if type(param_type) is type:
                return param_type.__name__
            if type(param_type) is enum.EnumType:
                return f"<one of: {', '.join(set(st.value for st in param_type))}>"
            return str(param_type)

        def get_annotation(param_annotation) -> str:
            if param_annotation is inspect._empty:
                return ""
            return f": {convert_type(param_annotation)}"

        def get_default(param_default) -> str:
            if param_default is inspect._empty:
                return ""
            return f" [default: {convert_type(param_default)}]"

        if self.app.tasks:
            db_entries = [
                CeleryTasksDbSchema(
                    task=task_name,
                    params=", ".join(
                        [
                            f"{name}{get_annotation(param.annotation)}{get_default(param.default)}"
                            for name, param in inspect.signature(task).parameters.items()
                            if name != "kwargs"
                        ]
                    ),
                    description=inspect.getdoc(task) if task.__doc__ else "",
                    tags=get_task_category(task.__module__),
                )
                for task_name, task in sorted(self.app.tasks.items(), key=lambda item: item[0])
                if not task_name.startswith("celery.")
            ]
            self.ModelTask.bulk_insert(data=db_entries)
            self.ModelTask.bulk_update(data=db_entries)

    def _fill_celery_tasks_schedule(self):
        """Fill celery_tasks_schedule table with application beat schedule."""
        if self.app.conf.beat_schedule:
            comment = (
                f"loaded from app celery beat schedule on {dt.datetime.now(tz=dt.UTC).isoformat()}"
            )
            db_entries = [
                CeleryTasksScheduleDbSchema(
                    task_key=get_task_key(task["task"], task["args"], task["kwargs"]),
                    task=task["task"],
                    args=json.dumps(task["args"]),
                    kwargs=json.dumps(task["kwargs"]),
                    schedule=(
                        f"{task['schedule']._orig_minute} "
                        f"{task['schedule']._orig_hour} "
                        f"{task['schedule']._orig_day_of_month} "
                        f"{task['schedule']._orig_month_of_year} "
                        f"{task['schedule']._orig_day_of_week}"
                    ),
                    comment=comment,
                )
                for task in self.app.conf.beat_schedule.values()
            ]
            self.ModelSchedule.bulk_insert(data=db_entries)

    def update_from_dict(self, dict_):
        self._schedule = self._schedule or {}
        self._schedule.update(
            {name: self._maybe_entry(name, entry) for name, entry in dict_.items()}
        )

    def _need_update(self) -> bool:
        now = dt.datetime.now(tz=dt.UTC)

        if self.max_interval < 30:  # noqa
            # Don't update at the beginning and at the end of a minute
            # to avoid overriding current heap
            if 0 <= now.second < 20 or now.second > 50:  # noqa
                return False

        # Sync beat schedule with db every 5 minutes
        # in case we didn't get sqlalchemy update event
        sync_threshold = now - dt.timedelta(minutes=MAX_SYNC_SCHEDULE_INTERVAL)

        if (  # noqa
            self._last_updated_at is None
            or self._last_updated_at < sync_threshold
            or self._last_updated_at < self.ModelMeta.get_last_updated_at()
        ):
            return True
        return False

    def _update_schedule(self):
        enabled_tasks = self._get_enabled_tasks()
        self._schedule = enabled_tasks
        self._last_updated_at = dt.datetime.now(tz=dt.UTC)
        self.install_default_entries(self._schedule)
        debug("Current schedule:\n" + "\n".join(repr(entry) for entry in self._schedule.values()))

    def _get_enabled_tasks(self) -> dict[str, ScheduleEntry]:
        """Return list of enabled periodic tasks."""
        rows = self.ModelSchedule.select_enabled_entries()
        entries = {}
        for row in rows:
            try:
                entries[row.task_key] = self._model_to_entry(model=row)
            except Exception as e:
                error(str(e))
        return entries

    def _model_to_entry(self, model: CeleryTasksScheduleModel) -> ScheduleEntry:
        args = json.loads(model.args)
        kwargs = json.loads(model.kwargs)
        minute, hour, day_of_month, month_of_year, day_of_week = model.schedule.split()
        schedule = crontab(
            minute=minute,
            hour=hour,
            day_of_week=day_of_week,
            day_of_month=day_of_month,
            month_of_year=month_of_year,
        )
        entry_dict = {
            "task": model.task,
            "args": args,
            "kwargs": kwargs,
            "schedule": schedule,
        }
        return self._maybe_entry(model.task_key, entry_dict)

    def _prepare_models(self):
        # ###
        # COPIED from: celery.backends.database.session.SessionManager.prepare_models
        # ###
        # SQLAlchemy will check if the items exist before trying to
        # create them, which is a race condition. If it raises an error
        # in one iteration, the next may pass all the existence checks
        # and the call will succeed.
        retries = 0
        while True:
            try:
                CeleryTasksScheduleBase.metadata.create_all(engine)
            except DatabaseError:
                if retries < PREPARE_MODELS_MAX_RETRIES:
                    sleep_amount_ms = get_exponential_backoff_interval(10, retries, 1000, True)
                    time.sleep(sleep_amount_ms / 1000)
                    retries += 1
                else:
                    raise
            else:
                break

        self.ModelMeta.init_last_updated_at()

    def _clean_deprecated(self):
        """Sync celery_tasks table with application tasks."""
        tasks = set(self.app.tasks)
        db_tasks = set(self.ModelTask.select_all_tasks())
        deprecated_tasks = db_tasks - tasks
        if deprecated_tasks:
            self.ModelTask.delete(tasks=list(deprecated_tasks))
