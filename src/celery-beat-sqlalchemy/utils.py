import re


def get_task_key(name, args, kwargs) -> str:
    """
    Get task unique identifier.
    The same task can be run with different args/kwargs, and they are different entries for celery beat.
    To uniquely identify those entries we use combination of task_name-args-kwargs.
    """
    args = "-".join(map(str, args))
    kwargs = "-".join([f"{k}-{v}" for k, v in kwargs.items()])
    # Zabbix compatible key format: 0-9a-zA-Z_-.
    return re.sub(r"[\[\](){}, '\"]", "", f"{name}-{args}-{kwargs}".strip("-"))


def get_task_category(task_module: str):
    """
    Get task category by its module:
    src.apps.users.tasks -> users;
    src.apps.base.tasks.tasks -> base.
    """
    return task_module.replace(".tasks", "").split(".")[-1]


def db_bindparam(name: str) -> str:
    return f"{name}_value"
