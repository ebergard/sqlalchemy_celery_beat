from dataclasses import dataclass, fields

from sqlalchemy import bindparam

from ...utils import db_bindparam


@dataclass
class BindparamDbSchema:
    @classmethod
    def get_bindparams(cls, exclude: list[str] | None = None):
        exclude = exclude or []
        return {
            f.name: bindparam(db_bindparam(f.name)) for f in fields(cls) if f.name not in exclude
        }

    def model_dump(self):
        return {db_bindparam(f.name): getattr(self, f.name) for f in fields(self)}
