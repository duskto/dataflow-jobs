from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


Columns = Annotated[
    list[Annotated[str, Field(min_length=1, max_length=200)]], Field(min_length=1, max_length=200)
]


class Merge(StrictModel):
    type: Literal["merge"]
    schema_policy: Literal["strict"] = "strict"


class Deduplicate(StrictModel):
    type: Literal["deduplicate"]
    keys: Columns
    keep: Literal["first"] = "first"


class Aggregate(StrictModel):
    type: Literal["aggregate"]
    group_by: Columns
    sum: Columns

    @model_validator(mode="after")
    def disjoint(self):
        if set(self.group_by) & set(self.sum):
            raise ValueError("分组列和求和列不能重叠")
        return self


Step = Annotated[Merge | Deduplicate | Aggregate, Field(discriminator="type")]


class CSVOptions(StrictModel):
    encoding: Literal["utf-8-sig", "utf-8", "gb18030"] = "utf-8-sig"
    delimiter: Literal[",", ";", "\t", "|"] = ","


class CreateJob(StrictModel):
    input_file_ids: list[UUID] = Field(min_length=1, max_length=20)
    steps: list[Step] = Field(min_length=1, max_length=8)
    csv_options: CSVOptions = Field(default_factory=CSVOptions)
    max_attempts: int = Field(default=3, ge=1, le=5)

    @model_validator(mode="after")
    def pipeline(self):
        merges = [i for i, step in enumerate(self.steps) if step.type == "merge"]
        if merges not in ([], [0]):
            raise ValueError("merge 只能出现在流水线的第一步")
        if len(self.input_file_ids) > 1 and merges != [0]:
            raise ValueError("多个输入文件必须先执行 merge")
        for step in self.steps:
            for name in ("keys", "group_by", "sum"):
                columns = getattr(step, name, [])
                if len(columns) != len(set(columns)):
                    raise ValueError(f"{name} 中存在重复列名")
        return self
