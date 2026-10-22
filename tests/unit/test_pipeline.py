import csv
from pathlib import Path

import pytest

from app.core.errors import ProcessingError
from app.processors.pipeline import process

OPTIONS = {"encoding": "utf-8-sig", "delimiter": ","}
STEPS = [
    {"type": "merge"},
    {"type": "deduplicate", "keys": ["order_id"]},
    {"type": "aggregate", "group_by": ["region"], "sum": ["amount"]},
]


def execute(tmp_path, contents, steps=None, options=None, **limits):
    paths = []
    for i, content in enumerate(contents):
        path = tmp_path / f"{i}.csv"
        path.write_text(content, encoding=(options or OPTIONS)["encoding"], newline="")
        paths.append(path)
    output = tmp_path / "result.csv"
    process(paths, output, steps or STEPS, options or OPTIONS, **limits)
    with output.open(encoding="utf-8-sig", newline="") as stream:
        return list(csv.DictReader(stream))


def test_ordered_merge_exact_decimal_and_first_duplicate(tmp_path):
    actual = execute(
        tmp_path,
        [
            "order_id,region,amount\n1,east,0.10\n2,east,0.20\n",
            "order_id,region,amount\n1,west,99\n3,west,1.00\n",
        ],
    )
    assert actual == [{"region": "east", "amount": "0.30"}, {"region": "west", "amount": "1.00"}]


def test_multiline_quotes_commas_and_bom(tmp_path):
    assert execute(tmp_path, ['id,text\n1,"a,b\nc"\n2,"say ""hi"""\n'], [{"type": "merge"}]) == [
        {"id": "1", "text": "a,b\nc"},
        {"id": "2", "text": 'say "hi"'},
    ]


@pytest.mark.parametrize(
    ("text", "code"),
    [
        ("", "INVALID_CSV"),
        ("id,id\n1,2\n", "INVALID_CSV"),
        ("id,\n1,2\n", "INVALID_CSV"),
        ("id,x\n1\n", "INVALID_CSV"),
        ('id,x\n1,"unfinished', "INVALID_CSV"),
    ],
)
def test_bad_csv(tmp_path, text, code):
    with pytest.raises(ProcessingError) as exc:
        execute(tmp_path, [text], [{"type": "merge"}])
    assert exc.value.code == code


@pytest.mark.parametrize("value", ["oops", "", "NaN", "Infinity", "1e999999", "1e-19"])
def test_invalid_amount_includes_row_and_column(tmp_path, value):
    with pytest.raises(ProcessingError, match="第 2 行列 amount") as exc:
        execute(tmp_path, [f"order_id,region,amount\n1,east,{value}\n"])
    assert exc.value.code == "INVALID_NUMBER"


def test_schema_order_must_match(tmp_path):
    with pytest.raises(ProcessingError) as exc:
        execute(tmp_path, ["a,b\n1,2\n", "b,a\n2,1\n"], [{"type": "merge"}])
    assert exc.value.code == "SCHEMA_MISMATCH"


def test_empty_header_only_allowed(tmp_path):
    assert execute(tmp_path, ["order_id,region,amount\n"]) == []


def test_missing_column_even_when_no_rows(tmp_path):
    with pytest.raises(ProcessingError) as exc:
        execute(tmp_path, ["order_id,region\n"])
    assert exc.value.code == "MISSING_COLUMN"


def test_empty_keys_are_equal(tmp_path):
    result = execute(
        tmp_path, ["id,value\n,first\n,second\n"], [{"type": "deduplicate", "keys": ["id"]}]
    )
    assert result == [{"id": "", "value": "first"}]


def test_explicit_encoding_and_delimiter(tmp_path):
    assert execute(
        tmp_path,
        ["区域;金额\n华东;1.2\n"],
        [{"type": "merge"}],
        {"encoding": "gb18030", "delimiter": ";"},
    ) == [{"区域": "华东", "金额": "1.2"}]


@pytest.mark.parametrize("limit", ["max_rows", "max_unique_keys", "max_groups"])
def test_resource_limits(tmp_path, limit):
    with pytest.raises(ProcessingError) as exc:
        execute(tmp_path, ["order_id,region,amount\n1,a,1\n2,b,2\n"], **{limit: 1})
    assert exc.value.code == "RESOURCE_LIMIT"


def test_requested_stage_order(tmp_path):
    result = execute(
        tmp_path,
        ["order_id,region,amount\n1,a,1\n2,a,2\n3,b,3\n"],
        [
            {"type": "aggregate", "group_by": ["region"], "sum": ["amount"]},
            {"type": "deduplicate", "keys": ["amount"]},
        ],
    )
    assert result == [{"region": "a", "amount": "3"}]


def test_sample_result(tmp_path):
    result = execute(
        tmp_path,
        [Path("samples/january.csv").read_text(), Path("samples/february.csv").read_text()],
    )
    with Path("samples/expected.csv").open() as stream:
        assert result == list(csv.DictReader(stream))
