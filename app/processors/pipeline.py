"""HTTP/DB-independent CSV pipeline. State is bounded, rows stream between stages."""

import csv
from collections.abc import Callable, Iterable, Iterator
from decimal import Decimal, InvalidOperation, localcontext
from pathlib import Path

from app.core.errors import ProcessingError

Row = dict[str, str]
Progress = Callable[[str, int], None]


def fail(code: str, message: str):
    raise ProcessingError(code, message)


def header(path: Path, encoding: str, delimiter: str, max_columns: int) -> list[str]:
    try:
        with path.open(encoding=encoding, newline="") as stream:
            columns = next(csv.reader(stream, delimiter=delimiter, strict=True), None)
        if not columns or any(not c.strip() for c in columns):
            fail("INVALID_CSV", "CSV 为空或包含空列名")
        if len(columns) != len(set(columns)):
            fail("INVALID_CSV", "CSV 包含重复列名")
        if len(columns) > max_columns or any(len(c) > 200 for c in columns):
            fail("RESOURCE_LIMIT", "列数或列名长度超过限制")
        return columns
    except (UnicodeError, csv.Error) as exc:
        fail("INVALID_CSV", f"表头解析失败：{exc}")


def merge_csv(
    paths: list[Path],
    columns: list[str],
    encoding: str,
    delimiter: str,
    max_rows: int,
    progress: Progress,
) -> Iterator[Row]:
    count = 0
    for index, path in enumerate(paths, 1):
        try:
            with path.open(encoding=encoding, newline="") as stream:
                reader = csv.reader(stream, delimiter=delimiter, strict=True)
                actual = next(reader, None)
                if actual != columns:
                    fail("SCHEMA_MISMATCH", f"输入文件 {index} 的列名或顺序不一致")
                for values in reader:
                    count += 1
                    if count > max_rows:
                        fail("RESOURCE_LIMIT", f"输入行数超过上限 {max_rows}")
                    if len(values) != len(columns):
                        fail("INVALID_CSV", f"输入文件 {index} 第 {reader.line_num} 行字段数量错误")
                    if count % 1000 == 0:
                        progress("读取", count)
                    yield dict(zip(columns, values, strict=True))
        except (UnicodeError, csv.Error) as exc:
            fail("INVALID_CSV", f"输入文件 {index} CSV 解析失败：{exc}")
    progress("读取", count)


def deduplicate(rows: Iterable[Row], keys: list[str], max_unique_keys: int) -> Iterator[Row]:
    seen: set[tuple[str, ...]] = set()
    for row in rows:
        key = tuple(row[k] for k in keys)
        if key not in seen:
            if len(seen) >= max_unique_keys:
                fail("RESOURCE_LIMIT", f"去重唯一键超过上限 {max_unique_keys}")
            seen.add(key)
            yield row


def aggregate(
    rows: Iterable[Row],
    group_by: list[str],
    sums: list[str],
    max_groups: int,
    progress: Progress,
) -> Iterator[Row]:
    groups: dict[tuple[str, ...], list[Decimal]] = {}
    # Each value has <=38 digits, exponent -18..18; at most 1M rows by default.
    # Precision 80 avoids float conversion and rounding for the supported bounds.
    with localcontext() as context:
        context.prec = 80
        for row_number, row in enumerate(rows, 2):
            key = tuple(row[k] for k in group_by)
            if key not in groups:
                if len(groups) >= max_groups:
                    fail("RESOURCE_LIMIT", f"分组数超过上限 {max_groups}")
                groups[key] = [Decimal(0) for _ in sums]
            for i, column in enumerate(sums):
                try:
                    value = Decimal(row[column])
                    if not value.is_finite():
                        raise InvalidOperation
                    digits = value.as_tuple()
                    if len(digits.digits) > 38 or abs(digits.exponent) > 18:
                        raise InvalidOperation
                except InvalidOperation:
                    fail("INVALID_NUMBER", f"本阶段第 {row_number} 行列 {column} 不是合法数值")
                groups[key][i] += value
            if row_number % 1000 == 0:
                progress("汇总", row_number - 1)
    for number, (key, totals) in enumerate(groups.items(), 1):
        if number % 1000 == 0:
            progress("写出", number)
        yield dict(zip(group_by + sums, list(key) + [format(v, "f") for v in totals], strict=True))


def process(
    paths: list[Path],
    output: Path,
    steps: list[dict],
    options: dict,
    *,
    max_rows: int = 1_000_000,
    max_unique_keys: int = 200_000,
    max_groups: int = 100_000,
    max_columns: int = 200,
    max_field_chars: int = 1_000_000,
    progress: Progress = lambda *_: None,
) -> int:
    csv.field_size_limit(max_field_chars)
    encoding, delimiter = options["encoding"], options["delimiter"]
    progress("读取", 0)
    columns = header(paths[0], encoding, delimiter, max_columns)
    rows = merge_csv(paths, columns, encoding, delimiter, max_rows, progress)
    for step in steps:
        kind = step["type"]
        required = step.get("keys", []) + step.get("group_by", []) + step.get("sum", [])
        missing = set(required) - set(columns)
        if missing:
            fail("MISSING_COLUMN", f"缺少列：{', '.join(sorted(missing))}")
        if kind == "deduplicate":
            rows = deduplicate(rows, step["keys"], max_unique_keys)
        elif kind == "aggregate":
            rows = aggregate(rows, step["group_by"], step["sum"], max_groups, progress)
            columns = step["group_by"] + step["sum"]
        elif kind != "merge":
            fail("INVALID_STEP", "不支持的处理步骤")
    count = 0
    # Output always UTF-8 BOM and comma separated, independent of input encoding.
    with output.open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=columns)
        writer.writeheader()
        for count, row in enumerate(rows, 1):
            writer.writerow(row)
            if count % 1000 == 0:
                progress("写出", count)
        stream.flush()
        import os

        os.fsync(stream.fileno())
    progress("写出", count)
    return count
