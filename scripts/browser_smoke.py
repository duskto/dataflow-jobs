"""Optional browser acceptance: uv run --group browser python scripts/browser_smoke.py."""

import argparse
import csv
from pathlib import Path

from playwright.sync_api import expect, sync_playwright


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:8000")
    parser.add_argument("--output", type=Path, default=Path("docs/screenshots"))
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1440, "height": 1100}, device_scale_factor=1)
        errors = []
        page.on("pageerror", lambda error: errors.append(str(error)))
        page.goto(args.url, wait_until="domcontentloaded")
        expect(page.get_by_role("heading", name="让数据有条不紊。")).to_be_visible()
        page.locator("#files").set_input_files(["samples/january.csv", "samples/february.csv"])
        page.get_by_role("button", name="提交后台任务").click()
        expect(page.get_by_role("button", name="下载结果")).to_be_visible(timeout=30000)
        with page.expect_download() as download_info:
            page.get_by_role("button", name="下载结果").click()
        path = download_info.value.path()
        with path.open(encoding="utf-8-sig", newline="") as stream:
            actual = list(csv.reader(stream))
        with Path("samples/expected.csv").open(encoding="utf-8", newline="") as stream:
            assert actual == list(csv.reader(stream))
        expect(page.locator("#ops-stats")).to_contain_text("在线 Worker")
        expect(page.locator("#ops-alerts")).to_contain_text("服务运行正常")
        page.screenshot(path=str(args.output / "desktop.png"), full_page=True)
        page.set_viewport_size({"width": 390, "height": 844})
        assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")
        page.screenshot(path=str(args.output / "mobile.png"), full_page=True)
        assert not errors, errors
        browser.close()
    print("PASS: browser upload → submit → poll → download; desktop/mobile; no JavaScript errors")


if __name__ == "__main__":
    main()
