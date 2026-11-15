"""Exercise the installed JavaScript asset's history semantics with Node's test runner."""

import os
import re
import shutil
import subprocess
from importlib.resources import files
from pathlib import Path

import pytest


def test_outcome_chart_semantics_against_packaged_assets():
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node is required for chart asset semantics")
    asset_path = Path(str(files("dewey.dashboard").joinpath("assets", "dashboard.js")))
    result = subprocess.run(
        [node, "--test", str(Path(__file__).with_name("test_chart.mjs"))],
        env={**os.environ, "DEWEY_DASHBOARD_JS_FILE": asset_path.as_uri()},
        text=True,
        capture_output=True,
        timeout=15,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_chart_legend_uses_compact_native_markers():
    js = files("dewey.dashboard").joinpath("assets", "dashboard.js").read_text()
    assert "pointStyleWidth: 8, boxWidth: 8, boxHeight: 8" in js


def test_dashboard_copy_keeps_caveats_in_collapsed_details_and_errors_visible():
    js = files("dewey.dashboard").joinpath("assets", "dashboard.js").read_text()
    assert "Task observations, not business-delivery guarantees." not in js
    assert 'const notes = el("details", undefined, "dw-metric-notes")' in js
    assert 'notes.append(el("summary", "About these metrics"))' in js
    assert 'notes.append(el("p", text, "dw-muted dw-note"))' in js
    assert 'panel("Active task claims")' in js
    assert 'result.setAttribute("role", "alert")' in js
    assert 'result.scrollIntoView({block: "nearest"})' in js


def test_dropdown_styles_override_fixed_host_height_and_keep_native_controls():
    css = files("dewey.dashboard").joinpath("assets", "dashboard.css").read_text()
    rule = re.search(r"\.dw-root select\s*\{([^}]+)\}", css)
    assert rule is not None
    declarations = dict(item.strip().split(":", 1) for item in rule[1].split(";") if item.strip())
    assert declarations["height"] == "auto"
    assert declarations["min-height"] == "38px"
    assert declarations["line-height"] == "1.5"
    assert declarations["appearance"] == "auto"
    # Native popups follow the resolved widget theme, not an unrelated host setting.
    light = re.search(r"\.dw-root\s*\{([^}]+)\}", css)
    dark = re.search(r"\.dw-root\[data-resolved-theme=dark\]\s*\{([^}]+)\}", css)
    assert light and "color-scheme:light;" in light[1]
    assert dark and "color-scheme:dark;" in dark[1]
