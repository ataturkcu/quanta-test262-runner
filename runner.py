#!/usr/bin/env python3
"""
test262-runner: Fast, configurable ECMAScript Test262 conformance runner.

Usage:
    python runner.py                  # uses config.json
    python runner.py myconfig.json    # uses custom config
"""

import json
import os
import re
import sys
import subprocess
import tempfile
import time
from datetime import datetime
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
from collections import defaultdict
from html import escape as html_escape

try:
    import yaml
    HAS_YAML = True
except ImportError:
    HAS_YAML = False


# ── Constants ──────────────────────────────────────────────────────────────────

FRONTMATTER_RE = re.compile(r'/\*---\s*\n(.*?)\n\s*---\*/', re.DOTALL)
ASYNC_PATTERN = re.compile(r'\$DONE')
ASYNC_COMPLETE = 'Test262:AsyncTestComplete'
ASYNC_FAILURE = 'Test262:AsyncTestFailure'

PASS = 'PASS'
FAIL = 'FAIL'
SKIP = 'SKIP'
TIMEOUT = 'TIMEOUT'
ERROR = 'ERROR'

DEFAULT_HARNESS = ['sta.js', 'assert.js']

DEFAULT_CONFIG = {
    "engine_path": "./engine",
    "engine_args": [],
    "module_flag": "--module",
    "test262_path": "./test262",
    "threads": 0,
    "timeout": 10,
    "save_errors": True,
    "run_both_modes": True,
    "count_per_file": True,
    "results_dir": "results",
    "test_dirs": ["test"],
    "unsupported_features": [],
    "skip_tests": [],
    "skip_dirs": [],
}


# ── Config ─────────────────────────────────────────────────────────────────────

def load_config(path):
    config = dict(DEFAULT_CONFIG)
    if os.path.isfile(path):
        with open(path, 'r', encoding='utf-8') as f:
            config.update(json.load(f))
    else:
        print(f"[WARN] Config '{path}' not found, using defaults.")
    if config['threads'] <= 0:
        cpu = os.cpu_count() or 4
        config['threads'] = max(1, int(cpu * 0.75))
    return config


# ── Frontmatter Parsing ───────────────────────────────────────────────────────

def parse_frontmatter(source):
    m = FRONTMATTER_RE.search(source)
    if not m:
        return {}
    raw = m.group(1)
    if HAS_YAML:
        try:
            r = yaml.safe_load(raw)
            return r if isinstance(r, dict) else {}
        except Exception:
            pass
    return _parse_fm_fallback(raw)


def _parse_fm_fallback(raw):
    """Minimal YAML parser for the test262 frontmatter subset."""
    result = {}
    lines = raw.split('\n')
    i = 0
    while i < len(lines):
        line = lines[i]
        s = line.strip()
        if not s or s.startswith('#'):
            i += 1
            continue
        m = re.match(r'^(\w[\w-]*)\s*:\s*(.*)', line)
        if not m:
            i += 1
            continue
        key, value = m.group(1), m.group(2).strip()

        # Inline list [a, b, c]
        if value.startswith('[') and value.endswith(']'):
            result[key] = [x.strip().strip("'\"") for x in value[1:-1].split(',') if x.strip()]
            i += 1
            continue

        # Block (nested dict or list)
        if value in ('', '>', '|'):
            items, sub = [], {}
            j = i + 1
            while j < len(lines):
                nl = lines[j]
                if nl and not nl[0].isspace():
                    break
                ns = nl.strip()
                if not ns:
                    j += 1
                    continue
                if ns.startswith('- '):
                    items.append(ns[2:].strip().strip("'\""))
                elif ':' in ns:
                    bk, bv = ns.split(':', 1)
                    sub[bk.strip()] = bv.strip().strip("'\"")
                j += 1
            result[key] = items if items else (sub if sub else value)
            i = j
            continue

        result[key] = value
        i += 1
    return result


# ── Worker Process ─────────────────────────────────────────────────────────────

_cfg = None
_harness = None
_unsupported = None
_skip_set = None


def _worker_init(config, harness_cache, unsupported_features, skip_tests):
    global _cfg, _harness, _unsupported, _skip_set
    _cfg = config
    _harness = harness_cache
    _unsupported = unsupported_features
    _skip_set = skip_tests


def run_test(task):
    """Execute a single test262 test. Returns result dict or None (mode-skip)."""
    test_path, rel_path, mode = task
    config = _cfg
    harness = _harness
    t0 = time.monotonic()

    res = {'test': rel_path, 'mode': mode, 'result': ERROR, 'error': '', 'duration': 0}

    try:
        # ── Read source ────────────────────────────────────────────────
        with open(test_path, 'r', encoding='utf-8', errors='replace') as f:
            source = f.read()

        meta = parse_frontmatter(source)
        flags = meta.get('flags') or []
        if isinstance(flags, str):
            flags = [flags]
        includes = meta.get('includes') or []
        if isinstance(includes, str):
            includes = [includes]
        features = meta.get('features') or []
        if isinstance(features, str):
            features = [features]
        negative = meta.get('negative')
        if not isinstance(negative, dict):
            negative = None

        is_module = 'module' in flags
        is_async = 'async' in flags or bool(ASYNC_PATTERN.search(source))
        is_raw = 'raw' in flags

        # ── Mode compatibility (return None to silently drop) ──────────
        if mode == 'strict':
            if 'noStrict' in flags or is_module or 'onlyStrict' in flags:
                return None  # default run already covers these
            use_strict = True
        else:  # 'default'
            use_strict = 'onlyStrict' in flags

        # ── Skip by config ─────────────────────────────────────────────
        if rel_path in _skip_set:
            res.update(result=SKIP, error='Skipped by config', duration=time.monotonic() - t0)
            return res

        # ── Skip by unsupported features ───────────────────────────────
        for feat in features:
            if feat in _unsupported:
                res.update(result=SKIP, error=f'Unsupported feature: {feat}',
                           duration=time.monotonic() - t0)
                return res

        # ── Build source ───────────────────────────────────────────────
        neg_phase = negative.get('phase', '') if negative else ''
        skip_harness = is_raw or neg_phase in ('parse', 'early')

        parts = []
        if use_strict:
            parts.append('"use strict";')

        if not skip_harness:
            # Always include default harness
            seen = set()
            for inc in DEFAULT_HARNESS:
                if inc in harness:
                    parts.append(harness[inc])
                    seen.add(inc)
            # Test-specific includes
            for inc in includes:
                if inc in seen:
                    continue
                if inc in harness:
                    parts.append(harness[inc])
                    seen.add(inc)
                else:
                    res.update(result=ERROR, error=f'Missing harness: {inc}',
                               duration=time.monotonic() - t0)
                    return res
            # Async harness
            if is_async and 'doneprintHandle.js' in harness and 'doneprintHandle.js' not in seen:
                parts.append(harness['doneprintHandle.js'])

        parts.append(source)
        final_source = '\n'.join(parts)

        # ── Execute ────────────────────────────────────────────────────
        suffix = '.mjs' if is_module else '.js'
        fd, tmp = tempfile.mkstemp(suffix=suffix, prefix='t262_')
        try:
            with os.fdopen(fd, 'w', encoding='utf-8') as fh:
                fh.write(final_source)

            cmd = [config['engine_path']] + list(config['engine_args'])
            if is_module:
                cmd.append(config['module_flag'])
            cmd.append(tmp)

            try:
                proc = subprocess.run(
                    cmd, capture_output=True, text=True,
                    timeout=config['timeout'], errors='replace',
                )
                exit_code = proc.returncode
                stdout = proc.stdout or ''
                stderr = proc.stderr or ''
            except subprocess.TimeoutExpired:
                res.update(result=TIMEOUT, error=f'Timeout ({config["timeout"]}s)',
                           duration=time.monotonic() - t0)
                return res
            except FileNotFoundError:
                res.update(result=ERROR, error=f'Engine not found: {config["engine_path"]}',
                           duration=time.monotonic() - t0)
                return res
            except Exception as e:
                res.update(result=ERROR, error=str(e), duration=time.monotonic() - t0)
                return res
        finally:
            try:
                os.unlink(tmp)
            except OSError:
                pass

        output = stdout + '\n' + stderr

        # ── Evaluate ───────────────────────────────────────────────────
        if negative:
            neg_type = negative.get('type', '')
            errored = exit_code != 0

            if errored:
                if neg_type and neg_type in output:
                    res['result'] = PASS
                elif not neg_type:
                    res['result'] = PASS
                else:
                    res['result'] = FAIL
                    res['error'] = f'Expected {neg_type}, got: {output[:300].strip()}'
            else:
                res['result'] = FAIL
                res['error'] = f'Expected {neg_type} ({neg_phase}) but no error thrown'
        elif is_async:
            if ASYNC_COMPLETE in stdout:
                res['result'] = PASS
            elif ASYNC_FAILURE in stdout:
                idx = stdout.index(ASYNC_FAILURE)
                res['result'] = FAIL
                res['error'] = stdout[idx:idx + 300].strip()
            elif exit_code != 0:
                res['result'] = FAIL
                res['error'] = output[:300].strip()
            else:
                res['result'] = FAIL
                res['error'] = '$DONE was never called'
        else:
            if exit_code == 0:
                res['result'] = PASS
            else:
                res['result'] = FAIL
                res['error'] = output[:300].strip()

        # Update reported mode
        if is_module:
            res['mode'] = 'module'
        elif use_strict:
            res['mode'] = 'strict'
        else:
            res['mode'] = 'non-strict'

    except Exception as e:
        res.update(result=ERROR, error=f'Runner error: {e}')

    res['duration'] = round(time.monotonic() - t0, 4)
    return res


# ── Test Discovery ─────────────────────────────────────────────────────────────

def discover_tests(config):
    t262 = Path(config['test262_path'])
    skip_dirs = set(config.get('skip_dirs', []))
    tests = []

    for td in config.get('test_dirs', ['test']):
        base = t262 / td
        if not base.is_dir():
            print(f"  [WARN] Test dir not found: {base}")
            continue
        for root, dirs, files in os.walk(str(base)):
            dirs[:] = sorted(d for d in dirs if not d.startswith('.'))
            rel_root = os.path.relpath(root, str(t262)).replace('\\', '/')
            if any(sd in rel_root for sd in skip_dirs if sd):
                dirs.clear()
                continue
            for fn in sorted(files):
                if not fn.endswith('.js'):
                    continue
                # Skip fixture files (test262 convention: *_FIXTURE.js)
                if fn.endswith('_FIXTURE.js'):
                    continue
                full = os.path.join(root, fn)
                rel = os.path.relpath(full, str(t262)).replace('\\', '/')
                tests.append((full, rel))
    return tests


def load_harness(config):
    hdir = Path(config['test262_path']) / 'harness'
    cache = {}
    if not hdir.is_dir():
        print(f"  [ERROR] Harness dir not found: {hdir}")
        return cache
    for f in hdir.iterdir():
        if f.suffix == '.js':
            try:
                cache[f.name] = f.read_text(encoding='utf-8', errors='replace')
            except Exception as e:
                print(f"  [WARN] Can't read harness {f.name}: {e}")
    return cache


# ── Reporting ──────────────────────────────────────────────────────────────────

def get_suite(rel_path):
    parts = rel_path.replace('\\', '/').split('/')
    # test/built-ins/... → built-ins
    if len(parts) >= 2:
        return parts[1] if parts[0] == 'test' else parts[0]
    return 'other'


def _merge_per_file(results):
    """Merge multiple mode runs into one result per file.
    Priority: FAIL > TIMEOUT > ERROR > SKIP > PASS
    If any mode fails, the file counts as fail."""
    PRIORITY = {FAIL: 0, TIMEOUT: 1, ERROR: 2, SKIP: 3, PASS: 4}
    merged = {}
    for r in results:
        key = r['test']
        if key not in merged:
            merged[key] = dict(r)
            merged[key]['modes'] = [r['mode']]
        else:
            existing = merged[key]
            existing['modes'].append(r['mode'])
            # Keep the worst result
            if PRIORITY.get(r['result'], 5) < PRIORITY.get(existing['result'], 5):
                existing['result'] = r['result']
                existing['error'] = r.get('error', '')
                existing['mode'] = r['mode']
            # Sum duration
            existing['duration'] = round(existing.get('duration', 0) + r.get('duration', 0), 4)
    return list(merged.values())


def build_summary(results, count_per_file=True):
    if count_per_file:
        results = _merge_per_file(results)

    summary = {'total': 0, 'pass': 0, 'fail': 0, 'skip': 0, 'timeout': 0, 'error': 0}
    suites = defaultdict(lambda: {'total': 0, 'pass': 0, 'fail': 0, 'skip': 0, 'timeout': 0, 'error': 0})

    key_map = {PASS: 'pass', FAIL: 'fail', SKIP: 'skip', TIMEOUT: 'timeout', ERROR: 'error'}

    for r in results:
        suite = get_suite(r['test'])
        k = key_map.get(r['result'], 'error')
        summary[k] += 1
        summary['total'] += 1
        suites[suite][k] += 1
        suites[suite]['total'] += 1

    countable = summary['total'] - summary['skip']
    summary['pass_rate'] = f"{summary['pass'] / countable * 100:.2f}%" if countable > 0 else "N/A"

    for s in suites.values():
        c = s['total'] - s['skip']
        s['pass_rate'] = f"{s['pass'] / c * 100:.2f}%" if c > 0 else "N/A"

    return summary, dict(suites)


def gen_json_report(results, summary, suites, t_start, t_end, config):
    non_pass = []
    for r in results:
        if r['result'] == PASS:
            continue
        entry = {
            'test': r['test'],
            'mode': r['mode'],
            'result': r['result'],
            'duration': r['duration'],
        }
        if config.get('save_errors', True) and r.get('error'):
            entry['error'] = r['error']
        non_pass.append(entry)

    return {
        'run_info': {
            'engine': config['engine_path'],
            'engine_args': config['engine_args'],
            'test262_path': config['test262_path'],
            'threads': config['threads'],
            'timeout': config['timeout'],
            'start': t_start.isoformat(),
            'end': t_end.isoformat(),
            'duration_s': round((t_end - t_start).total_seconds(), 2),
        },
        'summary': summary,
        'suites': suites,
        'non_passing': non_pass,
    }


def gen_html_report(summary, suites, t_start, t_end, config):
    dur = str(t_end - t_start).split('.')[0]
    suite_names = sorted(suites.keys())
    e = html_escape

    # ── Tab buttons ────────────────────────────────────────────────────
    tab_btns = '<button class="tab active" onclick="openTab(event,\'overall\')">Overall</button>\n'
    for sn in suite_names:
        tab_btns += f'    <button class="tab" onclick="openTab(event,\'{e(sn)}\')">{e(sn)}</button>\n'

    # ── Overall tab content ────────────────────────────────────────────
    rows = ''
    for sn in suite_names:
        s = suites[sn]
        c = s['total'] - s['skip']
        pct = s['pass'] / c * 100 if c > 0 else 0
        rows += f"""      <tr>
        <td>{e(sn)}</td><td>{s['total']}</td>
        <td class="pass">{s['pass']}</td><td class="fail">{s['fail']}</td>
        <td class="skip">{s['skip']}</td><td class="timeout">{s['timeout']}</td>
        <td class="error">{s['error']}</td>
        <td><div class="bar"><div class="bar-fill" style="width:{pct:.1f}%"></div></div></td>
        <td class="rate">{s['pass_rate']}</td>
      </tr>\n"""

    overall_tab = f"""<div id="overall" class="tab-content active">
  <table>
    <thead>
      <tr><th>Suite</th><th>Total</th><th>Pass</th><th>Fail</th><th>Skip</th><th>Timeout</th><th>Error</th><th style="min-width:120px">Bar</th><th>Rate</th></tr>
    </thead>
    <tbody>
{rows}    </tbody>
  </table>
</div>"""

    # ── Per-suite tabs ─────────────────────────────────────────────────
    suite_tabs = ''
    for sn in suite_names:
        s = suites[sn]
        c = s['total'] - s['skip']
        pct = s['pass'] / c * 100 if c > 0 else 0
        suite_tabs += f"""<div id="{e(sn)}" class="tab-content">
  <h3>{e(sn)}</h3>
  <div class="cards">
    <div class="card"><span class="num">{s['total']}</span><span class="lbl">Total</span></div>
    <div class="card"><span class="num pass">{s['pass']}</span><span class="lbl">Pass</span></div>
    <div class="card"><span class="num fail">{s['fail']}</span><span class="lbl">Fail</span></div>
    <div class="card"><span class="num skip">{s['skip']}</span><span class="lbl">Skip</span></div>
    <div class="card"><span class="num timeout">{s['timeout']}</span><span class="lbl">Timeout</span></div>
    <div class="card"><span class="num error">{s['error']}</span><span class="lbl">Error</span></div>
    <div class="card"><span class="num rate">{s['pass_rate']}</span><span class="lbl">Rate</span></div>
  </div>
  <div class="bar big"><div class="bar-fill" style="width:{pct:.1f}%"></div></div>
  <p class="detail">{s['pass']} / {c} tests passing (excluding {s['skip']} skipped)</p>
</div>\n"""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Test262 Results &mdash; {t_end.strftime('%Y-%m-%d %H:%M')}</title>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:#0d1117;color:#c9d1d9;padding:24px 32px}}
h1{{color:#58a6ff;font-size:1.5em;margin-bottom:4px}}
h3{{color:#c9d1d9;margin-bottom:12px}}
.meta{{color:#8b949e;font-size:.88em;margin-bottom:20px}}
.meta strong{{color:#c9d1d9}}
.summary{{display:flex;gap:12px;flex-wrap:wrap;margin-bottom:24px}}
.summary .card{{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:14px 18px;text-align:center;min-width:100px}}
.summary .card .num{{display:block;font-size:1.7em;font-weight:700}}
.summary .card .lbl{{display:block;font-size:.8em;color:#8b949e;margin-top:2px}}
.cards{{display:flex;gap:10px;flex-wrap:wrap;margin-bottom:14px}}
.cards .card{{background:#161b22;border:1px solid #30363d;border-radius:6px;padding:10px 14px;text-align:center;min-width:80px}}
.cards .card .num{{display:block;font-size:1.3em;font-weight:700}}
.cards .card .lbl{{display:block;font-size:.75em;color:#8b949e;margin-top:2px}}
.pass{{color:#3fb950}}.fail{{color:#f85149}}.skip{{color:#d29922}}.timeout{{color:#f0883e}}.error{{color:#bc4c00}}.rate{{color:#58a6ff}}
.tab-bar{{display:flex;gap:4px;flex-wrap:wrap;margin-bottom:16px;border-bottom:2px solid #21262d;padding-bottom:0}}
.tab{{background:transparent;border:none;color:#8b949e;padding:8px 14px;cursor:pointer;font-size:.9em;border-bottom:2px solid transparent;margin-bottom:-2px;border-radius:6px 6px 0 0;transition:.15s}}
.tab:hover{{color:#c9d1d9;background:#161b22}}
.tab.active{{color:#58a6ff;border-bottom-color:#58a6ff;background:#161b22}}
.tab-content{{display:none}}
.tab-content.active{{display:block}}
table{{width:100%;border-collapse:collapse;font-size:.88em;margin-top:8px}}
thead th{{text-align:left;padding:8px;border-bottom:2px solid #30363d;color:#8b949e;font-weight:600;white-space:nowrap}}
tbody td{{padding:6px 8px;border-bottom:1px solid #21262d}}
tbody tr:hover{{background:#161b22}}
.bar{{background:#21262d;border-radius:4px;height:8px;width:100%;overflow:hidden}}
.bar.big{{height:12px;margin-bottom:10px}}
.bar-fill{{height:100%;border-radius:4px;background:linear-gradient(90deg,#238636,#3fb950);transition:width .3s}}
.detail{{color:#8b949e;font-size:.85em;margin-top:6px}}
</style>
</head>
<body>
<h1>Test262 Results</h1>
<div class="meta">
  Engine: <strong>{e(config['engine_path'])}</strong> &nbsp;|&nbsp;
  Duration: <strong>{dur}</strong> &nbsp;|&nbsp;
  Threads: <strong>{config['threads']}</strong> &nbsp;|&nbsp;
  Finished: <strong>{t_end.strftime('%Y-%m-%d %I:%M %p')}</strong>
</div>

<div class="summary">
  <div class="card"><span class="num">{summary['total']}</span><span class="lbl">Total</span></div>
  <div class="card"><span class="num pass">{summary['pass']}</span><span class="lbl">Pass</span></div>
  <div class="card"><span class="num fail">{summary['fail']}</span><span class="lbl">Fail</span></div>
  <div class="card"><span class="num skip">{summary['skip']}</span><span class="lbl">Skip</span></div>
  <div class="card"><span class="num timeout">{summary['timeout']}</span><span class="lbl">Timeout</span></div>
  <div class="card"><span class="num error">{summary['error']}</span><span class="lbl">Error</span></div>
  <div class="card"><span class="num rate">{summary['pass_rate']}</span><span class="lbl">Pass Rate</span></div>
</div>

<div class="tab-bar">
  {tab_btns}
</div>

{overall_tab}
{suite_tabs}

<script>
function openTab(evt, id) {{
  document.querySelectorAll('.tab-content').forEach(el => el.classList.remove('active'));
  document.querySelectorAll('.tab').forEach(el => el.classList.remove('active'));
  document.getElementById(id).classList.add('active');
  evt.currentTarget.classList.add('active');
}}
</script>
</body>
</html>"""
    return html


# ── Validation ─────────────────────────────────────────────────────────────────

def validate_config(config):
    """Validate paths and settings before starting the run. Exits on fatal errors."""
    errors = []

    # Engine
    engine = config['engine_path']
    if not os.path.isfile(engine):
        # Try resolving via PATH (e.g. 'node')
        import shutil
        resolved = shutil.which(engine)
        if resolved:
            config['engine_path'] = resolved
            print(f"  Engine resolved via PATH: {resolved}")
        else:
            errors.append(f"Engine not found: {engine}")
    else:
        print(f"  Engine OK: {engine}")

    # Test262 root
    t262 = Path(config['test262_path'])
    if not t262.is_dir():
        errors.append(f"test262 path not found: {t262}")
    else:
        # Harness dir
        harness_dir = t262 / 'harness'
        if not harness_dir.is_dir():
            errors.append(f"Harness directory not found: {harness_dir}")
        else:
            # Check essential harness files
            for h in ('sta.js', 'assert.js'):
                if not (harness_dir / h).is_file():
                    errors.append(f"Essential harness file missing: {h}")

        # At least one test dir must exist
        any_test_dir = False
        for td in config.get('test_dirs', ['test']):
            if (t262 / td).is_dir():
                any_test_dir = True
                break
        if not any_test_dir:
            errors.append(f"No test directories found in {t262} (looked for: {config.get('test_dirs', ['test'])})")

    if errors:
        print()
        print("[FATAL] Cannot start run — configuration errors:")
        for e in errors:
            print(f"  ✗ {e}")
        print()
        print("Fix config.json and try again.")
        sys.exit(1)

    print("  Validation passed")


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    config_path = sys.argv[1] if len(sys.argv) > 1 else 'config.json'
    config = load_config(config_path)

    print("Test262 Runner")
    print(f"  Engine:     {config['engine_path']}")
    print(f"  Test262:    {config['test262_path']}")
    print(f"  Threads:    {config['threads']}")
    print(f"  Timeout:    {config['timeout']}s")
    print(f"  Both modes: {config['run_both_modes']}")
    print()

    # ── Validate ───────────────────────────────────────────────────────
    print("Validating config...")
    validate_config(config)
    print()

    # ── Harness ────────────────────────────────────────────────────────
    print("Loading harness...")
    harness = load_harness(config)
    if len(harness) == 0:
        print("[FATAL] No harness files loaded. Cannot run tests.")
        sys.exit(1)
    print(f"  {len(harness)} harness files loaded")

    # ── Discover ───────────────────────────────────────────────────────
    print("Discovering tests...")
    raw_tests = discover_tests(config)
    print(f"  {len(raw_tests)} test files found")
    if not raw_tests:
        print("[FATAL] No tests found. Check test262_path / test_dirs in config.")
        sys.exit(1)

    # ── Build tasks ────────────────────────────────────────────────────
    tasks = []
    for full, rel in raw_tests:
        tasks.append((full, rel, 'default'))
        if config['run_both_modes']:
            tasks.append((full, rel, 'strict'))

    print(f"  {len(tasks)} total test runs queued")
    print()

    # ── Execute ────────────────────────────────────────────────────────
    unsupported = set(config.get('unsupported_features', []))
    skip_tests = set(config.get('skip_tests', []))

    t_start = datetime.now()
    results = []
    done = 0
    counts = {PASS: 0, FAIL: 0, SKIP: 0, TIMEOUT: 0, ERROR: 0}
    total = len(tasks)
    last_tick = 0

    print(f"Running with {config['threads']} workers...")

    with ProcessPoolExecutor(
        max_workers=config['threads'],
        initializer=_worker_init,
        initargs=(config, harness, unsupported, skip_tests),
    ) as pool:
        futures = {pool.submit(run_test, t): t for t in tasks}

        for fut in as_completed(futures):
            try:
                r = fut.result()
            except Exception as exc:
                t = futures[fut]
                r = {'test': t[1], 'mode': t[2], 'result': ERROR,
                     'error': str(exc), 'duration': 0}

            if r is None:
                # Mode-skipped (e.g. noStrict test in strict run) – don't count
                done += 1
                total -= 1  # adjust total so percentages stay meaningful
                continue

            results.append(r)
            counts[r['result']] = counts.get(r['result'], 0) + 1
            done += 1

            now = time.monotonic()
            if now - last_tick >= 1.5 or done == len(futures):
                pct = done / len(futures) * 100
                sys.stdout.write(
                    f"\r  [{done}/{len(futures)}] {pct:.1f}%  "
                    f"P:{counts[PASS]} F:{counts[FAIL]} S:{counts[SKIP]} "
                    f"T:{counts[TIMEOUT]} E:{counts[ERROR]}"
                )
                sys.stdout.flush()
                last_tick = now

    t_end = datetime.now()
    print(f"\n\nFinished in {str(t_end - t_start).split('.')[0]}\n")

    # ── Summary ────────────────────────────────────────────────────────
    per_file = config.get('count_per_file', True)
    summary, suites = build_summary(results, count_per_file=per_file)

    mode_label = "per-file" if per_file else "per-run"
    print(f"─── Summary ({mode_label}) ───────────────────────────")
    print(f"  Total:   {summary['total']}")
    print(f"  Pass:    {summary['pass']}")
    print(f"  Fail:    {summary['fail']}")
    print(f"  Skip:    {summary['skip']}")
    print(f"  Timeout: {summary['timeout']}")
    print(f"  Error:   {summary['error']}")
    print(f"  Rate:    {summary['pass_rate']} (excl. skips)")
    print()
    for sn in sorted(suites):
        s = suites[sn]
        c = s['total'] - s['skip']
        rate = f"{s['pass']}/{c}" if c else "N/A"
        print(f"  {sn}: {rate}  ({s['pass_rate']})")
    print()

    # ── Save reports ───────────────────────────────────────────────────
    now = t_end
    # Format: month-day-2digityear-hour.minuteam/pm   e.g. 2-16-26-8.27pm
    h = int(now.strftime('%I'))
    folder_name = f"{now.month}-{now.day}-{now.strftime('%y')}-{h}.{now.strftime('%M%p').lower()}"
    out_dir = Path(config['results_dir']) / folder_name
    out_dir.mkdir(parents=True, exist_ok=True)

    # JSON
    report = gen_json_report(results, summary, suites, t_start, t_end, config)
    json_path = out_dir / 'results.json'
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    print(f"  JSON → {json_path}")

    # HTML
    html = gen_html_report(summary, suites, t_start, t_end, config)
    html_path = out_dir / 'results.html'
    with open(html_path, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"  HTML → {html_path}")

    print("\nDone!")


if __name__ == '__main__':
    main()
