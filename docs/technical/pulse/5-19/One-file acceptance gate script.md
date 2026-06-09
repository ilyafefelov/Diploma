
Here’s a ready‑to‑drop CI “acceptance gate” that sanity‑checks your `metrics.json` artifacts for ordering and drift—perfect for your V2+ / frozen_v2 / strict lanes and 4 rolling windows.

```python
#!/usr/bin/env python3
import glob, json, sys, os, statistics, argparse, re

def find_numeric_regret(obj):
    # common keys to try
    for k in ("regret_mean","regret","mean_regret","regret_val","regret_mean_ua"):
        if k in obj and isinstance(obj[k], (int, float)):
            return float(obj[k])
    if isinstance(obj.get("metrics"), dict):
        for k in ("regret_mean","regret","mean_regret","regret_val"):
            if k in obj["metrics"] and isinstance(obj["metrics"][k], (int, float)):
                return float(obj["metrics"][k])
    # fallback: any numeric key containing 'regret'
    for k,v in obj.items():
        if 'regret' in k.lower() and isinstance(v, (int, float)):
            return float(v)
    return None

def find_lane(obj, filename):
    for k in ("lane","model","experiment","tag","run_name","name"):
        if k in obj and isinstance(obj[k], str):
            s = obj[k].lower()
            return normalize_lane(s)
    fname = os.path.basename(filename).lower()
    return normalize_lane(fname)

def normalize_lane(s):
    if 'strict' in s:
        return 'strict'
    if 'frozen' in s and 'v2' in s:
        return 'frozen_v2'
    if 'frozen' in s:
        return 'frozen_v2'
    if 'v2+' in s or 'v2_plus' in s or ('v2' in s and 'plus' in s) or ('v2' in s and '+' in s):
        return 'v2_plus'
    if 'v2' in s and 'freeze' not in s:
        if 'plus' in s or '+' in s:
            return 'v2_plus'
    m = re.search(r'(v2\+|v2_plus|frozen_v2|frozen|strict)', s)
    if m:
        return normalize_lane(m.group(1))
    return s.replace(' ', '_')

def find_window(obj, filename):
    for k in ("window","window_idx","fold","cv_fold","window_index"):
        if k in obj:
            try:
                return int(obj[k])
            except Exception:
                pass
    m = re.search(r'(?:_w|window|win|fold)(\d+)', filename.lower())
    if m:
        return int(m.group(1))
    return None

def find_seed(obj, filename):
    for k in ("seed", "random_seed"):
        if k in obj:
            try:
                return int(obj[k])
            except Exception:
                pass
    m = re.search(r'seed[_\-]?(\d+)', filename.lower())
    if m:
        return int(m.group(1))
    return None

def main():
    p = argparse.ArgumentParser(description='CI acceptance gate for regret ordering and thresholds')
    p.add_argument('--metrics-glob', default='experiments/logs/**/metrics*.json', help='glob to find metrics json files')
    p.add_argument('--require-windows', type=int, default=4, help='number of rolling windows expected (default: 4)')
    p.add_argument('--v2plus-ref', type=float, default=174.77, help='reference mean for V2+')
    p.add_argument('--frozen-ref', type=float, default=206.37, help='reference mean for frozen V2')
    p.add_argument('--strict-ref', type=float, default=310.58, help='reference mean for strict')
    p.add_argument('--overall-tol', type=float, default=0.05, help='relative tolerance for v2_plus vs ref (default 0.05)')
    p.add_argument('--window-dev-tol', type=float, default=0.15, help='max relative deviation per-window from lane overall mean (default 0.15)')
    args = p.parse_args()

    files = glob.glob(args.metrics_glob, recursive=True)
    if not files:
        print('ERROR: no metrics files found with glob:', args.metrics_glob)
        sys.exit(2)

    data = {}
    seeds_by_lane = {}

    for f in files:
        try:
            with open(f, 'r') as fh:
                obj = json.load(fh)
        except Exception as e:
            print('WARN: could not read', f, e)
            continue
        val = find_numeric_regret(obj)
        if val is None:
            print('WARN: no numeric regret found in', f)
            continue
        lane = find_lane(obj, f)
        window = find_window(obj, f)
        seed = find_seed(obj, f)
        if lane not in data:
            data[lane] = {}
            seeds_by_lane[lane] = set()
        if window is None:
            window = -1
        data[lane].setdefault(window, []).append((seed, val))
        if seed is not None:
            seeds_by_lane[lane].add(seed)

    lanes_needed = ['v2_plus','frozen_v2','strict']
    present = set(data.keys())

    def find_key(sub):
        for k in present:
            if sub in k:
                return k
        return None

    mapped = {}
    for target in lanes_needed:
        found = None
        for k in data.keys():
            if target in k:
                found = k; break
        if not found:
            if target == 'v2_plus':
                found = find_key('v2+') or find_key('v2_plus') or find_key('v2')
            if target == 'frozen_v2':
                found = find_key('frozen') or find_key('frozen_v2')
            if target == 'strict':
                found = find_key('strict')
        if found:
            mapped[target] = found

    missing = [t for t in lanes_needed if t not in mapped]
    if missing:
        print('ERROR: missing expected lanes in metrics:', missing, 'found keys:', list(data.keys()))
        sys.exit(3)

    lane_overall = {}
    lane_per_window = {}
    for logical, real_key in mapped.items():
        vals = []
        per_w = {}
        for w, items in data[real_key].items():
            nums = [v for (s,v) in items]
            if nums:
                per_w[w] = statistics.mean(nums)
                vals.extend(nums)
        if not vals:
            print('ERROR: no numeric values for lane', real_key)
            sys.exit(4)
        lane_overall[logical] = statistics.mean(vals)
        lane_per_window[logical] = per_w

    failures = []
    rel = abs(lane_overall['v2_plus'] / args.v2plus_ref - 1.0)
    if rel > args.overall_tol:
        failures.append(f"v2_plus mean {lane_overall['v2_plus']:.3f} differs from ref {args.v2plus_ref} by {rel*100:.1f}% > {args.overall_tol*100:.1f}%")

    if not (lane_overall['v2_plus'] < lane_overall['frozen_v2'] < lane_overall['strict']):
        failures.append('ordering violation overall: v2_plus < frozen_v2 < strict not satisfied (values: v2_plus={:.3f}, frozen_v2={:.3f}, strict={:.3f})'.format(lane_overall['v2_plus'], lane_overall['frozen_v2'], lane_overall['strict']))

    expected_w = args.require_windows
    all_windows = set()
    for logical in lane_per_window:
        all_windows.update(lane_per_window[logical].keys())
    windows = sorted([w for w in all_windows if w is not None and w >= 0])
    if len(windows) < expected_w:
        failures.append(f'expected at least {expected_w} windows (found {len(windows)}: {windows})')

    for w in windows[:expected_w]:
        vals = {}
        for logical in lane_per_window:
            v = lane_per_window[logical].get(w, None)
            vals[logical] = v
        if None in vals.values():
            failures.append(f'window {w}: missing values for lanes: ' + ','.join([k for k,v in vals.items() if v is None]))
            continue
        if not (vals['v2_plus'] < vals['frozen_v2'] < vals['strict']):
            failures.append(f'window {w}: ordering violation (v2_plus={vals["v2_plus"]:.3f}, frozen_v2={vals["frozen_v2"]:.3f}, strict={vals["strict"]:.3f})')
        for logical in vals:
            overall = lane_overall[logical]
            dev = abs(vals[logical] / overall - 1.0)
            if dev > args.window_dev_tol:
                failures.append(f'window {w}: {logical} mean {vals[logical]:.3f} deviates from overall {overall:.3f} by {dev*100:.1f}% > {args.window_dev_tol*100:.1f}%')

    for logical, real_key in mapped.items():
        seeds = seeds_by_lane.get(real_key, set())
        if len(seeds) < 2:
            failures.append(f'lane {real_key} has less than 2 seeds ({len(seeds)}) -- insufficient for robust stats')

    print('\n--- acceptance gate summary ---')
    for logical in ('v2_plus','frozen_v2','strict'):
        print(f"{logical}: overall_mean={lane_overall[logical]:.3f}")
    if failures:
        print('\nGATE FAILED:')
        for fmsg in failures:
            print(' -', fmsg)
        sys.exit(5)
    else:
        print('\nGATE PASSED: all checks OK')
        sys.exit(0)

if __name__ == '__main__':
    main()
```

**What it does (fast):**

* Scans `experiments/logs/**/metrics*.json` (configurable).
* Extracts a numeric regret, lane (`v2_plus`, `frozen_v2`, `strict` via keys/filenames), window index, and seed.
* Checks:
  * Overall ordering: `v2_plus < frozen_v2 < strict`.
  * `v2_plus` mean within ±5% of 174.77 UAH (flags drift).
  * First 4 windows: ordering holds and each window stays within 15% of that lane’s overall mean.
  * Warns on weak seed coverage (<2 per lane).
* Prints a crisp summary and  **exits 0 on pass** , non‑zero on fail → perfect for CI.

**Save as** `ci_acceptance_gate.py` at repo root.

**Local run**

```bash
python ci_acceptance_gate.py --metrics-glob 'experiments/logs/**/metrics*.json'
```

**GitHub Actions step**

```yaml
- name: Acceptance gate
  run: python ci_acceptance_gate.py --metrics-glob 'experiments/logs/**/metrics*.json'
```

Want me to tailor the glob/thresholds to your current artifact layout or add a brief table printout per window?
