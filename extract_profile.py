#!/usr/bin/env python3
"""Extract self-CPU sample counts from async-profiler 4.x reverse flamegraph HTML."""
import re, sys, os
from collections import defaultdict

def parse_ap4_reverse_flamegraph(path):
    """Parse a REVERSE flamegraph. In reverse mode, level 0 = leaf (self) frames."""
    with open(path) as f:
        content = f.read()

    # 1. Extract and unpack cpool (prefix-compressed string pool)
    m = re.search(r"const\s+cpool\s*=\s*\[(.*?)\];\s*unpack\(cpool\)", content, re.DOTALL)
    if not m:
        return None
    raw = [sm.group(1) for sm in re.finditer(r"'((?:[^'\\\\]|\\\\.)*?)'", m.group(1))]
    cpool = [raw[0]] if raw else []
    for i in range(1, len(raw)):
        e = raw[i]
        pl = ord(e[0]) - 32 if e else 0
        cpool.append(cpool[i-1][:pl] + e[1:])

    # 2. Extract frame data
    dm = re.search(r'unpack\(cpool\)\s*;?\s*\n(.*?)(?:</script>|\Z)', content, re.DOTALL)
    if not dm:
        return None
    frame_data = dm.group(1)

    # 3. Parse f/u/n calls
    # In a REVERSE flamegraph:
    #   - Level 0 frames are the LEAF (self) methods
    #   - f() at level 0 with width = self samples for that method
    #   - Higher levels are callers
    level0_samples = defaultdict(int)  # method -> self samples at level 0
    total_root = 0
    level = 0
    last_width = 0

    for cm in re.finditer(r'([fun])\(([^)]*)\)', frame_data):
        func = cm.group(1)
        parts = cm.group(2).split(',')
        args = []
        for p in parts:
            p = p.strip()
            if p:
                try:
                    args.append(int(p))
                except ValueError:
                    args.append(0)
            else:
                args.append(0)

        if func == 'f':
            key = args[0]
            lv = args[1] if len(args) > 1 else 0
            w = args[3] if len(args) > 3 else last_width
            name = cpool[key >> 3] if (key >> 3) < len(cpool) else f'<unknown:{key>>3}>'
            level = lv
            last_width = w
            if lv == 0:
                level0_samples[name] += w
                total_root += w
        elif func == 'u':
            key = args[0]
            w = args[1] if len(args) > 1 and args[1] != 0 else last_width
            level += 1
            last_width = w
        elif func == 'n':
            key = args[0]
            w = args[1] if len(args) > 1 and args[1] != 0 else last_width
            name = cpool[key >> 3] if (key >> 3) < len(cpool) else f'<unknown:{key>>3}>'
            last_width = w
            if level == 0:
                level0_samples[name] += w
                total_root += w

    return level0_samples, total_root, cpool

def print_profile(label, path):
    result = parse_ap4_reverse_flamegraph(path)
    if not result:
        print(f"  Could not parse {path}")
        return
    samples, total, cpool = result
    sorted_samples = sorted(samples.items(), key=lambda x: -x[1])

    print(f"\n{'='*90}")
    print(f"  {label}")
    print(f"  Source: {os.path.basename(os.path.dirname(path))}")
    print(f"  Total level-0 (self) samples: {total}")
    print(f"{'='*90}")
    print(f"  {'Samples':>8}  {'%':>6}  Method")
    print(f"  {'-'*8}  {'-'*6}  {'-'*60}")
    for name, count in sorted_samples[:30]:
        pct = count * 100.0 / total if total > 0 else 0
        print(f"  {count:>8}  {pct:>5.1f}%  {name}")

if __name__ == '__main__':
    base = "profile-results"
    dirs = sorted(os.listdir(base)) if os.path.isdir(base) else []

    for d in dirs:
        rev = os.path.join(base, d, "flame-cpu-reverse.html")
        if os.path.isfile(rev):
            label = "DIRECTBUFFERPOOL" if "directbufferpool" in d else "MMAP" if "mmap" in d else d
            print_profile(label, rev)

    # Also check profile-investigation if it exists
    inv = "profile-investigation"
    if os.path.isdir(inv):
        for sub in ["kernel/directbufferpool", "kernel/mmap", "cpu-directbufferpool", "cpu-mmap"]:
            for fname in ["flame-cpu-reverse.html", "flame-reverse.html"]:
                p = os.path.join(inv, sub, fname)
                if os.path.isfile(p):
                    print_profile(sub.upper(), p)
