#!/usr/bin/env python3
"""Parse async-profiler 4.x HTML flame graphs."""
import re, os

def parse_flamegraph(html_path):
    with open(html_path, 'r') as f:
        content = f.read()
    cpool_match = re.search(r'const\s+cpool\s*=\s*\[(.*?)\];\s*unpack\(cpool\)', content, re.DOTALL)
    if not cpool_match:
        return None, None, 0
    raw = []
    for m in re.finditer(r"'((?:[^'\\]|\\.)*)'", cpool_match.group(1)):
        raw.append(m.group(1))
    cpool = [raw[0]] if raw else []
    for i in range(1, len(raw)):
        e = raw[i]
        pl = ord(e[0]) - 32 if e else 0
        cpool.append(cpool[i-1][:pl] + e[1:])
    data_match = re.search(r'unpack\(cpool\)\s*;?\s*\n(.*?)(?:</script>|$)', content, re.DOTALL)
    if not data_match:
        return cpool, None, 0
    frame_data = data_match.group(1)
