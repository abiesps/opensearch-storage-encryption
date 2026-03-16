#!/usr/bin/env python3
"""Parse JIT compilation XML logs to resolve method IDs and find unstable_if issues."""
import re
import sys

def parse_log(filepath):
    with open(filepath, 'r', errors='replace') as f:
        content = f.read()

    # Find all method declarations
    methods = {}
    for m in re.finditer(r'<method\s+id="(\d+)"\s+[^>]*?name="([^"]+)"[^>]*?holder="(\d+)"', content):
        mid, name, holder = m.group(1), m.group(2), m.group(3)
        methods[mid] = (name, holder)

    # Find all klass/type declarations
    klasses = {}
    for m in re.finditer(r'<(?:klass|type)\s+id="(\d+)"\s+name="([^"]+)"', content):
        kid, kname = m.group(1), m.group(2)
        klasses[kid] = kname

    # Find compilations with unstable_if_traps
    print(f"\n=== Analyzing {filepath} ===\n")
    print("Compilations with unstable_if_traps or decompiles:")
    for m in re.finditer(r'<nmethod\s+[^>]*?compile_id="(\d+)"[^>]*', content):
        line = m.group(0)
        if 'unstable_if_traps' in line or 'decompiles' in line:
            print(f"  {line[:300]}")

    # Find lambda compilations
    print("\nLambda compilations (sequentialRead):")
    for m in re.finditer(r'<task\s+compile_id="(\d+)"\s+[^>]*?method="([^"]*sequentialRead[^"]*)"', content):
        print(f"  compile_id={m.group(1)}: {m.group(2)}")

    # Find readByte compilations
    print("\nreadByte compilations:")
    for m in re.finditer(r'<task\s+compile_id="(\d+)"\s+[^>]*?method="([^"]*readByte[^"]*)"', content):
        print(f"  compile_id={m.group(1)}: {m.group(2)}")

    return methods, klasses, content

def resolve_methods_in_task(content, compile_id, global_methods, global_klasses):
    """Find method and klass declarations within a specific compilation task."""
    # Find the task block
    pattern = f'<task compile_id="{compile_id}"[^>]*>.*?</task>'
    task_match = re.search(pattern, content, re.DOTALL)
    if not task_match:
        print(f"  Task {compile_id} not found")
        return

    task_content = task_match.group(0)

    # Local method and klass declarations within this task
    local_methods = {}
    for m in re.finditer(r'<method\s+id="(\d+)"\s+[^>]*?name="([^"]+)"[^>]*?holder="(\d+)"', task_content):
        mid, name, holder = m.group(1), m.group(2), m.group(3)
        local_methods[mid] = (name, holder)

    local_klasses = {}
    for m in re.finditer(r'<(?:klass|type)\s+id="(\d+)"\s+name="([^"]+)"', task_content):
        kid, kname = m.group(1), m.group(2)
        local_klasses[kid] = kname

    print(f"\n=== Task compile_id={compile_id} ===")
    print(f"  Local methods: {len(local_methods)}, Local klasses: {len(local_klasses)}")

    # Find inline failures
    print("\n  Inline failures:")
    for m in re.finditer(r'<inline_fail method="(\d+)" reason="([^"]+)"', task_content):
        mid, reason = m.group(1), m.group(2)
        if mid in local_methods:
            name, holder = local_methods[mid]
            holder_name = local_klasses.get(holder, f'klass_{holder}')
            print(f"    {holder_name}.{name} - {reason}")
        else:
            print(f"    method_{mid} - {reason}")

    # Find uncommon traps
    print("\n  Uncommon traps:")
    for m in re.finditer(r'<uncommon_trap\s+[^>]*?reason="([^"]+)"[^>]*?action="([^"]+)"[^>]*', task_content):
        line = m.group(0)
        reason = m.group(1)
        action = m.group(2)
        comment_match = re.search(r'comment="([^"]+)"', line)
        comment = comment_match.group(1) if comment_match else ""
        if reason in ('unstable_if', 'class_check', 'null_check'):
            print(f"    reason={reason} action={action} comment={comment}")

    # Find inlined methods
    print("\n  Successfully inlined methods:")
    for m in re.finditer(r'<inline_success method="(\d+)"', task_content):
        mid = m.group(1)
        if mid in local_methods:
            name, holder = local_methods[mid]
            holder_name = local_klasses.get(holder, f'klass_{holder}')
            print(f"    {holder_name}.{name}")

    # Find observe traps
    print("\n  Observed traps:")
    for m in re.finditer(r'<observe\s+[^>]*', task_content):
        print(f"    {m.group(0)}")

if __name__ == '__main__':
    print("=" * 80)
    print("DIRECTBUFFERPOOL JIT LOG")
    print("=" * 80)
    methods, klasses, content = parse_log('/tmp/jit_dbp.log')

    # Find the lambda compile IDs
    lambda_ids = []
    for m in re.finditer(r'<task\s+compile_id="(\d+)"\s+[^>]*?method="([^"]*sequentialRead[^"]*)"', content):
        lambda_ids.append(m.group(1))

    # Also find readByte compile IDs
    readbyte_ids = []
    for m in re.finditer(r'<task\s+compile_id="(\d+)"\s+[^>]*?method="([^"]*readByte[^"]*)"', content):
        readbyte_ids.append(m.group(1))

    # Resolve methods in the last lambda compilation (most interesting)
    for cid in lambda_ids[-3:]:  # last 3
        resolve_methods_in_task(content, cid, methods, klasses)

    for cid in readbyte_ids[-2:]:  # last 2
        resolve_methods_in_task(content, cid, methods, klasses)

    print("\n" + "=" * 80)
    print("MMAP JIT LOG")
    print("=" * 80)
    methods2, klasses2, content2 = parse_log('/tmp/jit_mmap.log')

    lambda_ids2 = []
    for m in re.finditer(r'<task\s+compile_id="(\d+)"\s+[^>]*?method="([^"]*sequentialRead[^"]*)"', content2):
        lambda_ids2.append(m.group(1))

    for cid in lambda_ids2[-3:]:
        resolve_methods_in_task(content2, cid, methods2, klasses2)
