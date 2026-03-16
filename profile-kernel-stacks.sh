#!/bin/bash
# =============================================================================
# Kernel-level stack trace analysis using async-profiler
#
# BACKGROUND:
# - On Linux: async-profiler uses perf_events (kernel perf subsystem) which
#   gives you eBPF-equivalent mixed Java+kernel stacks. No actual eBPF probes
#   needed — perf_events is the standard way to get kernel stacks for JVM apps.
#
# - On macOS (this machine): async-profiler uses DTrace for kernel frames.
#   Requires either:
#   a) SIP (System Integrity Protection) disabled, OR
#   b) Running with sudo, OR
#   c) The process having the com.apple.security.cs.allow-dtrace entitlement
#
#   If DTrace is blocked, you'll still get Java + native (libc/JVM) frames
#   but NOT kernel frames (mach_msg_trap, vm_fault, etc.)
#
# WHAT THIS CAPTURES:
# - Java frames (compiled + interpreted)
# - JVM internal frames (C1/C2 compiler, GC, safepoints)
# - Native frames (libc, libpthread, JNI)
# - Kernel frames (syscalls, page faults, scheduler) — if DTrace works
#
# This is critical for understanding:
# - Are we hitting page faults on MemorySegment access?
# - Is there kernel-level contention (futex, mach_msg)?
# - How much time is in user vs kernel space?
# =============================================================================
set -e

export JAVA_HOME=/Library/Java/JavaVirtualMachines/amazon-corretto-21.jdk/Contents/Home
ASPROF_LIB="$(pwd)/async-profiler/async-profiler-4.3-macos/lib/libasyncProfiler.dylib"
ASPROF="$(pwd)/async-profiler/async-profiler-4.3-macos/bin/asprof"
OUT_DIR="profile-investigation/kernel"
mkdir -p "$OUT_DIR"

BENCH="sequentialReadBytesFromClone"

echo "Compiling..."
./gradlew compileJava compileJmhJava 2>&1 | tail -3

# ─────────────────────────────────────────────────────────────────────────────
# Method 1: agentpath with cstack=dwarf (best for mixed stacks)
#
# cstack=dwarf: Uses DWARF unwinding for native/kernel frames
#   - Most accurate native stack unwinding
#   - Works with stripped binaries
#   - Slightly higher overhead than fp (frame pointer) mode
#
# cstack=fp: Uses frame pointers (faster but may miss frames)
#   - Requires -fno-omit-frame-pointer compiled binaries
#   - Lower overhead
#   - May miss kernel frames if kernel wasn't compiled with frame pointers
#
# cstack=lbr: Uses Last Branch Record (Intel only, not available on ARM)
# ─────────────────────────────────────────────────────────────────────────────

for DIR_TYPE in directbufferpool mmap; do
  echo ""
  echo "============================================"
  echo "  Kernel+Java CPU profile: $DIR_TYPE"
  echo "  (cstack=dwarf for full native unwinding)"
  echo "============================================"

  PROF_OUT="$OUT_DIR/${DIR_TYPE}"
  mkdir -p "$PROF_OUT"

  # event=cpu: CPU profiling (itimer-based on macOS, perf_events on Linux)
  # cstack=dwarf: full DWARF-based native/kernel unwinding
  # alluser: include non-Java threads (GC, compiler, etc.)
  # jstackdepth=256: deep Java stacks
  # interval=1ms: 1ms sampling interval (1000 samples/sec)
  ./gradlew jmh \
    -Pjmh.includes="$BENCH" \
    -Pjmh.fork=1 \
    -Pjmh.wi=3 \
    -Pjmh.i=3 \
    -Pjmh.r=5s \
    -Pjmh.w=3s \
    -Pjmh.jvmArgs="-agentpath:${ASPROF_LIB}=start,event=cpu,file=${PROF_OUT}/cpu.jfr,cstack=dwarf,alluser,jstackdepth=256,interval=1000000 --enable-native-access=ALL-UNNAMED --enable-preview" \
    -Pjmh.params.directoryType="$DIR_TYPE" \
    -Pjmh.params.threadCount=32 \
    -Pjmh.params.fileSizeMB=1 \
    -Pjmh.params.cacheBlockSizeKB=8 \
    -Pjmh.params.disableL1Cache=false \
    2>&1 | tee "$OUT_DIR/bench-${DIR_TYPE}.log"

  if [ -f "$PROF_OUT/cpu.jfr" ]; then
    # Generate flamegraphs
    "$ASPROF" jfr2flame "$PROF_OUT/cpu.jfr" "$PROF_OUT/flame-forward.html" 2>/dev/null || true
    "$ASPROF" jfr2flame --reverse "$PROF_OUT/cpu.jfr" "$PROF_OUT/flame-reverse.html" 2>/dev/null || true

    # Collapsed stacks for text analysis
    "$ASPROF" jfr2flame --collapsed "$PROF_OUT/cpu.jfr" "$PROF_OUT/collapsed.txt" 2>/dev/null || true

    echo ""
    echo "=== Top 25 leaf (self) CPU frames for $DIR_TYPE ==="
    if [ -f "$PROF_OUT/collapsed.txt" ]; then
      awk -F';' '{split($NF, a, " "); print a[1]}' "$PROF_OUT/collapsed.txt" \
        | sort | uniq -c | sort -rn | head -25 | tee "$PROF_OUT/top-self.txt"
    fi

    echo ""
    echo "=== Kernel frames (if DTrace worked) ==="
    if [ -f "$PROF_OUT/collapsed.txt" ]; then
      # Kernel frames show up as [kernel] or with _kernel_ prefix
      grep -o '[^;]*' "$PROF_OUT/collapsed.txt" \
        | grep -iE 'kernel|_trap$|_fault$|syscall|mach_msg|vm_fault|page_fault|futex|sched_' \
        | sort | uniq -c | sort -rn | head -15 | tee "$PROF_OUT/kernel-frames.txt"
      if [ ! -s "$PROF_OUT/kernel-frames.txt" ]; then
        echo "(No kernel frames captured — DTrace may be blocked by SIP)"
        echo "To enable on macOS: csrutil disable (from Recovery Mode)"
        echo "Or on Linux: sudo sysctl kernel.perf_event_paranoid=1"
      fi
    fi

    echo ""
    echo "=== User vs Kernel time split ==="
    if [ -f "$PROF_OUT/collapsed.txt" ]; then
      TOTAL=$(awk -F';' '{split($NF, a, " "); sum += a[2]} END {print sum}' "$PROF_OUT/collapsed.txt" 2>/dev/null || echo 0)
      KERNEL=$(grep -i 'kernel\|_trap\|_fault\|syscall' "$PROF_OUT/collapsed.txt" | awk -F';' '{split($NF, a, " "); sum += a[2]} END {print sum+0}' 2>/dev/null || echo 0)
      echo "Total samples: $TOTAL"
      echo "Kernel samples: $KERNEL"
      if [ "$TOTAL" -gt 0 ]; then
        echo "Kernel %: $(echo "scale=1; $KERNEL * 100 / $TOTAL" | bc)%"
      fi
    fi
  fi
done

# ─────────────────────────────────────────────────────────────────────────────
# Method 2: Attach to running JVM (alternative approach)
#
# If you want to profile an already-running JVM:
#   asprof -e cpu -d 30 -f output.jfr --cstack dwarf <PID>
#
# Or for lock contention:
#   asprof -e lock -d 30 -f locks.jfr <PID>
#
# Or for memory allocation:
#   asprof -e alloc -d 30 -f alloc.jfr <PID>
# ─────────────────────────────────────────────────────────────────────────────

echo ""
echo "============================================"
echo "  COMPARISON"
echo "============================================"
echo ""
echo "--- directbufferpool top self-CPU ---"
cat "$OUT_DIR/directbufferpool/top-self.txt" 2>/dev/null || echo "(no data)"
echo ""
echo "--- mmap top self-CPU ---"
cat "$OUT_DIR/mmap/top-self.txt" 2>/dev/null || echo "(no data)"

echo ""
echo "============================================"
echo "  Output files:"
echo "  $OUT_DIR/directbufferpool/flame-forward.html  (open in browser)"
echo "  $OUT_DIR/directbufferpool/flame-reverse.html"
echo "  $OUT_DIR/mmap/flame-forward.html"
echo "  $OUT_DIR/mmap/flame-reverse.html"
echo "  $OUT_DIR/*/collapsed.txt  (grep-able text stacks)"
echo "  $OUT_DIR/*/cpu.jfr  (open in JDK Mission Control)"
echo "============================================"
