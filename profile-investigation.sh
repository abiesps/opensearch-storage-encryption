#!/bin/bash
# =============================================================================
# Profile Investigation: directbufferpool vs mmap sequentialReadBytesFromClone
#
# Collects 3 types of evidence:
#   1. async-profiler CPU flamegraphs (Java + kernel stacks)
#   2. C2 compiler inlining logs (what got inlined, what didn't)
#   3. async-profiler wall-clock profiles (to catch blocking/contention)
#
# On macOS: async-profiler uses DTrace for kernel frames (needs sudo or SIP off)
# On Linux: async-profiler uses perf_events (eBPF-like kernel stacks)
# =============================================================================
set -e

export JAVA_HOME=/Library/Java/JavaVirtualMachines/amazon-corretto-21.jdk/Contents/Home
ASPROF="$(pwd)/async-profiler/async-profiler-4.3-macos/bin/asprof"
ASPROF_LIB="$(pwd)/async-profiler/async-profiler-4.3-macos/lib/libasyncProfiler.dylib"
OUT_DIR="profile-investigation"
mkdir -p "$OUT_DIR"

# Benchmark params — single-threaded first for clean inlining analysis
BENCH="sequentialReadBytesFromClone"
FORK=1
WI=3        # warmup iterations
ITER=3      # measurement iterations
TIME="5s"

echo "============================================"
echo "  STEP 0: Compile"
echo "============================================"
./gradlew clean compileJava compileJmhJava 2>&1 | tail -5

# ─────────────────────────────────────────────────────────────────────────────
# PHASE 1: C2 Inlining Logs
# Shows exactly what C2 decided to inline (or not) in readByte()
# ─────────────────────────────────────────────────────────────────────────────
echo ""
echo "============================================"
echo "  PHASE 1: C2 Inlining Logs"
echo "============================================"
echo "  This shows what C2 inlined into readByte()"
echo "  Look for: 'too big', 'already compiled', 'callee is too large'"
echo "============================================"

for DIR_TYPE in directbufferpool mmap; do
  echo ""
  echo "--- C2 inlining for $DIR_TYPE ---"
  LOG="$OUT_DIR/c2-inline-${DIR_TYPE}.log"

  ./gradlew jmh \
    -Pjmh.includes="$BENCH" \
    -Pjmh.fork=$FORK \
    -Pjmh.wi=$WI \
    -Pjmh.i=1 \
    -Pjmh.r="$TIME" \
    -Pjmh.w=3s \
    -Pjmh.jvmArgs="-XX:+UnlockDiagnosticVMOptions -XX:+PrintInlining -XX:+PrintCompilation --enable-native-access=ALL-UNNAMED --enable-preview" \
    -Pjmh.params.directoryType="$DIR_TYPE" \
    -Pjmh.params.threadCount=1 \
    -Pjmh.params.fileSizeMB=1 \
    -Pjmh.params.cacheBlockSizeKB=8 \
    -Pjmh.params.disableL1Cache=false \
    2>&1 | tee "$LOG"

  # Extract readByte inlining decisions
  echo ""
  echo "=== readByte inlining tree for $DIR_TYPE ==="
  grep -A2 'readByte' "$LOG" | grep -i 'inline\|too big\|callee\|hot\|cold\|intrinsic' | head -40 > "$OUT_DIR/c2-readByte-inline-${DIR_TYPE}.txt" 2>/dev/null || true
  cat "$OUT_DIR/c2-readByte-inline-${DIR_TYPE}.txt"

  # Extract compilation events for our classes
  echo ""
  echo "=== Compilation events for $DIR_TYPE ==="
  grep -E '(DirectByteBuffer|CachedMemorySegment|MemorySegmentIndexInput|SparseLong|BlockTable)' "$LOG" | grep -v '^$' | head -30 > "$OUT_DIR/c2-compilations-${DIR_TYPE}.txt" 2>/dev/null || true
  cat "$OUT_DIR/c2-compilations-${DIR_TYPE}.txt"
done

# ─────────────────────────────────────────────────────────────────────────────
# PHASE 2: async-profiler CPU with kernel stacks
# On macOS this uses DTrace (may need: sudo sysctl kern.dtrace.dof_mode=1)
# On Linux this uses perf_events (eBPF-equivalent)
# ─────────────────────────────────────────────────────────────────────────────
echo ""
echo "============================================"
echo "  PHASE 2: async-profiler CPU profiles"
echo "  (Java + native + kernel stacks)"
echo "============================================"

for DIR_TYPE in directbufferpool mmap; do
  echo ""
  echo "--- CPU profile for $DIR_TYPE ---"

  PROF_OUT="$OUT_DIR/cpu-${DIR_TYPE}"
  mkdir -p "$PROF_OUT"

  # Use JMH's -prof async with agentpath for full kernel+Java stacks
  # event=cpu captures both user and kernel frames
  # cstack=dwarf gives full native/kernel unwinding
  # --all-user includes non-Java threads
  ./gradlew jmh \
    -Pjmh.includes="$BENCH" \
    -Pjmh.fork=$FORK \
    -Pjmh.wi=$WI \
    -Pjmh.i=$ITER \
    -Pjmh.r="$TIME" \
    -Pjmh.w=3s \
    -Pjmh.jvmArgs="-agentpath:${ASPROF_LIB}=start,event=cpu,file=${PROF_OUT}/cpu-raw.jfr,cstack=dwarf,alluser --enable-native-access=ALL-UNNAMED --enable-preview" \
    -Pjmh.params.directoryType="$DIR_TYPE" \
    -Pjmh.params.threadCount=32 \
    -Pjmh.params.fileSizeMB=1 \
    -Pjmh.params.cacheBlockSizeKB=8 \
    -Pjmh.params.disableL1Cache=false \
    2>&1 | tee "$OUT_DIR/bench-cpu-${DIR_TYPE}.log"

  # Convert JFR to flamegraph HTML and collapsed stacks
  if [ -f "$PROF_OUT/cpu-raw.jfr" ]; then
    "$ASPROF" jfr2flame "$PROF_OUT/cpu-raw.jfr" "$PROF_OUT/flame-cpu-forward.html" 2>/dev/null || true
    "$ASPROF" jfr2flame --reverse "$PROF_OUT/cpu-raw.jfr" "$PROF_OUT/flame-cpu-reverse.html" 2>/dev/null || true
    # Collapsed stacks for text analysis
    "$ASPROF" jfr2flame --collapsed "$PROF_OUT/cpu-raw.jfr" "$PROF_OUT/collapsed.txt" 2>/dev/null || true

    echo ""
    echo "=== Top 20 self-CPU hotspots for $DIR_TYPE ==="
    if [ -f "$PROF_OUT/collapsed.txt" ]; then
      # Parse collapsed stacks: last frame in each line is the leaf (self)
      awk -F';' '{split($NF, a, " "); print a[1]}' "$PROF_OUT/collapsed.txt" \
        | sort | uniq -c | sort -rn | head -20 > "$PROF_OUT/top-self-cpu.txt"
      cat "$PROF_OUT/top-self-cpu.txt"
    fi
  fi
done

# ─────────────────────────────────────────────────────────────────────────────
# PHASE 3: Wall-clock profile (catches contention, locks, parking)
# ─────────────────────────────────────────────────────────────────────────────
echo ""
echo "============================================"
echo "  PHASE 3: Wall-clock profiles (contention)"
echo "============================================"

for DIR_TYPE in directbufferpool mmap; do
  echo ""
  echo "--- Wall-clock profile for $DIR_TYPE ---"

  PROF_OUT="$OUT_DIR/wall-${DIR_TYPE}"
  mkdir -p "$PROF_OUT"

  ./gradlew jmh \
    -Pjmh.includes="$BENCH" \
    -Pjmh.fork=$FORK \
    -Pjmh.wi=$WI \
    -Pjmh.i=$ITER \
    -Pjmh.r="$TIME" \
    -Pjmh.w=3s \
    -Pjmh.jvmArgs="-agentpath:${ASPROF_LIB}=start,event=wall,file=${PROF_OUT}/wall-raw.jfr,cstack=dwarf,alluser --enable-native-access=ALL-UNNAMED --enable-preview" \
    -Pjmh.params.directoryType="$DIR_TYPE" \
    -Pjmh.params.threadCount=32 \
    -Pjmh.params.fileSizeMB=1 \
    -Pjmh.params.cacheBlockSizeKB=8 \
    -Pjmh.params.disableL1Cache=false \
    2>&1 | tee "$OUT_DIR/bench-wall-${DIR_TYPE}.log"

  if [ -f "$PROF_OUT/wall-raw.jfr" ]; then
    "$ASPROF" jfr2flame "$PROF_OUT/wall-raw.jfr" "$PROF_OUT/flame-wall-forward.html" 2>/dev/null || true
    "$ASPROF" jfr2flame --reverse "$PROF_OUT/wall-raw.jfr" "$PROF_OUT/flame-wall-reverse.html" 2>/dev/null || true
    "$ASPROF" jfr2flame --collapsed "$PROF_OUT/wall-raw.jfr" "$PROF_OUT/collapsed.txt" 2>/dev/null || true

    echo ""
    echo "=== Top 20 wall-clock hotspots for $DIR_TYPE ==="
    if [ -f "$PROF_OUT/collapsed.txt" ]; then
      awk -F';' '{split($NF, a, " "); print a[1]}' "$PROF_OUT/collapsed.txt" \
        | sort | uniq -c | sort -rn | head -20 > "$PROF_OUT/top-self-wall.txt"
      cat "$PROF_OUT/top-self-wall.txt"
    fi
  fi
done

# ─────────────────────────────────────────────────────────────────────────────
# PHASE 4: Side-by-side comparison
# ─────────────────────────────────────────────────────────────────────────────
echo ""
echo "============================================"
echo "  PHASE 4: Side-by-side comparison"
echo "============================================"

echo ""
echo "=== CPU self-samples: directbufferpool vs mmap ==="
echo "--- directbufferpool ---"
cat "$OUT_DIR/cpu-directbufferpool/top-self-cpu.txt" 2>/dev/null || echo "(no data)"
echo ""
echo "--- mmap ---"
cat "$OUT_DIR/cpu-mmap/top-self-cpu.txt" 2>/dev/null || echo "(no data)"

echo ""
echo "=== C2 inlining: readByte differences ==="
echo "--- directbufferpool ---"
cat "$OUT_DIR/c2-readByte-inline-directbufferpool.txt" 2>/dev/null || echo "(no data)"
echo ""
echo "--- mmap ---"
cat "$OUT_DIR/c2-readByte-inline-mmap.txt" 2>/dev/null || echo "(no data)"

echo ""
echo "============================================"
echo "  All results in: $OUT_DIR/"
echo "  Flamegraphs: open **/flame-*.html in browser"
echo "  JFR files: open *.jfr in JDK Mission Control"
echo "============================================"
