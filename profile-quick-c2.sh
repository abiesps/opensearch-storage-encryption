#!/bin/bash
# =============================================================================
# Quick C2 inlining analysis — captures what C2 decided to inline in readByte()
# Takes ~60s per directory type (1 fork, 2 warmup, 1 measurement).
# =============================================================================
set -e

export JAVA_HOME=/Library/Java/JavaVirtualMachines/amazon-corretto-21.jdk/Contents/Home
OUT_DIR="profile-investigation"
mkdir -p "$OUT_DIR"

BENCH="sequentialReadBytesFromClone"

echo "Compiling..."
./gradlew compileJava compileJmhJava 2>&1 | tail -3

for DIR_TYPE in directbufferpool mmap; do
  echo ""
  echo "============================================"
  echo "  C2 Inlining: $DIR_TYPE"
  echo "============================================"
  LOG="$OUT_DIR/c2-full-${DIR_TYPE}.log"

  # -XX:+PrintInlining shows the inlining tree
  # -XX:+PrintCompilation shows what methods C2 compiled
  ./gradlew jmh \
    -Pjmh.includes="$BENCH" \
    -Pjmh.fork=1 \
    -Pjmh.wi=2 \
    -Pjmh.i=1 \
    -Pjmh.r=3s \
    -Pjmh.w=2s \
    "-Pjmh.jvmArgs=-XX:+UnlockDiagnosticVMOptions -XX:+PrintInlining -XX:+PrintCompilation" \
    -Pjmh.params.directoryType="$DIR_TYPE" \
    -Pjmh.params.threadCount=1 \
    -Pjmh.params.fileSizeMB=1 \
    -Pjmh.params.cacheBlockSizeKB=8 \
    -Pjmh.params.disableL1Cache=false \
    2>&1 | tee "$LOG"

  echo ""
  echo "=== C2 compilations of readByte / hot-path methods ==="
  grep -n 'readByte\|readBytes\|switchToBlock\|loadFromCaffeine\|loadBlockForRead\|SparseLong\|blockTable\|MemorySegment.*get\b\|checkBounds\|sessionImpl\|checkAccess' "$LOG" \
    | grep -v '^$' | head -60
  echo ""
  echo "(Full log: $LOG — grep for 'readByte' to see inlining tree)"

  echo ""
  echo "=== Inlining decisions around readByte ==="
  grep -n 'DirectByteBufferIndexInput::readByte\|MemorySegmentIndexInput::readByte\|CachedMemorySegmentIndexInput::readByte' "$LOG" \
    | head -10

  echo ""
  echo "=== Methods NOT inlined (too big / too deep) ==="
  grep 'too big\|callee is too large\|already compiled into a big method\|hot method too big\|inline (hot)\|not inlineable' "$LOG" \
    | grep -i 'readByte\|switchToBlock\|loadFrom\|blockTable\|SparseLong\|MemorySegment\|checkBounds\|sessionImpl' \
    | head -20

  echo ""
  echo "=== Intrinsic recognitions ==="
  grep 'intrinsic' "$LOG" \
    | grep -i 'bitCount\|MemorySegment\|Unsafe\|VarHandle\|arraycopy' \
    | sort | uniq -c | sort -rn | head -15
done

echo ""
echo "============================================"
echo "  Done. Full logs in $OUT_DIR/c2-full-*.log"
echo "============================================"
