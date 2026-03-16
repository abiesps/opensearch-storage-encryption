"""Replace releasePinnedBlockIfSlice body with no-op."""
import sys

filepath = sys.argv[1]
with open(filepath, 'r') as f:
    content = f.read()

old = """    private void releasePinnedBlockIfSlice() {
        if (!isSlice)
            return;

        final BlockCacheValue<RefCountedMemorySegment> b = currentBlock;
        if (b != null) {
            currentBlock = null;
            currentBlockOffset = -1L;
            b.unpin();
        } else {
            currentBlockOffset = -1L;
        }
    }"""

new = """    private void releasePinnedBlockIfSlice() {
        // BENCHMARKING: no-op
    }"""

if old not in content:
    print("ERROR: Could not find releasePinnedBlockIfSlice body to replace!")
    sys.exit(1)

content = content.replace(old, new)
with open(filepath, 'w') as f:
    f.write(content)
print("OK: releasePinnedBlockIfSlice no-op'd")
