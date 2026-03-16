"""Replace manually-inlined readByte with method-call version (still Unsafe)."""
import sys

filepath = sys.argv[1]
with open(filepath, 'r') as f:
    content = f.read()

# The manually inlined readByte from the full patch
old = """    @Override
    public final byte readByte() throws IOException {
        final long currentPos = curPosition;
        try {
            // Manually inlined fast path \u2014 avoid method call overhead entirely.
            // Sequential reads within the same 8KB block hit this path >99% of the time.
            final long fileOffset = absoluteBaseOffset + currentPos;
            final long blockOffset = fileOffset & ~CACHE_BLOCK_MASK;

            long baseAddr;
            int offset;
            if (blockOffset == currentBlockOffset && currentBlock != null) {
                // Hot path: same block, pure arithmetic \u2014 no method call, no cache lookup
                baseAddr = currentBaseAddress;
                offset = (int) (fileOffset - blockOffset);
            } else {
                // Cold path: block boundary crossed, do full cache lookup
                baseAddr = getBaseAddressWithOffset(currentPos);
                offset = lastOffsetInBlock;
            }

            final byte v = UNSAFE.getByte(baseAddr + offset);"""

# Replace with non-inlined version that still uses Unsafe via getBaseAddressWithOffset
new = """    @Override
    public final byte readByte() throws IOException {
        final long currentPos = curPosition;
        try {
            final long baseAddr = getBaseAddressWithOffset(currentPos);
            final byte v = UNSAFE.getByte(baseAddr + lastOffsetInBlock);"""

if old not in content:
    print("ERROR: Could not find manually-inlined readByte to replace!")
    sys.exit(1)

content = content.replace(old, new)
with open(filepath, 'w') as f:
    f.write(content)
print("OK: readByte un-inlined (still uses Unsafe via method call)")
