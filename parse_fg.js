// Parse async-profiler 4.x flame graph HTML using native JS execution
const fs = require('fs');
const path = require('path');

function parseFlameGraph(htmlPath) {
  const content = fs.readFileSync(htmlPath, 'utf8');
  
  // Extract the cpool array source
  const cpoolMatch = content.match(/const\s+cpool\s*=\s*\[([\s\S]*?)\];\s*unpack\(cpool\)/);
  if (!cpoolMatch) { console.error('No cpool found in', htmlPath); return null; }
  
  // Eval the cpool array
  const cpool = eval('[' + cpoolMatch[1] + ']');
  
  // Unpack it (same algorithm as in the HTML)
  for (let i = 1; i < cpool.length; i++) {
    cpool[i] = cpool[i - 1].substring(0, cpool[i].charCodeAt(0) - 32) + cpool[i].substring(1);
  }

  // Extract frame data after unpack(cpool)
  const dataMatch = content.match(/unpack\(cpool\)\s*;?\s*\n([\s\S]*?)(?:<\/script>|$)/);
  if (!dataMatch) { console.error('No frame data found'); return null; }
  
  // Build a tree to compute self samples
  // f(key, level, left, width) - width = total samples for this frame
  // u(key, width) - child at level+1
  // n(key, width) - sibling at same level
  
  // We'll track: for each frame, its total width (samples)
  // and accumulate "total" samples per method name (inclusive)
  // and "self" samples per method name
  
  const totalByName = {};  // inclusive samples
  const selfByName = {};   // self (leaf) samples
  let grandTotal = 0;
  
  // Simulate the frame building to compute self samples
  // Stack tracks [nameIdx, width, childrenWidth] at each level
  const stack = []; // stack[level] = {name, width, childrenWidth}
  let level0 = 0;
  let width0 = 0;
  let left0 = 0;
  
  function flushAbove(targetLevel) {
    // Any frame above targetLevel that hasn't been flushed gets its self computed
    while (stack.length > targetLevel + 1) {
      const frame = stack.pop();
      if (frame) {
        const selfSamples = frame.width - frame.childrenWidth;
        if (selfSamples > 0) {
          selfByName[frame.name] = (selfByName[frame.name] || 0) + selfSamples;
        }
      }
    }
  }
  
  function addFrame(key, level, width) {
    const name = cpool[key >>> 3] || `<unknown:${key>>>3}>`;
    
    // Flush any frames deeper than this level
    flushAbove(level);
    
    // If there's already a frame at this level, flush it
    if (stack[level]) {
      const prev = stack[level];
      const selfSamples = prev.width - prev.childrenWidth;
      if (selfSamples > 0) {
        selfByName[prev.name] = (selfByName[prev.name] || 0) + selfSamples;
      }
    }
    
    // Add width to parent's childrenWidth
    if (level > 0 && stack[level - 1]) {
      stack[level - 1].childrenWidth += width;
    }
    
    // Record total (inclusive) samples
    totalByName[name] = (totalByName[name] || 0) + width;
    
    // Set this frame
    stack[level] = { name, width, childrenWidth: 0 };
    // Trim stack
    stack.length = level + 1;
    
    return level;
  }
  
  // Parse and execute the frame calls
  const frameData = dataMatch[1];
  const callRegex = /([nfu])\(([^)]*)\)/g;
  let match;
  
  while ((match = callRegex.exec(frameData)) !== null) {
    const func = match[1];
    const args = match[2].split(',').map(s => parseInt(s.trim()) || 0);
    
    if (func === 'f') {
      // f(key, level, left, width, inln, c1, int)
      const key = args[0], level = args[1], left = args[2], width = args[3] || width0;
      level0 = addFrame(key, level, width);
      width0 = width;
      left0 += left;
    } else if (func === 'u') {
      // u(key, width, inln, c1, int) - child at level0+1
      const key = args[0], width = args[1] || width0;
      level0 = addFrame(key, level0 + 1, width);
      width0 = width;
    } else if (func === 'n') {
      // n(key, width, inln, c1, int) - sibling at same level
      const key = args[0], width = args[1] || width0;
      level0 = addFrame(key, level0, width);
      width0 = width;
      grandTotal += width; // root-level frames contribute to grand total
    }
  }
  
  // Flush remaining stack
  flushAbove(-1);
  
  return { cpool, selfByName, totalByName, grandTotal };
}

// Main
const base = process.argv[2] || 'build/async-profiler-results';
const dirs = fs.readdirSync(base);
const bpDir = dirs.find(d => d.includes('bufferpool'));
const mmapDir = dirs.find(d => d.includes('mmap'));

function printTop(label, data, n) {
  console.log('='.repeat(90));
  console.log(label + ' - Top methods by SELF CPU samples');
  console.log('='.repeat(90));
  console.log(`Grand total root samples: ${data.grandTotal}`);
  const sorted = Object.entries(data.selfByName).sort((a,b) => b[1]-a[1]);
  for (let i = 0; i < Math.min(n, sorted.length); i++) {
    const [name, count] = sorted[i];
    const pct = (count / data.grandTotal * 100).toFixed(1);
    console.log(`  ${String(count).padStart(6)} (${pct.padStart(5)}%)  ${name}`);
  }
}

if (bpDir) {
  const bp = parseFlameGraph(path.join(base, bpDir, 'flame-cpu-forward.html'));
  if (bp) printTop('BUFFERPOOL (V2)', bp, 30);
}
console.log();
if (mmapDir) {
  const mm = parseFlameGraph(path.join(base, mmapDir, 'flame-cpu-forward.html'));
  if (mm) printTop('MMAP', mm, 30);
}

// Side-by-side comparison
console.log();
console.log('='.repeat(130));
console.log('SIDE-BY-SIDE: Key hot-path methods (SELF CPU samples)');
console.log('='.repeat(130));

const keywords = [
  'readByte', 'getCacheBlockWithOffset', 'MemorySegment.get',
  'checkAccess', 'checkBounds', 'checkValidStateRaw', 'unsafeGetOffset',
  'getByte', 'getByteInternal', 'sessionImpl',
  'tryPin', 'decRef', 'getGeneration', 'getVolatile', 'setRelease',
  'acquireRefCountedValue', 'CaffeineBlockCache',
  'clone', 'buildSlice', 'seek', 'ensureOpen',
  'sequentialRea