/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.benchmark;

/**
 * Cold path benchmark: data is NOT in block cache or page cache.
 * Measures read latency including I/O and (for BufferPool) decryption overhead.
 *
 * <p>Eviction strategy:
 * <ul>
 *   <li>BufferPool: {@code cache.invalidateAll()} clears the Caffeine block cache</li>
 *   <li>MMap: creates a fresh file per invocation to guarantee cold pages</li>
 * </ul>
 */
// ToDo Add Cold Path Benchmarks later
// @BenchmarkMode(Mode.AverageTime)
// @OutputTimeUnit(TimeUnit.MICROSECONDS)
// @Warmup(iterations = 2, time = 3)
// @Measurement(iterations = 5, time = 5)
// @Fork(value = 2, jvmArgsAppend = { "--enable-native-access=ALL-UNNAMED", "--enable-preview" })
// @State(Scope.Benchmark)
// @Threads(1)
// public class ColdPathBenchmark extends ReadBenchmarkBase {
//
// @Param({ "1", "4" })
// public int threadCount;
//
// private ExecutorService executor;
//
// // For MMap cold path: per-invocation fresh file tracking
// private final AtomicInteger mmapColdFileCounter = new AtomicInteger();
// private MMapDirectory coldMmapDir;
// private Path coldMmapPath;
//
// @Setup(Level.Trial)
// public void setupColdTrial() throws Exception {
// super.setupTrial();
// coldMmapPath = tempDir.resolve("mmap_cold");
// Files.createDirectories(coldMmapPath);
// coldMmapDir = new MMapDirectory(coldMmapPath);
// }
//
// @Setup(Level.Iteration)
// public void setupColdIteration() throws IOException {
//// super.openInput();
// executor = Executors.newFixedThreadPool(threadCount);
// }
//
// private void evictBufferPoolCache() {
// if (poolResources != null && poolResources.getBlockCache() instanceof CaffeineBlockCache<?, ?> caffeineCache) {
// caffeineCache.getCache().invalidateAll();
// }
// }
//
// /**
// * Creates a fresh MMap file that has never been read, guaranteeing cold pages.
// */
// private IndexInput openFreshMmapInput() throws IOException {
// String freshName = "cold_" + mmapColdFileCounter.getAndIncrement();
// try (IndexOutput out = coldMmapDir.createOutput(freshName, IOContext.DEFAULT)) {
// out.writeBytes(fileData, fileData.length);
// }
// return coldMmapDir.openInput(freshName, IOContext.DEFAULT);
// }
//
// @FunctionalInterface
// interface ColdReaderTask {
// void run(IndexInput input, Random rng, byte[] buf, Blackhole bh) throws IOException;
// }
//
// /**
// * Runs a cold read task with proper cache eviction.
// * For BufferPool: evicts cache, then runs concurrent readers on clones.
// * For MMap: each reader gets a fresh file (never cached).
// */
// private void runColdConcurrent(ColdReaderTask task, Blackhole bh) throws Exception {
// List<Future<?>> futures = new ArrayList<>(threadCount);
//
// if ("bufferpool".equals(directoryType)) {
// evictBufferPoolCache();
// for (int t = 0; t < threadCount; t++) {
// final int threadId = t;
// futures.add(executor.submit(() -> {
// IndexInput clone = input().clone();
// try {
// Random rng = new Random(BenchmarkConfig.RANGE_SEED + threadId);
// byte[] buf = new byte[CACHE_BLOCK_SIZE * 2];
// task.run(clone, rng, buf, bh);
// } catch (IOException e) {
// throw new RuntimeException(e);
// } finally {
// try {
// clone.close();
// } catch (IOException ignored) {}
// }
// }));
// }
// } else {
// for (int t = 0; t < threadCount; t++) {
// final int threadId = t;
// futures.add(executor.submit(() -> {
// try (IndexInput fresh = openFreshMmapInput()) {
// IndexInput clone = fresh.clone();
// try {
// Random rng = new Random(BenchmarkConfig.RANGE_SEED + threadId);
// byte[] buf = new byte[CACHE_BLOCK_SIZE * 2];
// task.run(clone, rng, buf, bh);
// } finally {
// clone.close();
// }
// } catch (IOException e) {
// throw new RuntimeException(e);
// }
// }));
// }
// }
//
// for (Future<?> f : futures) {
// f.get();
// }
// }
//
//
// @TearDown(Level.Iteration)
// public void tearDownColdIteration() throws IOException {
// super.closeInputs();
// if (executor != null) {
// executor.shutdownNow();
// executor = null;
// }
// }
//
// @TearDown(Level.Trial)
// public void tearDownCold() throws Exception {
// if (coldMmapDir != null)
// coldMmapDir.close();
// super.tearDownTrial();
// }
// }
