/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.harness;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.opensearch.index.store.block_cache_v2.DirectMemoryAllocator;
import org.opensearch.index.store.block_cache_v2.DirectMemoryAllocator.DiagnosticSnapshot;

/**
 * Collects {@link DiagnosticSnapshot}s at 1-second intervals and writes them
 * to a CSV file. Also appends JVM-level metrics (ZGC cycles, CPU usage).
 */
public final class MetricsCollector implements AutoCloseable {

    private static final String HEADER =
        "timestamp_ms,allocs_per_sec,bytes_alloc_per_sec,stalls_per_sec,"
        + "total_allocs,total_bytes_allocated,allocated_by_us,reclaimed_by_gc,"
        + "pressure_level,gc_debt_ema,deficit_ema,cleaner_lag_approx,"
        + "target_headroom,last_headroom,"
        + "stall_count,stall_nanos_total,"
        + "gc_hint_count,gc_hint_skipped,"
        + "shrink_count,shrink_blocks_total,restore_count,restore_blocks_total,blocks_evicted,"
        + "tier1_count,tier2_count,tier3_count,"
        + "native_used,external_usage,external_usage_ema,"
        + "consecutive_stall_windows,lyapunov_violations,last_action,"
        + "zgc_cycle_count,zgc_pause_ms,cpu_pct";

    private final DirectMemoryAllocator allocator;
    private final BufferedWriter writer;
    private final ScheduledExecutorService scheduler;
    private final GarbageCollectorMXBean zgcBean;
    private final OperatingSystemMXBean osBean;
    private volatile DiagnosticSnapshot prev;

    public MetricsCollector(DirectMemoryAllocator allocator, Path outputFile) {
        this.allocator = allocator;
        this.osBean = ManagementFactory.getOperatingSystemMXBean();
        this.zgcBean = ManagementFactory.getGarbageCollectorMXBeans().stream()
            .filter(b -> b.getName().contains("ZGC"))
            .findFirst().orElse(null);
        try {
            Files.createDirectories(outputFile.getParent());
            this.writer = Files.newBufferedWriter(outputFile);
            writer.write(HEADER);
            writer.newLine();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "metrics-collector");
            t.setDaemon(true);
            return t;
        });
    }

    public void start() {
        scheduler.scheduleAtFixedRate(this::sample, 1, 1, TimeUnit.SECONDS);
    }

    private void sample() {
        try {
            DiagnosticSnapshot s = allocator.snapshot();
            long zgcCount = zgcBean != null ? zgcBean.getCollectionCount() : -1;
            long zgcMs = zgcBean != null ? zgcBean.getCollectionTime() : -1;
            double cpu = osBean.getSystemLoadAverage();

            // Delta rates (per second)
            long allocsPerSec = prev != null ? s.totalAllocations() - prev.totalAllocations() : 0;
            long bytesPerSec = prev != null ? s.totalBytesAllocated() - prev.totalBytesAllocated() : 0;
            long stallsPerSec = prev != null ? s.stallCount() - prev.stallCount() : 0;
            prev = s;

            writer.write(String.join(",",
                Long.toString(s.timestampMs()),
                Long.toString(allocsPerSec), Long.toString(bytesPerSec), Long.toString(stallsPerSec),
                Long.toString(s.totalAllocations()), Long.toString(s.totalBytesAllocated()),
                Long.toString(s.allocatedByUs()), Long.toString(s.reclaimedByGc()),
                String.format("%.4f", s.pressureLevel()),
                String.format("%.0f", s.gcDebtEma()), String.format("%.0f", s.deficitEma()),
                Long.toString(s.cleanerLagApprox()),
                Long.toString(s.targetHeadroomBytes()), Long.toString(s.lastHeadroomBytes()),
                Long.toString(s.stallCount()), Long.toString(s.stallNanosTotal()),
                Long.toString(s.gcHintCount()), Long.toString(s.gcHintSkippedCooldown()),
                Long.toString(s.cacheShrinkCount()), Long.toString(s.cacheShrinkBlocksTotal()),
                Long.toString(s.cacheRestoreCount()), Long.toString(s.cacheRestoreBlocksTotal()),
                Long.toString(s.totalBlocksEvicted()),
                Long.toString(s.tier1Count()), Long.toString(s.tier2Count()), Long.toString(s.tier3Count()),
                Long.toString(s.lastNativeUsedBytes()), Long.toString(s.lastExternalUsage()),
                String.format("%.0f", s.externalUsageEma()),
                Integer.toString(s.consecutiveStallWindows()), Long.toString(s.lyapunovViolationCount()),
                s.lastAction().name(),
                Long.toString(zgcCount), Long.toString(zgcMs),
                String.format("%.2f", cpu)
            ));
            writer.newLine();
            writer.flush();
        } catch (IOException e) {
            System.err.println("MetricsCollector write failed: " + e.getMessage());
        }
    }

    @Override
    public void close() {
        scheduler.shutdown();
        try {
            scheduler.awaitTermination(5, TimeUnit.SECONDS);
            writer.close();
        } catch (InterruptedException | IOException e) {
            Thread.currentThread().interrupt();
        }
    }
}
