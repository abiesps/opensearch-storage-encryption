/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.rest;

import static java.util.Collections.singletonList;
import static org.opensearch.rest.RestRequest.Method.GET;

import java.io.IOException;
import java.util.List;

import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.store.CryptoDirectoryFactory;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.block_cache.CaffeineBlockCache;
import org.opensearch.index.store.bufferpoolfs.PrefetchEffectivenessTracker;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.transport.client.node.NodeClient;

/**
 * REST action to get buffer pool cache stats, prefetch stats, and prefetch effectiveness tracking.
 *
 * Route: GET /_plugins/_opensearch_storage_encryption/_stats
 *
 * Optional query param: ?reset=true to reset effectiveness tracking counters.
 */
public class RestBufferPoolStatsAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "bufferpool_stats_action";
    }

    @Override
    public List<Route> routes() {
        return singletonList(new Route(GET, "/_plugins/_opensearch_storage_encryption/_stats"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        boolean resetTracking = request.paramAsBoolean("reset", false);

        BlockCache<?> cache = CryptoDirectoryFactory.getSharedBlockCache();

        XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint().lfAtEnd();
        builder.startObject();

        // Cache stats
        if (cache != null) {
            builder.startObject("cache");
            builder.field("size", cache.getCacheSize());
            builder.field("stats", cache.cacheStats());
            if (cache instanceof CaffeineBlockCache<?, ?> caffeineCache) {
                builder.field("prefetch_stats", caffeineCache.prefetchStats());
                builder.field("hit_rate", caffeineCache.getHitRate());
            }
            builder.endObject();
        } else {
            builder.startObject("cache");
            builder.field("status", "not_initialized");
            builder.endObject();
        }

        // Prefetch effectiveness tracking
        builder.startObject("prefetch_effectiveness");
        builder.field("tracking_enabled", CryptoDirectoryFactory.isPrefetchTrackingEnabled());

        if (CryptoDirectoryFactory.isPrefetchTrackingEnabled()) {
            PrefetchEffectivenessTracker tracker = PrefetchEffectivenessTracker.getInstance();
            builder.field("total_prefetches", tracker.getTotalPrefetches());
            builder.field("effective_prefetches", tracker.getEffectivePrefetches());
            builder.field("wasted_prefetches", tracker.getWastedPrefetches());
            long total = tracker.getTotalPrefetches();
            builder.field("effective_rate_pct", total > 0 ? (tracker.getEffectivePrefetches() * 100.0 / total) : 0);
            builder.field("total_reads", tracker.getTotalReads());
            builder.field("cold_reads", tracker.getColdReads());
            builder.field("pending", tracker.getPendingCount());
            builder.field("avg_prefetch_to_read_latency_us", tracker.getAvgLatencyMicros());
            builder.field("summary", tracker.stats());
            builder.field("sample_keys", tracker.sampleKeys());

            if (resetTracking) {
                tracker.reset();
                builder.field("reset", true);
            }
        }
        builder.endObject();

        // Settings
        builder.startObject("settings");
        builder.field("encryption_enabled", false); // from CryptoDirectoryFactory
        builder.field("readahead_enabled", CryptoDirectoryFactory.isReadaheadEnabled());
        builder.field("lucene_prefetch_enabled", CryptoDirectoryFactory.isPrefetchEnabled());
        builder.field("prefetch_tracking_enabled", CryptoDirectoryFactory.isPrefetchTrackingEnabled());
        builder.field("prefetch_batch_size", CryptoDirectoryFactory.getPrefetchBatchSize());
        builder.endObject();

        builder.endObject();

        return channel -> { channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder)); };
    }
}
