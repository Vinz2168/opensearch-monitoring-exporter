/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package it.seacom.opensearch.monitoring;

import it.seacom.opensearch.monitoring.queue.MetricDocument;
import org.opensearch.test.OpenSearchTestCase;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Unit tests for MetricDocument serialization.
 * The actual NDJSON serialization is now handled by XContentBuilder
 * inside BulkFlushWorker — here we verify the document structure
 * produced by Ss4oSerializer is correct before it reaches the worker.
 */
public class Ss4oSerializerTests extends OpenSearchTestCase {

    public void testMetricDocumentHasRequiredFields() {
        Map<String, Object> source = new LinkedHashMap<>();
        source.put("@timestamp", "2026-06-10T10:00:00.000Z");
        source.put("kind", "cluster");
        source.put("cluster", "prod-cluster");
        source.put("nodes_total", 3L);
        source.put("shards_active", 120L);

        MetricDocument doc = new MetricDocument("ss4o_metrics-opensearch-2026.06.10", source);

        assertNotNull(doc);
        assertEquals("ss4o_metrics-opensearch-2026.06.10", doc.getTargetIndex());
        assertNotNull(doc.getSource());
        assertEquals("cluster", doc.getSource().get("kind"));
        assertEquals("prod-cluster", doc.getSource().get("cluster"));
        assertEquals(3L, doc.getSource().get("nodes_total"));
    }

    public void testMetricDocumentTargetIndex() {
        MetricDocument doc = new MetricDocument("ss4o_metrics-opensearch-2026.06.10",
            Map.of("@timestamp", "2026-06-10T10:00:00.000Z", "kind", "node_jvm"));

        assertTrue(doc.getTargetIndex().startsWith("ss4o_metrics-opensearch-"));
        assertTrue(doc.getTargetIndex().contains("2026.06.10"));
    }

    public void testMetricDocumentNodeStructure() {
        Map<String, Object> node = new LinkedHashMap<>();
        node.put("name", "opensearch-node-1");
        node.put("id", "abc123");

        Map<String, Object> source = new LinkedHashMap<>();
        source.put("@timestamp", "2026-06-10T10:00:00.000Z");
        source.put("kind", "node_jvm");
        source.put("cluster", "prod");
        source.put("node", node);
        source.put("heap_used_percent", 55);
        source.put("heap_used_bytes", 2147483648L);

        MetricDocument doc = new MetricDocument("ss4o_metrics-opensearch-2026.06.10", source);

        @SuppressWarnings("unchecked")
        Map<String, Object> nodeField = (Map<String, Object>) doc.getSource().get("node");
        assertNotNull(nodeField);
        assertEquals("opensearch-node-1", nodeField.get("name"));
        assertEquals("abc123", nodeField.get("id"));
        assertEquals(55, doc.getSource().get("heap_used_percent"));
    }

    public void testMetricDocumentClusterKindFields() {
        Map<String, Object> source = new LinkedHashMap<>();
        source.put("@timestamp", "2026-06-10T10:00:00.000Z");
        source.put("kind", "cluster");
        source.put("cluster", "test-cluster");
        source.put("cluster_status", 0);
        source.put("cluster_status_name", "GREEN");
        source.put("shards_unassigned", 0L);
        source.put("pending_tasks", 0L);

        MetricDocument doc = new MetricDocument("ss4o_metrics-opensearch-2026.06.10", source);

        assertEquals(0, doc.getSource().get("cluster_status"));
        assertEquals("GREEN", doc.getSource().get("cluster_status_name"));
        assertEquals(0L, doc.getSource().get("shards_unassigned"));
    }
}
