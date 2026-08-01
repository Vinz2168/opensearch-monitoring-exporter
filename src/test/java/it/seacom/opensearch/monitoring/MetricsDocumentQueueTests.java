/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package it.seacom.opensearch.monitoring;

import it.seacom.opensearch.monitoring.queue.MetricDocument;
import it.seacom.opensearch.monitoring.queue.MetricsDocumentQueue;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;

/**
 * Unit tests for {@link MetricsDocumentQueue}: bounded capacity, backpressure
 * (drop-on-full instead of blocking), partial drain, and counters.
 * No cluster required — pure in-memory data structure.
 */
public class MetricsDocumentQueueTests extends OpenSearchTestCase {

    private MetricDocument doc(String suffix) {
        return new MetricDocument("ss4o_metrics-opensearch-2026.06.10",
            Map.of("@timestamp", "2026-06-10T10:00:00.000Z", "kind", "test", "seq", suffix));
    }

    public void testOfferWithinCapacitySucceeds() {
        MetricsDocumentQueue queue = new MetricsDocumentQueue(10);

        assertTrue(queue.offer(doc("1")));
        assertTrue(queue.offer(doc("2")));

        assertEquals(2, queue.size());
        assertEquals(2L, queue.getEnqueuedCount());
        assertEquals(0L, queue.getDroppedCount());
    }

    public void testOfferBeyondCapacityDropsWithoutBlocking() {
        MetricsDocumentQueue queue = new MetricsDocumentQueue(2);

        assertTrue(queue.offer(doc("1")));
        assertTrue(queue.offer(doc("2")));
        // La coda è piena: offer() non deve bloccare, deve rifiutare e basta
        assertFalse(queue.offer(doc("3")));
        assertFalse(queue.offer(doc("4")));

        assertEquals(2, queue.size());
        assertEquals(2L, queue.getEnqueuedCount());
        assertEquals(2L, queue.getDroppedCount());
    }

    public void testDrainRemovesUpToMaxItemsInFifoOrder() {
        MetricsDocumentQueue queue = new MetricsDocumentQueue(10);
        queue.offer(doc("1"));
        queue.offer(doc("2"));
        queue.offer(doc("3"));

        List<MetricDocument> batch = queue.drain(2);

        assertEquals(2, batch.size());
        assertEquals("1", batch.get(0).getSource().get("seq"));
        assertEquals("2", batch.get(1).getSource().get("seq"));
        // Solo 1 documento deve restare in coda
        assertEquals(1, queue.size());
    }

    public void testDrainOnEmptyQueueReturnsEmptyList() {
        MetricsDocumentQueue queue = new MetricsDocumentQueue(10);

        List<MetricDocument> batch = queue.drain(5);

        assertNotNull(batch);
        assertTrue(batch.isEmpty());
    }

    public void testDrainRequestingMoreThanAvailableReturnsAllAvailable() {
        MetricsDocumentQueue queue = new MetricsDocumentQueue(10);
        queue.offer(doc("1"));
        queue.offer(doc("2"));

        List<MetricDocument> batch = queue.drain(100);

        assertEquals(2, batch.size());
        assertEquals(0, queue.size());
    }

    public void testQueueCanAcceptAgainAfterDrain() {
        MetricsDocumentQueue queue = new MetricsDocumentQueue(2);
        queue.offer(doc("1"));
        queue.offer(doc("2"));
        assertFalse(queue.offer(doc("3"))); // piena, scartato

        queue.drain(10); // svuota tutto

        // Ora deve tornare ad accettare
        assertTrue(queue.offer(doc("4")));
        assertEquals(1, queue.size());
    }
}
