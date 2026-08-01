/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package it.seacom.opensearch.monitoring.queue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.LongAdder;

/**
 * Coda in-memory bounded per i documenti ss4o da inviare al cluster di monitoring.
 *
 * Design:
 *  - ArrayBlockingQueue non-blocking in offer (drop se piena)
 *  - I documenti sono Map{String,Object} già pronti per IndexRequest
 *  - Il thread di flush chiama drain() per svuotare fino a batchSize elementi
 *  - Drop counter per monitorare la perdita di documenti
 */
public class MetricsDocumentQueue {

    private static final Logger log = LogManager.getLogger(MetricsDocumentQueue.class);

    private final ArrayBlockingQueue<MetricDocument> queue;
    private final LongAdder droppedCount = new LongAdder();
    private final LongAdder enqueuedCount = new LongAdder();

    public MetricsDocumentQueue(int capacity) {
        this.queue = new ArrayBlockingQueue<>(capacity);
    }

    /**
     * Aggiunge un documento alla coda.
     * Se la coda è piena il documento viene scartato (drop) senza bloccare.
     *
     * @return true se accodato, false se scartato
     */
    public boolean offer(MetricDocument doc) {
        boolean accepted = queue.offer(doc);
        if (accepted) {
            enqueuedCount.increment();
        } else {
            droppedCount.increment();
            if (droppedCount.sum() % 1000 == 0) {
                log.warn("[monitoring-exporter] Coda piena: {} documenti scartati finora. " +
                         "Considera di aumentare monitoring.exporter.queue.capacity " +
                         "o ridurre monitoring.exporter.flush.interval_seconds",
                         droppedCount.sum());
            }
        }
        return accepted;
    }

    /**
     * Svuota fino a {@code maxItems} elementi dalla coda nella lista fornita.
     *
     * @param maxItems dimensione massima del batch
     * @return lista di documenti estratti (può essere vuota)
     */
    public List<MetricDocument> drain(int maxItems) {
        List<MetricDocument> batch = new ArrayList<>(maxItems);
        queue.drainTo(batch, maxItems);
        return batch;
    }

    public int size() {
        return queue.size();
    }

    public long getDroppedCount() {
        return droppedCount.sum();
    }

    public long getEnqueuedCount() {
        return enqueuedCount.sum();
    }
}
