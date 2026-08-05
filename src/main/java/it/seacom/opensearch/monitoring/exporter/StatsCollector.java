/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package it.seacom.opensearch.monitoring.exporter;

import it.seacom.opensearch.monitoring.config.MonitoringExporterSettings;
import it.seacom.opensearch.monitoring.queue.MetricDocument;
import it.seacom.opensearch.monitoring.queue.MetricsDocumentQueue;
import it.seacom.opensearch.monitoring.serializer.Ss4oSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.cluster.health.ClusterHealthRequest;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.action.admin.cluster.state.ClusterStateRequest;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.action.admin.indices.stats.IndicesStatsRequest;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.client.Requests;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class StatsCollector implements AutoCloseable {

    private static final Logger log = LogManager.getLogger(StatsCollector.class);

    private final Settings settings;
    private final Client client;
    private final ClusterService clusterService;
    private final MetricsDocumentQueue queue;
    private final Ss4oSerializer serializer;

    // Non final: aggiornabili a runtime via ClusterSettings.addSettingsUpdateConsumer
    // (vedi MonitoringExporterPlugin), cosi' i Property.Dynamic dichiarati in
    // MonitoringExporterSettings hanno effetto reale senza restart del nodo.
    private volatile boolean collectIndices;
    private volatile boolean collectClusterSettings;
    private volatile String nodesFilter;
    private volatile int collectIntervalSeconds;

    private final ScheduledExecutorService scheduler;
    private volatile ScheduledFuture<?> collectTask;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    // Contatore atomico per tracciare la backpressure (documenti rifiutati a causa della coda piena)
    private final AtomicLong droppedDocumentsCounter = new AtomicLong(0);

    public StatsCollector(Settings settings,
                          Client client,
                          ClusterService clusterService,
                          MetricsDocumentQueue queue) {
        this.settings               = settings;
        this.client                 = client;
        this.clusterService         = clusterService;
        this.queue                  = queue;
        this.serializer             = new Ss4oSerializer(settings);
        this.collectIndices         = MonitoringExporterSettings.COLLECT_INDICES.get(settings);
        this.collectClusterSettings = MonitoringExporterSettings.COLLECT_CLUSTER_SETTINGS.get(settings);
        this.nodesFilter            = MonitoringExporterSettings.NODES_FILTER.get(settings);

        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "monitoring-exporter-collector");
            t.setDaemon(true);
            return t;
        });
    }

    public void start() {
        this.collectIntervalSeconds = MonitoringExporterSettings.COLLECT_INTERVAL_SECONDS.get(settings);
        this.collectTask = scheduler.scheduleWithFixedDelay(
            this::collect, collectIntervalSeconds, collectIntervalSeconds, TimeUnit.SECONDS);
        log.info("[monitoring-exporter] StatsCollector avviato: raccolta ogni {}s", collectIntervalSeconds);
    }

    private void collect() {
        if (closed.get()) return;
        ClusterHealthRequest req = Requests.clusterHealthRequest().local(true);
        req.level(ClusterHealthRequest.Level.SHARDS);

        client.admin().cluster().health(req, new ActionListener<ClusterHealthResponse>() {
            @Override public void onResponse(ClusterHealthResponse r) { collectNodesStats(r); }
            @Override public void onFailure(Exception e) {
                log.warn("[monitoring-exporter] ClusterHealthRequest fallita: {}", e.getMessage());
            }
        });
    }

    private void collectNodesStats(ClusterHealthResponse health) {
        NodesStatsRequest req = Requests.nodesStatsRequest(nodesFilter).clear().all();
        client.admin().cluster().nodesStats(req, new ActionListener<NodesStatsResponse>() {
            @Override public void onResponse(NodesStatsResponse r) {
                if (collectIndices) collectIndicesStats(health, r);
                else collectClusterState(health, r, null);
            }
            @Override public void onFailure(Exception e) {
                log.warn("[monitoring-exporter] NodesStatsRequest fallita: {}", e.getMessage());
            }
        });
    }

    private void collectIndicesStats(ClusterHealthResponse health, NodesStatsResponse nodes) {
        if (!isClusterManagerNode()) {
            collectClusterState(health, nodes, null);
            return;
        }
        client.admin().indices().stats(new IndicesStatsRequest().all(),
            new ActionListener<IndicesStatsResponse>() {
                @Override public void onResponse(IndicesStatsResponse r) {
                    collectClusterState(health, nodes, r);
                }
                @Override public void onFailure(Exception e) {
                    log.warn("[monitoring-exporter] IndicesStatsRequest fallita: {}", e.getMessage());
                    collectClusterState(health, nodes, null);
                }
            });
    }

    private void collectClusterState(ClusterHealthResponse health,
                                     NodesStatsResponse nodes,
                                     IndicesStatsResponse indices) {
        if (!collectClusterSettings || !isClusterManagerNode()) {
            enqueue(health, nodes, indices, null);
            return;
        }
        client.admin().cluster().state(
            Requests.clusterStateRequest().clear().metadata(true).local(false),
            new ActionListener<ClusterStateResponse>() {
                @Override public void onResponse(ClusterStateResponse r) {
                    enqueue(health, nodes, indices, r);
                }
                @Override public void onFailure(Exception e) {
                    log.warn("[monitoring-exporter] ClusterStateRequest fallita: {}", e.getMessage());
                    enqueue(health, nodes, indices, null);
                }
            });
    }

    /**
     * Gestione attiva della backpressure.
     * Se la coda è piena, i documenti vengono rifiutati e viene emesso un alert nei log.
     */
    private void enqueue(ClusterHealthResponse health,
                         NodesStatsResponse nodes,
                         IndicesStatsResponse indices,
                         ClusterStateResponse state) {
        try {
            Instant now = Instant.now();
            List<MetricDocument> docs = serializer.serialize(now, health, nodes, indices, state);

            int enqueued = 0;
            int dropped = 0;

            for (MetricDocument doc : docs) {
                if (queue.offer(doc)) {
                    enqueued++;
                } else {
                    dropped++;
                    droppedDocumentsCounter.incrementAndGet();
                }
            }

            if (dropped > 0) {
                log.error("[monitoring-exporter] CODA PIENA: rifiutati {} documenti in questo ciclo. " +
                          "Totale persi dall'avvio: {}", dropped, droppedDocumentsCounter.get());
            }

            log.debug("[monitoring-exporter] Raccolta completata: {}/{} documenti accodati.",
                      enqueued, docs.size());

        } catch (Exception e) {
            log.error("[monitoring-exporter] Errore serializzazione: {}", e.getMessage(), e);
        }
    }

    private boolean isClusterManagerNode() {
        String localNodeId   = clusterService.localNode().getId();
        String managerNodeId = clusterService.state().nodes().getClusterManagerNodeId();
        return localNodeId != null && localNodeId.equals(managerNodeId);
    }

    /** Esposto per telemetria/health check. */
    public long getDroppedDocumentsCount() {
        return droppedDocumentsCounter.get();
    }

    // -------------------------------------------------------------------------
    // Setter per settaggi dinamici (Property.Dynamic) — chiamati dai consumer
    // registrati in MonitoringExporterPlugin quando l'operatore cambia il
    // valore via PUT _cluster/settings, senza richiedere un restart del nodo.
    // -------------------------------------------------------------------------

    public void setCollectIndices(boolean collectIndices) {
        this.collectIndices = collectIndices;
    }

    public void setCollectClusterSettings(boolean collectClusterSettings) {
        this.collectClusterSettings = collectClusterSettings;
    }

    public void setNodesFilter(String nodesFilter) {
        this.nodesFilter = nodesFilter;
    }

    public void setTargetIndexPattern(String indexPattern) {
        this.serializer.setIndexPattern(indexPattern);
    }

    public synchronized void setCollectIntervalSeconds(int seconds) {
        if (seconds == this.collectIntervalSeconds || closed.get()) {
            this.collectIntervalSeconds = seconds;
            return;
        }
        this.collectIntervalSeconds = seconds;
        if (collectTask != null) {
            collectTask.cancel(false);
        }
        collectTask = scheduler.scheduleWithFixedDelay(this::collect, seconds, seconds, TimeUnit.SECONDS);
        log.info("[monitoring-exporter] StatsCollector: intervallo di raccolta aggiornato a {}s", seconds);
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }
}
