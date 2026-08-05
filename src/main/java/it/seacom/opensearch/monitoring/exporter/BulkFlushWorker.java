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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.settings.Settings;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Worker schedulato che drena la MetricsDocumentQueue e invia BulkRequest
 * al cluster di monitoring via Bulk API HTTP usando java.net.http.HttpClient
 * (JDK 11+) — zero dipendenze esterne.
 */
public class BulkFlushWorker implements AutoCloseable {

    private static final Logger log = LogManager.getLogger(BulkFlushWorker.class);

    private final MetricsDocumentQueue queue;
    // Non final: aggiornabili a runtime via ClusterSettings.addSettingsUpdateConsumer
    // (vedi MonitoringExporterPlugin), cosi' i Property.Dynamic dichiarati in
    // MonitoringExporterSettings hanno effetto reale senza restart del nodo.
    private volatile HttpClient httpClient;
    private volatile String bulkEndpoint;
    private volatile String authHeader;
    private volatile int batchSize;
    private volatile int maxRetries;
    private volatile int flushIntervalSeconds;
    private final ScheduledExecutorService scheduler;
    private volatile ScheduledFuture<?> flushTask;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public BulkFlushWorker(Settings settings,
                            MetricsDocumentQueue queue,
                            HttpClient httpClient,
                            String bulkEndpoint,
                            String authHeader) {
        this.queue        = queue;
        this.httpClient   = httpClient;
        this.bulkEndpoint = bulkEndpoint;
        this.authHeader   = authHeader;
        this.batchSize    = MonitoringExporterSettings.BATCH_SIZE.get(settings);
        this.maxRetries   = MonitoringExporterSettings.MAX_RETRIES.get(settings);
        this.flushIntervalSeconds = MonitoringExporterSettings.FLUSH_INTERVAL_SECONDS.get(settings);

        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "monitoring-exporter-flush");
            t.setDaemon(true);
            return t;
        });

        this.flushTask = scheduler.scheduleWithFixedDelay(
            this::flush, flushIntervalSeconds, flushIntervalSeconds, TimeUnit.SECONDS);
        log.info("[monitoring-exporter] BulkFlushWorker avviato: flush ogni {}s, batchSize={}",
                 flushIntervalSeconds, batchSize);
    }

    public void flush() {
        if (closed.get()) return;
        List<MetricDocument> batch = queue.drain(batchSize);
        if (batch.isEmpty()) return;

        log.debug("[monitoring-exporter] Flush {} documenti", batch.size());
        String body = buildNdjson(batch);
        sendWithRetry(body, batch.size(), 0);
    }

    /**
     * Costruisce il corpo NDJSON per la Bulk API di OpenSearch.
     * Ogni documento genera due righe:
     *   1. action: { "index": { "_index": "..." } }
     *   2. source: { ...campi ss4o... }
     */
    private String buildNdjson(List<MetricDocument> batch) {
        StringBuilder sb = new StringBuilder();
        for (MetricDocument doc : batch) {
            sb.append("{\"index\":{\"_index\":\"")
              .append(doc.getTargetIndex())
              .append("\"}}\n");
            sb.append(toJson(doc.getSource()))
              .append('\n');
        }
        return sb.toString();
    }

    /**
     * Serializzazione JSON minimale di Map{String,Object}.
     * Gestisce: String, Number, Boolean, null, Map nested, List.
     * Non richiede librerie esterne.
     */
    @SuppressWarnings("unchecked")
    public static String toJson(Object value) {
        if (value == null) return "null";
        if (value instanceof Boolean || value instanceof Number) return value.toString();
        if (value instanceof String) {
            return "\"" + escapeJson((String) value) + "\"";
        }
        if (value instanceof Map) {
            StringBuilder sb = new StringBuilder("{");
            boolean first = true;
            for (Map.Entry<?, ?> e : ((Map<?, ?>) value).entrySet()) {
                if (!first) sb.append(',');
                sb.append('"').append(escapeJson(e.getKey().toString())).append("\":")
                  .append(toJson(e.getValue()));
                first = false;
            }
            return sb.append('}').toString();
        }
        if (value instanceof List) {
            StringBuilder sb = new StringBuilder("[");
            boolean first = true;
            for (Object item : (List<?>) value) {
                if (!first) sb.append(',');
                sb.append(toJson(item));
                first = false;
            }
            return sb.append(']').toString();
        }
        return "\"" + escapeJson(value.toString()) + "\"";
    }

    private static String escapeJson(String s) {
        StringBuilder sb = new StringBuilder();
        for (char c : s.toCharArray()) {
            switch (c) {
                case '\\': sb.append("\\\\"); break;
                case '"':  sb.append("\\\""); break;
                case '\n': sb.append("\\n");  break;
                case '\r': sb.append("\\r");  break;
                case '\t': sb.append("\\t");  break;
                default:   sb.append(c);
            }
        }
        return sb.toString();
    }

    private void sendWithRetry(String body, int docCount, int attempt) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(bulkEndpoint))
                .header("Content-Type", "application/x-ndjson")
                .header("Authorization", authHeader)
                .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
                .timeout(Duration.ofSeconds(30))
                .build();

            HttpResponse<String> response = httpClient.send(
                request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() >= 200 && response.statusCode() < 300) {
                if (response.body().contains("\"errors\":true")) {
                    log.warn("[monitoring-exporter] BulkResponse con errori parziali: {}",
                             response.body().substring(0, Math.min(500, response.body().length())));
                } else {
                    log.debug("[monitoring-exporter] {} doc inviati OK (HTTP {})",
                              docCount, response.statusCode());
                }
            } else {
                throw new RuntimeException("HTTP " + response.statusCode()
                                           + ": " + response.body());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            if (attempt < maxRetries) {
                long backoffMs = 1000L * (attempt + 1);
                log.warn("[monitoring-exporter] BulkRequest fallita ({}/{}), retry tra {}ms: {}",
                         attempt + 1, maxRetries, backoffMs, e.getMessage());
                try {
                    Thread.sleep(backoffMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
                sendWithRetry(body, docCount, attempt + 1);
            } else {
                log.error("[monitoring-exporter] BulkRequest fallita dopo {} tentativi, "
                          + "{} doc persi: {}", maxRetries, docCount, e.getMessage(), e);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Setter per settaggi dinamici (Property.Dynamic) — chiamati dai consumer
    // registrati in MonitoringExporterPlugin quando l'operatore cambia il
    // valore via PUT _cluster/settings, senza richiedere un restart del nodo.
    // -------------------------------------------------------------------------

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }

    public synchronized void setFlushIntervalSeconds(int seconds) {
        if (seconds == this.flushIntervalSeconds || closed.get()) {
            this.flushIntervalSeconds = seconds;
            return;
        }
        this.flushIntervalSeconds = seconds;
        if (flushTask != null) {
            flushTask.cancel(false);
        }
        flushTask = scheduler.scheduleWithFixedDelay(this::flush, seconds, seconds, TimeUnit.SECONDS);
        log.info("[monitoring-exporter] BulkFlushWorker: intervallo di flush aggiornato a {}s", seconds);
    }

    /**
     * Sostituisce atomicamente client HTTP, endpoint bulk e header di
     * autenticazione — usato quando cambiano target.hosts, target.username o
     * i settaggi TLS, che richiedono di ricostruire l'HttpClient (SSLContext
     * diverso) e/o l'header Authorization.
     */
    public synchronized void reconfigureEndpoint(HttpClient httpClient, String bulkEndpoint, String authHeader) {
        this.httpClient   = httpClient;
        this.bulkEndpoint = bulkEndpoint;
        this.authHeader   = authHeader;
        log.info("[monitoring-exporter] BulkFlushWorker: endpoint aggiornato -> {}", bulkEndpoint);
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            scheduler.shutdown();
            try {
                scheduler.awaitTermination(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
            flush();
        }
    }
}
