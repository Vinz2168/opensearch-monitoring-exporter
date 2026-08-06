/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package it.seacom.opensearch.monitoring.plugin;

import it.seacom.opensearch.monitoring.config.MonitoringExporterSettings;
import it.seacom.opensearch.monitoring.exporter.BulkFlushWorker;
import it.seacom.opensearch.monitoring.exporter.StatsCollector;
import it.seacom.opensearch.monitoring.queue.MetricsDocumentQueue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.settings.SecureString;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import javax.net.ssl.*;
import java.io.InputStream;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Plugin OpenSearch che esporta le metriche del cluster (ClusterHealth,
 * NodesStats, IndicesStats, ClusterState) verso un cluster di monitoring
 * dedicato, usando la Bulk API HTTP.
 *
 * Usa esclusivamente:
 *  - API interne di OpenSearch (Admin Client, ClusterService) per la raccolta
 *  - java.net.http.HttpClient (JDK 11+) per l'invio HTTP
 * Zero dipendenze esterne — nessun jar aggiuntivo nel plugin zip.
 */
public class MonitoringExporterPlugin extends Plugin {

    private static final Logger log = LogManager.getLogger(MonitoringExporterPlugin.class);

    private final Settings settings;
    private StatsCollector statsCollector;
    private BulkFlushWorker flushWorker;
    private HttpClient httpClient;
    private ClusterService clusterService;

    public MonitoringExporterPlugin(Settings settings) {
        this.settings = settings;
    }

    @Override
    public Collection<Object> createComponents(
            Client client,
            ClusterService clusterService,
            ThreadPool threadPool,
            ResourceWatcherService resourceWatcherService,
            ScriptService scriptService,
            NamedXContentRegistry xContentRegistry,
            Environment environment,
            NodeEnvironment nodeEnvironment,
            NamedWriteableRegistry namedWriteableRegistry,
            IndexNameExpressionResolver indexNameExpressionResolver,
            Supplier<RepositoriesService> repositoriesServiceSupplier) {

        this.clusterService = clusterService;

        try {
            List<String> hosts = MonitoringExporterSettings.TARGET_HOSTS.get(settings);
            log.info("[monitoring-exporter] Inizializzazione. Target: {}", hosts);

            this.httpClient = buildHttpClient(environment);

            // Passa tutti gli host come stringa CSV — BulkFlushWorker fa il parse
            // e applica round-robin + quarantena per ogni endpoint
            String bulkEndpoints = hosts.stream()
                .map(h -> h + "/_bulk")
                .collect(Collectors.joining(","));

            String username = MonitoringExporterSettings.TARGET_USERNAME.get(settings);
            final String password;
            try (SecureString pwd = MonitoringExporterSettings.TARGET_PASSWORD.get(settings)) {
                password = pwd != null ? pwd.toString() : "";
            }
            String authHeader = "Basic " + Base64.getEncoder().encodeToString(
                (username + ":" + password).getBytes(StandardCharsets.UTF_8));

            MetricsDocumentQueue queue = new MetricsDocumentQueue(
                MonitoringExporterSettings.QUEUE_CAPACITY.get(settings));

            this.flushWorker = new BulkFlushWorker(
                settings, queue, httpClient, bulkEndpoints, authHeader);

            this.statsCollector = new StatsCollector(settings, client, clusterService, queue);
            this.statsCollector.start();

            registerDynamicSettingsConsumers(clusterService, environment);

            log.info("[monitoring-exporter] Plugin avviato. endpoints={}", bulkEndpoints);
        } catch (Exception e) {
            log.error("[monitoring-exporter] Avvio fallito: {}", e.getMessage(), e);
        }

        return List.of();
    }

    // -------------------------------------------------------------------------
    // Settaggi dinamici (Property.Dynamic) — senza questi consumer, un
    // PUT _cluster/settings viene accettato (acknowledged: true) ma non ha
    // alcun effetto: i valori venivano letti una sola volta, allo startup.
    // -------------------------------------------------------------------------

    private void registerDynamicSettingsConsumers(ClusterService clusterService, Environment environment) {
        var clusterSettings = clusterService.getClusterSettings();

        // Settaggi che il BulkFlushWorker puo' applicare da solo, senza toccare
        // l'HttpClient/endpoint.
        clusterSettings.addSettingsUpdateConsumer(
            MonitoringExporterSettings.BATCH_SIZE, flushWorker::setBatchSize);
        clusterSettings.addSettingsUpdateConsumer(
            MonitoringExporterSettings.MAX_RETRIES, flushWorker::setMaxRetries);
        clusterSettings.addSettingsUpdateConsumer(
            MonitoringExporterSettings.FLUSH_INTERVAL_SECONDS, flushWorker::setFlushIntervalSeconds);

        // Settaggi che lo StatsCollector puo' applicare da solo.
        clusterSettings.addSettingsUpdateConsumer(
            MonitoringExporterSettings.COLLECT_INTERVAL_SECONDS, statsCollector::setCollectIntervalSeconds);
        clusterSettings.addSettingsUpdateConsumer(
            MonitoringExporterSettings.COLLECT_INDICES, statsCollector::setCollectIndices);
        clusterSettings.addSettingsUpdateConsumer(
            MonitoringExporterSettings.COLLECT_CLUSTER_SETTINGS, statsCollector::setCollectClusterSettings);
        clusterSettings.addSettingsUpdateConsumer(
            MonitoringExporterSettings.NODES_FILTER, statsCollector::setNodesFilter);
        clusterSettings.addSettingsUpdateConsumer(
            MonitoringExporterSettings.TARGET_INDEX, statsCollector::setTargetIndexPattern);

        // Settaggi che richiedono di ricostruire HttpClient/endpoint/header
        // insieme (target.hosts, target.username, TLS) -- il nuovo valore del
        // singolo setting viene ignorato qui: rebuildHttpConfig rilegge lo
        // stato corrente di tutti e quattro da ClusterSettings, cosi' restano
        // sempre coerenti tra loro anche se cambiano in chiamate separate.
        clusterSettings.addSettingsUpdateConsumer(
            MonitoringExporterSettings.TARGET_HOSTS, v -> rebuildHttpConfig(environment));
        clusterSettings.addSettingsUpdateConsumer(
            MonitoringExporterSettings.TARGET_USERNAME, v -> rebuildHttpConfig(environment));
        clusterSettings.addSettingsUpdateConsumer(
            MonitoringExporterSettings.TLS_VERIFY_HOSTNAME, v -> rebuildHttpConfig(environment));
        clusterSettings.addSettingsUpdateConsumer(
            MonitoringExporterSettings.TLS_TRUSTSTORE_PATH, v -> rebuildHttpConfig(environment));
    }

    private synchronized void rebuildHttpConfig(Environment environment) {
        try {
            var clusterSettings = clusterService.getClusterSettings();
            List<String> hosts = clusterSettings.get(MonitoringExporterSettings.TARGET_HOSTS);
            String username = clusterSettings.get(MonitoringExporterSettings.TARGET_USERNAME);

            HttpClient newClient = buildHttpClient(environment);
            String bulkEndpoints = hosts.stream()
                .map(h -> h + "/_bulk")
                .collect(Collectors.joining(","));

            final String password;
            try (SecureString pwd = MonitoringExporterSettings.TARGET_PASSWORD.get(settings)) {
                password = pwd != null ? pwd.toString() : "";
            }
            String authHeader = "Basic " + Base64.getEncoder().encodeToString(
                (username + ":" + password).getBytes(StandardCharsets.UTF_8));

            HttpClient oldClient = this.httpClient;
            this.httpClient = newClient;
            flushWorker.reconfigureEndpoint(newClient, bulkEndpoints, authHeader);
            if (oldClient != null) {
                try { oldClient.close(); } catch (Exception ignored) { /* best effort */ }
            }

            log.info("[monitoring-exporter] Configurazione HTTP ricaricata dinamicamente. endpoints={}",
                      bulkEndpoints);
        } catch (Exception e) {
            log.error("[monitoring-exporter] Impossibile ricaricare la configurazione HTTP: {}",
                       e.getMessage(), e);
        }
    }

    @Override
    public List<Setting<?>> getSettings() {
        return MonitoringExporterSettings.all();
    }

    @Override
    public void close() {
        if (statsCollector != null) statsCollector.close();
        if (flushWorker != null)    flushWorker.close();
        if (httpClient != null) {
            try { httpClient.close(); } catch (Exception e) { /* ignore */ }
        }
    }

    // -------------------------------------------------------------------------
    // HttpClient con SSL configurabile
    // -------------------------------------------------------------------------

    private HttpClient buildHttpClient(Environment environment) throws Exception {
        SSLContext sslContext = buildSslContext(environment);
        return HttpClient.newBuilder()
            .sslContext(sslContext)
            .version(HttpClient.Version.HTTP_1_1)
            .build();
    }

    private SSLContext buildSslContext(Environment environment) throws Exception {
        // Legge da ClusterSettings, non dall'oggetto Settings immutabile fissato
        // all'avvio del nodo: TLS_VERIFY_HOSTNAME e TLS_TRUSTSTORE_PATH sono
        // Property.Dynamic, quindi il valore corrente (post PUT _cluster/settings)
        // vive li', non in `settings`. Al primo avvio (clusterService == null,
        // chiamata da createComponents prima che il campo sia assegnato) i due
        // insiemi coincidono comunque con i valori iniziali.
        boolean verify = clusterService != null
            ? clusterService.getClusterSettings().get(MonitoringExporterSettings.TLS_VERIFY_HOSTNAME)
            : MonitoringExporterSettings.TLS_VERIFY_HOSTNAME.get(settings);
        if (!verify) return buildTrustAllContext();

        String truststorePath = clusterService != null
            ? clusterService.getClusterSettings().get(MonitoringExporterSettings.TLS_TRUSTSTORE_PATH)
            : MonitoringExporterSettings.TLS_TRUSTSTORE_PATH.get(settings);
        if (truststorePath == null || truststorePath.isBlank()) {
            return SSLContext.getDefault();
        }

        KeyStore ts = KeyStore.getInstance("JKS");
        try (SecureString pwd = MonitoringExporterSettings.TLS_TRUSTSTORE_PASSWORD.get(settings);
             InputStream fis = Files.newInputStream(
                 environment.configDir().resolve(truststorePath))) {
            ts.load(fis, pwd != null ? pwd.toString().toCharArray() : new char[0]);
        }
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(
            TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(ts);

        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(null, tmf.getTrustManagers(), new SecureRandom());
        return ctx;
    }

    private SSLContext buildTrustAllContext() throws Exception {
        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(null, new TrustManager[]{new X509TrustManager() {
            public X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }
            public void checkClientTrusted(X509Certificate[] c, String a) {}
            public void checkServerTrusted(X509Certificate[] c, String a) {}
        }}, new SecureRandom());
        return ctx;
    }
}
