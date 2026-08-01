/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package it.seacom.opensearch.monitoring;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import it.seacom.opensearch.monitoring.plugin.MonitoringExporterPlugin;
import it.seacom.opensearch.monitoring.queue.MetricDocument;
import it.seacom.opensearch.monitoring.serializer.Ss4oSerializer;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.action.admin.indices.stats.IndicesStatsRequest;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.opensearch.transport.client.Requests;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Integration test for the opensearch-monitoring-exporter plugin, run against
 * an embedded single-node OpenSearch cluster (OpenSearchSingleNodeTestCase).
 *
 * Covers two levels:
 *  1. Plugin lifecycle — loads correctly, no startup exceptions.
 *  2. Ss4oSerializer against REAL responses from the embedded cluster's Admin
 *     Client — not mocked data — verifying the actual document shape that
 *     would be sent to the monitoring cluster via the Bulk API.
 *
 * ThreadLeakFilters esclude il thread SelectorManager del JDK HttpClient
 * che viene chiuso in modo asincrono dopo il close() del plugin.
 */
@ThreadLeakFilters(filters = { MonitoringExporterIT.HttpClientThreadFilter.class })
public class MonitoringExporterIT extends OpenSearchSingleNodeTestCase {

    /**
     * Filter per il thread SelectorManager del JDK HttpClient.
     * Questo thread è un daemon thread interno del JDK che si chiude
     * in modo asincrono dopo la chiusura dell'HttpClient.
     */
    public static class HttpClientThreadFilter
        implements com.carrotsearch.randomizedtesting.ThreadFilter {
        @Override
        public boolean reject(Thread t) {
            return t.getName().startsWith("HttpClient-");
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(MonitoringExporterPlugin.class);
    }

    // -------------------------------------------------------------------
    // 1. Plugin lifecycle
    // -------------------------------------------------------------------

    public void testPluginLoaded() {
        ClusterService cs = getInstanceFromNode(ClusterService.class);
        assertNotNull(cs);
        assertNotNull(cs.state().nodes().getLocalNode());
    }

    // -------------------------------------------------------------------
    // 2. Ss4oSerializer contro dati reali del cluster embedded
    // -------------------------------------------------------------------

    public void testSerializerProducesClusterHealthDocument() {
        ClusterHealthResponse health = client().admin().cluster()
            .health(Requests.clusterHealthRequest().local(true))
            .actionGet();

        Ss4oSerializer serializer = new Ss4oSerializer(clusterSettings());
        List<MetricDocument> docs = serializer.serialize(
            Instant.now(), health, null, null, null);

        assertEquals(1, docs.size());
        Map<String, Object> source = docs.get(0).getSource();
        assertEquals("cluster", source.get("kind"));
        assertNotNull(source.get("@timestamp"));
        assertNotNull(source.get("cluster_status_name"));
        assertTrue((Integer) source.get("nodes_total") >= 1);
        assertTrue(docs.get(0).getTargetIndex().startsWith("ss4o_metrics-opensearch-"));
    }

    public void testSerializerProducesNodeStatsDocumentsForLocalNode() {
        NodesStatsResponse nodesStats = client().admin().cluster()
            .nodesStats(Requests.nodesStatsRequest("_local").clear().all())
            .actionGet();

        Ss4oSerializer serializer = new Ss4oSerializer(clusterSettings());
        List<MetricDocument> docs = serializer.serialize(
            Instant.now(), null, nodesStats, null, null);

        // Ci aspettiamo un documento per ciascuna categoria: jvm, os, process,
        // thread_pools, transport, http, fs, indices — per l'unico nodo locale
        List<String> kinds = docs.stream()
            .map(d -> (String) d.getSource().get("kind"))
            .toList();

        assertTrue("expected node_jvm document, got: " + kinds, kinds.contains("node_jvm"));
        assertTrue("expected node_os document, got: " + kinds, kinds.contains("node_os"));
        assertTrue("expected node_process document, got: " + kinds, kinds.contains("node_process"));
        assertTrue("expected node_thread_pools document, got: " + kinds, kinds.contains("node_thread_pools"));

        // Verifica struttura del documento JVM: campi numerici presenti e coerenti
        Map<String, Object> jvmDoc = docs.stream()
            .filter(d -> "node_jvm".equals(d.getSource().get("kind")))
            .findFirst().orElseThrow().getSource();

        assertNotNull(jvmDoc.get("heap_used_bytes"));
        assertNotNull(jvmDoc.get("heap_used_percent"));
        @SuppressWarnings("unchecked")
        Map<String, Object> node = (Map<String, Object>) jvmDoc.get("node");
        assertNotNull(node.get("name"));
        assertNotNull(node.get("id"));
    }

    public void testSerializerProducesIndicesDocumentsWhenIndexExists() {
        createIndex("test-monitored-index");
        ensureGreen("test-monitored-index");

        ClusterHealthResponse health = client().admin().cluster()
            .health(Requests.clusterHealthRequest().local(true))
            .actionGet();
        IndicesStatsResponse indicesStats = client().admin().indices()
            .stats(new IndicesStatsRequest().all())
            .actionGet();

        Ss4oSerializer serializer = new Ss4oSerializer(clusterSettings());
        List<MetricDocument> docs = serializer.serialize(
            Instant.now(), health, null, indicesStats, null);

        // Un documento aggregato + un documento per indice (test-monitored-index)
        boolean hasAggregate = docs.stream()
            .anyMatch(d -> "indices_aggregate".equals(d.getSource().get("kind")));
        boolean hasPerIndex = docs.stream()
            .anyMatch(d -> "index".equals(d.getSource().get("kind"))
                        && "test-monitored-index".equals(d.getSource().get("index")));

        assertTrue("expected indices_aggregate document", hasAggregate);
        assertTrue("expected per-index document for test-monitored-index", hasPerIndex);
    }

    public void testSerializerHandlesAllNullInputsGracefully() {
        Ss4oSerializer serializer = new Ss4oSerializer(clusterSettings());
        List<MetricDocument> docs = serializer.serialize(
            Instant.now(), null, null, null, null);

        assertNotNull(docs);
        assertTrue(docs.isEmpty());
    }

    private Settings clusterSettings() {
        return getInstanceFromNode(ClusterService.class).getSettings();
    }
}
