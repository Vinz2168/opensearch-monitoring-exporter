/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package it.seacom.opensearch.monitoring.serializer;

import it.seacom.opensearch.monitoring.config.MonitoringExporterSettings;
import it.seacom.opensearch.monitoring.queue.MetricDocument;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.cluster.node.stats.NodeStats;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.action.admin.indices.stats.CommonStats;
import org.opensearch.action.admin.indices.stats.IndexStats;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.common.Nullable;
import org.opensearch.common.settings.Settings;
import org.opensearch.monitor.fs.FsInfo;
import org.opensearch.monitor.jvm.JvmStats;
import org.opensearch.monitor.os.OsStats;
import org.opensearch.monitor.process.ProcessStats;
import org.opensearch.threadpool.ThreadPoolStats;
import org.opensearch.transport.TransportStats;
import org.opensearch.http.HttpStats;
import org.opensearch.indices.NodeIndicesStats;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class Ss4oSerializer {

    private static final DateTimeFormatter TS_FMT =
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.ROOT).withZone(ZoneOffset.UTC);
    private static final DateTimeFormatter IDX_FMT =
        DateTimeFormatter.ofPattern("yyyy.MM.dd", Locale.ROOT).withZone(ZoneOffset.UTC);

    private final String indexPattern;
    private final String clusterName;

    public Ss4oSerializer(Settings settings) {
        this.indexPattern = MonitoringExporterSettings.TARGET_INDEX.get(settings);
        this.clusterName  = settings.get("cluster.name", "unknown");
    }

    public List<MetricDocument> serialize(
            Instant now,
            @Nullable ClusterHealthResponse health,
            @Nullable NodesStatsResponse nodesStats,
            @Nullable IndicesStatsResponse indicesStats,
            @Nullable ClusterStateResponse clusterState) {

        String ts    = TS_FMT.format(now);
        String index = indexPattern.replace("%{yyyy.MM.dd}", IDX_FMT.format(now));
        List<MetricDocument> docs = new ArrayList<>();

        if (health != null)
            docs.add(serializeClusterHealth(health, ts, index));

        if (nodesStats != null)
            for (NodeStats ns : nodesStats.getNodes())
                docs.addAll(serializeNodeStats(ns, ts, index));

        if (indicesStats != null) {
            docs.add(serializeIndicesAggregate(indicesStats, ts, index));
            for (Map.Entry<String, IndexStats> e : indicesStats.getIndices().entrySet())
                docs.add(serializePerIndex(e.getKey(), e.getValue(), health, ts, index));
        }

        if (clusterState != null)
            docs.add(serializeClusterSettings(clusterState, ts, index));

        return docs;
    }

    // -------------------------------------------------------------------------
    // Cluster health
    // -------------------------------------------------------------------------

    private MetricDocument serializeClusterHealth(ClusterHealthResponse h, String ts, String idx) {
        Map<String, Object> doc = base(ts, "cluster");
        doc.put("cluster_status",            h.getStatus().value());
        doc.put("cluster_status_name",       h.getStatus().name());
        doc.put("nodes_total",               h.getNumberOfNodes());
        doc.put("nodes_data",                h.getNumberOfDataNodes());
        doc.put("shards_active",             h.getActiveShards());
        doc.put("shards_active_primary",     h.getActivePrimaryShards());
        doc.put("shards_active_percent",     h.getActiveShardsPercent());
        doc.put("shards_relocating",         h.getRelocatingShards());
        doc.put("shards_initializing",       h.getInitializingShards());
        doc.put("shards_unassigned",         h.getUnassignedShards());
        doc.put("shards_delayed_unassigned", h.getDelayedUnassignedShards());
        doc.put("pending_tasks",             h.getNumberOfPendingTasks());
        doc.put("task_max_wait_time_ms",     h.getTaskMaxWaitingTime().getMillis());
        doc.put("inflight_fetch",            h.getNumberOfInFlightFetch());
        doc.put("timed_out",                 h.isTimedOut() ? 1 : 0);
        return new MetricDocument(idx, doc);
    }

    // -------------------------------------------------------------------------
    // Node stats
    // -------------------------------------------------------------------------

    private List<MetricDocument> serializeNodeStats(NodeStats ns, String ts, String idx) {
        List<MetricDocument> docs = new ArrayList<>();
        String name = ns.getNode().getName();
        String id   = ns.getNode().getId();

        if (ns.getJvm()       != null) docs.add(serializeJvm(ns.getJvm(), name, id, ts, idx));
        if (ns.getOs()        != null) docs.add(serializeOs(ns.getOs(), name, id, ts, idx));
        if (ns.getProcess()   != null) docs.add(serializeProcess(ns.getProcess(), name, id, ts, idx));
        if (ns.getThreadPool()!= null) docs.add(serializeThreadPools(ns.getThreadPool(), name, id, ts, idx));
        if (ns.getTransport() != null) docs.add(serializeTransport(ns.getTransport(), name, id, ts, idx));
        if (ns.getHttp()      != null) docs.add(serializeHttp(ns.getHttp(), name, id, ts, idx));
        if (ns.getFs()        != null) docs.add(serializeFs(ns.getFs(), name, id, ts, idx));
        if (ns.getIndices()   != null) docs.add(serializeNodeIndices(ns.getIndices(), name, id, ts, idx));
        return docs;
    }

    private MetricDocument serializeJvm(JvmStats jvm, String node, String id, String ts, String idx) {
        Map<String, Object> doc = baseNode(ts, "node_jvm", node, id);
        JvmStats.Mem mem = jvm.getMem();
        doc.put("heap_used_bytes",        mem.getHeapUsed().getBytes());
        doc.put("heap_committed_bytes",   mem.getHeapCommitted().getBytes());
        doc.put("heap_max_bytes",         mem.getHeapMax().getBytes());
        doc.put("heap_used_percent",      mem.getHeapUsedPercent());
        doc.put("nonheap_used_bytes",     mem.getNonHeapUsed().getBytes());
        doc.put("nonheap_committed_bytes",mem.getNonHeapCommitted().getBytes());
        Map<String, Object> gcMap = new LinkedHashMap<>();
        for (JvmStats.GarbageCollector gc : jvm.getGc().getCollectors()) {
            Map<String, Object> s = new LinkedHashMap<>();
            s.put("collection_count",   gc.getCollectionCount());
            s.put("collection_time_ms", gc.getCollectionTime().getMillis());
            gcMap.put(gc.getName().replace(" ", "_").toLowerCase(Locale.ROOT), s);
        }
        doc.put("gc",             gcMap);
        doc.put("threads_count", jvm.getThreads().getCount());
        doc.put("threads_peak",  jvm.getThreads().getPeakCount());
        doc.put("uptime_ms",     jvm.getUptime().getMillis());
        return new MetricDocument(idx, doc);
    }

    private MetricDocument serializeOs(OsStats os, String node, String id, String ts, String idx) {
        Map<String, Object> doc = baseNode(ts, "node_os", node, id);
        OsStats.Cpu cpu = os.getCpu();
        if (cpu != null) {
            doc.put("cpu_percent", cpu.getPercent());
            double[] la = cpu.getLoadAverage();
            if (la != null && la.length >= 3) {
                doc.put("load_avg_1m",  la[0]);
                doc.put("load_avg_5m",  la[1]);
                doc.put("load_avg_15m", la[2]);
            }
        }
        OsStats.Mem mem = os.getMem();
        if (mem != null) {
            doc.put("mem_free_bytes",   mem.getFree().getBytes());
            doc.put("mem_used_bytes",   mem.getUsed().getBytes());
            doc.put("mem_total_bytes",  mem.getTotal().getBytes());
            doc.put("mem_free_percent", mem.getFreePercent());
            doc.put("mem_used_percent", mem.getUsedPercent());
        }
        OsStats.Swap swap = os.getSwap();
        if (swap != null) {
            doc.put("swap_free_bytes",  swap.getFree().getBytes());
            doc.put("swap_used_bytes",  swap.getUsed().getBytes());
            doc.put("swap_total_bytes", swap.getTotal().getBytes());
        }
        return new MetricDocument(idx, doc);
    }

    private MetricDocument serializeProcess(ProcessStats ps, String node, String id, String ts, String idx) {
        Map<String, Object> doc = baseNode(ts, "node_process", node, id);
        doc.put("cpu_percent",              ps.getCpu().getPercent());
        doc.put("cpu_total_ms",             ps.getCpu().getTotal().getMillis());
        doc.put("mem_total_virtual_bytes",  ps.getMem().getTotalVirtual().getBytes());
        doc.put("open_file_descriptors",    ps.getOpenFileDescriptors());
        doc.put("max_file_descriptors",     ps.getMaxFileDescriptors());
        return new MetricDocument(idx, doc);
    }

    private MetricDocument serializeThreadPools(ThreadPoolStats tps, String node, String id, String ts, String idx) {
        Map<String, Object> doc = baseNode(ts, "node_thread_pools", node, id);
        Map<String, Object> pools = new LinkedHashMap<>();
        for (ThreadPoolStats.Stats s : tps) {
            Map<String, Object> pool = new LinkedHashMap<>();
            pool.put("threads",   s.getThreads());
            pool.put("queue",     s.getQueue());
            pool.put("active",    s.getActive());
            pool.put("rejected",  s.getRejected());
            pool.put("largest",   s.getLargest());
            pool.put("completed", s.getCompleted());
            pools.put(s.getName(), pool);
        }
        doc.put("pools", pools);
        return new MetricDocument(idx, doc);
    }

    private MetricDocument serializeTransport(TransportStats ts2, String node, String id, String ts, String idx) {
        Map<String, Object> doc = baseNode(ts, "node_transport", node, id);
        doc.put("server_open",   ts2.serverOpen());
        doc.put("rx_count",      ts2.rxCount());
        doc.put("rx_size_bytes", ts2.rxSize().getBytes());
        doc.put("tx_count",      ts2.txCount());
        doc.put("tx_size_bytes", ts2.txSize().getBytes());
        return new MetricDocument(idx, doc);
    }

    private MetricDocument serializeHttp(HttpStats hs, String node, String id, String ts, String idx) {
        Map<String, Object> doc = baseNode(ts, "node_http", node, id);
        doc.put("current_open", hs.getServerOpen());
        doc.put("total_opened", hs.getTotalOpen());
        return new MetricDocument(idx, doc);
    }

    private MetricDocument serializeFs(FsInfo fs, String node, String id, String ts, String idx) {
        Map<String, Object> doc = baseNode(ts, "node_fs", node, id);
        FsInfo.Path total = fs.getTotal();
        doc.put("total_bytes",     total.getTotal().getBytes());
        doc.put("free_bytes",      total.getFree().getBytes());
        doc.put("available_bytes", total.getAvailable().getBytes());
        doc.put("used_bytes",      total.getTotal().getBytes() - total.getFree().getBytes());

        // IoStats in 3.6: campi pubblici diretti, non getTotalStats()
        FsInfo.IoStats io = fs.getIoStats();
        if (io != null) {
            doc.put("io_read_ops",        io.getTotalReadOperations());
            doc.put("io_write_ops",       io.getTotalWriteOperations());
            doc.put("io_read_kilobytes",  io.getTotalReadKilobytes());
            doc.put("io_write_kilobytes", io.getTotalWriteKilobytes());
        }
        return new MetricDocument(idx, doc);
    }

    private MetricDocument serializeNodeIndices(NodeIndicesStats ni, String node, String id, String ts, String idx) {
        Map<String, Object> doc = baseNode(ts, "node_indices", node, id);
        doc.put("docs_count",    ni.getDocs().getCount());
        doc.put("docs_deleted",  ni.getDocs().getDeleted());
        doc.put("store_size_bytes", ni.getStore().getSizeInBytes());

        // IndexingStats 3.6: getIndexCount(), getIndexTime().getMillis(), getDeleteCount()
        doc.put("indexing_index_total",   ni.getIndexing().getTotal().getIndexCount());
        doc.put("indexing_index_time_ms", ni.getIndexing().getTotal().getIndexTime().getMillis());
        doc.put("indexing_delete_total",  ni.getIndexing().getTotal().getDeleteCount());

        // SearchStats 3.6: getTotal().getQueryCount(), getQueryTimeInMillis(), getFetchCount(), getFetchTimeInMillis()
        doc.put("search_query_total",   ni.getSearch().getTotal().getQueryCount());
        doc.put("search_query_time_ms", ni.getSearch().getTotal().getQueryTimeInMillis());
        doc.put("search_fetch_total",   ni.getSearch().getTotal().getFetchCount());
        doc.put("search_fetch_time_ms", ni.getSearch().getTotal().getFetchTimeInMillis());

        doc.put("merges_total",          ni.getMerge().getTotal());
        doc.put("merges_total_time_ms",  ni.getMerge().getTotalTimeInMillis());
        doc.put("refresh_total",         ni.getRefresh().getTotal());
        doc.put("refresh_time_ms",       ni.getRefresh().getTotalTimeInMillis());
        doc.put("flush_total",           ni.getFlush().getTotal());
        doc.put("flush_time_ms",         ni.getFlush().getTotalTimeInMillis());
        doc.put("segments_count",        ni.getSegments().getCount());
        // getMemoryInBytes() rimosso in 3.x — usa indexWriterMemoryInBytes
        doc.put("segments_index_writer_memory_bytes", ni.getSegments().getIndexWriterMemoryInBytes());
        return new MetricDocument(idx, doc);
    }

    // -------------------------------------------------------------------------
    // Indices aggregate
    // -------------------------------------------------------------------------

    private MetricDocument serializeIndicesAggregate(IndicesStatsResponse isr, String ts, String idx) {
        Map<String, Object> doc = base(ts, "indices_aggregate");
        CommonStats total = isr.getTotal();
        doc.put("index_count",           isr.getIndices().size());
        doc.put("docs_count",            total.getDocs().getCount());
        doc.put("docs_deleted",          total.getDocs().getDeleted());
        doc.put("store_size_bytes",      total.getStore().getSizeInBytes());
        doc.put("indexing_index_total",  total.getIndexing().getTotal().getIndexCount());
        doc.put("search_query_total",    total.getSearch().getTotal().getQueryCount());
        doc.put("search_query_time_ms",  total.getSearch().getTotal().getQueryTimeInMillis());
        doc.put("segments_count",        total.getSegments().getCount());
        doc.put("segments_index_writer_memory_bytes", total.getSegments().getIndexWriterMemoryInBytes());
        return new MetricDocument(idx, doc);
    }

    // -------------------------------------------------------------------------
    // Per-index
    // -------------------------------------------------------------------------

    private MetricDocument serializePerIndex(String indexName, IndexStats is,
                                              @Nullable ClusterHealthResponse health,
                                              String ts, String targetIdx) {
        Map<String, Object> doc = base(ts, "index");
        doc.put("index", indexName);
        CommonStats total = is.getTotal();
        doc.put("docs_count",             total.getDocs().getCount());
        doc.put("docs_deleted",           total.getDocs().getDeleted());
        doc.put("store_size_bytes",       total.getStore().getSizeInBytes());
        doc.put("indexing_index_total",   total.getIndexing().getTotal().getIndexCount());
        doc.put("indexing_index_time_ms", total.getIndexing().getTotal().getIndexTime().getMillis());
        doc.put("search_query_total",     total.getSearch().getTotal().getQueryCount());
        doc.put("search_query_time_ms",   total.getSearch().getTotal().getQueryTimeInMillis());
        doc.put("segments_count",         total.getSegments().getCount());

        if (health != null && health.getIndices() != null) {
            var ih = health.getIndices().get(indexName);
            if (ih != null) {
                doc.put("status",            ih.getStatus().value());
                doc.put("status_name",       ih.getStatus().name());
                doc.put("shards_active",     ih.getActiveShards());
                doc.put("shards_primary",    ih.getActivePrimaryShards());
                doc.put("shards_relocating", ih.getRelocatingShards());
                doc.put("shards_init",       ih.getInitializingShards());
                doc.put("shards_unassigned", ih.getUnassignedShards());
            }
        }
        return new MetricDocument(targetIdx, doc);
    }

    // -------------------------------------------------------------------------
    // Cluster settings
    // -------------------------------------------------------------------------

    private MetricDocument serializeClusterSettings(ClusterStateResponse cs, String ts, String idx) {
        Map<String, Object> doc = base(ts, "cluster_settings");
        Settings persistent = cs.getState().metadata().persistentSettings();
        Settings transient_ = cs.getState().metadata().transientSettings();
        extractSetting(doc, persistent, transient_,
            "cluster.routing.allocation.disk.watermark.low",        "disk_watermark_low");
        extractSetting(doc, persistent, transient_,
            "cluster.routing.allocation.disk.watermark.high",       "disk_watermark_high");
        extractSetting(doc, persistent, transient_,
            "cluster.routing.allocation.disk.watermark.flood_stage","disk_watermark_flood_stage");
        extractSetting(doc, persistent, transient_,
            "cluster.routing.allocation.enable",                    "routing_allocation_enable");
        return new MetricDocument(idx, doc);
    }

    private void extractSetting(Map<String, Object> doc, Settings p, Settings t,
                                 String key, String docKey) {
        String val = t.get(key, p.get(key, null));
        if (val != null) doc.put(docKey, val);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private Map<String, Object> base(String ts, String kind) {
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("@timestamp", ts);
        doc.put("kind",       kind);
        doc.put("cluster",    clusterName);
        return doc;
    }

    private Map<String, Object> baseNode(String ts, String kind, String node, String id) {
        Map<String, Object> doc = base(ts, kind);
        Map<String, Object> n = new LinkedHashMap<>();
        n.put("name", node);
        n.put("id",   id);
        doc.put("node", n);
        return doc;
    }
}
