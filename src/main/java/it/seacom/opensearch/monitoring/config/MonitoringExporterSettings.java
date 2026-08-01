/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package it.seacom.opensearch.monitoring.config;

import org.opensearch.common.settings.SecureSetting;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.core.common.settings.SecureString;

import java.util.Arrays;
import java.util.List;

public final class MonitoringExporterSettings {

    private MonitoringExporterSettings() {}

    public static final Setting<List<String>> TARGET_HOSTS = Setting.listSetting(
        "monitoring.exporter.target.hosts",
        List.of("https://localhost:9200"),
        s -> s, Property.NodeScope, Property.Dynamic);

    public static final Setting<String> TARGET_USERNAME = Setting.simpleString(
        "monitoring.exporter.target.username", "admin",
        Property.NodeScope, Property.Dynamic);

    public static final Setting<SecureString> TARGET_PASSWORD = SecureSetting.secureString(
        "monitoring.exporter.target.password", null);

    public static final Setting<Boolean> TLS_VERIFY_HOSTNAME = Setting.boolSetting(
        "monitoring.exporter.tls.verify_hostname", true,
        Property.NodeScope, Property.Dynamic);

    public static final Setting<String> TLS_TRUSTSTORE_PATH = Setting.simpleString(
        "monitoring.exporter.tls.truststore.path", "",
        Property.NodeScope, Property.Dynamic);

    public static final Setting<SecureString> TLS_TRUSTSTORE_PASSWORD = SecureSetting.secureString(
        "monitoring.exporter.tls.truststore.password", null);

    public static final Setting<String> TLS_KEYSTORE_PATH = Setting.simpleString(
        "monitoring.exporter.tls.keystore.path", "",
        Property.NodeScope, Property.Dynamic);

    public static final Setting<SecureString> TLS_KEYSTORE_PASSWORD = SecureSetting.secureString(
        "monitoring.exporter.tls.keystore.password", null);

    public static final Setting<String> NODES_FILTER = Setting.simpleString(
        "monitoring.exporter.collect.nodes_filter", "_local",
        Property.NodeScope, Property.Dynamic);

    public static final Setting<Boolean> COLLECT_INDICES = Setting.boolSetting(
        "monitoring.exporter.collect.indices", true,
        Property.NodeScope, Property.Dynamic);

    public static final Setting<Boolean> COLLECT_CLUSTER_SETTINGS = Setting.boolSetting(
        "monitoring.exporter.collect.cluster_settings", true,
        Property.NodeScope, Property.Dynamic);

    public static final Setting<Integer> COLLECT_INTERVAL_SECONDS = Setting.intSetting(
        "monitoring.exporter.collect.interval_seconds", 30, 10, 300,
        Property.NodeScope, Property.Dynamic);

    public static final Setting<String> TARGET_INDEX = Setting.simpleString(
        "monitoring.exporter.index",
        "ss4o_metrics-opensearch-%{yyyy.MM.dd}",
        Property.NodeScope, Property.Dynamic);

    public static final Setting<Integer> FLUSH_INTERVAL_SECONDS = Setting.intSetting(
        "monitoring.exporter.flush.interval_seconds", 30, 5, 300,
        Property.NodeScope, Property.Dynamic);

    public static final Setting<Integer> BATCH_SIZE = Setting.intSetting(
        "monitoring.exporter.batch.size", 500, 10, 5000,
        Property.NodeScope, Property.Dynamic);

    public static final Setting<Integer> QUEUE_CAPACITY = Setting.intSetting(
        "monitoring.exporter.queue.capacity", 10_000, 100, 100_000,
        Property.NodeScope);

    public static final Setting<Integer> MAX_RETRIES = Setting.intSetting(
        "monitoring.exporter.retry.max", 3, 0, 10,
        Property.NodeScope, Property.Dynamic);

    public static List<Setting<?>> all() {
        return Arrays.asList(
            TARGET_HOSTS, TARGET_USERNAME, TARGET_PASSWORD,
            TLS_VERIFY_HOSTNAME, TLS_TRUSTSTORE_PATH, TLS_TRUSTSTORE_PASSWORD,
            TLS_KEYSTORE_PATH, TLS_KEYSTORE_PASSWORD,
            NODES_FILTER, COLLECT_INDICES, COLLECT_CLUSTER_SETTINGS,
            COLLECT_INTERVAL_SECONDS,
            TARGET_INDEX,
            FLUSH_INTERVAL_SECONDS, BATCH_SIZE, QUEUE_CAPACITY, MAX_RETRIES
        );
    }
}
