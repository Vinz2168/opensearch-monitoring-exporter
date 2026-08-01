/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package it.seacom.opensearch.monitoring.queue;

import java.util.Map;

/**
 * Documento ss4o (Simple Schema for Observability) pronto per essere
 * inviato al cluster di monitoring via Bulk API.
 *
 * Il campo {@code targetIndex} è già risolto con la data corrente.
 */
public class MetricDocument {

    private final String targetIndex;
    private final Map<String, Object> source;

    public MetricDocument(String targetIndex, Map<String, Object> source) {
        this.targetIndex = targetIndex;
        this.source      = source;
    }

    public String getTargetIndex() { return targetIndex; }
    public Map<String, Object> getSource() { return source; }
}
