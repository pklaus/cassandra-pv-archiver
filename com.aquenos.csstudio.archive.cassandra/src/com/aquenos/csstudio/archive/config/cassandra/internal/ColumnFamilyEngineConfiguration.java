/*
 * Copyright 2012-2013 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.csstudio.archive.config.cassandra.internal;

import java.net.URISyntaxException;
import java.util.LinkedList;
import java.util.List;

import org.csstudio.archive.config.EngineConfig;

import com.aquenos.csstudio.archive.cassandra.internal.ColumnFamilyUtil;
import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.ddl.ColumnDefinition;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.serializers.StringSerializer;

/**
 * Provides access to the data stored in the "engineConfiguration" column
 * family. This class is only intended for internal use by other classes in the
 * same bundle.
 * 
 * @author Sebastian Marsching
 */
public abstract class ColumnFamilyEngineConfiguration {

    public final static String NAME = "engineConfiguration";

    public final static ColumnFamily<String, String> CF = new ColumnFamily<String, String>(
            NAME, StringSerializer.get(), StringSerializer.get());

    public final static String COLUMN_DESCRIPTION = "description";
    public final static String COLUMN_URL = "url";

    public final static String[] ALL_COLUMNS = new String[] {
            COLUMN_DESCRIPTION, COLUMN_URL };

    public static void createOrCheckColumnFamily(Cluster cluster,
            Keyspace keyspace) {
        List<ColumnDefinition> columns = new LinkedList<ColumnDefinition>();
        columns.add(cluster.makeColumnDefinition().setName(COLUMN_DESCRIPTION)
                .setValidationClass("UTF8Type"));
        columns.add(cluster.makeColumnDefinition().setName(COLUMN_URL)
                .setValidationClass("UTF8Type"));
        ColumnFamilyUtil.createOrVerifyColumnFamily(cluster, keyspace, NAME,
                "UTF8Type", "UTF8Type", "BytesType", columns);
    }

    public static EngineConfig readEngineConfig(String engineName,
            ColumnList<String> columns) {
        String description = columns.getStringValue(COLUMN_DESCRIPTION, null);
        String url = columns.getStringValue(COLUMN_URL, null);
        if (description != null && url != null) {
            try {
                return new EngineConfig(engineName, description, url);
            } catch (URISyntaxException e) {
                return null;
            }
        } else {
            return null;
        }
    }

    public static void insertEngineConfig(MutationBatch mutationBatch,
            String engineName, String description, String engineUrl)
            throws URISyntaxException {
        mutationBatch.withRow(CF, engineName)
                .putColumn(COLUMN_DESCRIPTION, description)
                .putColumn(COLUMN_URL, engineUrl);
    }
}
