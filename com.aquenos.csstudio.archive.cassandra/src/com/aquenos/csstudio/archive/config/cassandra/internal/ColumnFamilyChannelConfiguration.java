/*
 * Copyright 2012-2013 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.csstudio.archive.config.cassandra.internal;

import java.util.LinkedList;
import java.util.List;

import org.csstudio.archive.config.SampleMode;

import com.aquenos.csstudio.archive.cassandra.SampleStore;
import com.aquenos.csstudio.archive.cassandra.internal.ColumnFamilyUtil;
import com.aquenos.csstudio.archive.config.cassandra.CassandraChannelConfig;
import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.ddl.ColumnDefinition;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.serializers.StringSerializer;

/**
 * Provides access to the data stored in the "channelConfiguration" column
 * family. This class is only intended for internal use by other classes in the
 * same bundle.
 * 
 * @author Sebastian Marsching
 */
public abstract class ColumnFamilyChannelConfiguration {

    public final static String NAME = "channelConfiguration";

    public final static ColumnFamily<String, String> CF = new ColumnFamily<String, String>(
            NAME, StringSerializer.get(), StringSerializer.get());

    public final static String COLUMN_ENGINE = "engine";
    public final static String COLUMN_GROUP = "group";
    public final static String COLUMN_SAMPLE_MODE = "sampleMode";
    public final static String COLUMN_SAMPLE_PERIOD = "samplePeriod";
    public final static String COLUMN_SAMPLE_DELTA = "sampleDelta";

    public final static String[] ALL_COLUMNS = new String[] { COLUMN_ENGINE,
            COLUMN_GROUP, COLUMN_SAMPLE_MODE, COLUMN_SAMPLE_PERIOD,
            COLUMN_SAMPLE_DELTA };

    public static void createOrCheckColumnFamily(Cluster cluster,
            Keyspace keyspace) {
        List<ColumnDefinition> columns = new LinkedList<ColumnDefinition>();
        columns.add(cluster.makeColumnDefinition().setName(COLUMN_ENGINE)
                .setValidationClass("UTF8Type"));
        columns.add(cluster.makeColumnDefinition().setName(COLUMN_GROUP)
                .setValidationClass("UTF8Type"));
        columns.add(cluster.makeColumnDefinition().setName(COLUMN_SAMPLE_MODE)
                .setValidationClass("Int32Type"));
        columns.add(cluster.makeColumnDefinition()
                .setName(COLUMN_SAMPLE_PERIOD).setValidationClass("DoubleType"));
        columns.add(cluster.makeColumnDefinition().setName(COLUMN_SAMPLE_DELTA)
                .setValidationClass("DoubleType"));
        ColumnFamilyUtil.createOrVerifyColumnFamily(cluster, keyspace, NAME,
                "UTF8Type", "UTF8Type", "BytesType", columns);
    }

    public static CassandraChannelConfig readChannelConfig(String channelName,
            ColumnList<String> columns, SampleStore sampleStore) {
        String engineName = readEngine(columns);
        String groupName = readGroup(columns);
        Boolean sampleModeBoolean = readSampleMode(columns);
        Double sampleDelta = readSampleDelta(columns);
        Double samplePeriod = readSamplePeriod(columns);
        if (engineName != null && groupName != null
                && sampleModeBoolean != null && sampleDelta != null
                && samplePeriod != null) {
            return new CassandraChannelConfig(engineName, groupName,
                    channelName, new SampleMode(sampleModeBoolean, sampleDelta,
                            samplePeriod), sampleStore);
        } else {
            return null;
        }
    }

    public static void insertChannelConfig(MutationBatch mutationBatch,
            String engineName, String groupName, String channelName,
            SampleMode sampleMode) {
        mutationBatch.withRow(CF, channelName)
                .putColumn(COLUMN_ENGINE, engineName)
                .putColumn(COLUMN_GROUP, groupName)
                .putColumn(COLUMN_SAMPLE_MODE, sampleMode.isMonitor() ? 1 : 0)
                .putColumn(COLUMN_SAMPLE_DELTA, sampleMode.getDelta())
                .putColumn(COLUMN_SAMPLE_PERIOD, sampleMode.getPeriod());
    }

    private static String readEngine(ColumnList<String> columns) {
        return columns.getStringValue(COLUMN_ENGINE, null);
    }

    private static String readGroup(ColumnList<String> columns) {
        return columns.getStringValue(COLUMN_GROUP, null);
    }

    private static Boolean readSampleMode(ColumnList<String> columns) {
        Integer sampleModeInt = columns.getIntegerValue(COLUMN_SAMPLE_MODE,
                null);
        if (sampleModeInt != null) {
            if (sampleModeInt == 0) {
                return false;
            } else if (sampleModeInt == 1) {
                return true;
            }
        }
        return null;
    }

    private static Double readSampleDelta(ColumnList<String> columns) {
        return columns.getDoubleValue(COLUMN_SAMPLE_DELTA, null);
    }

    private static Double readSamplePeriod(ColumnList<String> columns) {
        return columns.getDoubleValue(COLUMN_SAMPLE_PERIOD, null);
    }

}
