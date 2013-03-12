/*
 * Copyright 2012-2013 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.csstudio.archive.config.cassandra.internal;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.UTF8Type;

import com.aquenos.csstudio.archive.cassandra.internal.ColumnFamilyUtil;
import com.aquenos.csstudio.archive.cassandra.util.Pair;
import com.aquenos.csstudio.archive.config.cassandra.CassandraGroupConfig;
import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.ddl.ColumnDefinition;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.serializers.StringSerializer;

/**
 * Provides access to the data stored in the "groupConfiguration" column family.
 * This class is only intended for internal use by other classes in the same
 * bundle.
 * 
 * @author Sebastian Marsching
 */
public abstract class ColumnFamilyGroupConfiguration {

    public final static String NAME = "groupConfiguration";

    public final static CompositeType KEY_TYPE = CompositeType
            .getInstance(Arrays.<AbstractType<?>> asList(UTF8Type.instance,
                    UTF8Type.instance));

    public final static ColumnFamily<Pair<String, String>, String> CF = new ColumnFamily<Pair<String, String>, String>(
            NAME, StringPairSerializer.get(), StringSerializer.get());

    public final static String COLUMN_ENABLING_CHANNEL = "enablingChannel";

    public final static String[] ALL_COLUMNS = new String[] { COLUMN_ENABLING_CHANNEL };

    public static void createOrCheckColumnFamily(Cluster cluster,
            Keyspace keyspace) {
        List<ColumnDefinition> columns = new LinkedList<ColumnDefinition>();
        columns.add(cluster.makeColumnDefinition()
                .setName(COLUMN_ENABLING_CHANNEL)
                .setValidationClass("UTF8Type"));
        ColumnFamilyUtil.createOrVerifyColumnFamily(cluster, keyspace, NAME,
                "CompositeType(UTF8Type,UTF8Type)", "UTF8Type", "BytesType",
                columns);
    }

    public static Pair<String, String> getKey(String engineName,
            String groupName) {
        return new Pair<String, String>(engineName, groupName);
    }

    public static CassandraGroupConfig readGroupConfig(String engineName,
            String groupName, ColumnList<String> columns) {
        Column<String> enablingChannelColumn = columns
                .getColumnByName(COLUMN_ENABLING_CHANNEL);
        if (engineName != null && groupName != null
                && enablingChannelColumn != null) {
            return new CassandraGroupConfig(engineName, groupName,
                    enablingChannelColumn.getStringValue());
        } else {
            return null;
        }
    }

    public static void insertGroupConfig(MutationBatch mutationBatch,
            String engineName, String groupName, String enablingChannel) {
        Pair<String, String> groupKey = getKey(engineName, groupName);
        mutationBatch.withRow(CF, groupKey).putColumn(COLUMN_ENABLING_CHANNEL,
                "");
    }

}
