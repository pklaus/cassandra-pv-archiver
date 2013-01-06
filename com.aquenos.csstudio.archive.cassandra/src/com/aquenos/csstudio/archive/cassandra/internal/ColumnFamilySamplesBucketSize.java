/*
 * Copyright 2013 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.csstudio.archive.cassandra.internal;

import java.util.LinkedList;
import java.util.List;

import org.csstudio.data.values.ITimestamp;

import com.aquenos.csstudio.archive.cassandra.util.TimestampSerializer;
import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.ddl.ColumnDefinition;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.StringSerializer;

/**
 * Provides methods for accessing data stored in the "samplesBucketSize" column
 * families. This class is only intended for internal use by other classes in
 * the same bundle.
 * 
 * @author Sebastian Marsching
 */
public class ColumnFamilySamplesBucketSize {
    public final static String NAME_PREFIX = "samplesBucketSize";

    private String name;
    private ColumnFamily<String, ITimestamp> columnFamily;

    public ColumnFamilySamplesBucketSize(long compressionPeriod) {
        if (compressionPeriod < 0) {
            throw new IllegalArgumentException(
                    "Compression period must not be negative.");
        } else if (compressionPeriod == 0) {
            this.name = NAME_PREFIX;
        } else {
            this.name = NAME_PREFIX + "_" + Long.toString(compressionPeriod);
        }
        this.columnFamily = new ColumnFamily<String, ITimestamp>(this.name,
                StringSerializer.get(), TimestampSerializer.get());
    }

    public void createOrCheckColumnFamily(Cluster cluster, Keyspace keyspace) {
        List<ColumnDefinition> columns = new LinkedList<ColumnDefinition>();
        ColumnFamilyUtil.createOrVerifyColumnFamily(cluster, keyspace,
                getName(), "UTF8Type", "IntegerType", "IntegerType", columns);
    }

    public String getName() {
        return this.name;
    }

    public ColumnFamily<String, ITimestamp> getCF() {
        return this.columnFamily;
    }

}
