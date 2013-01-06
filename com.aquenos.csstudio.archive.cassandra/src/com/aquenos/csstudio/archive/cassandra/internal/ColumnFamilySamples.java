/*
 * Copyright 2012-2013 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.csstudio.archive.cassandra.internal;

import java.math.BigInteger;
import java.util.LinkedList;
import java.util.List;

import org.csstudio.data.values.ITimestamp;

import com.aquenos.csstudio.archive.cassandra.util.TimestampSerializer;
import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.ddl.ColumnDefinition;
import com.netflix.astyanax.model.ColumnFamily;

/**
 * Provides methods for accessing data stored in the "samples" column families.
 * This class is only intended for internal use by other classes in the same
 * bundle.
 * 
 * @author Sebastian Marsching
 */
public class ColumnFamilySamples {
    public final static String NAME_PREFIX = "samples";

    private String name;
    private ColumnFamily<SampleBucketKey, ITimestamp> columnFamily;

    public ColumnFamilySamples(long compressionPeriod) {
        if (compressionPeriod < 0) {
            throw new IllegalArgumentException(
                    "Compression period must not be negative.");
        } else if (compressionPeriod == 0) {
            this.name = NAME_PREFIX;
        } else {
            this.name = NAME_PREFIX + "_" + Long.toString(compressionPeriod);
        }
        this.columnFamily = new ColumnFamily<SampleBucketKey, ITimestamp>(
                this.name, SampleBucketKeySerializer.get(),
                TimestampSerializer.get());
    }

    public void createOrCheckColumnFamily(Cluster cluster, Keyspace keyspace) {
        List<ColumnDefinition> columns = new LinkedList<ColumnDefinition>();
        ColumnFamilyUtil.createOrVerifyColumnFamily(cluster, keyspace,
                getName(), "CompositeType(UTF8Type,IntegerType,IntegerType)",
                "IntegerType", "BytesType", columns);
    }

    public String getName() {
        return this.name;
    }

    public ColumnFamily<SampleBucketKey, ITimestamp> getCF() {
        return this.columnFamily;
    }

    public static SampleBucketKey getKey(String channelName,
            BigInteger bucketSize, ITimestamp bucketStartTime) {
        return new SampleBucketKey(channelName, bucketSize, bucketStartTime);
    }

}
