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

import com.aquenos.csstudio.archive.cassandra.internal.ColumnFamilyUtil;
import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.ddl.ColumnDefinition;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.StringSerializer;

/**
 * Provides access to the data stored in the "engineConfigurationToGroups"
 * column family. This class is only intended for internal use by other classes
 * in the same bundle.
 * 
 * @author Sebastian Marsching
 */
public abstract class ColumnFamilyEngineConfigurationToGroups {

    public final static String NAME = "engineConfigurationToGroups";

    public final static ColumnFamily<String, String> CF = new ColumnFamily<String, String>(
            NAME, StringSerializer.get(), StringSerializer.get());

    public static void createOrCheckColumnFamily(Cluster cluster,
            Keyspace keyspace) {
        List<ColumnDefinition> columns = new LinkedList<ColumnDefinition>();
        ColumnFamilyUtil.createOrVerifyColumnFamily(cluster, keyspace, NAME,
                "UTF8Type", "UTF8Type", "UTF8Type", columns);
    }

}
