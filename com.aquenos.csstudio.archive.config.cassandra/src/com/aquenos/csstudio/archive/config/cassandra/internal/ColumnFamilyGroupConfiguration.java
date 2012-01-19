/*
 * Copyright 2012 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.csstudio.archive.config.cassandra.internal;

import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.mutation.Mutator;

import com.aquenos.csstudio.archive.cassandra.ColumnReader;
import com.aquenos.csstudio.archive.cassandra.ColumnWriter;
import com.aquenos.csstudio.archive.config.cassandra.CassandraGroupConfig;

/**
 * Provides access to the data stored in the "groupConfiguration" column family.
 * This class is only intended for internal use by other classes in the same
 * bundle.
 * 
 * @author Sebastian Marsching
 */
public abstract class ColumnFamilyGroupConfiguration {
	public final static String NAME = "groupConfiguration";

	public final static String COLUMN_ENABLING_CHANNEL = "enablingChannel";

	public final static String[] ALL_COLUMNS = new String[] { COLUMN_ENABLING_CHANNEL };

	public static byte[] getKey(String engineName, String groupName) {
		return CompressionLevelOrGroupConfigurationKey.generateDatabaseKey(
				engineName, groupName);
	}

	public static CassandraGroupConfig readGroupConfig(String engineName,
			byte[] rowKey, ColumnSlice<String, byte[]> slice) {
		String groupName = CompressionLevelOrGroupConfigurationKey
				.extractSecondName(rowKey);
		String enablingChannel = ColumnReader.readString(slice,
				ColumnFamilyGroupConfiguration.COLUMN_ENABLING_CHANNEL);
		if (groupName != null && enablingChannel != null) {
			return new CassandraGroupConfig(engineName, groupName,
					enablingChannel);
		} else {
			return null;
		}
	}

	public static void insertGroupConfig(Mutator<byte[]> mutator,
			String engineName, String groupName, String enablingChannel) {
		byte[] groupKey = getKey(engineName, groupName);
		ColumnWriter.insertString(mutator, groupKey,
				ColumnFamilyGroupConfiguration.NAME,
				ColumnFamilyGroupConfiguration.COLUMN_ENABLING_CHANNEL, "");
	}
}
