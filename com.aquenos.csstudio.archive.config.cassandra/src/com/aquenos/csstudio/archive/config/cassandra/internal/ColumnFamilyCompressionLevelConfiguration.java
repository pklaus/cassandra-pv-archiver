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

import org.csstudio.data.values.ITimestamp;
import org.csstudio.data.values.TimestampFactory;

import com.aquenos.csstudio.archive.cassandra.ColumnReader;
import com.aquenos.csstudio.archive.cassandra.ColumnWriter;
import com.aquenos.csstudio.archive.config.cassandra.CompressionLevelConfig;
import com.aquenos.csstudio.archive.config.cassandra.CompressionLevelState;

/**
 * Provides access to the data stored in the "compressionLevelConfiguration"
 * column family. This class is only intended for internal use by other classes
 * in the same bundle.
 * 
 * @author Sebastian Marsching
 */
public abstract class ColumnFamilyCompressionLevelConfiguration {
	public final static String NAME = "compressionLevelConfiguration";

	public final static String COLUMN_COMPRESSION_PERIOD = "compressionPeriod";
	public final static String COLUMN_RETENTION_PERIOD = "retentionPeriod";
	public final static String COLUMN_LAST_SAVED_SAMPLE_TIME = "lastSavedSampleTime";
	public final static String COLUMN_NEXT_SAMPLE_TIME = "nextSampleTime";

	public final static String[] ALL_COLUMNS = new String[] {
			COLUMN_COMPRESSION_PERIOD, COLUMN_RETENTION_PERIOD,
			COLUMN_LAST_SAVED_SAMPLE_TIME, COLUMN_NEXT_SAMPLE_TIME };
	public final static String[] CONFIG_COLUMNS = new String[] {
			COLUMN_COMPRESSION_PERIOD, COLUMN_RETENTION_PERIOD };
	public final static String[] STATE_COLUMNS = new String[] {
			COLUMN_LAST_SAVED_SAMPLE_TIME, COLUMN_NEXT_SAMPLE_TIME };

	public static byte[] getKey(String channelName, String compressionLevelName) {
		return CompressionLevelOrGroupConfigurationKey.generateDatabaseKey(
				channelName, compressionLevelName);
	}

	public static CompressionLevelConfig readCompressionLevelConfig(
			String channelName, byte[] rowKey, ColumnSlice<String, byte[]> slice) {
		String compressionLevelName = CompressionLevelOrGroupConfigurationKey
				.extractSecondName(rowKey);
		Long compressionPeriod = ColumnReader.readLong(slice,
				COLUMN_COMPRESSION_PERIOD);
		Long retentionPeriod = ColumnReader.readLong(slice,
				COLUMN_RETENTION_PERIOD);
		if (compressionLevelName != null && compressionPeriod != null
				&& retentionPeriod != null) {
			return new CompressionLevelConfig(compressionLevelName,
					channelName, compressionPeriod, retentionPeriod);
		} else {
			return null;
		}
	}

	public static CompressionLevelState readCompressionLevelState(
			String channelName, byte[] rowKey, ColumnSlice<String, byte[]> slice) {
		String compressionLevelName = CompressionLevelOrGroupConfigurationKey
				.extractSecondName(rowKey);
		ITimestamp lastSavedSampleTime = ColumnReader.readTimestamp(slice,
				COLUMN_LAST_SAVED_SAMPLE_TIME);
		ITimestamp nextSampleTime = ColumnReader.readTimestamp(slice,
				COLUMN_NEXT_SAMPLE_TIME);
		if (compressionLevelName != null) {
			return new CompressionLevelState(compressionLevelName, channelName,
					lastSavedSampleTime, nextSampleTime);
		} else {
			return null;
		}
	}

	public static void insertCompressionLevelConfig(Mutator<byte[]> mutator,
			String compressionLevelName, String channelName,
			long compressionPeriod, long retentionPeriod) {
		byte[] rowKey = getKey(channelName, compressionLevelName);
		ColumnWriter.insertLong(mutator, rowKey, NAME,
				COLUMN_COMPRESSION_PERIOD, compressionPeriod);
		ColumnWriter.insertLong(mutator, rowKey, NAME, COLUMN_RETENTION_PERIOD,
				retentionPeriod);
	}

	public static void insertCompressionLevelState(Mutator<byte[]> mutator,
			String compressionLeveName, String channelName,
			ITimestamp lastSavedSampleTime, ITimestamp nextSampleTime) {
		byte[] rowKey = getKey(channelName, compressionLeveName);
		if (lastSavedSampleTime == null) {
			lastSavedSampleTime = TimestampFactory.createTimestamp(0L, 0L);
		}
		if (nextSampleTime == null) {
			nextSampleTime = TimestampFactory.createTimestamp(0L, 0L);
		}
		ColumnWriter.insertTimestamp(mutator, rowKey, NAME,
				COLUMN_LAST_SAVED_SAMPLE_TIME, lastSavedSampleTime);
		ColumnWriter.insertTimestamp(mutator, rowKey, NAME,
				COLUMN_NEXT_SAMPLE_TIME, nextSampleTime);
	}
}
