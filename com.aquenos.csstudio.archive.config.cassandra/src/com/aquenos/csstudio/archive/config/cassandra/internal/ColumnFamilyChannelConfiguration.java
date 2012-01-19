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

import org.csstudio.archive.config.SampleMode;
import org.csstudio.data.values.ITimestamp;
import org.csstudio.data.values.TimestampFactory;

import com.aquenos.csstudio.archive.cassandra.ColumnReader;
import com.aquenos.csstudio.archive.cassandra.ColumnWriter;
import com.aquenos.csstudio.archive.config.cassandra.CassandraChannelConfig;

/**
 * Provides access to the data stored in the "channelConfiguration" column
 * family. This class is only intended for internal use by other classes in the
 * same bundle.
 * 
 * @author Sebastian Marsching
 */
public abstract class ColumnFamilyChannelConfiguration {
	public final static String NAME = "channelConfiguration";

	public final static String COLUMN_ENGINE = "engine";
	public final static String COLUMN_GROUP = "group";
	public final static String COLUMN_SAMPLE_MODE = "sampleMode";
	public final static String COLUMN_SAMPLE_PERIOD = "samplePeriod";
	public final static String COLUMN_SAMPLE_DELTA = "sampleDelta";
	public final static String COLUMN_LAST_SAMPLE_TIME = "lastSampleTime";

	public final static String[] ALL_COLUMNS = new String[] { COLUMN_ENGINE,
			COLUMN_GROUP, COLUMN_SAMPLE_MODE, COLUMN_SAMPLE_PERIOD,
			COLUMN_SAMPLE_DELTA, COLUMN_LAST_SAMPLE_TIME };
	public final static String[] ALL_COLUMNS_BUT_LAST_SAMPLE_TIME = new String[] {
			COLUMN_ENGINE, COLUMN_GROUP, COLUMN_SAMPLE_MODE,
			COLUMN_SAMPLE_PERIOD, COLUMN_SAMPLE_DELTA };

	public static byte[] getKey(String channelName) {
		return EngineOrChannelConfigurationKey.generateDatabaseKey(channelName);
	}

	public static String getChannelName(byte[] key) {
		return EngineOrChannelConfigurationKey.extractRealKey(key);
	}

	public static CassandraChannelConfig readChannelConfig(byte[] rowKey,
			ColumnSlice<String, byte[]> slice) {
		String channelName = EngineOrChannelConfigurationKey
				.extractRealKey(rowKey);
		String engineName = readEngine(slice);
		String groupName = readGroup(slice);
		Boolean sampleModeBoolean = readSampleMode(slice);
		Double sampleDelta = readSampleDelta(slice);
		Double samplePeriod = readSamplePeriod(slice);
		ITimestamp lastSampleTime = readLastSampleTime(slice);
		if (engineName != null && groupName != null
				&& sampleModeBoolean != null && sampleDelta != null
				&& samplePeriod != null) {
			return new CassandraChannelConfig(engineName, groupName,
					channelName, new SampleMode(sampleModeBoolean, sampleDelta,
							samplePeriod), lastSampleTime);
		} else {
			return null;
		}
	}

	public static void insertChannelConfig(Mutator<byte[]> mutator,
			String engineName, String groupName, String channelName,
			SampleMode sampleMode, ITimestamp lastSampleTime) {
		byte[] channelKey = EngineOrChannelConfigurationKey
				.generateDatabaseKey(channelName);
		ColumnWriter.insertString(mutator, channelKey,
				ColumnFamilyChannelConfiguration.NAME,
				ColumnFamilyChannelConfiguration.COLUMN_ENGINE, engineName);
		ColumnWriter.insertString(mutator, channelKey,
				ColumnFamilyChannelConfiguration.NAME,
				ColumnFamilyChannelConfiguration.COLUMN_GROUP, groupName);
		ColumnWriter.insertByte(mutator, channelKey,
				ColumnFamilyChannelConfiguration.NAME,
				ColumnFamilyChannelConfiguration.COLUMN_SAMPLE_MODE,
				(byte) (sampleMode.isMonitor() ? 1 : 0));
		ColumnWriter.insertDouble(mutator, channelKey,
				ColumnFamilyChannelConfiguration.NAME,
				ColumnFamilyChannelConfiguration.COLUMN_SAMPLE_DELTA,
				sampleMode.getDelta());
		ColumnWriter.insertDouble(mutator, channelKey,
				ColumnFamilyChannelConfiguration.NAME,
				ColumnFamilyChannelConfiguration.COLUMN_SAMPLE_PERIOD,
				sampleMode.getPeriod());
		if (lastSampleTime != null) {
			ColumnWriter.insertTimestamp(mutator, channelKey,
					ColumnFamilyChannelConfiguration.NAME,
					ColumnFamilyChannelConfiguration.COLUMN_LAST_SAMPLE_TIME,
					lastSampleTime);
		}
	}

	private static String readEngine(ColumnSlice<String, byte[]> slice) {
		return ColumnReader.readString(slice, COLUMN_ENGINE);
	}

	private static String readGroup(ColumnSlice<String, byte[]> slice) {
		return ColumnReader.readString(slice, COLUMN_GROUP);
	}

	private static Boolean readSampleMode(ColumnSlice<String, byte[]> slice) {
		Byte sampleModeByte = ColumnReader.readByte(slice,
				ColumnFamilyChannelConfiguration.COLUMN_SAMPLE_MODE);
		if (sampleModeByte != null) {
			if (sampleModeByte == 0) {
				return false;
			} else if (sampleModeByte == 1) {
				return true;
			}
		}
		return null;
	}

	private static Double readSampleDelta(ColumnSlice<String, byte[]> slice) {
		return ColumnReader.readDouble(slice,
				ColumnFamilyChannelConfiguration.COLUMN_SAMPLE_DELTA);
	}

	private static Double readSamplePeriod(ColumnSlice<String, byte[]> slice) {
		return ColumnReader.readDouble(slice,
				ColumnFamilyChannelConfiguration.COLUMN_SAMPLE_PERIOD);
	}

	private static ITimestamp readLastSampleTime(
			ColumnSlice<String, byte[]> slice) {
		ITimestamp lastSampleTime = ColumnReader.readTimestamp(slice,
				ColumnFamilyChannelConfiguration.COLUMN_LAST_SAMPLE_TIME);
		if (lastSampleTime == null) {
			lastSampleTime = TimestampFactory.createTimestamp(0, 0);
		}
		return lastSampleTime;
	}
}
