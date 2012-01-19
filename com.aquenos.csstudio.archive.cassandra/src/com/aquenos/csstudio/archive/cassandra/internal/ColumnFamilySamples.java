/*
 * Copyright 2012 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.csstudio.archive.cassandra.internal;

import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.mutation.Mutator;

import org.csstudio.data.values.IDoubleValue;
import org.csstudio.data.values.IEnumeratedMetaData;
import org.csstudio.data.values.IEnumeratedValue;
import org.csstudio.data.values.ILongValue;
import org.csstudio.data.values.IMetaData;
import org.csstudio.data.values.IMinMaxDoubleValue;
import org.csstudio.data.values.INumericMetaData;
import org.csstudio.data.values.ISeverity;
import org.csstudio.data.values.IStringValue;
import org.csstudio.data.values.ITimestamp;
import org.csstudio.data.values.IValue;
import org.csstudio.data.values.IValue.Quality;
import org.csstudio.data.values.ValueFactory;

import com.aquenos.csstudio.archive.cassandra.ColumnReader;
import com.aquenos.csstudio.archive.cassandra.ColumnWriter;
import com.aquenos.csstudio.archive.cassandra.Sample;

/**
 * Provides methods for accessing data stored in the "samples" column family.
 * This class is only intended for internal use by other classes in the same
 * bundle.
 * 
 * @author Sebastian Marsching
 */
public abstract class ColumnFamilySamples {
	public final static String NAME = "samples";

	public final static String COLUMN_SEVERITY = "severity";
	public final static String COLUMN_STATUS = "status";
	public final static String COLUMN_DOUBLE_VALUE = "valueDouble";
	public final static String COLUMN_DOUBLE_MIN = "valueDoubleMin";
	public final static String COLUMN_DOUBLE_MAX = "valueDoubleMax";
	public final static String COLUMN_ENUM_VALUE = "valueEnum";
	public final static String COLUMN_LONG_VALUE = "valueLong";
	public final static String COLUMN_STRING_VALUE = "valueString";
	public final static String COLUMN_META_DATA_ENUM_STATES = "metaDataEnumStates";
	public final static String COLUMN_META_DATA_NUM_DISPLAY_LOW = "metaDataNumDispLow";
	public final static String COLUMN_META_DATA_NUM_DISPLAY_HIGH = "metaDataNumDispHigh";
	public final static String COLUMN_META_DATA_NUM_WARN_LOW = "metaDataNumWarnLow";
	public final static String COLUMN_META_DATA_NUM_WARN_HIGH = "metaDataNumWarnHigh";
	public final static String COLUMN_META_DATA_NUM_ALARM_LOW = "metaDataNumAlarmLow";
	public final static String COLUMN_META_DATA_NUM_ALARM_HIGH = "metaDataNumAlarmHigh";
	public final static String COLUMN_META_DATA_NUM_PRECISION = "metaDataNumPrecision";
	public final static String COLUMN_META_DATA_NUM_UNITS = "metaDataNumUnits";
	public final static String COLUMN_PRECEDING_SAMPLE_TIME = "precedingSampleTime";

	public final static String[] ALL_COLUMNS = new String[] { COLUMN_SEVERITY,
			COLUMN_STATUS, COLUMN_DOUBLE_VALUE, COLUMN_DOUBLE_MIN,
			COLUMN_DOUBLE_MAX, COLUMN_ENUM_VALUE, COLUMN_LONG_VALUE,
			COLUMN_STRING_VALUE, COLUMN_META_DATA_ENUM_STATES,
			COLUMN_META_DATA_NUM_DISPLAY_LOW,
			COLUMN_META_DATA_NUM_DISPLAY_HIGH, COLUMN_META_DATA_NUM_WARN_LOW,
			COLUMN_META_DATA_NUM_WARN_HIGH, COLUMN_META_DATA_NUM_ALARM_LOW,
			COLUMN_META_DATA_NUM_ALARM_HIGH, COLUMN_META_DATA_NUM_PRECISION,
			COLUMN_META_DATA_NUM_UNITS, COLUMN_PRECEDING_SAMPLE_TIME };

	public static byte[] getKey(String compressionLevelName,
			String channelName, ITimestamp timestamp) {
		return SampleKey.generateKey(compressionLevelName, channelName,
				timestamp);
	}

	public static Sample readSample(String compressionLevelName, byte[] rowKey,
			ColumnSlice<String, byte[]> slice, Quality quality) {
		String channelName = SampleKey.extractChannelName(rowKey);
		ITimestamp timestamp = SampleKey.extractTimestamp(rowKey);
		double[] doubleValue = ColumnReader.readDoubleArray(slice,
				COLUMN_DOUBLE_VALUE);
		Double doubleMin = ColumnReader.readDouble(slice, COLUMN_DOUBLE_MIN);
		Double doubleMax = ColumnReader.readDouble(slice, COLUMN_DOUBLE_MAX);
		int[] enumValue = ColumnReader.readIntegerArray(slice,
				COLUMN_ENUM_VALUE);
		long[] longValue = ColumnReader.readLongArray(slice, COLUMN_LONG_VALUE);
		String[] stringValue = ColumnReader.readStringArray(slice,
				COLUMN_STRING_VALUE);
		Byte severityByte = ColumnReader.readByte(slice, COLUMN_SEVERITY);
		ISeverity severity = null;
		if (severityByte != null) {
			severity = SeverityMapper.numberToSeverity(severityByte);
		}
		String status = ColumnReader.readString(slice, COLUMN_STATUS);
		ITimestamp precedingSampleTimestamp = ColumnReader.readTimestamp(slice,
				COLUMN_PRECEDING_SAMPLE_TIME);
		if (timestamp == null || severity == null || status == null) {
			return null;
		}
		IValue value;
		if (doubleValue != null) {
			INumericMetaData metaData = readSampleNumericMetaData(slice);
			if (doubleMin != null && doubleMax != null) {
				value = ValueFactory.createMinMaxDoubleValue(timestamp,
						severity, status, metaData, quality, doubleValue,
						doubleMin, doubleMax);
			} else {
				value = ValueFactory.createDoubleValue(timestamp, severity,
						status, metaData, quality, doubleValue);
			}
		} else if (enumValue != null) {
			IEnumeratedMetaData metaData = readSampleEnumeratedMetaData(slice);
			value = ValueFactory.createEnumeratedValue(timestamp, severity,
					status, metaData, quality, enumValue);
		} else if (longValue != null) {
			INumericMetaData metaData = readSampleNumericMetaData(slice);
			value = ValueFactory.createLongValue(timestamp, severity, status,
					metaData, quality, longValue);
		} else if (stringValue != null) {
			value = ValueFactory.createStringValue(timestamp, severity, status,
					quality, stringValue);
		} else {
			return null;
		}
		return new Sample(compressionLevelName, channelName, value,
				precedingSampleTimestamp);
	}

	public static void insertSample(Mutator<byte[]> mutator,
			String compressionLevelName, String channelName, IValue value,
			ITimestamp precedingSampleTime) {
		String columnFamilyName = NAME;
		byte[] key = SampleKey.generateKey(compressionLevelName, channelName,
				value.getTime());
		if (value instanceof IDoubleValue) {
			IDoubleValue doubleValue = (IDoubleValue) value;
			ColumnWriter.insertDoubleArray(mutator, key, columnFamilyName,
					COLUMN_DOUBLE_VALUE, doubleValue.getValues());
			if (value instanceof IMinMaxDoubleValue) {
				IMinMaxDoubleValue minMaxDoubleValue = (IMinMaxDoubleValue) value;
				ColumnWriter.insertDouble(mutator, key, columnFamilyName,
						COLUMN_DOUBLE_MIN, minMaxDoubleValue.getMinimum());
				ColumnWriter.insertDouble(mutator, key, columnFamilyName,
						COLUMN_DOUBLE_MAX, minMaxDoubleValue.getMaximum());
			}
		} else if (value instanceof IEnumeratedValue) {
			IEnumeratedValue enumeratedValue = (IEnumeratedValue) value;
			ColumnWriter.insertIntegerArray(mutator, key, columnFamilyName,
					COLUMN_ENUM_VALUE, enumeratedValue.getValues());
		} else if (value instanceof ILongValue) {
			ILongValue longValue = (ILongValue) value;
			ColumnWriter.insertLongArray(mutator, key, columnFamilyName,
					COLUMN_LONG_VALUE, longValue.getValues());
		} else if (value instanceof IStringValue) {
			IStringValue stringValue = (IStringValue) value;
			ColumnWriter.insertStringArray(mutator, key, columnFamilyName,
					COLUMN_STRING_VALUE, stringValue.getValues());
		}
		IMetaData metaData = value.getMetaData();
		if (metaData != null) {
			if (metaData instanceof IEnumeratedMetaData) {
				insertSampleEnumeratedMetaData(mutator, columnFamilyName, key,
						(IEnumeratedMetaData) metaData);
			} else if (metaData instanceof INumericMetaData) {
				insertSampleNumericMetaData(mutator, columnFamilyName, key,
						(INumericMetaData) metaData);
			}
		}
		byte severity = SeverityMapper.severityToNumber(value.getSeverity());
		ColumnWriter.insertByte(mutator, key, columnFamilyName,
				ColumnFamilySamples.COLUMN_SEVERITY, severity);
		String status = value.getStatus();
		if (status != null) {
			ColumnWriter.insertString(mutator, key, columnFamilyName,
					ColumnFamilySamples.COLUMN_STATUS, status);
		}
		if (precedingSampleTime != null) {
			ColumnWriter.insertTimestamp(mutator, key, columnFamilyName,
					ColumnFamilySamples.COLUMN_PRECEDING_SAMPLE_TIME,
					precedingSampleTime);
		}
	}

	private static IEnumeratedMetaData readSampleEnumeratedMetaData(
			ColumnSlice<String, byte[]> slice) {
		String[] enumStates = ColumnReader.readStringArray(slice,
				COLUMN_META_DATA_ENUM_STATES);
		if (enumStates == null) {
			return null;
		}
		return ValueFactory.createEnumeratedMetaData(enumStates);
	}

	private static void insertSampleEnumeratedMetaData(Mutator<byte[]> mutator,
			String columnFamilyName, byte[] key, IEnumeratedMetaData metaData) {
		ColumnWriter.insertStringArray(mutator, key, columnFamilyName,
				ColumnFamilySamples.COLUMN_META_DATA_ENUM_STATES,
				metaData.getStates());
	}

	private static INumericMetaData readSampleNumericMetaData(
			ColumnSlice<String, byte[]> slice) {
		Double displayLow = ColumnReader.readDouble(slice,
				COLUMN_META_DATA_NUM_DISPLAY_LOW);
		Double displayHigh = ColumnReader.readDouble(slice,
				COLUMN_META_DATA_NUM_DISPLAY_HIGH);
		Double warnLow = ColumnReader.readDouble(slice,
				COLUMN_META_DATA_NUM_WARN_LOW);
		Double warnHigh = ColumnReader.readDouble(slice,
				COLUMN_META_DATA_NUM_WARN_HIGH);
		Double alarmLow = ColumnReader.readDouble(slice,
				COLUMN_META_DATA_NUM_ALARM_LOW);
		Double alarmHigh = ColumnReader.readDouble(slice,
				COLUMN_META_DATA_NUM_ALARM_HIGH);
		Integer precision = ColumnReader.readInteger(slice,
				COLUMN_META_DATA_NUM_PRECISION);
		String units = ColumnReader.readString(slice,
				COLUMN_META_DATA_NUM_UNITS);
		if (displayLow != null && displayHigh != null && warnLow != null
				&& warnHigh != null && alarmLow != null && alarmHigh != null
				&& precision != null && units != null) {
			return ValueFactory.createNumericMetaData(displayLow, displayHigh,
					warnLow, warnHigh, alarmLow, alarmHigh, precision, units);
		} else {
			return null;
		}
	}

	private static void insertSampleNumericMetaData(Mutator<byte[]> mutator,
			String columnFamilyName, byte[] key, INumericMetaData metaData) {
		ColumnWriter.insertDouble(mutator, key, columnFamilyName,
				COLUMN_META_DATA_NUM_DISPLAY_LOW, metaData.getDisplayLow());
		ColumnWriter.insertDouble(mutator, key, columnFamilyName,
				COLUMN_META_DATA_NUM_DISPLAY_LOW, metaData.getDisplayLow());
		ColumnWriter.insertDouble(mutator, key, columnFamilyName,
				COLUMN_META_DATA_NUM_DISPLAY_HIGH, metaData.getDisplayHigh());
		ColumnWriter.insertDouble(mutator, key, columnFamilyName,
				COLUMN_META_DATA_NUM_WARN_LOW, metaData.getWarnLow());
		ColumnWriter.insertDouble(mutator, key, columnFamilyName,
				COLUMN_META_DATA_NUM_WARN_HIGH, metaData.getWarnHigh());
		ColumnWriter.insertDouble(mutator, key, columnFamilyName,
				COLUMN_META_DATA_NUM_ALARM_LOW, metaData.getAlarmLow());
		ColumnWriter.insertDouble(mutator, key, columnFamilyName,
				COLUMN_META_DATA_NUM_ALARM_HIGH, metaData.getAlarmHigh());
		ColumnWriter.insertInteger(mutator, key, columnFamilyName,
				COLUMN_META_DATA_NUM_PRECISION, metaData.getPrecision());
		ColumnWriter.insertString(mutator, key, columnFamilyName,
				COLUMN_META_DATA_NUM_UNITS, metaData.getUnits());
	}
}
