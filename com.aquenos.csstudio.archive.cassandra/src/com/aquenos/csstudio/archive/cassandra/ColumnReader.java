/*
 * Copyright 2012 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.csstudio.archive.cassandra;

import java.nio.ByteBuffer;

import me.prettyprint.cassandra.serializers.DoubleSerializer;
import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;

import org.csstudio.data.values.ITimestamp;
import org.csstudio.data.values.TimestampFactory;

/**
 * Provides methods that simplify reading data of certain data-types from column
 * slices retrieved from the Cassandra database. All of these methods expect the
 * column-names to be UTF-8 encoded strings.
 * 
 * @author Sebastian Marsching
 * @see ColumnWriter
 */
public abstract class ColumnReader {

	/**
	 * Reads a UTF-8 encoded string from a column.
	 * 
	 * @param slice
	 *            column slice to read from.
	 * @param columnName
	 *            name of the column to read from.
	 * @return string read or <code>null</code> if the column does not exist
	 *         within the column slice.
	 */
	public static String readString(ColumnSlice<String, byte[]> slice,
			String columnName) {
		HColumn<String, byte[]> column = slice.getColumnByName(columnName);
		if (column != null) {
			return StringSerializer.get()
					.fromByteBuffer(column.getValueBytes());
		} else {
			return null;
		}
	}

	/**
	 * Reads an array of UTF-8 encoded strings from a column. The strings are
	 * expected to be stored as a 32-bit integer specifying the number of
	 * strings followed by a 32-bit integer specifying the number of bytes for
	 * the string and the bytes of the string itself for each string.
	 * 
	 * @param slice
	 *            column slice to read from.
	 * @param columnName
	 *            name of the column to read from.
	 * @return string read or <code>null</code> if the the column does not exist
	 *         within the column slice.
	 */
	public static String[] readStringArray(ColumnSlice<String, byte[]> slice,
			String columnName) {
		HColumn<String, byte[]> column = slice.getColumnByName(columnName);
		if (column == null) {
			return null;
		}
		ByteBuffer buffer = ByteBuffer.wrap(column.getValue());
		int numberOfElements = buffer.getInt();
		String[] valueArray = new String[numberOfElements];
		for (int i = 0; i < numberOfElements; i++) {
			int stringLength = buffer.getInt();
			byte[] stringBytes = new byte[stringLength];
			buffer.get(stringBytes);
			valueArray[i] = StringSerializer.get().fromBytes(stringBytes);
		}
		return valueArray;
	}

	/**
	 * Reads a byte from a column. If the column contains more than a single
	 * byte, only the first byte is returned.
	 * 
	 * @param slice
	 *            column slice to read from.
	 * @param columnName
	 *            name of the column to read from.
	 * @return byte read or <code>null</code> if the column is empty or does not
	 *         exist within the column slice.
	 */
	public static Byte readByte(ColumnSlice<String, byte[]> slice,
			String columnName) {
		HColumn<String, byte[]> column = slice.getColumnByName(columnName);
		if (column != null) {
			byte[] value = column.getValue();
			if (value.length > 0) {
				return value[0];
			} else {
				return null;
			}
		} else {
			return null;
		}
	}

	/**
	 * Reads an array of all bytes stored in a column.
	 * 
	 * @param slice
	 *            column slice to read from.
	 * @param columnName
	 *            name of the column to read from.
	 * @return array containing all bytes stored in the column or
	 *         <code>null</code> if the column does not exist within the column
	 *         slice.
	 */
	public static byte[] readByteArray(ColumnSlice<String, byte[]> slice,
			String columnName) {
		HColumn<String, byte[]> column = slice.getColumnByName(columnName);
		if (column != null) {
			return column.getValue();
		} else {
			return null;
		}
	}

	/**
	 * Reads a double from a column. If the column contains more than eight
	 * bytes, all bytes following the first eight bytes are ignored.
	 * 
	 * @param slice
	 *            column slice to read from.
	 * @param columnName
	 *            name of the column to read from.
	 * @return double value stored in the column or <code>null</code> if the
	 *         column contains less than eight bytes or does not exist within
	 *         the column slice.
	 */
	public static Double readDouble(ColumnSlice<String, byte[]> slice,
			String columnName) {
		HColumn<String, byte[]> column = slice.getColumnByName(columnName);
		if (column != null) {
			ByteBuffer buffer = column.getValueBytes();
			if (buffer.remaining() >= 8) {
				return DoubleSerializer.get().fromByteBuffer(
						column.getValueBytes());
			}
		}
		return null;
	}

	/**
	 * Reads an array of doubles from a column. If the number of bytes stored in
	 * the column is not a multiple of eight, the bytes following the last
	 * integer multiple of eight are ignored.
	 * 
	 * @param slice
	 *            column slice to read from.
	 * @param columnName
	 *            name of the column to read from.
	 * @return array of double values stored in the column or <code>null</code>
	 *         if the column does not exist within the column slice.
	 */
	public static double[] readDoubleArray(ColumnSlice<String, byte[]> slice,
			String columnName) {
		HColumn<String, byte[]> column = slice.getColumnByName(columnName);
		if (column != null) {
			ByteBuffer buffer = column.getValueBytes();
			int numberOfElements = buffer.remaining() / 8;
			double[] valueArray = new double[numberOfElements];
			for (int i = 0; i < numberOfElements; i++) {
				valueArray[i] = buffer.getDouble();
			}
			return valueArray;
		}
		return null;
	}

	/**
	 * Reads a 32-bit signed integer from a column. If the column contains more
	 * than four bytes, all bytes following the first four bytes are ignored.
	 * 
	 * @param slice
	 *            column slice to read from.
	 * @param columnName
	 *            name of the column to read from.
	 * @return integer value stored in the column or <code>null</code> if the
	 *         column contains less than four bytes or does not exist within the
	 *         column slice.
	 */
	public static Integer readInteger(ColumnSlice<String, byte[]> slice,
			String columnName) {
		HColumn<String, byte[]> column = slice.getColumnByName(columnName);
		if (column != null) {
			ByteBuffer buffer = column.getValueBytes();
			if (buffer.remaining() >= 4) {
				return IntegerSerializer.get().fromByteBuffer(
						column.getValueBytes());
			}
		}
		return null;
	}

	/**
	 * Reads an array of 32-bit signed integers from a column. If the number of
	 * bytes stored in the column is not a multiple of four, the bytes following
	 * the last integer multiple of four are ignored.
	 * 
	 * @param slice
	 *            column slice to read from.
	 * @param columnName
	 *            name of the column to read from.
	 * @return array of integer values stored in the column or <code>null</code>
	 *         if the column does not exist within the column slice.
	 */
	public static int[] readIntegerArray(ColumnSlice<String, byte[]> slice,
			String columnName) {
		HColumn<String, byte[]> column = slice.getColumnByName(columnName);
		if (column != null) {
			ByteBuffer buffer = column.getValueBytes();
			int numberOfElements = buffer.remaining() / 8;
			int[] valueArray = new int[numberOfElements];
			for (int i = 0; i < numberOfElements; i++) {
				valueArray[i] = buffer.getInt();
			}
			return valueArray;
		}
		return null;
	}

	/**
	 * Reads a 64-bit signed integer from a column. If the column contains more
	 * than eight bytes, all bytes following the first eight bytes are ignored.
	 * 
	 * @param slice
	 *            column slice to read from.
	 * @param columnName
	 *            name of the column to read from.
	 * @return long integer value stored in the column or <code>null</code> if
	 *         the column contains less than eight bytes or does not exist
	 *         within the column slice.
	 */
	public static Long readLong(ColumnSlice<String, byte[]> slice,
			String columnName) {
		HColumn<String, byte[]> column = slice.getColumnByName(columnName);
		if (column != null) {
			ByteBuffer buffer = column.getValueBytes();
			if (buffer.remaining() >= 8) {
				return LongSerializer.get().fromByteBuffer(
						column.getValueBytes());
			}
		}
		return null;
	}

	/**
	 * Reads an array of 64-bit signed integers from a column. If the number of
	 * bytes stored in the column is not a multiple of eight, the bytes
	 * following the last integer multiple of eight are ignored.
	 * 
	 * @param slice
	 *            column slice to read from.
	 * @param columnName
	 *            name of the column to read from.
	 * @return array of integer values stored in the column or <code>null</code>
	 *         if the column does not exist within the column slice.
	 */
	public static long[] readLongArray(ColumnSlice<String, byte[]> slice,
			String columnName) {
		HColumn<String, byte[]> column = slice.getColumnByName(columnName);
		if (column != null) {
			ByteBuffer buffer = column.getValueBytes();
			int numberOfElements = buffer.remaining() / 8;
			long[] valueArray = new long[numberOfElements];
			for (int i = 0; i < numberOfElements; i++) {
				valueArray[i] = buffer.getLong();
			}
			return valueArray;
		}
		return null;
	}

	/**
	 * Reads a timestamp from a column. The timestamp is expected as first a
	 * 64-bit signed integer specifying the number of seconds since 1970-01-01
	 * 00:00:00 UTC, followed by a 32-bit signed integer specifying the number
	 * nanoseconds to add to the number of seconds. Only values with a number of
	 * seconds greater than or equal zero and a number of nanoseconds greater
	 * than or equal zero but less than one billion are considered valid. If the
	 * column contains more than twelve bytes, all following bytes are ignored.
	 * 
	 * @param slice
	 *            column slice to read from.
	 * @param columnName
	 *            name of the column to read from.
	 * @return timestamp read from the column or <code>null</code> if the column
	 *         contains less than twelve bytes or does not exist within the
	 *         column slice.
	 */
	public static ITimestamp readTimestamp(ColumnSlice<String, byte[]> slice,
			String columnName) {
		HColumn<String, byte[]> column = slice.getColumnByName(columnName);
		if (column != null) {
			ByteBuffer buffer = column.getValueBytes();
			if (buffer.remaining() >= 12) {
				long seconds = buffer.getLong();
				int nanoseconds = buffer.getInt();
				return TimestampFactory.createTimestamp(seconds, nanoseconds);
			}
		}
		return null;
	}

}
