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
import java.nio.charset.Charset;

import me.prettyprint.cassandra.serializers.ByteBufferSerializer;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.DoubleSerializer;
import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;

import org.csstudio.data.values.ITimestamp;

/**
 * Contains methods that simplify writing data of certain types to columns in
 * the Cassandra database. All of these methods expect the column-names to be
 * UTF-8 encoded strings. All row keys are expected as arrays of bytes.
 * 
 * @author Sebastian Marsching
 * @see ColumnReader
 * 
 */
public abstract class ColumnWriter {

	private final static Charset UTF8_CHARSET = Charset.forName("UTF-8");

	/**
	 * Writes a UTF8-encoded string to a column.
	 * 
	 * @param mutator
	 *            mutator that is used to perform the insert operation.
	 * @param key
	 *            key identifying the row in the column family.
	 * @param columnFamily
	 *            name of column family to write to.
	 * @param column
	 *            name of the column to write to.
	 * @param value
	 *            value being written to the column.
	 */
	public static void insertString(Mutator<byte[]> mutator, byte[] key,
			String columnFamily, String column, String value) {
		mutator.addInsertion(key, columnFamily, HFactory.createColumn(column,
				value, StringSerializer.get(), StringSerializer.get()));
	}

	/**
	 * Writes an array of UTF8-encoded strings to a column. First a 32-bit
	 * signed integer specifying the number of strings in the array is written.
	 * Then, for each string in the array first the number of bytes the string
	 * consists of is written as a 32-bit signed integer, followed by the bytes
	 * themselves.
	 * 
	 * @param mutator
	 *            mutator that is used to perform the insert operation.
	 * @param key
	 *            key identifying the row in the column family.
	 * @param columnFamily
	 *            name of column family to write to.
	 * @param column
	 *            name of the column to write to.
	 * @param valueArray
	 *            array of values being written to the column.
	 */
	public static void insertStringArray(Mutator<byte[]> mutator, byte[] key,
			String columnFamily, String column, String[] valueArray) {
		byte[][] buffers = new byte[valueArray.length][];
		int totalLength = 0;
		for (int i = 0; i < valueArray.length; i++) {
			buffers[i] = valueArray[i].getBytes(UTF8_CHARSET);
			totalLength += 4 + buffers[i].length;
		}
		// For an array of strings we also save the number of elements,
		// because we cannot calculate it from the buffer length.
		ByteBuffer buffer = ByteBuffer.allocate(4 + totalLength);
		buffer.putInt(valueArray.length);
		for (byte[] byteArray : buffers) {
			buffer.putInt(byteArray.length);
			buffer.put(byteArray);
		}
		buffer.rewind();
		mutator.addInsertion(key, columnFamily, HFactory.createColumn(column,
				buffer, StringSerializer.get(), ByteBufferSerializer.get()));
	}

	/**
	 * Writes a single byte to a column.
	 * 
	 * @param mutator
	 *            mutator that is used to perform the insert operation.
	 * @param key
	 *            key identifying the row in the column family.
	 * @param columnFamily
	 *            name of column family to write to.
	 * @param column
	 *            name of the column to write to.
	 * @param value
	 *            value being written to the column.
	 */
	public static void insertByte(Mutator<byte[]> mutator, byte[] key,
			String columnFamily, String column, byte value) {
		mutator.addInsertion(key, columnFamily, HFactory.createColumn(column,
				new byte[] { value }, StringSerializer.get(),
				BytesArraySerializer.get()));
	}

	/**
	 * Writes an array of bytes to a column. All bytes are written as they are.
	 * 
	 * @param mutator
	 *            mutator that is used to perform the insert operation.
	 * @param key
	 *            key identifying the row in the column family.
	 * @param columnFamily
	 *            name of column family to write to.
	 * @param column
	 *            name of the column to write to.
	 * @param valueArray
	 *            array of bytes being written to the column.
	 */
	public static void insertByteArray(Mutator<byte[]> mutator, byte[] key,
			String columnFamily, String column, byte[] value) {
		mutator.addInsertion(key, columnFamily, HFactory.createColumn(column,
				value, StringSerializer.get(), BytesArraySerializer.get()));
	}

	/**
	 * Writes a double to a column as eight bytes of data.
	 * 
	 * @param mutator
	 *            mutator that is used to perform the insert operation.
	 * @param key
	 *            key identifying the row in the column family.
	 * @param columnFamily
	 *            name of column family to write to.
	 * @param column
	 *            name of the column to write to.
	 * @param value
	 *            value being written to the column.
	 */
	public static void insertDouble(Mutator<byte[]> mutator, byte[] key,
			String columnFamily, String column, double value) {
		mutator.addInsertion(key, columnFamily, HFactory.createColumn(column,
				value, StringSerializer.get(), DoubleSerializer.get()));
	}

	/**
	 * Writes an array of doubles to a column. Eight bytes are written for each
	 * value stored in the array.
	 * 
	 * @param mutator
	 *            mutator that is used to perform the insert operation.
	 * @param key
	 *            key identifying the row in the column family.
	 * @param columnFamily
	 *            name of column family to write to.
	 * @param column
	 *            name of the column to write to.
	 * @param valueArray
	 *            array of values being written to the column.
	 */
	public static void insertDoubleArray(Mutator<byte[]> mutator, byte[] key,
			String columnFamily, String column, double[] valueArray) {
		ByteBuffer buffer = ByteBuffer.allocate(valueArray.length * 8);
		for (double value : valueArray) {
			buffer.putDouble(value);
		}
		buffer.rewind();
		mutator.addInsertion(key, columnFamily, HFactory.createColumn(column,
				buffer, StringSerializer.get(), ByteBufferSerializer.get()));
	}

	/**
	 * Writes a 32-bit signed integer to a column.
	 * 
	 * @param mutator
	 *            mutator that is used to perform the insert operation.
	 * @param key
	 *            key identifying the row in the column family.
	 * @param columnFamily
	 *            name of column family to write to.
	 * @param column
	 *            name of the column to write to.
	 * @param value
	 *            value being written to the column.
	 */
	public static void insertInteger(Mutator<byte[]> mutator, byte[] key,
			String columnFamily, String column, int value) {
		mutator.addInsertion(key, columnFamily, HFactory.createColumn(column,
				value, StringSerializer.get(), IntegerSerializer.get()));
	}

	/**
	 * Writes an array of 32-bit signed integers to a column. Four bytes are
	 * written for each value stored in the array.
	 * 
	 * @param mutator
	 *            mutator that is used to perform the insert operation.
	 * @param key
	 *            key identifying the row in the column family.
	 * @param columnFamily
	 *            name of column family to write to.
	 * @param column
	 *            name of the column to write to.
	 * @param valueArray
	 *            array of values being written to the column.
	 */
	public static void insertIntegerArray(Mutator<byte[]> mutator, byte[] key,
			String columnFamily, String column, int[] valueArray) {
		ByteBuffer buffer = ByteBuffer.allocate(valueArray.length * 4);
		for (int value : valueArray) {
			buffer.putInt(value);
		}
		buffer.rewind();
		mutator.addInsertion(key, columnFamily, HFactory.createColumn(column,
				buffer, StringSerializer.get(), ByteBufferSerializer.get()));
	}

	/**
	 * Writes a 64-bit signed integer to a column.
	 * 
	 * @param mutator
	 *            mutator that is used to perform the insert operation.
	 * @param key
	 *            key identifying the row in the column family.
	 * @param columnFamily
	 *            name of column family to write to.
	 * @param column
	 *            name of the column to write to.
	 * @param value
	 *            value being written to the column.
	 */
	public static void insertLong(Mutator<byte[]> mutator, byte[] key,
			String columnFamily, String column, long value) {
		mutator.addInsertion(key, columnFamily, HFactory.createColumn(column,
				value, StringSerializer.get(), LongSerializer.get()));
	}

	/**
	 * Writes an array of 64-bit signed integers to a column. Eight bytes are
	 * written for each value stored in the array.
	 * 
	 * @param mutator
	 *            mutator that is used to perform the insert operation.
	 * @param key
	 *            key identifying the row in the column family.
	 * @param columnFamily
	 *            name of column family to write to.
	 * @param column
	 *            name of the column to write to.
	 * @param valueArray
	 *            array of values being written to the column.
	 */
	public static void insertLongArray(Mutator<byte[]> mutator, byte[] key,
			String columnFamily, String column, long[] valueArray) {
		ByteBuffer buffer = ByteBuffer.allocate(valueArray.length * 8);
		for (long value : valueArray) {
			buffer.putLong(value);
		}
		buffer.rewind();
		mutator.addInsertion(key, columnFamily, HFactory.createColumn(column,
				buffer, StringSerializer.get(), ByteBufferSerializer.get()));
	}

	/**
	 * Writes a timestamp to a column. The timestamp is written as a 64-bit
	 * signed integer specifying the number of seconds since 1970-01-01 00:00:00
	 * UTC followed by a 32-bit-signed integer specifying the number nanoseconds
	 * to add to these seconds.
	 * 
	 * @param mutator
	 *            mutator that is used to perform the insert operation.
	 * @param key
	 *            key identifying the row in the column family.
	 * @param columnFamily
	 *            name of column family to write to.
	 * @param column
	 *            name of the column to write to.
	 * @param value
	 *            value being written to the column.
	 */
	public static void insertTimestamp(Mutator<byte[]> mutator, byte[] key,
			String columnFamily, String column, ITimestamp value) {
		long seconds = value.seconds();
		long nanoseconds = value.nanoseconds();
		if (nanoseconds < 0 || nanoseconds >= 1000000000L) {
			throw new IllegalArgumentException(
					"Timestamps nanosecond field is out of range.");
		}
		ByteBuffer buffer = ByteBuffer.allocate(12);
		buffer.putLong(seconds);
		buffer.putInt((int) nanoseconds);
		buffer.rewind();
		mutator.addInsertion(key, columnFamily, HFactory.createColumn(column,
				buffer, StringSerializer.get(), ByteBufferSerializer.get()));
	}

}
