/*
 * Copyright 2012 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.csstudio.archive.cassandra.internal;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import me.prettyprint.cassandra.serializers.StringSerializer;

import org.csstudio.data.values.ITimestamp;
import org.csstudio.data.values.TimestampFactory;

/**
 * Provides methods for generating keys for rows in the column-family "samples"
 * and for extracting the components from keys in this column-family. This class
 * is only intended for internal use by other classes in the same bundle.
 * 
 * @author Sebastian Marsching
 */
public abstract class SampleKey {
	private final static Charset UTF8_CHARSET = Charset.forName("UTF-8");
	private final static byte SEPARATOR = 0;

	public static byte[] generateKey(String compressionLevelName,
			String channelName, ITimestamp timestamp) {
		// Length of key is:
		// Length of MD5: 16
		// Length of separator: 1
		// Length of compression level name: ?
		// Length of separator: 1
		// Length of channel name: ?
		// Length of separator: 1
		// Length of seconds timestamp: 8
		// Length of nanoseconds timestamp: 4
		compressionLevelName = nullToRaw(compressionLevelName);
		byte[] compressionLevelNameBytes = getBytes(compressionLevelName);
		byte[] channelNameBytes = getBytes(channelName);
		ByteBuffer key = ByteBuffer.allocate(compressionLevelNameBytes.length
				+ channelNameBytes.length + 31);
		byte[] combinedBytes = new byte[compressionLevelNameBytes.length
				+ channelNameBytes.length];
		System.arraycopy(compressionLevelNameBytes, 0, combinedBytes, 0,
				compressionLevelNameBytes.length);
		System.arraycopy(channelNameBytes, 0, combinedBytes,
				compressionLevelNameBytes.length, channelNameBytes.length);
		key.put(getMD5(combinedBytes));
		key.put(SEPARATOR);
		key.put(compressionLevelNameBytes);
		key.put(SEPARATOR);
		key.put(channelNameBytes);
		key.put(SEPARATOR);
		key.putLong(timestamp.seconds());
		key.putInt((int) timestamp.nanoseconds());
		return key.array();
	}

	private static String nullToRaw(String compressionLevelName) {
		if (compressionLevelName == null || compressionLevelName.length() == 0) {
			return "raw";
		} else {
			return compressionLevelName;
		}
	}

	private static byte[] getMD5(byte[] bytes) {
		try {
			return MessageDigest.getInstance("MD5").digest(bytes);
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
		}
	}

	private static byte[] getBytes(String string) {
		return string.getBytes(UTF8_CHARSET);
	}

	public static String extractCompressionLevelName(byte[] rowKey) {
		ByteBuffer buffer = ByteBuffer.wrap(rowKey);
		// Skip over MD5 and first separator.
		int offset = 17;
		buffer.position(offset);
		while (buffer.get() != 0) {
			// Consume the compression level name.
		}
		int stringLength = buffer.position() - offset - 1;
		byte[] stringBuffer = new byte[stringLength];
		buffer.position(offset);
		buffer.get(stringBuffer);
		return StringSerializer.get().fromBytes(stringBuffer);
	}

	public static String extractChannelName(byte[] rowKey) {
		ByteBuffer buffer = ByteBuffer.wrap(rowKey);
		// Skip over MD5 and first separator.
		int offset = 17;
		buffer.position(offset);
		while (buffer.get() != 0) {
			// Consume the compression level name.
		}
		offset = buffer.position();
		while (buffer.get() != 0) {
			// Consume the channel name.
		}
		int stringLength = buffer.position() - offset - 1;
		byte[] stringBuffer = new byte[stringLength];
		buffer.position(offset);
		buffer.get(stringBuffer);
		return StringSerializer.get().fromBytes(stringBuffer);
	}

	public static ITimestamp extractTimestamp(byte[] rowKey) {
		ByteBuffer buffer = ByteBuffer.wrap(rowKey);
		// Skip over MD5 and first separator.
		buffer.position(17);
		while (buffer.get() != 0) {
			// Consume the compression level name.
		}
		while (buffer.get() != 0) {
			// Consume the channel name.
		}
		long seconds = buffer.getLong();
		int nanoseconds = buffer.getInt();
		return TimestampFactory.createTimestamp(seconds, nanoseconds);
	}
}
