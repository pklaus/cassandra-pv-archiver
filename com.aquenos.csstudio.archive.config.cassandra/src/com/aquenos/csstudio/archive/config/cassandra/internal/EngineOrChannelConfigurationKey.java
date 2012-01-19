/*
 * Copyright 2012 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.csstudio.archive.config.cassandra.internal;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Provides methods for generating keys and extracting data from keys for the
 * engine and channel column families.
 * 
 * @author Sebastian Marsching
 */
public abstract class EngineOrChannelConfigurationKey {
	private final static Charset UTF8_CHARSET = Charset.forName("UTF-8");
	private final static byte SEPARATOR = 0;

	public static byte[] generateDatabaseKey(String realKey) {
		// Generate the row key for the database. We use the
		// format <MD5 digest> separator <real key> in order to ensure
		// that the rows are distributed among the nodes.
		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			byte[] key = realKey.getBytes(UTF8_CHARSET);
			byte[] md5 = md.digest(key);
			ByteBuffer buffer = ByteBuffer.allocate(key.length + 17);
			buffer.put(md5);
			buffer.put(SEPARATOR);
			buffer.put(key);
			return buffer.array();
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
		}
	}

	public static String extractRealKey(byte[] databaseKey) {
		// Extract the actual key string from the DB key.
		// The DB key has the format <MD5 digest> 0 <real key>
		int offset = 17;
		if (databaseKey.length < offset) {
			return null;
		}
		return new String(databaseKey, offset, databaseKey.length - offset,
				UTF8_CHARSET);
	}
}
