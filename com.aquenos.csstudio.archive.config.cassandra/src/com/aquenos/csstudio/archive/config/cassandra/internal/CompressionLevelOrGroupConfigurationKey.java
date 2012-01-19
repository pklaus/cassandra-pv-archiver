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
 * group and compression level column families.
 * 
 * @author Sebastian Marsching
 */
public abstract class CompressionLevelOrGroupConfigurationKey {
	private final static Charset UTF8_CHARSET = Charset.forName("UTF-8");
	private final static byte SEPARATOR = 0;

	public static byte[] generateDatabaseKey(String firstName, String secondName) {
		// Generate the row key for the database. We use the
		// format <MD5 digest> separator <first name> separator <second name> in
		// order to ensure that the rows are distributed among the nodes.
		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			byte[] engine = firstName.getBytes(UTF8_CHARSET);
			byte[] group = secondName.getBytes(UTF8_CHARSET);
			md.update(engine);
			md.update(group);
			byte[] md5 = md.digest();
			ByteBuffer buffer = ByteBuffer.allocate(engine.length
					+ group.length + 18);
			buffer.put(md5);
			buffer.put(SEPARATOR);
			buffer.put(engine);
			buffer.put(SEPARATOR);
			buffer.put(group);
			return buffer.array();
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
		}
	}

	public static String extractFirstName(byte[] databaseKey) {
		// Extract the first key string from the DB key.
		// The DB key has the format <MD5 digest> separator <first name>
		// separator <second name>
		int offset = 17;
		if (databaseKey.length < offset) {
			return null;
		}
		// Skip over first string (engine name) and look for second separator.
		for (int i = offset; i < databaseKey.length; i++) {
			if (databaseKey[i] == SEPARATOR) {
				return new String(databaseKey, offset, i - offset, UTF8_CHARSET);
			}
		}
		return null;
	}

	public static String extractSecondName(byte[] databaseKey) {
		// Extract the second key string from the DB key.
		// The DB key has the format <MD5 digest> separator <first name>
		// separator <second name>
		int offset = 17;
		if (databaseKey.length < offset) {
			return null;
		}
		// Skip over first string (engine name) and look for second separator.
		for (int i = offset; i < databaseKey.length; i++) {
			if (databaseKey[i] == SEPARATOR) {
				offset = i + 1;
				break;
			}
		}
		if (offset > 18) {
			return new String(databaseKey, offset, databaseKey.length - offset,
					UTF8_CHARSET);
		} else {
			return null;
		}
	}
}
