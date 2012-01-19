/*
 * Copyright 2012 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.csstudio.archive.config.cassandra.internal;

import java.net.URISyntaxException;

import org.csstudio.archive.config.EngineConfig;

import com.aquenos.csstudio.archive.cassandra.ColumnReader;
import com.aquenos.csstudio.archive.cassandra.ColumnWriter;

import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.mutation.Mutator;

/**
 * Provides access to the data stored in the "engineConfiguration" column
 * family. This class is only intended for internal use by other classes in the
 * same bundle.
 * 
 * @author Sebastian Marsching
 */
public abstract class ColumnFamilyEngineConfiguration {
	public final static String NAME = "engineConfiguration";

	public final static String COLUMN_DESCRIPTION = "description";
	public final static String COLUMN_URL = "url";

	public final static String[] ALL_COLUMNS = new String[] {
			COLUMN_DESCRIPTION, COLUMN_URL };

	public static byte[] getKey(String engineName) {
		return EngineOrChannelConfigurationKey.generateDatabaseKey(engineName);
	}

	public static EngineConfig readEngineConfig(byte[] engineKey,
			ColumnSlice<String, byte[]> slice) {
		String name = EngineOrChannelConfigurationKey.extractRealKey(engineKey);
		String description = ColumnReader.readString(slice,
				ColumnFamilyEngineConfiguration.COLUMN_DESCRIPTION);
		String url = ColumnReader.readString(slice,
				ColumnFamilyEngineConfiguration.COLUMN_URL);
		if (description != null && url != null) {
			try {
				return new EngineConfig(name, description, url);
			} catch (URISyntaxException e) {
				return null;
			}
		} else {
			return null;
		}
	}

	public static void insertEngineConfig(Mutator<byte[]> mutator,
			String engineName, String description, String engineUrl)
			throws URISyntaxException {
		byte[] engineKey = EngineOrChannelConfigurationKey
				.generateDatabaseKey(engineName);
		ColumnWriter
				.insertString(mutator, engineKey,
						ColumnFamilyEngineConfiguration.NAME,
						ColumnFamilyEngineConfiguration.COLUMN_DESCRIPTION,
						description);
		ColumnWriter.insertString(mutator, engineKey,
				ColumnFamilyEngineConfiguration.NAME,
				ColumnFamilyEngineConfiguration.COLUMN_URL, engineUrl);
	}
}
