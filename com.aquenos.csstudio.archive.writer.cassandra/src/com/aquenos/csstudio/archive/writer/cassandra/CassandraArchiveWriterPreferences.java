/*
 * Copyright 2012 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.csstudio.archive.writer.cassandra;

import org.eclipse.core.runtime.Platform;
import org.eclipse.core.runtime.preferences.IPreferencesService;

/**
 * Provides preferences that are specific to the writer bundle of the Cassandra
 * Archiver. Uses the OSGi preferences service to store preferences.
 * 
 * @author Sebastian Marsching
 */
public class CassandraArchiveWriterPreferences {
	private final static String BUNDLE_NAME = "com.aquenos.csstudio.archive.writer.cassandra";
	private final static String PREF_NUM_COMPRESSOR_WORKERS = "numCompressorWorkers";

	private static int getIntegerPreference(String name, int defaultValue) {
		IPreferencesService preferenceService = Platform
				.getPreferencesService();
		if (preferenceService != null) {
			return preferenceService.getInt(BUNDLE_NAME, name, defaultValue,
					null);
		}
		return defaultValue;
	}

	/**
	 * Returns the number of workers the sample compressor should use. If this
	 * setting is less than one, the sample compressor is not activated.
	 * Defaults to <code>1</code>.
	 * 
	 * @return number of worker threads used for the sample compressor.
	 */
	public static int getNumberOfCompressorWorkers() {
		return getIntegerPreference(PREF_NUM_COMPRESSOR_WORKERS, 1);
	}

}
