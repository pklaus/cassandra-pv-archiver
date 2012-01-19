/*
 * Copyright 2012 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.csstudio.archive.config.cassandra.internal;

/**
 * Provides access to the data stored in the "engineConfigurationToGroups"
 * column family. This class is only intended for internal use by other classes
 * in the same bundle.
 * 
 * @author Sebastian Marsching
 */
public abstract class ColumnFamilyEngineConfigurationToGroups {
	public final static String NAME = "engineConfigurationToGroups";

	public static byte[] getKey(String engineName) {
		return EngineOrChannelConfigurationKey.generateDatabaseKey(engineName);
	}
}
