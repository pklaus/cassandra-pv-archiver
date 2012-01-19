/*
 * Copyright 2012 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.csstudio.archive.config.cassandra;

import org.csstudio.archive.config.GroupConfig;

/**
 * Group configuration for the Cassandra archiver.
 * 
 * @author Sebastian Marsching
 * @see CassandraArchiveConfig
 */
public class CassandraGroupConfig extends GroupConfig {

	private String engineName;

	/**
	 * Creates a group configuration object.
	 * 
	 * @param engineName
	 *            name of the engine for this group.
	 * @param groupName
	 *            name of this group.
	 * @param enablingChannel
	 *            name of the channel that enables this group (or the empty
	 *            string if no such channel exists).
	 */
	public CassandraGroupConfig(String engineName, String groupName,
			String enablingChannel) {
		super(groupName, enablingChannel);
		this.engineName = engineName;
	}

	/**
	 * Returns the name of the engine for this group.
	 * 
	 * @return engine name for this group.
	 */
	public String getEngineName() {
		return engineName;
	}
}
