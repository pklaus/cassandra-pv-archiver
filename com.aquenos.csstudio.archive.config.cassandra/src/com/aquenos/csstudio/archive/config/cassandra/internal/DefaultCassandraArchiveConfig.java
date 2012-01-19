/*
 * Copyright 2012 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.csstudio.archive.config.cassandra.internal;

import com.aquenos.csstudio.archive.cassandra.CassandraArchivePreferences;
import com.aquenos.csstudio.archive.config.cassandra.CassandraArchiveConfig;

/**
 * Subclass of {@link CassandraArchiveConfig} that provides a
 * default-constructor reading the settings from the
 * {@link CassandraArchivePreferences}. This class is only provided in order to
 * have no dependency on any OSGi service in the {@link CassandraArchiveConfig}
 * class but still be able to provide an extension for the archive configuration
 * extension point.
 * 
 * @author Sebastian Marsching
 */
public class DefaultCassandraArchiveConfig extends CassandraArchiveConfig {

	public DefaultCassandraArchiveConfig() {
		super(CassandraArchivePreferences.getHosts(),
				CassandraArchivePreferences.getPort(),
				CassandraArchivePreferences.getKeyspace(),
				CassandraArchivePreferences.getConsistencyLevelPolicy(),
				CassandraArchivePreferences.getFailoverPolicy(),
				CassandraArchivePreferences.getUsername(),
				CassandraArchivePreferences.getPassword());
	}

}
