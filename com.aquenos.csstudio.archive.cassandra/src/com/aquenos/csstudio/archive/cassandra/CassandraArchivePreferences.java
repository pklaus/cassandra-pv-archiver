/*
 * Copyright 2012 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.csstudio.archive.cassandra;

import me.prettyprint.cassandra.model.QuorumAllConsistencyLevelPolicy;
import me.prettyprint.cassandra.service.FailoverPolicy;
import me.prettyprint.hector.api.ConsistencyLevelPolicy;

import org.eclipse.core.runtime.Platform;
import org.eclipse.core.runtime.preferences.IPreferencesService;

/**
 * Provides preferences common to all bundles of the Cassandra Archiver. Uses
 * the OSGi preferences service to store preferences.
 * 
 * @author Sebastian Marsching
 */
public class CassandraArchivePreferences {
	private final static String BUNDLE_NAME = "com.aquenos.csstudio.archive.cassandra";
	private final static String PREF_HOSTS = "hosts";
	private final static String PREF_PORT = "port";
	private final static String PREF_KEYSPACE = "keyspace";
	private final static String PREF_USERNAME = "username";
	private final static String PREF_PASSWORD = "password";

	private static String getStringPreference(String name, String defaultValue) {
		IPreferencesService preferenceService = Platform
				.getPreferencesService();
		if (preferenceService != null) {
			String value = preferenceService.getString(BUNDLE_NAME, name, null,
					null);
			if (value != null) {
				return value;
			}
		}
		return defaultValue;
	}

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
	 * Returns the list of Cassandra hosts. This is either a single hostname or
	 * IP address or a list of hostnames and/or IP addresses separated by
	 * commas. Defaults to "localhost".
	 * 
	 * @return string representing the hosts to contact for accessing the
	 *         Cassandra database.
	 */
	public static String getHosts() {
		return getStringPreference(PREF_HOSTS, "localhost");
	}

	/**
	 * Returns the TCP port number used for contacting the Cassandra servers.
	 * The port must be the same for all hosts returned by {@link #getHosts()}
	 * Defaults to 9160.
	 * 
	 * @return port number used to contact the Cassandra database.
	 */
	public static int getPort() {
		return getIntegerPreference(PREF_PORT, 9160);
	}

	/**
	 * Returns the name of the keyspace in the Cassandra cluster, used to
	 * archive data. Defaults to "cssArchive".
	 * 
	 * @return name of the Cassandra keyspace used to store archive data.
	 */
	public static String getKeyspace() {
		return getStringPreference(PREF_KEYSPACE, "cssArchive");
	}

	/**
	 * Returns the username used for authentication with the Cassandra database.
	 * This setting is only used if {@link #getPassword()} also returns a value
	 * that is not <code>null</code>. Defaults to <code>null</code>.
	 * 
	 * @return username used for authentication with the Cassandra database.
	 */
	public static String getUsername() {
		return getStringPreference(PREF_USERNAME, null);
	}

	/**
	 * Returns the password used for authentication with the Cassandra database.
	 * This setting is only used if {@link #getUsername()} also returns a value
	 * that is not <code>null</code>. Defaults to <code>null</code>.
	 * 
	 * @return password used for authentication with the Cassandra database.
	 */
	public static String getPassword() {
		return getStringPreference(PREF_PASSWORD, null);
	}

	/**
	 * Returns the consistency level policy used by the Cassandra client
	 * library. The consistency policy describes how many nodes in a cassandra
	 * cluster data has to be written to, before the write method returns and
	 * how many nodes data has to be read from before it is returned. Defaults
	 * to {@link QuorumAllConsistencyLevelPolicy}, which equals the
	 * <code>QUORUM</code> consistency level as described in the <a
	 * href="http://wiki.apache.org/cassandra/API">Cassandra API
	 * documentation.</a>.
	 * 
	 * @return consistency level policy in use.
	 */
	public static ConsistencyLevelPolicy getConsistencyLevelPolicy() {
		// TODO Add property for consistency-level policy
		return new QuorumAllConsistencyLevelPolicy();
	}

	/**
	 * Returns the fail-over policy used by the Cassandra client library. The
	 * fail-over policy describes, how many nodes the client tries to connect to
	 * before giving up and returning an error. Defaults to
	 * {@link FailoverPolicy#ON_FAIL_TRY_ALL_AVAILABLE}, which means that all
	 * known hosts of the cluster are tried before an error is returned.
	 * 
	 * @return fail-over policy in use.
	 */
	public static FailoverPolicy getFailoverPolicy() {
		// TODO Add property for fail-over policy
		return FailoverPolicy.ON_FAIL_TRY_ALL_AVAILABLE;
	}
}
