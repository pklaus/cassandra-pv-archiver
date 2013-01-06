/*
 * Copyright 2012-2013 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.csstudio.archive.cassandra;

import org.eclipse.core.runtime.Platform;
import org.eclipse.core.runtime.preferences.IPreferencesService;

import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.retry.RetryNTimes;
import com.netflix.astyanax.retry.RetryPolicy;

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
    private final static String PREF_READ_DATA_CL = "readDataConsistencyLevel";
    private final static String PREF_WRITE_DATA_CL = "writeDataConsistencyLevel";
    private final static String PREF_READ_META_DATA_CL = "readMetaDataConsistencyLevel";
    private final static String PREF_WRITE_META_DATA_CL = "writeMetaDataConsistencyLevel";

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

    private static ConsistencyLevel getConsistencyLevelPreference(String name,
            ConsistencyLevel defaultValue) {
        IPreferencesService preferenceService = Platform
                .getPreferencesService();
        if (preferenceService != null) {
            String enumString = preferenceService.getString(BUNDLE_NAME, name,
                    null, null);
            if (enumString != null) {
                try {
                    return ConsistencyLevel.valueOf("CL_"
                            + enumString.toUpperCase());
                } catch (IllegalArgumentException e) {
                    return defaultValue;
                }
            }
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
     * Returns the consistency level policy used by the Cassandra client library
     * when reading sample data. The consistency policy describes how many nodes
     * in a cassandra cluster data has to be written to, before the write method
     * returns and how many nodes data has to be read from before it is
     * returned. Defaults to {@link ConsistencyLevel#CL_QUORUM}, which equals
     * the <code>QUORUM</code> consistency level as described in the <a
     * href="http://wiki.apache.org/cassandra/API">Cassandra API
     * documentation.</a>.
     * 
     * @return consistency level policy in use.
     */
    public static ConsistencyLevel getReadDataConsistencyLevel() {
        return getConsistencyLevelPreference(PREF_READ_DATA_CL,
                ConsistencyLevel.CL_QUORUM);
    }

    /**
     * Returns the consistency level policy used by the Cassandra client library
     * when writing sample data. The consistency policy describes how many nodes
     * in a cassandra cluster data has to be written to, before the write method
     * returns and how many nodes data has to be read from before it is
     * returned. Defaults to {@link ConsistencyLevel#CL_QUORUM}, which equals
     * the <code>QUORUM</code> consistency level as described in the <a
     * href="http://wiki.apache.org/cassandra/API">Cassandra API
     * documentation.</a>.
     * 
     * @return consistency level policy in use.
     */
    public static ConsistencyLevel getWriteDataConsistencyLevel() {
        return getConsistencyLevelPreference(PREF_WRITE_DATA_CL,
                ConsistencyLevel.CL_QUORUM);
    }

    /**
     * Returns the consistency level policy used by the Cassandra client library
     * when reading meta-data (configuration and sample bucket sizes). The
     * consistency policy describes how many nodes in a cassandra cluster data
     * has to be written to, before the write method returns and how many nodes
     * data has to be read from before it is returned. Defaults to
     * {@link ConsistencyLevel#CL_QUORUM}, which equals the <code>QUORUM</code>
     * consistency level as described in the <a
     * href="http://wiki.apache.org/cassandra/API">Cassandra API
     * documentation.</a>.
     * 
     * @return consistency level policy in use.
     */
    public static ConsistencyLevel getReadMetaDataConsistencyLevel() {
        return getConsistencyLevelPreference(PREF_READ_META_DATA_CL,
                ConsistencyLevel.CL_QUORUM);
    }

    /**
     * Returns the consistency level policy used by the Cassandra client library
     * when writing meta-data (configuration and sample bucket sizes). The
     * consistency policy describes how many nodes in a cassandra cluster data
     * has to be written to, before the write method returns and how many nodes
     * data has to be read from before it is returned. Defaults to
     * {@link ConsistencyLevel#CL_QUORUM}, which equals the <code>QUORUM</code>
     * consistency level as described in the <a
     * href="http://wiki.apache.org/cassandra/API">Cassandra API
     * documentation.</a>.
     * 
     * @return consistency level policy in use.
     */
    public static ConsistencyLevel getWriteMetaDataConsistencyLevel() {
        return getConsistencyLevelPreference(PREF_WRITE_META_DATA_CL,
                ConsistencyLevel.CL_QUORUM);
    }

    /**
     * Returns the retry policy used by the Cassandra client library. The retry
     * policy describes, how often the client tries to reconnect before giving
     * up and returning an error. Defaults to {@link RetryNTimes} with a max
     * attempts parameter of 2, which means that an action is retried twice
     * before failing finally.
     * 
     * @return retry policy in use.
     */
    public static RetryPolicy getRetryPolicy() {
        // TODO Add property for retry policy.
        return new RetryNTimes(2);
    }
}
