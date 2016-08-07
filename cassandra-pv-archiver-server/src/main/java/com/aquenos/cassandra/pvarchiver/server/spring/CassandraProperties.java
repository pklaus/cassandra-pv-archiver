/*
 * Copyright 2015 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.spring;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;

import com.aquenos.cassandra.pvarchiver.server.database.CassandraProvider;

/**
 * <p>
 * Configuration properties that provide the configuration for the
 * {@link CassandraProvider}. This object is injected with properties having the
 * <code>cassandra.*</code> prefix.
 * </p>
 * 
 * <p>
 * The following properties are supported by this configuration object:
 * </p>
 * 
 * <ul>
 * <li><code>cassandra.hosts</code> (see {@link #getHosts()})</li>
 * <li><code>cassandra.port</code> (see {@link #getPort()},
 * {@link #setPort(int)})</li>
 * <li><code>cassandra.username</code> (see {@link #getUsername()},
 * {@link #setUsername(String)})</li>
 * <li><code>cassandra.password</code> (see {@link #getPassword()},
 * {@link #setPassword(String)})</li>
 * <li><code>cassandra.keyspace</code> (see {@link #getKeyspace()},
 * {@link #setKeyspace(String)})</li>
 * <li><code>cassandra.useLocalConsistencyLevel</code> (see
 * {@link #isUseLocalConsistencyLevel()},
 * {@link #setUseLocalConsistencyLevel(boolean)})</li>
 * </ul>
 * 
 * <p>
 * Instances of this class are safe for concurrent read access but are not safe
 * for concurrent write access. Typically, this should not be a problem because
 * an instance of this class is initialized once at application startup and then
 * only used for read access.
 * </p>
 * 
 * @author Sebastian Marsching
 */
@ConfigurationProperties(prefix = "cassandra", ignoreUnknownFields = false)
public class CassandraProperties implements InitializingBean {

    private List<String> hosts;
    private String keyspace = "pv_archive";
    private boolean useLocalConsistencyLevel = false;
    private String password;
    private int port = 9042;
    private String username;

    /**
     * Default constructor. Sets the list of hosts to contain "localhost", the
     * port number to 9042, the keyspace to "pv_archive", and
     * <code>useLocalConsistencyLevel</code> to <code>false</code>.
     */
    public CassandraProperties() {
        this.hosts = new ArrayList<String>();
        this.hosts.add("localhost");
    }

    /**
     * <p>
     * Returns the list of hosts that are uses to discover the cluster topology
     * when initially connecting to the Cassandra cluster. Once the topology has
     * been discovered, all servers in the cluster are used. However, it still
     * makes sense to specify several servers so that the connection with the
     * cluster can be established when one of the servers is down.
     * </p>
     * 
     * <p>
     * After initialization is complete ( {@link #afterPropertiesSet()} has been
     * called), the list returned by this method is read-only and attempts to
     * modify it will result in an {@link UnsupportedOperationException}. Before
     * that, the list can be modified so that the property injection mechanism
     * provided by Spring can work.
     * </p>
     * 
     * <p>
     * This method never returns <code>null</code>. After
     * {@link #afterPropertiesSet()} has been called, the list is guaranteed to
     * not contain any <code>null</code> elements. If null elements have been
     * added to the list, {@link #afterPropertiesSet()} will throw a
     * {@link NullPointerException}.
     * </p>
     * 
     * @return list of hosts that can be used when initially connecting to the
     *         Cassandra cluster.
     */
    public List<String> getHosts() {
        return hosts;
    }

    /**
     * Returns the name of the keyspace that is used by the Cassandra PV
     * Archiver. The default is "pv_archive".
     * 
     * @return name of the keyspace that is used by the archiver instance.
     */
    public String getKeyspace() {
        return keyspace;
    }

    /**
     * Sets the name of the keyspace that is used by the Cassandra PV Archiver.
     * The default is "pv_archive".
     * 
     * @param keyspace
     *            name of the keyspace that shall be used.
     * @throws NullPointerException
     *             if <code>keyspace</code> is <code>null</code>).
     */
    public void setKeyspace(String keyspace) {
        if (keyspace == null) {
            throw new NullPointerException();
        }
        this.keyspace = keyspace;
    }

    /**
     * Tells whether the <code>LOCAL_</code> variants of the consistency levels
     * shall be used. One might want to use these variants if all archiver nodes
     * are operating in the same datacenter, but the Cassandra cluster has a
     * second datacenter that is used for replication (read-only). In this case,
     * the archiver can continue operation when the connection to the second
     * datacenter is interrupted. The default is <code>false</code>.
     * 
     * @return <code>true</code> if the <code>LOCAL_</code> variants of the
     *         consistency levels should be used (e.g. <code>LOCAL_QUORUM</code>
     *         instead of <code>QUORUM</code>), <code>false</code> if the
     *         regular (global) consistency levels should be used.
     */
    public boolean isUseLocalConsistencyLevel() {
        return useLocalConsistencyLevel;
    }

    /**
     * Defines whether the <code>LOCAL_</code> variants of the consistency
     * levels shall be used. One might want to use these variants if all
     * archiver nodes are operating in the same datacenter, but the Cassandra
     * cluster has a second datacenter that is used for replication (read-only).
     * In this case, the archiver can continue operation when the connection to
     * the second datacenter is interrupted. The default is <code>false</code>.
     * 
     * @param useLocalConsistencyLevel
     *            <code>true</code> if the <code>LOCAL_</code> variants of the
     *            consistency levels should be used (e.g.
     *            <code>LOCAL_QUORUM</code> instead of <code>QUORUM</code>),
     *            <code>false</code> if the regular (global) consistency levels
     *            should be used.
     */
    public void setUseLocalConsistencyLevel(boolean useLocalConsistencyLevel) {
        this.useLocalConsistencyLevel = useLocalConsistencyLevel;
    }

    /**
     * Returns the password that is used in combination with the username in
     * order to authenticate with the Cassandra cluster.
     * 
     * @return password used for authentication or <code>null</code> (or the
     *         empty string) if no password has been set.
     * @see #getUsername()
     */
    public String getPassword() {
        return password;
    }

    /**
     * Sets the password that is used in combination with the username in order
     * to authenticate with the Cassandra cluster.
     * 
     * @param password
     *            password used for authentication (or <code>null</code> or the
     *            empty string if no password is required).
     * @see #setUsername(String)
     */
    public void setPassword(String password) {
        this.password = password;
    }

    /**
     * Returns the port number on which the Cassandra database servers listen.
     * This port number must be identical for all hosts in the Cassandra
     * cluster. The default is 9042.
     * 
     * @return port number of the Cassandra database server(s).
     */
    public int getPort() {
        return port;
    }

    /**
     * Sets the port number on which the Cassandra database servers listen. This
     * port number must be identical for all hosts in the Cassandra cluster. The
     * default is 9042.
     * 
     * @param port
     *            port number of the Cassandra database server(s).
     * @throws IllegalArgumentException
     *             if <code>port</code> is less than 1 or greater than 65535.
     */
    public void setPort(int port) {
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("Invalid port number: " + port
                    + ". Port number must be between 1 and 65535.");
        }
        this.port = port;
    }

    /**
     * Returns the username used in combination with the password in order to
     * authenticate with the Cassandra cluster. If empty or <code>null</code>,
     * no authentication attempt is made and anonymous access is used. The
     * default is <code>null</code>.
     * 
     * @return username used for authentication or <code>null</code> (or the
     *         empty string) if anonymous access shall be used.
     * @see #getPassword()
     */
    public String getUsername() {
        return username;
    }

    /**
     * Sets the username used in combination with the password in order to
     * authenticate with the Cassandra cluster. If empty or <code>null</code>,
     * no authentication attempt is made and anonymous access is used. The
     * default is <code>null</code>.
     * 
     * @param username
     *            username used for authentication or <code>null</code> (or the
     *            empty string) if anonymous access shall be used.
     * @see #setPassword(String)
     */
    public void setUsername(String username) {
        this.username = username;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        for (String host : hosts) {
            if (host == null) {
                throw new NullPointerException(
                        "Lists of hosts must not contain null elements.");
            }
        }
        // We wrap the hosts list in an unmodifiable list so that it cannot be
        // changed any longer.
        hosts = Collections.unmodifiableList(hosts);
    }

}
