/*
 * Copyright 2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.tests;

import java.util.UUID;

import org.cassandraunit.utils.EmbeddedCassandraServerHelper;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

/**
 * Embedded Cassandra server that can be used for running tests. This class uses
 * CassandraUnit's {@link EmbeddedCassandraServerHelper} for creating an
 * embedded Cassandra instance using a random port number. It provides
 * {@link Session} instances for accessing this embedded server through the
 * {@link #getSession()} method.
 * 
 * @author Sebastian Marsching
 */
public final class EmbeddedCassandraServer {

    private static Cluster cluster;
    private static final Object initializationLock = new Object();
    private static boolean initialized;

    private EmbeddedCassandraServer() {
    }

    /**
     * <p>
     * Returns a new session that is connected to the embedded Cassandra
     * instance. Each time this method is called, a new session is created and
     * connected to a newly created keyspace with a randomly generated name.
     * </p>
     * 
     * <p>
     * In order to save resources (each sessions uses valuable resources), each
     * component test should try to reuse the same session across many test
     * cases, typically by calling this method from a static methods annotated
     * with JUnit's <code>@BeforeClass</code> annotation.
     * </p>
     * 
     * <p>
     * This method will block, waiting for the Cassandra server to be
     * initialized, the keyspace to be created, and the session to be connected.
     * </p>
     * 
     * @return new session connected to an empty keyspace.
     */
    public static Session getSession() {
        initializeCassandraServer();
        Session session = cluster.connect();
        String randomKeyspaceName = "ks"
                + UUID.randomUUID().toString().replace("-", "");
        session.execute("CREATE KEYSPACE "
                + randomKeyspaceName
                + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};");
        session.execute("USE " + randomKeyspaceName + ";");
        return session;
    }

    private static void initializeCassandraServer() {
        synchronized (initializationLock) {
            if (initialized) {
                return;
            }
            try {
                EmbeddedCassandraServerHelper
                        .startEmbeddedCassandra(EmbeddedCassandraServerHelper.CASSANDRA_RNDPORT_YML_FILE);
            } catch (Exception e) {
                throw new RuntimeException(
                        "The initialization of the embedded Cassandra server failed.",
                        e);
            }
            String cassandraHost = EmbeddedCassandraServerHelper.getHost();
            int cassandraPort = EmbeddedCassandraServerHelper
                    .getNativeTransportPort();
            cluster = Cluster.builder().addContactPoint(cassandraHost)
                    .withPort(cassandraPort).build();
            initialized = true;
        }
    }

}
