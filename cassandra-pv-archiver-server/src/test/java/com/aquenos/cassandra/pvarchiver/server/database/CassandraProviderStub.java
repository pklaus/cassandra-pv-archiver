/*
 * Copyright 2015-2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.database;

import org.junit.Rule;
import org.junit.rules.ExternalResource;

import com.aquenos.cassandra.pvarchiver.tests.EmbeddedCassandraServer;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * Simple stub implementation of the {@link CassandraProvider}, using
 * CassandraUnit to start an embedded Cassandra server and creating a keyspace
 * that can be used for test purposes. This stub is designed to be used with a
 * {@link Rule} annotation in JUnit tests.
 * 
 * @author Sebastian Marsching
 */
public class CassandraProviderStub extends ExternalResource implements
        CassandraProvider {

    private Cluster cluster;
    private Session session;

    @Override
    public Cluster getCluster() {
        return this.cluster;
    }

    @Override
    public ListenableFuture<Cluster> getClusterFuture() {
        return Futures.immediateFuture(cluster);
    }

    @Override
    public boolean isInitialized() {
        return true;
    }

    @Override
    public Session getSession() {
        return session;
    }

    @Override
    public ListenableFuture<Session> getSessionFuture() {
        return Futures.immediateFuture(session);
    }

    @Override
    protected void after() {
        // We only close the session, not the cluster because the cluster might
        // still be used by other sessions.
        session.close();
        this.session = null;
        this.cluster = null;
    }

    @Override
    protected void before() {
        session = EmbeddedCassandraServer.getSession();
        cluster = session.getCluster();
    }

}
