/*
 * Copyright 2015-2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.database;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Date;
import java.util.UUID;

import org.junit.ClassRule;
import org.junit.Test;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;

import com.aquenos.cassandra.pvarchiver.server.database.ClusterServersDAO.ClusterServer;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;

/**
 * Tests for the {@link ClusterServersDAOImpl}.
 * 
 * @author Sebastian Marsching
 */
public class ClusterServersDAOImplTest {

    /**
     * Cassandra provider that provides access to the embedded Cassandra server.
     */
    @ClassRule
    public static CassandraProviderStub cassandraProvider = new CassandraProviderStub();

    private ClusterServersDAOImpl dao;

    /**
     * Creates the test suite.
     */
    public ClusterServersDAOImplTest() {
        dao = new ClusterServersDAOImpl();
        dao.setApplicationEventPublisher(new ApplicationEventPublisher() {
            @Override
            public void publishEvent(Object event) {
            }

            @Override
            public void publishEvent(ApplicationEvent event) {
            }
        });
        dao.setCassandraProvider(cassandraProvider);
        dao.afterSingletonsInstantiated();
    }

    /**
     * Tests adding, modifying, and removing cluster servers.
     */
    @Test
    public void testCreateUpdateDeleteClusterServers() {
        ClusterServer server1 = new ClusterServer("abc", new Date(123L),
                UUID.randomUUID(), "foobar");
        ClusterServer server2 = new ClusterServer("xyz", new Date(0L),
                UUID.randomUUID(), "test");
        assertEquals(0, Iterables.size(Futures.getUnchecked(dao.getServers())));
        Futures.getUnchecked(dao.createOrUpdateServer(server1.getServerId(),
                server1.getServerName(),
                server1.getInterNodeCommunicationUrl(),
                server1.getLastOnlineTime()));
        assertEquals(1, Iterables.size(Futures.getUnchecked(dao.getServers())));
        assertEquals(server1,
                Futures.getUnchecked(dao.getServer(server1.getServerId())));
        Futures.getUnchecked(dao.createOrUpdateServer(server2.getServerId(),
                server2.getServerName(),
                server2.getInterNodeCommunicationUrl(),
                server2.getLastOnlineTime()));
        assertEquals(2, Iterables.size(Futures.getUnchecked(dao.getServers())));
        assertEquals(server2,
                Futures.getUnchecked(dao.getServer(server2.getServerId())));
        ClusterServer server1Updated = new ClusterServer("foo", new Date(),
                server1.getServerId(), "123");
        Futures.getUnchecked(dao.createOrUpdateServer(
                server1Updated.getServerId(), server1Updated.getServerName(),
                server1Updated.getInterNodeCommunicationUrl(),
                server1Updated.getLastOnlineTime()));
        assertEquals(2, Iterables.size(Futures.getUnchecked(dao.getServers())));
        assertEquals(server1Updated, Futures.getUnchecked(dao
                .getServer(server1Updated.getServerId())));
        ClusterServer server2Updated = new ClusterServer(
                server2.getInterNodeCommunicationUrl(), new Date(987L),
                server2.getServerId(), server2.getServerName());
        Futures.getUnchecked(dao.updateServerLastOnlineTime(
                server2Updated.getServerId(),
                server2Updated.getLastOnlineTime()));
        assertEquals(2, Iterables.size(Futures.getUnchecked(dao.getServers())));
        assertEquals(server2Updated, Futures.getUnchecked(dao
                .getServer(server2Updated.getServerId())));
        Futures.getUnchecked(dao.deleteServer(server1.getServerId()));
        assertEquals(1, Iterables.size(Futures.getUnchecked(dao.getServers())));
        assertNull(Futures.getUnchecked(dao.getServer(server1.getServerId())));
        Futures.getUnchecked(dao.deleteServer(server2.getServerId()));
        assertEquals(0, Iterables.size(Futures.getUnchecked(dao.getServers())));
        assertNull(Futures.getUnchecked(dao.getServer(server2.getServerId())));
    }

}
