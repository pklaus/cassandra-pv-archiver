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

import java.util.UUID;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.ClassRule;
import org.junit.Test;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;

import com.aquenos.cassandra.pvarchiver.server.database.GenericDataStoreDAO.DataItem;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;

/**
 * Tests for the {@link GenericDataStoreDAOImpl}.
 * 
 * @author Sebastian Marsching
 */
public class GenericDataStoreDAOImplTest {

    /**
     * Cassandra provider that provides access to the embedded Cassandra server.
     */
    @ClassRule
    public static CassandraProviderStub cassandraProvider = new CassandraProviderStub();

    private GenericDataStoreDAOImpl dao;

    /**
     * Creates the test suite.
     */
    public GenericDataStoreDAOImplTest() {
        dao = new GenericDataStoreDAOImpl();
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
     * Tests adding, modifying, and removing data items.
     */
    @Test
    public void testCreateUpdateDeleteDataItems() {
        UUID componentId1 = UUID.randomUUID();
        String key1 = "foo";
        String value1a = "bar";
        String value1b = "abc";
        String key2 = "xyz";
        String value2a = "123";
        String value2b = "456";
        assertEquals(0, Iterables.size(Futures.getUnchecked(dao
                .getAllItems(componentId1))));
        Futures.getUnchecked(dao
                .createOrUpdateItem(componentId1, key1, value1a));
        assertEquals(1, Iterables.size(Futures.getUnchecked(dao
                .getAllItems(componentId1))));
        DataItem item1 = Futures.getUnchecked(dao.getItem(componentId1, key1));
        assertEquals(componentId1, item1.getComponentId());
        assertEquals(key1, item1.getKey());
        assertEquals(value1a, item1.getValue());
        Futures.getUnchecked(dao
                .createOrUpdateItem(componentId1, key1, value1b));
        assertEquals(1, Iterables.size(Futures.getUnchecked(dao
                .getAllItems(componentId1))));
        item1 = Futures.getUnchecked(dao.getItem(componentId1, key1));
        assertEquals(value1b, item1.getValue());
        Pair<Boolean, String> operationResult = Futures.getUnchecked(dao
                .createItem(componentId1, key1, value1a));
        assertEquals(false, operationResult.getLeft());
        assertEquals(1, Iterables.size(Futures.getUnchecked(dao
                .getAllItems(componentId1))));
        assertEquals(value1b, operationResult.getRight());
        item1 = Futures.getUnchecked(dao.getItem(componentId1, key1));
        assertEquals(value1b, item1.getValue());
        operationResult = Futures.getUnchecked(dao.updateItem(componentId1,
                key2, "", value2a));
        assertEquals(false, operationResult.getLeft());
        assertNull(operationResult.getRight());
        assertEquals(1, Iterables.size(Futures.getUnchecked(dao
                .getAllItems(componentId1))));
        operationResult = Futures.getUnchecked(dao.createItem(componentId1,
                key2, value2a));
        assertEquals(true, operationResult.getLeft());
        assertNull(operationResult.getRight());
        assertEquals(2, Iterables.size(Futures.getUnchecked(dao
                .getAllItems(componentId1))));
        operationResult = Futures.getUnchecked(dao.updateItem(componentId1,
                key2, value2b, value2b));
        assertEquals(false, operationResult.getLeft());
        assertEquals(value2a, operationResult.getRight());
        operationResult = Futures.getUnchecked(dao.updateItem(componentId1,
                key2, value2a, value2b));
        assertEquals(true, operationResult.getLeft());
        assertNull(operationResult.getRight());
        assertEquals(value2b,
                Futures.getUnchecked(dao.getItem(componentId1, key2))
                        .getValue());
        Futures.getUnchecked(dao.deleteItem(componentId1, key1));
        assertEquals(1, Iterables.size(Futures.getUnchecked(dao
                .getAllItems(componentId1))));
        assertNull(Futures.getUnchecked(dao.getItem(componentId1, key1)));
        Futures.getUnchecked(dao.deleteAllItems(componentId1));
        assertEquals(0, Iterables.size(Futures.getUnchecked(dao
                .getAllItems(componentId1))));
        assertNull(Futures.getUnchecked(dao.getItem(componentId1, key2)));
    }

}
