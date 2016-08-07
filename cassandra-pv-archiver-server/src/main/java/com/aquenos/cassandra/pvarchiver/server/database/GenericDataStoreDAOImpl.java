/*
 * Copyright 2015-2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.database;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.context.event.EventListener;
import org.springframework.core.annotation.Order;

import com.aquenos.cassandra.pvarchiver.server.util.FutureUtils;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.annotations.Accessor;
import com.datastax.driver.mapping.annotations.Query;
import com.datastax.driver.mapping.annotations.QueryParameters;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * Implementation of a data access object for storing generic pieces of
 * information. This object encapsulates the logic for accessing the
 * <code>generic_data_store</code> table in the Apache Cassandra database.
 * 
 * @author Sebastian Marsching
 */
public class GenericDataStoreDAOImpl implements ApplicationEventPublisherAware,
        GenericDataStoreDAO, SmartInitializingSingleton {

    /**
     * Interface used for queries using the mapping support of the Cassandra
     * driver.
     * 
     * @author Sebastian Marsching
     */
    @Accessor
    private interface GenericDataStoreAccessor {

        @Query("INSERT INTO " + TABLE + " (" + COLUMN_COMPONENT_ID + ", "
                + COLUMN_ITEM_KEY + ", " + COLUMN_ITEM_VALUE
                + ") VALUES (?, ?, ?) IF NOT EXISTS;")
        ResultSetFuture createItemIfNotExists(UUID componentId, String key,
                String value);

        @Query("INSERT INTO " + TABLE + " (" + COLUMN_COMPONENT_ID + ", "
                + COLUMN_ITEM_KEY + ", " + COLUMN_ITEM_VALUE
                + ") VALUES (?, ?, ?);")
        ResultSetFuture createOrUpdateItem(UUID componentId, String key,
                String value);

        @Query("SELECT * FROM " + TABLE + " WHERE " + COLUMN_COMPONENT_ID
                + " = ?;")
        ResultSetFuture getAllItems(UUID componentId);

        @Query("SELECT * FROM " + TABLE + " WHERE " + COLUMN_COMPONENT_ID
                + " = ? AND " + COLUMN_ITEM_KEY + " = ?;")
        @QueryParameters(fetchSize = Integer.MAX_VALUE)
        ResultSetFuture getItem(UUID componentId, String key);

        @Query("DELETE FROM " + TABLE + " WHERE " + COLUMN_COMPONENT_ID
                + " = ?;")
        ResultSetFuture removeAllItems(UUID componentId);

        @Query("DELETE FROM " + TABLE + " WHERE " + COLUMN_COMPONENT_ID
                + " = ? AND " + COLUMN_ITEM_KEY + " = ?;")
        ResultSetFuture removeItem(UUID componentId, String key);

        @Query("UPDATE " + TABLE + " SET " + COLUMN_ITEM_VALUE + " = ? WHERE "
                + COLUMN_COMPONENT_ID + " = ? AND " + COLUMN_ITEM_KEY
                + " = ? IF " + COLUMN_ITEM_VALUE + " = ?;")
        ResultSetFuture updateItemIfValueMatches(String newValue,
                UUID componentId, String key, String oldValue);

    }

    private static final String TABLE = "generic_data_store";
    private static final String COLUMN_COMPONENT_ID = "component_id";
    private static final String COLUMN_ITEM_KEY = "item_key";
    private static final String COLUMN_ITEM_VALUE = "item_value";

    /**
     * Logger for this object. The logger uses the actual type of this object
     * and the base type where this field is declared.
     */
    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    // We make the accessor volatile because it cannot be created at
    // initialization time if the cluster is not available yet and we do not
    // want to use a mutex.
    private volatile GenericDataStoreAccessor accessor;
    private ApplicationEventPublisher applicationEventPublisher;
    private CassandraProvider cassandraProvider;
    private AtomicBoolean initializationInProgress = new AtomicBoolean();

    /**
     * Handles {@link CassandraProviderInitializedEvent}s. This event is used
     * when the {@link CassandraProvider} set using
     * {@link #setCassandraProvider(CassandraProvider)} is not ready yet when
     * this object is initialized. In this case, some actions that usually are
     * performed during initialization (like creating tables, initializing
     * access objects) have to be deferred until the Apache Cassandra database
     * becomes available.
     * 
     * @param event
     *            initialization event sent by the {@link CassandraProvider}.
     */
    @EventListener
    // DAOs should receive the event before other components that might depend
    // on them.
    @Order(1000)
    public void onCassandraProviderInitializedEvent(
            CassandraProviderInitializedEvent event) {
        if (event.getSource() != this.cassandraProvider) {
            return;
        }
        // We do the initialization in a separate thread so that the calling
        // thread (which is the event notification thread) does not block.
        Thread initializationThread = new Thread() {
            @Override
            public void run() {
                initialize();
            }
        };
        initializationThread.setDaemon(true);
        initializationThread.start();
    }

    /**
     * Sets the Cassandra provider that provides access to the Apache Cassandra
     * database.
     * 
     * @param cassandraProvider
     *            provider that provides a connection to the Apache Cassandra
     *            database.
     */
    @Autowired
    public void setCassandraProvider(CassandraProvider cassandraProvider) {
        this.cassandraProvider = cassandraProvider;
    }

    @Override
    public void afterSingletonsInstantiated() {
        // We do this check here and not in the setCassandraProvider method
        // because only after this method got called we can be sure that we will
        // not miss any events.
        if (this.cassandraProvider.isInitialized()) {
            initialize();
        }
    }

    @Override
    public void setApplicationEventPublisher(
            ApplicationEventPublisher applicationEventPublisher) {
        this.applicationEventPublisher = applicationEventPublisher;
    }

    private void checkInitialized() {
        if (!isInitialized()) {
            throw new IllegalStateException(
                    "The GenericDataStoreDAO has not been completely initialized yet.");
        }
    }

    private void createAccessor() {
        // The accessor should only be created once.
        if (accessor != null) {
            return;
        }
        MappingManager mappingManager = new MappingManager(
                cassandraProvider.getSession());
        // The accessor field is volatile so we can do this assignment without a
        // mutex. In the worst case, we create the accessor twice. This is not
        // optimal for performance reasons, but does not have a major impact and
        // is for sure better than protecting every access with a mutex.
        accessor = mappingManager
                .createAccessor(GenericDataStoreAccessor.class);
    }

    private void createTable() {
        Session session = cassandraProvider.getSession();
        session.execute(SchemaBuilder.createTable(TABLE)
                .addPartitionKey(COLUMN_COMPONENT_ID, DataType.uuid())
                .addClusteringColumn(COLUMN_ITEM_KEY, DataType.text())
                .addColumn(COLUMN_ITEM_VALUE, DataType.text()).ifNotExists());
    }

    private void initialize() {
        // We want to avoid running to initializations threads in parallel
        // because this would generate warnings about preparing the same queries
        // again.
        if (!initializationInProgress.compareAndSet(false, true)) {
            return;
        }
        try {
            createTable();
            createAccessor();
            applicationEventPublisher
                    .publishEvent(new GenericDataStoreDAOInitializedEvent(this));
        } catch (Throwable t) {
            log.error(
                    "The initialization of the GenericDataStoreDAO failed. Most likely, this is caused by the database being temporarily unavailable. The next initialization attempt is going to be made the next time the connection to the database is reestablished.",
                    t);
        } finally {
            initializationInProgress.set(false);
        }
    }

    @Override
    public ListenableFuture<Pair<Boolean, String>> createItem(UUID componentId,
            String key, String value) {
        // We wrap the whole method body in a try-catch block, because we want
        // to avoid this method throwing an exception directly. Instead, we
        // return a future that throws the exception. This way, the calling code
        // can deal with exceptions in a unified way instead of having to deal
        // with exceptions from both this method and the returned future.
        try {
            checkInitialized();
            return Futures.transform(
                    accessor.createItemIfNotExists(componentId, key, value),
                    new Function<ResultSet, Pair<Boolean, String>>() {
                        @Override
                        public Pair<Boolean, String> apply(ResultSet input) {
                            Row row = input.one();
                            boolean applied = input.wasApplied();
                            String existingValue = null;
                            if (!applied) {
                                existingValue = row
                                        .getString(COLUMN_ITEM_VALUE);
                            }
                            return Pair.of(applied, existingValue);
                        }
                    });
        } catch (Exception e) {
            // We do not catch errors, because they typically indicate a very
            // severe exception and it is possible that trying to wrap them in
            // a future would fail again.
            return Futures.immediateFailedFuture(e);
        }
    }

    @Override
    public ListenableFuture<Void> createOrUpdateItem(UUID componentId,
            String key, String value) {
        // We wrap the whole method body in a try-catch block, because we want
        // to avoid this method throwing an exception directly. Instead, we
        // return a future that throws the exception. This way, the calling code
        // can deal with exceptions in a unified way instead of having to deal
        // with exceptions from both this method and the returned future.
        try {
            checkInitialized();
            return FutureUtils.transformAnyToVoid(accessor.createOrUpdateItem(
                    componentId, key, value));
        } catch (Exception e) {
            // We do not catch errors, because they typically indicate a very
            // severe exception and it is possible that trying to wrap them in
            // a future would fail again.
            return Futures.immediateFailedFuture(e);
        }
    }

    @Override
    public ListenableFuture<Void> deleteAllItems(UUID componentId) {
        // We wrap the whole method body in a try-catch block, because we want
        // to avoid this method throwing an exception directly. Instead, we
        // return a future that throws the exception. This way, the calling code
        // can deal with exceptions in a unified way instead of having to deal
        // with exceptions from both this method and the returned future.
        try {
            checkInitialized();
            return FutureUtils.transformAnyToVoid(accessor
                    .removeAllItems(componentId));
        } catch (Exception e) {
            // We do not catch errors, because they typically indicate a very
            // severe exception and it is possible that trying to wrap them in
            // a future would fail again.
            return Futures.immediateFailedFuture(e);
        }
    }

    @Override
    public ListenableFuture<Void> deleteItem(UUID componentId, String key) {
        // We wrap the whole method body in a try-catch block, because we want
        // to avoid this method throwing an exception directly. Instead, we
        // return a future that throws the exception. This way, the calling code
        // can deal with exceptions in a unified way instead of having to deal
        // with exceptions from both this method and the returned future.
        try {
            checkInitialized();
            return FutureUtils.transformAnyToVoid(accessor.removeItem(
                    componentId, key));
        } catch (Exception e) {
            // We do not catch errors, because they typically indicate a very
            // severe exception and it is possible that trying to wrap them in
            // a future would fail again.
            return Futures.immediateFailedFuture(e);
        }
    }

    @Override
    public ListenableFuture<? extends Iterable<? extends DataItem>> getAllItems(
            UUID componentId) {
        // We wrap the whole method body in a try-catch block, because we want
        // to avoid this method throwing an exception directly. Instead, we
        // return a future that throws the exception. This way, the calling code
        // can deal with exceptions in a unified way instead of having to deal
        // with exceptions from both this method and the returned future.
        try {
            checkInitialized();
            return Futures.transform(accessor.getAllItems(componentId),
                    new Function<ResultSet, Iterable<? extends DataItem>>() {
                        @Override
                        public Iterable<? extends DataItem> apply(
                                ResultSet input) {
                            final int componentIdIndex = input
                                    .getColumnDefinitions().getIndexOf(
                                            COLUMN_COMPONENT_ID);
                            final int itemKeyIndex = input
                                    .getColumnDefinitions().getIndexOf(
                                            COLUMN_ITEM_KEY);
                            final int itemValueIndex = input
                                    .getColumnDefinitions().getIndexOf(
                                            COLUMN_ITEM_VALUE);
                            return Iterables.transform(input,
                                    new Function<Row, DataItem>() {
                                        @Override
                                        public DataItem apply(Row input) {
                                            return new DataItem(
                                                    input.getUUID(componentIdIndex),
                                                    input.getString(itemKeyIndex),
                                                    input.getString(itemValueIndex));
                                        }
                                    });
                        }
                    });
        } catch (Exception e) {
            // We do not catch errors, because they typically indicate a very
            // severe exception and it is possible that trying to wrap them in
            // a future would fail again.
            return Futures.immediateFailedFuture(e);
        }
    }

    @Override
    public ListenableFuture<? extends DataItem> getItem(UUID componentId,
            String key) {
        // We wrap the whole method body in a try-catch block, because we want
        // to avoid this method throwing an exception directly. Instead, we
        // return a future that throws the exception. This way, the calling code
        // can deal with exceptions in a unified way instead of having to deal
        // with exceptions from both this method and the returned future.
        try {
            checkInitialized();
            return Futures.transform(accessor.getItem(componentId, key),
                    new Function<ResultSet, DataItem>() {
                        @Override
                        public DataItem apply(ResultSet input) {
                            Row row = input.one();
                            if (row == null) {
                                return null;
                            }
                            return new DataItem(row
                                    .getUUID(COLUMN_COMPONENT_ID), row
                                    .getString(COLUMN_ITEM_KEY), row
                                    .getString(COLUMN_ITEM_VALUE));
                        }
                    });
        } catch (Exception e) {
            // We do not catch errors, because they typically indicate a very
            // severe exception and it is possible that trying to wrap them in
            // a future would fail again.
            return Futures.immediateFailedFuture(e);
        }
    }

    @Override
    public boolean isInitialized() {
        return accessor != null;
    }

    @Override
    public ListenableFuture<Pair<Boolean, String>> updateItem(UUID componentId,
            String key, String oldValue, String newValue) {
        // We wrap the whole method body in a try-catch block, because we want
        // to avoid this method throwing an exception directly. Instead, we
        // return a future that throws the exception. This way, the calling code
        // can deal with exceptions in a unified way instead of having to deal
        // with exceptions from both this method and the returned future.
        try {
            checkInitialized();
            return Futures.transform(accessor.updateItemIfValueMatches(
                    newValue, componentId, key, oldValue),
                    new Function<ResultSet, Pair<Boolean, String>>() {
                        @Override
                        public Pair<Boolean, String> apply(ResultSet input) {
                            Row row = input.one();
                            boolean applied = input.wasApplied();
                            String existingValue = null;
                            if (!applied) {
                                try {
                                    existingValue = row
                                            .getString(COLUMN_ITEM_VALUE);
                                } catch (IllegalArgumentException e) {
                                    // If the column does not exist, this simply
                                    // means that the specified row does not
                                    // exist and thus there is no value.
                                }
                            }
                            return Pair.of(applied, existingValue);
                        }
                    });
        } catch (Exception e) {
            // We do not catch errors, because they typically indicate a very
            // severe exception and it is possible that trying to wrap them in
            // a future would fail again.
            return Futures.immediateFailedFuture(e);
        }
    }

}
