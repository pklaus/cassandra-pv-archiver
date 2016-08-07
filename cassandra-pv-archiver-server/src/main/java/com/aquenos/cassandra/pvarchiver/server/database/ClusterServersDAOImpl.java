/*
 * Copyright 2015-2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.database;

import java.util.Date;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

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
 * Implementation of the data access object for accessing the servers within the
 * archiving cluster. This object encapsulates the logic for accessing the
 * <code>cluster_servers</code> table in the Apache Cassandra database.
 * 
 * @author Sebastian Marsching
 */
public class ClusterServersDAOImpl implements ApplicationEventPublisherAware,
        ClusterServersDAO, SmartInitializingSingleton {

    /**
     * Interface used for queries using the mapping support of the Cassandra
     * driver.
     * 
     * @author Sebastian Marsching
     */
    @Accessor
    private interface ClusterServersAccessor {

        @Query("INSERT INTO " + TABLE + " (" + COLUMN_CLUSTER_ID + ", "
                + COLUMN_SERVER_ID + ", " + COLUMN_SERVER_NAME + ", "
                + COLUMN_INTER_NODE_COMMUNICATION_URL + ", "
                + COLUMN_LAST_ONLINE_TIME + ") VALUES ("
                + GLOBAL_CLUSTER_ID_STRING + ", ?, ?, ?, ?);")
        ResultSetFuture createServer(UUID serverId, String serverName,
                String interNodeCommunicationUrl, Date lastOnlineTime);

        @Query("SELECT * FROM " + TABLE + " WHERE " + COLUMN_CLUSTER_ID + " = "
                + GLOBAL_CLUSTER_ID_STRING + ";")
        ResultSetFuture getAllServers();

        @Query("SELECT * FROM " + TABLE + " WHERE " + COLUMN_CLUSTER_ID + " = "
                + GLOBAL_CLUSTER_ID_STRING + " AND " + COLUMN_SERVER_ID
                + " = ?;")
        @QueryParameters(fetchSize = Integer.MAX_VALUE)
        ResultSetFuture getServer(UUID serverId);

        @Query("DELETE FROM " + TABLE + " WHERE " + COLUMN_CLUSTER_ID + " = "
                + GLOBAL_CLUSTER_ID_STRING + " AND " + COLUMN_SERVER_ID
                + " = ?;")
        ResultSetFuture removeServer(UUID serverId);

        @Query("INSERT INTO " + TABLE + " (" + COLUMN_CLUSTER_ID + ", "
                + COLUMN_SERVER_ID + ", " + COLUMN_LAST_ONLINE_TIME
                + ") VALUES (" + GLOBAL_CLUSTER_ID_STRING + ", ?, ?);")
        ResultSetFuture updateServerLastOnlineTime(UUID serverId,
                Date lastOnlineTime);

    }

    /**
     * Logger for this object. The logger uses the actual type of this object
     * and the base type where this field is declared.
     */
    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    private static final String GLOBAL_CLUSTER_ID_STRING = "bf78db6c-c2f5-422f-906e-a6892a68b51c";
    private static final String TABLE = "cluster_servers";
    private static final String COLUMN_CLUSTER_ID = "cluster_id";
    private static final String COLUMN_SERVER_ID = "server_id";
    private static final String COLUMN_SERVER_NAME = "server_name";
    private static final String COLUMN_INTER_NODE_COMMUNICATION_URL = "inter_node_communication_url";
    private static final String COLUMN_LAST_ONLINE_TIME = "last_online_time";

    // We make the accessor volatile because it cannot be created at
    // initialization time if the cluster is not available yet and we do not
    // want to use a mutex.
    private volatile ClusterServersAccessor accessor;
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
                    "The ClusterServersDAO has not been completely initialized yet.");
        }
    }

    private void createTable() {
        Session session = cassandraProvider.getSession();
        session.execute(SchemaBuilder
                .createTable(TABLE)
                .addPartitionKey(COLUMN_CLUSTER_ID, DataType.uuid())
                .addClusteringColumn(COLUMN_SERVER_ID, DataType.uuid())
                .addColumn(COLUMN_SERVER_NAME, DataType.text())
                .addColumn(COLUMN_INTER_NODE_COMMUNICATION_URL, DataType.text())
                .addColumn(COLUMN_LAST_ONLINE_TIME, DataType.timestamp())
                .ifNotExists());
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
        accessor = mappingManager.createAccessor(ClusterServersAccessor.class);
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
                    .publishEvent(new ClusterServersDAOInitializedEvent(this));
        } catch (Throwable t) {
            log.error(
                    "The initialization of the ClusterServersDAO failed. Most likely, this is caused by the database being temporarily unavailable. The next initialization attempt is going to be made the next time the connection to the database is reestablished.",
                    t);
        } finally {
            initializationInProgress.set(false);
        }
    }

    @Override
    public ListenableFuture<Void> createOrUpdateServer(UUID serverId,
            String serverName, String interNodeCommunicationUrl,
            Date lastOnlineTime) {
        // We wrap the whole method body in a try-catch block, because we want
        // to avoid this method throwing an exception directly. Instead, we
        // return a future that throws the exception. This way, the calling code
        // can deal with exceptions in a unified way instead of having to deal
        // with exceptions from both this method and the returned future.
        try {
            checkInitialized();
            return FutureUtils.transformAnyToVoid(accessor.createServer(
                    serverId, serverName, interNodeCommunicationUrl,
                    lastOnlineTime));
        } catch (Exception e) {
            // We do not catch errors, because they typically indicate a very
            // severe exception and it is possible that trying to wrap them in
            // a future would fail again.
            return Futures.immediateFailedFuture(e);
        }
    }

    @Override
    public ListenableFuture<Void> deleteServer(UUID serverId) {
        // We wrap the whole method body in a try-catch block, because we want
        // to avoid this method throwing an exception directly. Instead, we
        // return a future that throws the exception. This way, the calling code
        // can deal with exceptions in a unified way instead of having to deal
        // with exceptions from both this method and the returned future.
        try {
            checkInitialized();
            return FutureUtils.transformAnyToVoid(accessor
                    .removeServer(serverId));
        } catch (Exception e) {
            // We do not catch errors, because they typically indicate a very
            // severe exception and it is possible that trying to wrap them in
            // a future would fail again.
            return Futures.immediateFailedFuture(e);
        }
    }

    @Override
    public ListenableFuture<? extends ClusterServer> getServer(UUID serverId) {
        // We wrap the whole method body in a try-catch block, because we want
        // to avoid this method throwing an exception directly. Instead, we
        // return a future that throws the exception. This way, the calling code
        // can deal with exceptions in a unified way instead of having to deal
        // with exceptions from both this method and the returned future.
        try {
            checkInitialized();
            return Futures.transform(accessor.getServer(serverId),
                    new Function<ResultSet, ClusterServer>() {
                        @Override
                        public ClusterServer apply(ResultSet input) {
                            Row row = input.one();
                            if (row == null) {
                                return null;
                            }
                            return new ClusterServer(
                                    row.getString(COLUMN_INTER_NODE_COMMUNICATION_URL),
                                    row.getTimestamp(COLUMN_LAST_ONLINE_TIME),
                                    row.getUUID(COLUMN_SERVER_ID), row
                                            .getString(COLUMN_SERVER_NAME));
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
    public ListenableFuture<? extends Iterable<? extends ClusterServer>> getServers() {
        // We wrap the whole method body in a try-catch block, because we want
        // to avoid this method throwing an exception directly. Instead, we
        // return a future that throws the exception. This way, the calling code
        // can deal with exceptions in a unified way instead of having to deal
        // with exceptions from both this method and the returned future.
        try {
            checkInitialized();
            return Futures
                    .transform(
                            accessor.getAllServers(),
                            new Function<ResultSet, Iterable<? extends ClusterServer>>() {
                                @Override
                                public Iterable<? extends ClusterServer> apply(
                                        final ResultSet input) {
                                    final int serverIdIndex = input
                                            .getColumnDefinitions().getIndexOf(
                                                    COLUMN_SERVER_ID);
                                    final int serverNameIndex = input
                                            .getColumnDefinitions().getIndexOf(
                                                    COLUMN_SERVER_NAME);
                                    final int interNodeCommunicationIndex = input
                                            .getColumnDefinitions()
                                            .getIndexOf(
                                                    COLUMN_INTER_NODE_COMMUNICATION_URL);
                                    final int lastOnlineTimeIndex = input
                                            .getColumnDefinitions().getIndexOf(
                                                    COLUMN_LAST_ONLINE_TIME);
                                    return Iterables.transform(input,
                                            new Function<Row, ClusterServer>() {
                                                @Override
                                                public ClusterServer apply(
                                                        Row input) {
                                                    return new ClusterServer(
                                                            input.getString(interNodeCommunicationIndex),
                                                            input.getTimestamp(lastOnlineTimeIndex),
                                                            input.getUUID(serverIdIndex),
                                                            input.getString(serverNameIndex));
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
    public boolean isInitialized() {
        return accessor != null;
    }

    @Override
    public ListenableFuture<Void> updateServerLastOnlineTime(UUID serverId,
            Date lastOnlineTime) {
        // We wrap the whole method body in a try-catch block, because we want
        // to avoid this method throwing an exception directly. Instead, we
        // return a future that throws the exception. This way, the calling code
        // can deal with exceptions in a unified way instead of having to deal
        // with exceptions from both this method and the returned future.
        try {
            checkInitialized();
            return FutureUtils.transformAnyToVoid(accessor
                    .updateServerLastOnlineTime(serverId, lastOnlineTime));
        } catch (Exception e) {
            // We do not catch errors, because they typically indicate a very
            // severe exception and it is possible that trying to wrap them in
            // a future would fail again.
            return Futures.immediateFailedFuture(e);
        }
    }

}
