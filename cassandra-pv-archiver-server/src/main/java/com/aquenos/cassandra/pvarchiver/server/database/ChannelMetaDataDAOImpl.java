/*
 * Copyright 2015-2017 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.database;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.event.EventListener;
import org.springframework.core.annotation.Order;

import com.aquenos.cassandra.pvarchiver.common.AbstractObjectResultSet;
import com.aquenos.cassandra.pvarchiver.common.ObjectResultSet;
import com.aquenos.cassandra.pvarchiver.server.util.FutureUtils;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.WriteTimeoutException;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.annotations.Accessor;
import com.datastax.driver.mapping.annotations.Query;
import com.datastax.driver.mapping.annotations.QueryParameters;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.common.util.concurrent.SettableFuture;

import io.netty.util.internal.ThreadLocalRandom;

/**
 * Implementation of the data-access object for accessing the channel meta-data.
 * This object encapsulates the logic for accessing the <code>channels</code>,
 * <code>channels_by_server</code>, and
 * <code>pending_operations_by_server</code> tables in the Apache Cassandra
 * database.
 * 
 * @author Sebastian Marsching
 */
public class ChannelMetaDataDAOImpl implements ChannelMetaDataDAO,
        InitializingBean, SmartInitializingSingleton {

    /**
     * Interfaces used for queries to the <code>channels</code> table using the
     * mapping support of the Cassandra driver.
     * 
     * @author Sebastian Marsching
     */
    @Accessor
    private interface ChannelsAccessor {

        @Query("SELECT DISTINCT " + COLUMN_CHANNEL_NAME + ", "
                + COLUMN_CHANNEL_DATA_ID + ", " + COLUMN_CONTROL_SYSTEM_TYPE
                + ", " + COLUMN_DECIMATION_LEVELS + ", " + COLUMN_SERVER_ID
                + " FROM " + TABLE_CHANNELS + ";")
        ResultSetFuture getAllChannels();

        @Query("SELECT DISTINCT " + COLUMN_CHANNEL_NAME + ", "
                + COLUMN_CHANNEL_DATA_ID + ", " + COLUMN_CONTROL_SYSTEM_TYPE
                + ", " + COLUMN_DECIMATION_LEVELS + ", " + COLUMN_SERVER_ID
                + " FROM " + TABLE_CHANNELS + " WHERE " + COLUMN_CHANNEL_NAME
                + " = ?;")
        @QueryParameters(fetchSize = Integer.MAX_VALUE)
        ResultSetFuture getChannel(String channelName);

        @Query("SELECT * FROM " + TABLE_CHANNELS + " WHERE "
                + COLUMN_CHANNEL_NAME + " = ?;")
        ResultSetFuture getChannelWithSampleBuckets(String channelName);

        @Query("SELECT " + COLUMN_CHANNEL_NAME + ", " + COLUMN_CHANNEL_DATA_ID
                + ", " + COLUMN_DECIMATION_LEVEL + ", "
                + COLUMN_BUCKET_START_TIME + ", " + COLUMN_BUCKET_END_TIME
                + " FROM " + TABLE_CHANNELS + " WHERE " + COLUMN_CHANNEL_NAME
                + " = ?;")
        ResultSetFuture getSampleBuckets(String channelName);

        // Even though it looks irritating, we have to use the decimation_level
        // column instead of the bucket_start_time column in the ORDER BY clause
        // and this actually results in the desired effect. See
        // https://issues.apache.org/jira/browse/CASSANDRA-10271 and
        // http://stackoverflow.com/questions/13285461/ for more information.
        @Query("SELECT " + COLUMN_CHANNEL_NAME + ", " + COLUMN_CHANNEL_DATA_ID
                + ", " + COLUMN_DECIMATION_LEVEL + ", "
                + COLUMN_BUCKET_START_TIME + ", " + COLUMN_BUCKET_END_TIME
                + " FROM " + TABLE_CHANNELS + " WHERE " + COLUMN_CHANNEL_NAME
                + " = ? AND " + COLUMN_DECIMATION_LEVEL + " = ? ORDER BY "
                + COLUMN_DECIMATION_LEVEL + " ASC;")
        ResultSetFuture getSampleBuckets(String channelName,
                int decimationLevel);

        @Query("SELECT " + COLUMN_CHANNEL_NAME + ", " + COLUMN_CHANNEL_DATA_ID
                + ", " + COLUMN_DECIMATION_LEVEL + ", "
                + COLUMN_BUCKET_START_TIME + ", " + COLUMN_BUCKET_END_TIME
                + " FROM " + TABLE_CHANNELS + " WHERE " + COLUMN_CHANNEL_NAME
                + " = ? AND " + COLUMN_DECIMATION_LEVEL + " = ? ORDER BY "
                + COLUMN_DECIMATION_LEVEL + " DESC LIMIT ?;")
        ResultSetFuture getSampleBucketsReversed(String channelName,
                int decimationLevel, int limit);

        @Query("SELECT " + COLUMN_CHANNEL_NAME + ", " + COLUMN_CHANNEL_DATA_ID
                + ", " + COLUMN_DECIMATION_LEVEL + ", "
                + COLUMN_BUCKET_START_TIME + ", " + COLUMN_BUCKET_END_TIME
                + " FROM " + TABLE_CHANNELS + " WHERE " + COLUMN_CHANNEL_NAME
                + " = ? AND " + COLUMN_DECIMATION_LEVEL + " = ? AND "
                + COLUMN_BUCKET_START_TIME + " <= ? ORDER BY "
                + COLUMN_DECIMATION_LEVEL + " DESC LIMIT ?;")
        ResultSetFuture getSampleBucketsReversedWithStartTimeLessThanOrEqualTo(
                String channelName, int decimationLevel,
                long startTimeLessThanOrEqualTo, int limit);

        @Query("SELECT " + COLUMN_CHANNEL_NAME + ", " + COLUMN_CHANNEL_DATA_ID
                + ", " + COLUMN_DECIMATION_LEVEL + ", "
                + COLUMN_BUCKET_START_TIME + ", " + COLUMN_BUCKET_END_TIME
                + " FROM " + TABLE_CHANNELS + " WHERE " + COLUMN_CHANNEL_NAME
                + " = ? AND " + COLUMN_DECIMATION_LEVEL + " = ? AND "
                + COLUMN_BUCKET_START_TIME + " >= ? ORDER BY "
                + COLUMN_DECIMATION_LEVEL + " ASC LIMIT ?;")
        ResultSetFuture getSampleBucketsWithStartTimeGreaterThanOrEqualTo(
                String channelName, int decimationLevel,
                long startTimeGreaterThanOrEqualTo, int limit);

        @Query("SELECT " + COLUMN_CHANNEL_NAME + ", " + COLUMN_CHANNEL_DATA_ID
                + ", " + COLUMN_DECIMATION_LEVEL + ", "
                + COLUMN_BUCKET_START_TIME + ", " + COLUMN_BUCKET_END_TIME
                + " FROM " + TABLE_CHANNELS + " WHERE " + COLUMN_CHANNEL_NAME
                + " = ? AND " + COLUMN_DECIMATION_LEVEL + " = ? AND "
                + COLUMN_BUCKET_START_TIME + " >= ? AND "
                + COLUMN_BUCKET_START_TIME + " <=  ? ORDER BY "
                + COLUMN_DECIMATION_LEVEL + " ASC;")
        ResultSetFuture getSampleBucketsWithStartTimeGreaterThanOrEqualToAndLessThanOrEqualTo(
                String channelName, int decimationLevel,
                long startTimeGreaterThanOrEqualTo,
                long startTimeLessThanOrEqualTo);

        @Query("INSERT INTO " + TABLE_CHANNELS + " (" + COLUMN_CHANNEL_NAME
                + ", " + COLUMN_CHANNEL_DATA_ID + ", "
                + COLUMN_CONTROL_SYSTEM_TYPE + ", " + COLUMN_DECIMATION_LEVELS
                + ", " + COLUMN_SERVER_ID + ") VALUES (?, ?, ?, ?, ?);")
        Statement prepareCreateChannel(String channelName, UUID channelDataId,
                String controlSystemType, Set<Integer> decimationLevels,
                UUID serverId);

        @Query("UPDATE " + TABLE_CHANNELS + " SET " + COLUMN_DECIMATION_LEVELS
                + " = " + COLUMN_DECIMATION_LEVELS + " + ? WHERE "
                + COLUMN_CHANNEL_NAME + " = ?;")
        Statement prepareCreateDecimationLevel(Set<Integer> decimationLevels,
                String channelName);

        @Query("INSERT INTO " + TABLE_CHANNELS + " (" + COLUMN_CHANNEL_NAME
                + ", " + COLUMN_DECIMATION_LEVEL + ", "
                + COLUMN_BUCKET_START_TIME + ", " + COLUMN_BUCKET_END_TIME
                + ") VALUES (?, ?, ?, ?);")
        Statement prepareCreateSampleBucket(String channelName,
                int decimationLevel, long bucketStartTime, long bucketEndTime);

        @Query("DELETE FROM " + TABLE_CHANNELS + " WHERE " + COLUMN_CHANNEL_NAME
                + " = ?;")
        Statement prepareDeleteChannel(String channelName);

        @Query("UPDATE " + TABLE_CHANNELS + " SET " + COLUMN_DECIMATION_LEVELS
                + " = " + COLUMN_DECIMATION_LEVELS + " - ? WHERE "
                + COLUMN_CHANNEL_NAME + " = ?;")
        Statement prepareDeleteDecimationLevels(Set<Integer> decimationLevels,
                String channelName);

        @Query("DELETE FROM " + TABLE_CHANNELS + " WHERE " + COLUMN_CHANNEL_NAME
                + " = ? AND " + COLUMN_DECIMATION_LEVEL + " = ? AND "
                + COLUMN_BUCKET_START_TIME + " = ?;")
        Statement prepareDeleteSampleBucket(String channelName,
                int decimationLevel, long bucketStartTime);

        @Query("DELETE FROM " + TABLE_CHANNELS + " WHERE " + COLUMN_CHANNEL_NAME
                + " = ? AND " + COLUMN_DECIMATION_LEVEL + " = ?;")
        Statement prepareDeleteSampleBuckets(String channelName,
                int decimationLevel);

        @Query("UPDATE " + TABLE_CHANNELS + " SET " + COLUMN_SERVER_ID
                + " = ? WHERE " + COLUMN_CHANNEL_NAME + " = ?;")
        Statement prepareUpdateChannelServerId(UUID newServerId,
                String channelName);

        @Query("UPDATE " + TABLE_CHANNELS + " SET " + COLUMN_BUCKET_END_TIME
                + " = ? WHERE " + COLUMN_CHANNEL_NAME + " = ? AND "
                + COLUMN_DECIMATION_LEVEL + " = ? AND "
                + COLUMN_BUCKET_START_TIME + " = ?;")
        Statement prepareUpdateSampleBucketEndTime(long newBucketEndTime,
                String channelName, int decimationLevel, long bucketStartTime);

    }

    /**
     * Interfaces used for queries to the <code>channels_by_server</code> table
     * using the mapping support of the Cassandra driver.
     * 
     * @author Sebastian Marsching
     */
    @Accessor
    private interface ChannelsByServerAccessor {

        @Query("SELECT * FROM " + TABLE_CHANNELS_BY_SERVER + " WHERE "
                + COLUMN_SERVER_ID + " = ? AND " + COLUMN_CHANNEL_NAME
                + " = ?;")
        @QueryParameters(fetchSize = Integer.MAX_VALUE)
        ResultSetFuture getChannel(UUID serverId, String channelName);

        @Query("SELECT * FROM " + TABLE_CHANNELS_BY_SERVER + " WHERE "
                + COLUMN_SERVER_ID + " = ?;")
        ResultSetFuture getChannels(UUID serverId);

        @Query("INSERT INTO " + TABLE_CHANNELS_BY_SERVER + " ("
                + COLUMN_SERVER_ID + ", " + COLUMN_CHANNEL_NAME + ", "
                + COLUMN_CHANNEL_DATA_ID + ", " + COLUMN_CONTROL_SYSTEM_TYPE
                + ", " + COLUMN_DECIMATION_LEVEL_TO_CURRENT_BUCKET_START_TIME
                + ", " + COLUMN_DECIMATION_LEVEL_TO_RETENTION_PERIOD + ", "
                + COLUMN_ENABLED + ", " + COLUMN_OPTIONS
                + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?);")
        Statement prepareCreateChannel(UUID serverId, String channelName,
                UUID channelDataId, String controlSystemType,
                Map<Integer, Long> decimationLevelToCurrentBucketStartTime,
                Map<Integer, Integer> decimationLevelToRetentionPeriod,
                boolean enabled, Map<String, String> options);

        @Query("UPDATE " + TABLE_CHANNELS_BY_SERVER + " SET "
                + COLUMN_DECIMATION_LEVEL_TO_CURRENT_BUCKET_START_TIME + " = "
                + COLUMN_DECIMATION_LEVEL_TO_CURRENT_BUCKET_START_TIME
                + " + ?, " + COLUMN_DECIMATION_LEVEL_TO_RETENTION_PERIOD + " = "
                + COLUMN_DECIMATION_LEVEL_TO_RETENTION_PERIOD + " + ? WHERE "
                + COLUMN_SERVER_ID + " = ? AND " + COLUMN_CHANNEL_NAME
                + " = ?;")
        Statement prepareCreateDecimationLevels(
                Map<Integer, Long> decimationLevelToCurrentBucketStartTime,
                Map<Integer, Integer> decimationLevelToRetentionPeriod,
                UUID serverId, String channelName);

        @Query("DELETE FROM " + TABLE_CHANNELS_BY_SERVER + " WHERE "
                + COLUMN_SERVER_ID + " = ? AND " + COLUMN_CHANNEL_NAME
                + " = ?;")
        Statement prepareDeleteChannel(UUID serverId, String channelName);

        @Query("UPDATE " + TABLE_CHANNELS_BY_SERVER + " SET "
                + COLUMN_DECIMATION_LEVEL_TO_CURRENT_BUCKET_START_TIME + " = "
                + COLUMN_DECIMATION_LEVEL_TO_CURRENT_BUCKET_START_TIME
                + " - ?, " + COLUMN_DECIMATION_LEVEL_TO_RETENTION_PERIOD + " = "
                + COLUMN_DECIMATION_LEVEL_TO_RETENTION_PERIOD + " - ? WHERE "
                + COLUMN_SERVER_ID + " = ? AND " + COLUMN_CHANNEL_NAME
                + " = ?;")
        Statement prepareDeleteDecimationLevels(Set<Integer> decimationLevels,
                Set<Integer> decimationLevelsRepeated, UUID serverId,
                String channelName);

        @Query("UPDATE " + TABLE_CHANNELS_BY_SERVER + " SET "
                + COLUMN_DECIMATION_LEVEL_TO_CURRENT_BUCKET_START_TIME
                + "[?] = ? WHERE " + COLUMN_SERVER_ID + " = ? AND "
                + COLUMN_CHANNEL_NAME + " = ?;")
        Statement prepareUpdateChannelCurrentSampleBucket(int decimationLevel,
                long newCurrentSampleBucketStartTime, UUID serverId,
                String channelName);

        @Query("UPDATE " + TABLE_CHANNELS_BY_SERVER + " SET "
                + COLUMN_DECIMATION_LEVEL_TO_RETENTION_PERIOD + " = "
                + COLUMN_DECIMATION_LEVEL_TO_RETENTION_PERIOD + " + ?, "
                + COLUMN_ENABLED + "= ?, " + COLUMN_OPTIONS + " = ? WHERE "
                + COLUMN_SERVER_ID + " = ? AND " + COLUMN_CHANNEL_NAME
                + " = ?;")
        ResultSetFuture updateChannel(
                Map<Integer, Integer> updateDecimationLevelToRetentionPeriod,
                boolean newEnabled, Map<String, String> newOptions,
                UUID serverId, String channelName);

    }

    /**
     * Interfaces used for queries to the
     * <code>pending_channel_operations_by_server</code> table using the mapping
     * support of the Cassandra driver.
     * 
     * @author Sebastian Marsching
     */
    @Accessor
    private interface PendingChannelOperationsByServerAccessor {

        @Query("INSERT INTO " + TABLE_PENDING_CHANNEL_OPERATIONS_BY_SERVER
                + " (" + COLUMN_SERVER_ID + ", " + COLUMN_CHANNEL_NAME + ", "
                + COLUMN_OPERATION_DATA + ", " + COLUMN_OPERATION_ID + ", "
                + COLUMN_OPERATION_TYPE
                + ") VALUES (?, ?, ?, ?, ?) IF NOT EXISTS USING TTL ?;")
        ResultSetFuture createOperationIfNotExists(UUID serverId,
                String channelName, String operationData, UUID operationId,
                String operationType, int ttl);

        @Query("DELETE FROM " + TABLE_PENDING_CHANNEL_OPERATIONS_BY_SERVER
                + " WHERE " + COLUMN_SERVER_ID + " = ? AND "
                + COLUMN_CHANNEL_NAME + " = ? IF " + COLUMN_OPERATION_ID
                + " = ?;")
        ResultSetFuture deleteOperationIfIdMatches(UUID serverId,
                String channelName, UUID operationId);

        @Query("SELECT * FROM " + TABLE_PENDING_CHANNEL_OPERATIONS_BY_SERVER
                + " WHERE " + COLUMN_SERVER_ID + " = ? AND "
                + COLUMN_CHANNEL_NAME + " = ?;")
        @QueryParameters(fetchSize = Integer.MAX_VALUE)
        Statement getOperation(UUID serverId, String channelName);

        @Query("SELECT * FROM " + TABLE_PENDING_CHANNEL_OPERATIONS_BY_SERVER
                + " WHERE " + COLUMN_SERVER_ID + " = ?;")
        ResultSetFuture getOperations(UUID serverId);

        @Query("UPDATE " + TABLE_PENDING_CHANNEL_OPERATIONS_BY_SERVER
                + " USING TTL ? SET " + COLUMN_OPERATION_DATA + " = ?, "
                + COLUMN_OPERATION_ID + " = ?, " + COLUMN_OPERATION_TYPE
                + " = ? WHERE " + COLUMN_SERVER_ID + " = ? AND "
                + COLUMN_CHANNEL_NAME + " = ? IF " + COLUMN_OPERATION_ID
                + " = ?;")
        ResultSetFuture updateOperationIfIdMatches(int ttl,
                String newOperationData, UUID newOperationId,
                String newOperationType, UUID serverId, String channelName,
                UUID oldOperationId);

    }

    /**
     * Function for converting a result set containing data from the
     * <code>channels</code> table into sample bucket meta-data.
     * 
     * @author Sebastian Marsching
     */
    private static class CqlResultSetToSampleBucketInformationSet implements
            Function<ResultSet, ObjectResultSet<SampleBucketInformation>> {

        @Override
        public ObjectResultSet<SampleBucketInformation> apply(ResultSet input) {
            final ResultSet resultSet = input;
            ColumnDefinitions columnDefinitions = resultSet
                    .getColumnDefinitions();
            final int bucketEndTimeIndex = columnDefinitions
                    .getIndexOf(COLUMN_BUCKET_END_TIME);
            final int bucketStartTimeIndex = columnDefinitions
                    .getIndexOf(COLUMN_BUCKET_START_TIME);
            final int channelDataIdIndex = columnDefinitions
                    .getIndexOf(COLUMN_CHANNEL_DATA_ID);
            final int channelNameIndex = columnDefinitions
                    .getIndexOf(COLUMN_CHANNEL_NAME);
            final int decimationLevelIndex = columnDefinitions
                    .getIndexOf(COLUMN_DECIMATION_LEVEL);
            // If there are no sample buckets at all, Cassandra will still
            // return one row because there are static columns. This row will
            // have a null clustering key (decimation level and bucket
            // start-time are null). As we want to filter such a row, we cannot
            // simply transform the ResultSet but have to use a slightly more
            // complex implementation.
            final Predicate<Row> bucketIdNotNull = new Predicate<Row>() {
                @Override
                public boolean apply(Row input) {
                    // We only test the decimation-level column because the
                    // bucket start-time must also be null if the decimation
                    // level is null.
                    return !input.isNull(COLUMN_DECIMATION_LEVEL);
                }
            };
            final Function<Row, SampleBucketInformation> rowToSampleBucketInformation = new Function<Row, SampleBucketInformation>() {
                @Override
                public SampleBucketInformation apply(Row input) {
                    return new SampleBucketInformation(
                            input.getLong(bucketEndTimeIndex),
                            input.getLong(bucketStartTimeIndex),
                            input.getUUID(channelDataIdIndex),
                            input.getString(channelNameIndex),
                            input.getInt(decimationLevelIndex));
                }
            };
            return new AbstractObjectResultSet<SampleBucketInformation>() {
                @Override
                protected ListenableFuture<Iterator<SampleBucketInformation>> fetchNextPage() {
                    int availableWithoutFetching = resultSet
                            .getAvailableWithoutFetching();
                    if (availableWithoutFetching == 0
                            && resultSet.isFullyFetched()) {
                        return Futures.immediateFuture(null);
                    } else if (availableWithoutFetching == 0) {
                        return Futures.transform(resultSet.fetchMoreResults(),
                                new AsyncFunction<ResultSet, Iterator<SampleBucketInformation>>() {
                                    @Override
                                    public ListenableFuture<Iterator<SampleBucketInformation>> apply(
                                            ResultSet input) {
                                        // When the results are
                                        // available, we call
                                        // fetchNextPage again to
                                        // return those results.
                                        return fetchNextPage();
                                    }
                                });
                    } else {
                        return Futures
                                .immediateFuture(
                                        Iterators.transform(
                                                Iterators.filter(
                                                        Iterators
                                                                .limit(resultSet
                                                                        .iterator(),
                                                                        availableWithoutFetching),
                                                        bucketIdNotNull),
                                                rowToSampleBucketInformation));
                    }
                }
            };
        }

    }

    private static final String COLUMN_BUCKET_END_TIME = "bucket_end_time";
    private static final String COLUMN_BUCKET_START_TIME = "bucket_start_time";
    private static final String COLUMN_CHANNEL_DATA_ID = "channel_data_id";
    private static final String COLUMN_CHANNEL_NAME = "channel_name";
    private static final String COLUMN_CONTROL_SYSTEM_TYPE = "control_system_type";
    private static final String COLUMN_DECIMATION_LEVEL = "decimation_level";
    private static final String COLUMN_DECIMATION_LEVELS = "decimation_levels";
    private static final String COLUMN_DECIMATION_LEVEL_TO_CURRENT_BUCKET_START_TIME = "decimation_level_to_current_bucket_start_time";
    private static final String COLUMN_DECIMATION_LEVEL_TO_RETENTION_PERIOD = "decimation_level_to_retention_period";
    private static final String COLUMN_ENABLED = "enabled";
    private static final String COLUMN_OPERATION_DATA = "operation_data";
    private static final String COLUMN_OPERATION_ID = "operation_id";
    private static final String COLUMN_OPERATION_TYPE = "operation_type";
    private static final String COLUMN_OPTIONS = "options";
    private static final String COLUMN_SERVER_ID = "server_id";
    private static final Function<ResultSet, ObjectResultSet<SampleBucketInformation>> FUNCTION_CQL_RESULT_SET_TO_SAMPLE_BUCKET_INFORMATION_SET = new CqlResultSetToSampleBucketInformationSet();
    private static final String TABLE_CHANNELS = "channels";
    private static final String TABLE_CHANNELS_BY_SERVER = "channels_by_server";
    private static final String TABLE_PENDING_CHANNEL_OPERATIONS_BY_SERVER = "pending_channel_operations_by_server";

    /**
     * Logger for this object. The logger uses the actual type of this object
     * and the base type where this field is declared.
     */
    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    private CassandraProvider cassandraProvider;
    private ChannelsAccessor channelsAccessor;
    private ChannelsByServerAccessor channelsByServerAccessor;
    private AtomicBoolean initializationInProgress = new AtomicBoolean();
    private long pendingChannelOperationMaxSleepMilliseconds = 2200L;
    private int pendingChannelOperationMaxTries = 3;
    private long pendingChannelOperationMinSleepMilliseconds = 800L;
    private PendingChannelOperationsByServerAccessor pendingChannelOperationsByServerAccessor;
    private ThreadPoolExecutor renameChannelExecutor = new ThreadPoolExecutor(0,
            1, 30L, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
    private ConsistencyLevel serialConsistencyLevel;
    // The session field is volatile so that we can do the initialization
    // without having to synchronize on a mutex in order to check whether the
    // initialization has completed.
    private volatile Session session;
    private Timer timer;

    private static ListenableFuture<ObjectResultSet<SampleBucketInformation>> resultSetFutureToSampleBucketInformationSetFuture(
            ResultSetFuture resultSetFuture) {
        return Futures.transform(resultSetFuture,
                FUNCTION_CQL_RESULT_SET_TO_SAMPLE_BUCKET_INFORMATION_SET);
    }

    private static void rethrow(Throwable t) {
        if (t instanceof RuntimeException) {
            throw (RuntimeException) t;
        } else if (t instanceof Error) {
            throw (Error) t;
        } else {
            throw new RuntimeException(t);
        }
    }

    private static void rethrowIfNotWriteTimeoutException(Throwable t) {
        if (!(t instanceof WriteTimeoutException)) {
            rethrow(t);
        }
    }

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
     * database. Typically, this should be initialized with a Cassandra provider
     * that provides a throttled session.
     * 
     * @param cassandraProvider
     *            provider that provides a connection to the Apache Cassandra
     *            database.
     */
    public void setCassandraProvider(CassandraProvider cassandraProvider) {
        this.cassandraProvider = cassandraProvider;
    }

    /**
     * <p>
     * Sets the maximum time to wait before retrying a failed write operation
     * (light-weight transaction) for a pending channel operation. The time is
     * specified in milliseconds and should be greater than or equal to the time
     * set through
     * {@link #setPendingChannelOperationMinSleepMilliseconds(long)}.
     * </p>
     * <p>
     * This setting only has an effect if the number of tries (set through
     * {@link #setPendingChannelOperationMaxTries(int)}) is greater than one.
     * </p>
     * <p>
     * If not configured explicitly, the upper limit for the delay is 2200 ms.
     * </p>
     * 
     * @param pendingChannelOperationMaxSleepMilliseconds
     *            max. time to wait between two write attempts (in
     *            milliseconds). Must be greater than or equal to zero.
     * @throws IllegalArgumentException
     *             if <code>pendingChannelOperationMinSleepMilliseconds</code>
     *             is negative.
     * @see #setPendingChannelOperationMinSleepMilliseconds(long)
     * @see #setPendingChannelOperationMaxTries(int)
     */
    public void setPendingChannelOperationMaxSleepMilliseconds(
            long pendingChannelOperationMaxSleepMilliseconds) {
        Preconditions.checkArgument(
                pendingChannelOperationMaxSleepMilliseconds >= 0L,
                "The sleep time must be greater than or equal to zero.");
        this.pendingChannelOperationMaxSleepMilliseconds = pendingChannelOperationMaxSleepMilliseconds;
    }

    /**
     * <p>
     * Set the maximum number of attempts that are made when creating, updating,
     * or deleting a pending channel operation.
     * </p>
     * <p>
     * All modifications regarding pending channel operations are implemented as
     * light-weight transactions (LWTs). These kind of operations have are more
     * likely to timeout than other write operations. For this reason, this
     * parameter allows to define the number of attempts that are made in case
     * of a timeout. In case of a different error, no attempts to retry an
     * operation are made and the operation fails immediately, even if the
     * number of tries has not been reached yet.
     * </p>
     * <p>
     * An artificial delay is introduced between two attempts in order to give
     * an overloaded database cluster a chance to recover. This delay is chosen
     * randomly, but always within the range set through the
     * {@link #setPendingChannelOperationMinSleepMilliseconds(long)} and
     * {@link #setPendingChannelOperationMaxSleepMilliseconds(long)} methods.
     * </p>
     * <p>
     * If not configured explicitly, up to three attempts are made by default.
     * </p>
     * 
     * @param pendingChannelOperationMaxTries
     *            max. number of attempts when creating, updating, or deleting a
     *            pending channel operation. Must be equal to or greater than
     *            one.
     * @throws IllegalArgumentException
     *             if <code>pendingChannelOperationMaxTries</code> is less than
     *             one.
     * @see #setPendingChannelOperationMaxSleepMilliseconds(long)
     * @see #setPendingChannelOperationMinSleepMilliseconds(long)
     */
    public void setPendingChannelOperationMaxTries(
            int pendingChannelOperationMaxTries) {
        Preconditions.checkArgument(pendingChannelOperationMaxTries >= 1,
                "The max. number of tries must at least be one.");
        this.pendingChannelOperationMaxTries = pendingChannelOperationMaxTries;
    }

    /**
     * <p>
     * Sets the minimum time to wait before retrying a failed write operation
     * (light-weight transaction) for a pending channel operation. The time is
     * specified in milliseconds and should be less than or equal to the time
     * set through
     * {@link #setPendingChannelOperationMaxSleepMilliseconds(long)}.
     * </p>
     * <p>
     * This setting only has an effect if the number of tries (set through
     * {@link #setPendingChannelOperationMaxTries(int)}) is greater than one.
     * </p>
     * <p>
     * If not configured explicitly, the upper limit for the delay is 800 ms.
     * </p>
     * 
     * @param pendingChannelOperationMinSleepMilliseconds
     *            min. time to wait between two write attempts (in
     *            milliseconds). Must be greater than or equal to zero.
     * @throws IllegalArgumentException
     *             if <code>pendingChannelOperationMinSleepMilliseconds</code>
     *             is negative.
     * @see #setPendingChannelOperationMaxSleepMilliseconds(long)
     * @see #setPendingChannelOperationMaxTries(int)
     */
    public void setPendingChannelOperationMinSleepMilliseconds(
            long pendingChannelOperationMinSleepMilliseconds) {
        Preconditions.checkArgument(
                pendingChannelOperationMinSleepMilliseconds >= 0L,
                "The sleep time must be greater than or equal to zero.");
        this.pendingChannelOperationMinSleepMilliseconds = pendingChannelOperationMinSleepMilliseconds;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        if (pendingChannelOperationMaxSleepMilliseconds < pendingChannelOperationMinSleepMilliseconds) {
            pendingChannelOperationMaxSleepMilliseconds = pendingChannelOperationMinSleepMilliseconds;
        }
        timer = new Timer("Timer-ChannelMetaDataDAOImpl", true);
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
    public ListenableFuture<Void> createChannel(String channelName,
            UUID channelDataId, String controlSystemType,
            Set<Integer> decimationLevels,
            Map<Integer, Integer> decimationLevelToRetentionPeriod,
            boolean enabled, Map<String, String> options, UUID serverId) {
        // We wrap the whole method body in a try-catch block, because we want
        // to avoid this method throwing an exception directly. Instead, we
        // return a future that throws the exception. This way, the calling code
        // can deal with exceptions in a unified way instead of having to deal
        // with exceptions from both this method and the returned future.
        try {
            if (log.isDebugEnabled()) {
                log.debug("createChannel(channelName=\""
                        + StringEscapeUtils.escapeJava(channelName)
                        + ", channelDataId=" + channelDataId
                        + ", controlSystemType=" + controlSystemType
                        + ", ..., enabled=" + enabled + ", ..., serverId="
                        + serverId + ")");
            }
            checkInitialized();
            // When creating a channel, we have to add it to both the channels
            // and channels_by_server tables. We do this in a batch, so that
            // both changes or neither change is applied. There is no guarantee
            // of atomicity. Therefore, the calling code has to ensure that the
            // channel does not exist when attempting to create it and that it
            // is not created concurrently by another client.
            BatchStatement batchStatement = new BatchStatement();
            batchStatement.add(channelsAccessor.prepareCreateChannel(
                    channelName, channelDataId, controlSystemType,
                    decimationLevels, serverId));
            Map<Integer, Long> decimationLevelToCurrentBucketStartTime = Maps
                    .asMap(decimationLevels, Functions.constant(-1L));
            // Cassandra collections do not support null values and we want to
            // convert negative retention periods to zero, so that the data is
            // homogeneous. We also filter the map to remove entries for
            // decimation levels that are not listed in the set. If a decimation
            // level is listed in the set but does not have an entry in the
            // map, we add an entry with a value of zero.
            Map<Integer, Integer> decimationLevelToRetentionPeriodConverted = new HashMap<Integer, Integer>();
            decimationLevelToRetentionPeriodConverted.putAll(
                    Maps.asMap(decimationLevels, Functions.constant(0)));
            decimationLevelToRetentionPeriodConverted
                    .putAll(Maps.transformValues(
                            Maps.filterKeys(decimationLevelToRetentionPeriod,
                                    Predicates.in(decimationLevels)),
                            new Function<Integer, Integer>() {
                                @Override
                                public Integer apply(Integer input) {
                                    if (input == null || input < 0) {
                                        return 0;
                                    } else {
                                        return input;
                                    }
                                }
                            }));
            batchStatement.add(channelsByServerAccessor.prepareCreateChannel(
                    serverId, channelName, channelDataId, controlSystemType,
                    decimationLevelToCurrentBucketStartTime,
                    decimationLevelToRetentionPeriodConverted, enabled,
                    options));
            return FutureUtils.transformAnyToVoid(cassandraProvider.getSession()
                    .executeAsync(batchStatement));
        } catch (Exception e) {
            // We do not catch errors, because they typically indicate a very
            // severe exception and it is possible that trying to wrap them in
            // a future would fail again.
            return Futures.immediateFailedFuture(e);
        }
    }

    @Override
    public ListenableFuture<Void> createChannelDecimationLevels(
            String channelName, UUID serverId, Set<Integer> decimationLevels,
            Map<Integer, Integer> decimationLevelToRetentionPeriod) {
        // We wrap the whole method body in a try-catch block, because we want
        // to avoid this method throwing an exception directly. Instead, we
        // return a future that throws the exception. This way, the calling code
        // can deal with exceptions in a unified way instead of having to deal
        // with exceptions from both this method and the returned future.
        try {
            if (log.isDebugEnabled()) {
                log.debug("createChannelDecimationLevels(channelName=\""
                        + StringEscapeUtils.escapeJava(channelName)
                        + "\", serverId=" + serverId + ", ...)");
            }
            checkInitialized();
            // Adding a decimation level involves changes in two tables,
            // therefore we run the actions in a batch.
            BatchStatement batchStatement = new BatchStatement();
            batchStatement.add(channelsAccessor.prepareCreateDecimationLevel(
                    decimationLevels, channelName));
            Map<Integer, Long> decimationLevelToCurrentBucketStartTime = Maps
                    .asMap(decimationLevels, Functions.constant(-1L));
            // Cassandra collections do not support null values and we want to
            // convert negative retention periods to zero, so that the data is
            // homogeneous. We also filter the map to remove entries for
            // decimation levels that are not listed in the set. If a decimation
            // level is listed in the set, but does not have an entry in the
            // map, we add an entry with a value of zero.
            Map<Integer, Integer> decimationLevelToRetentionPeriodConverted = new HashMap<Integer, Integer>();
            decimationLevelToRetentionPeriodConverted.putAll(
                    Maps.asMap(decimationLevels, Functions.constant(0)));
            decimationLevelToRetentionPeriodConverted
                    .putAll(Maps.transformValues(
                            Maps.filterKeys(decimationLevelToRetentionPeriod,
                                    Predicates.in(decimationLevels)),
                            new Function<Integer, Integer>() {
                                @Override
                                public Integer apply(Integer input) {
                                    if (input == null || input < 0) {
                                        return 0;
                                    } else {
                                        return input;
                                    }
                                }
                            }));
            batchStatement
                    .add(channelsByServerAccessor.prepareCreateDecimationLevels(
                            decimationLevelToCurrentBucketStartTime,
                            decimationLevelToRetentionPeriodConverted, serverId,
                            channelName));
            return FutureUtils
                    .transformAnyToVoid(session.executeAsync(batchStatement));
        } catch (Exception e) {
            // We do not catch errors, because they typically indicate a very
            // severe exception and it is possible that trying to wrap them in
            // a future would fail again.
            return Futures.immediateFailedFuture(e);
        }
    }

    @Override
    public ListenableFuture<Pair<Boolean, UUID>> createPendingChannelOperation(
            final UUID serverId, final String channelName,
            final UUID operationId, final String operationType,
            final String operationData, final int ttl) {
        // We wrap the whole method body in a try-catch block, because we want
        // to avoid this method throwing an exception directly. Instead, we
        // return a future that throws the exception. This way, the calling code
        // can deal with exceptions in a unified way instead of having to deal
        // with exceptions from both this method and the returned future.
        try {
            if (log.isDebugEnabled()) {
                log.debug("createPendingChannelOperation(serverId=" + serverId
                        + ", channelName=\""
                        + StringEscapeUtils.escapeJava(channelName)
                        + "\", operationId=" + operationId
                        + ", operationType=\"" + operationType
                        + "\", operationData=\""
                        + StringEscapeUtils.escapeJava(operationData)
                        + "\", ttl=" + ttl + ")");
            }
            checkInitialized();
            // The create operation is a bit tricky: If there is a stale row (a
            // row which has expired and thus has an operation ID of null, but
            // which still exists because its primary key has a longer TTL), a
            // CREATE statement with the "IF NOT EXISTS" clause fails because
            // Cassandra treats the row as still existing. In this case, we us
            // an UPDATE instead of an INSERT statement, where we use the
            // condition "IF operation_id = null", which will work.
            // Unfortunately, the UPDATE statement does not work when the row
            // never existed, so we cannot make it the default. To be extra
            // safe, we also try the CREATE statement again if the UPDATE fails,
            // because it is not clear in which situations the UPDATE on the
            // missing row is allowed (the documentation does not specify and it
            // might actually be a bug that it is allowed in certain situations
            // at all).
            return Futures.transform(pendingChannelOperationsByServerAccessor
                    .createOperationIfNotExists(serverId, channelName,
                            operationData, operationId, operationType, ttl),
                    new AsyncFunction<ResultSet, Pair<Boolean, UUID>>() {
                        @Override
                        public ListenableFuture<Pair<Boolean, UUID>> apply(
                                ResultSet input) {
                            Row row = input.one();
                            boolean applied = input.wasApplied();
                            if (applied) {
                                return Futures.immediateFuture(
                                        Pair.<Boolean, UUID> of(true, null));
                            }
                            UUID existingOperationId = row
                                    .getUUID(COLUMN_OPERATION_ID);
                            if (existingOperationId != null) {
                                // Another operation already exists, so
                                // we simply return its ID.
                                return Futures.immediateFuture(
                                        Pair.of(false, existingOperationId));
                            }
                            // The operation failed because of a stale row, so
                            // we try again with an update. We cannot return the
                            // result of the update directly, because we might
                            // have to run another create attempt if the update
                            // fails.
                            return Futures.transform(
                                    updatePendingChannelOperation(serverId,
                                            channelName, null, operationId,
                                            operationType, operationData, ttl),
                                    new AsyncFunction<Pair<Boolean, UUID>, Pair<Boolean, UUID>>() {
                                        @Override
                                        public ListenableFuture<Pair<Boolean, UUID>> apply(
                                                Pair<Boolean, UUID> input)
                                                throws Exception {
                                            // If the operation succeeded, we
                                            // can simply return its result.
                                            // Otherwise, depending on the
                                            // failure reason, we might have to
                                            // try again.
                                            if (input.getLeft()) {
                                                return Futures
                                                        .immediateFuture(input);
                                            }
                                            if (input.getRight() != null) {
                                                return Futures
                                                        .immediateFuture(input);
                                            }
                                            // The update failed because there
                                            // was no row that could be updated,
                                            // so we try the CREATE again.
                                            return createPendingChannelOperation(
                                                    serverId, channelName,
                                                    operationId, operationType,
                                                    operationData, ttl);
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
    public ListenableFuture<Pair<Boolean, UUID>> createPendingChannelOperationRelaxed(
            UUID serverId, String channelName, UUID operationId,
            String operationType, String operationData, int ttl) {
        return createPendingChannelOperationRelaxed(serverId, channelName,
                operationId, operationType, operationData, ttl,
                pendingChannelOperationMaxTries - 1);
    }

    @Override
    public ListenableFuture<Void> createSampleBucket(String channelName,
            int decimationLevel, long bucketStartTime, long bucketEndTime,
            Long precedingBucketStartTime, boolean isNewCurrentBucket,
            UUID serverId) {
        // We wrap the whole method body in a try-catch block, because we want
        // to avoid this method throwing an exception directly. Instead, we
        // return a future that throws the exception. This way, the calling code
        // can deal with exceptions in a unified way instead of having to deal
        // with exceptions from both this method and the returned future.
        try {
            if (log.isDebugEnabled()) {
                log.debug("createSampleBucket(channelName=\""
                        + StringEscapeUtils.escapeJava(channelName)
                        + "\", decimationLevel=" + decimationLevel
                        + ", bucketStartTime=" + bucketStartTime
                        + ", bucketEndTime=" + bucketEndTime
                        + ", precedingBucketStartTime="
                        + precedingBucketStartTime + ", isNewCurrentBucket="
                        + isNewCurrentBucket + ", serverId=" + serverId + ")");
            }
            checkInitialized();
            // If the newly created bucket should also be set as the current
            // bucket, we want to apply both statements in a batch. If we have
            // to update the current bucket ID, we also need the server ID.
            Preconditions
                    .checkArgument(!isNewCurrentBucket || serverId != null);
            BatchStatement batchStatement = new BatchStatement();
            batchStatement
                    .add(channelsAccessor.prepareCreateSampleBucket(channelName,
                            decimationLevel, bucketStartTime, bucketEndTime));
            if (isNewCurrentBucket) {
                batchStatement.add(channelsByServerAccessor
                        .prepareUpdateChannelCurrentSampleBucket(
                                decimationLevel, bucketStartTime, serverId,
                                channelName));
            }
            if (precedingBucketStartTime != null) {
                batchStatement
                        .add(channelsAccessor.prepareUpdateSampleBucketEndTime(
                                bucketStartTime - 1L, channelName,
                                decimationLevel, precedingBucketStartTime));
            }
            return FutureUtils
                    .transformAnyToVoid(session.executeAsync(batchStatement));
        } catch (Exception e) {
            // We do not catch errors, because they typically indicate a very
            // severe exception and it is possible that trying to wrap them in
            // a future would fail again.
            return Futures.immediateFailedFuture(e);
        }
    }

    @Override
    public ListenableFuture<Void> deleteChannel(String channelName,
            UUID serverId) {
        // We wrap the whole method body in a try-catch block, because we want
        // to avoid this method throwing an exception directly. Instead, we
        // return a future that throws the exception. This way, the calling code
        // can deal with exceptions in a unified way instead of having to deal
        // with exceptions from both this method and the returned future.
        try {
            if (log.isDebugEnabled()) {
                log.debug("deleteChannel(channelName=\""
                        + StringEscapeUtils.escapeJava(channelName)
                        + ", serverId=" + serverId + ")");
            }
            checkInitialized();
            // We use a batch statement because we want to ensure that the
            // channel is removed from both tables.
            BatchStatement batchStatement = new BatchStatement();
            batchStatement
                    .add(channelsAccessor.prepareDeleteChannel(channelName));
            batchStatement.add(channelsByServerAccessor
                    .prepareDeleteChannel(serverId, channelName));
            return FutureUtils
                    .transformAnyToVoid(session.executeAsync(batchStatement));
        } catch (Exception e) {
            // We do not catch errors, because they typically indicate a very
            // severe exception and it is possible that trying to wrap them in
            // a future would fail again.
            return Futures.immediateFailedFuture(e);
        }
    }

    @Override
    public ListenableFuture<Void> deleteChannelDecimationLevels(
            String channelName, UUID serverId, Set<Integer> decimationLevels) {
        // We wrap the whole method body in a try-catch block, because we want
        // to avoid this method throwing an exception directly. Instead, we
        // return a future that throws the exception. This way, the calling code
        // can deal with exceptions in a unified way instead of having to deal
        // with exceptions from both this method and the returned future.
        try {
            if (log.isDebugEnabled()) {
                log.debug("deleteChannelDecimationLevels(channelName=\""
                        + StringEscapeUtils.escapeJava(channelName)
                        + "\", serverId=" + serverId + ", ...)");
            }
            checkInitialized();
            // Removing a decimation level involves changes in two tables,
            // therefore we run the actions in a batch.
            BatchStatement batchStatement = new BatchStatement();
            batchStatement.add(channelsAccessor.prepareDeleteDecimationLevels(
                    decimationLevels, channelName));
            // We also want to remove all sample buckets associated with the
            // decimation levels.
            for (int decimationLevel : decimationLevels) {
                batchStatement.add(channelsAccessor.prepareDeleteSampleBuckets(
                        channelName, decimationLevel));
            }
            batchStatement.add(channelsByServerAccessor
                    .prepareDeleteDecimationLevels(decimationLevels,
                            decimationLevels, serverId, channelName));
            return FutureUtils
                    .transformAnyToVoid(session.executeAsync(batchStatement));
        } catch (Exception e) {
            // We do not catch errors, because they typically indicate a very
            // severe exception and it is possible that trying to wrap them in
            // a future would fail again.
            return Futures.immediateFailedFuture(e);
        }
    }

    @Override
    public ListenableFuture<Pair<Boolean, UUID>> deletePendingChannelOperation(
            UUID serverId, String channelName, UUID operationId) {
        return deletePendingChannelOperation(serverId, channelName, operationId,
                pendingChannelOperationMaxTries);
    }

    @Override
    public ListenableFuture<Void> deleteSampleBucket(String channelName,
            int decimationLevel, long bucketStartTime, boolean isCurrentBucket,
            UUID serverId) {
        // We wrap the whole method body in a try-catch block, because we want
        // to avoid this method throwing an exception directly. Instead, we
        // return a future that throws the exception. This way, the calling code
        // can deal with exceptions in a unified way instead of having to deal
        // with exceptions from both this method and the returned future.
        try {
            if (log.isDebugEnabled()) {
                log.debug("deleteSampleBucket(channelName=\""
                        + StringEscapeUtils.escapeJava(channelName)
                        + "\", decimationLevel=" + decimationLevel
                        + ", bucketStartTime=" + bucketStartTime
                        + ", isCurrentBucket=" + isCurrentBucket + ", serverId="
                        + serverId + ")");
            }
            checkInitialized();
            // If we remove the current sample bucket, we need a server ID.
            Preconditions.checkArgument(!isCurrentBucket || serverId != null);
            if (isCurrentBucket) {
                // If we remove the current bucket, we need to update two
                // tables. We do this within a batch statement to ensure that
                // both changes are applied.
                BatchStatement batchStatement = new BatchStatement();
                batchStatement.add(channelsAccessor.prepareDeleteSampleBucket(
                        channelName, decimationLevel, bucketStartTime));
                batchStatement.add(channelsByServerAccessor
                        .prepareUpdateChannelCurrentSampleBucket(
                                decimationLevel, -1L, serverId, channelName));
                return FutureUtils.transformAnyToVoid(
                        session.executeAsync(batchStatement));
            } else {
                return FutureUtils.transformAnyToVoid(session.executeAsync(
                        channelsAccessor.prepareDeleteSampleBucket(channelName,
                                decimationLevel, bucketStartTime)));
            }
        } catch (Exception e) {
            // We do not catch errors, because they typically indicate a very
            // severe exception and it is possible that trying to wrap them in
            // a future would fail again.
            return Futures.immediateFailedFuture(e);
        }
    }

    @Override
    public ListenableFuture<ChannelInformation> getChannel(String channelName) {
        // We wrap the whole method body in a try-catch block, because we want
        // to avoid this method throwing an exception directly. Instead, we
        // return a future that throws the exception. This way, the calling code
        // can deal with exceptions in a unified way instead of having to deal
        // with exceptions from both this method and the returned future.
        try {
            checkInitialized();
            return Futures.transform(channelsAccessor.getChannel(channelName),
                    new Function<ResultSet, ChannelInformation>() {
                        @Override
                        public ChannelInformation apply(ResultSet input) {
                            Row row = input.one();
                            if (row == null) {
                                return null;
                            }
                            return new ChannelInformation(
                                    row.getUUID(COLUMN_CHANNEL_DATA_ID),
                                    row.getString(COLUMN_CHANNEL_NAME),
                                    row.getString(COLUMN_CONTROL_SYSTEM_TYPE),
                                    row.getSet(COLUMN_DECIMATION_LEVELS,
                                            Integer.class),
                                    row.getUUID(COLUMN_SERVER_ID));
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
    public ListenableFuture<ChannelConfiguration> getChannelByServer(
            UUID serverId, String channelName) {
        // We wrap the whole method body in a try-catch block, because we want
        // to avoid this method throwing an exception directly. Instead, we
        // return a future that throws the exception. This way, the calling code
        // can deal with exceptions in a unified way instead of having to deal
        // with exceptions from both this method and the returned future.
        try {
            checkInitialized();
            return Futures.transform(
                    channelsByServerAccessor.getChannel(serverId, channelName),
                    new Function<ResultSet, ChannelConfiguration>() {
                        @Override
                        public ChannelConfiguration apply(ResultSet input) {
                            Row row = input.one();
                            if (row == null) {
                                return null;
                            }
                            return new ChannelConfiguration(
                                    row.getUUID(COLUMN_CHANNEL_DATA_ID),
                                    row.getString(COLUMN_CHANNEL_NAME),
                                    row.getString(COLUMN_CONTROL_SYSTEM_TYPE),
                                    row.getMap(
                                            COLUMN_DECIMATION_LEVEL_TO_CURRENT_BUCKET_START_TIME,
                                            Integer.class, Long.class),
                                    row.getMap(
                                            COLUMN_DECIMATION_LEVEL_TO_RETENTION_PERIOD,
                                            Integer.class, Integer.class),
                                    row.getBool(COLUMN_ENABLED),
                                    row.getMap(COLUMN_OPTIONS, String.class,
                                            String.class),
                                    row.getUUID(COLUMN_SERVER_ID));
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
    public ListenableFuture<? extends Iterable<ChannelInformation>> getChannels() {
        // We wrap the whole method body in a try-catch block, because we want
        // to avoid this method throwing an exception directly. Instead, we
        // return a future that throws the exception. This way, the calling code
        // can deal with exceptions in a unified way instead of having to deal
        // with exceptions from both this method and the returned future.
        try {
            checkInitialized();
            return Futures.transform(channelsAccessor.getAllChannels(),
                    new Function<ResultSet, Iterable<ChannelInformation>>() {
                        @Override
                        public Iterable<ChannelInformation> apply(
                                ResultSet input) {
                            final int channelDataIdIndex = input
                                    .getColumnDefinitions()
                                    .getIndexOf(COLUMN_CHANNEL_DATA_ID);
                            final int channelNameIndex = input
                                    .getColumnDefinitions()
                                    .getIndexOf(COLUMN_CHANNEL_NAME);
                            final int controlSystemTypeIndex = input
                                    .getColumnDefinitions()
                                    .getIndexOf(COLUMN_CONTROL_SYSTEM_TYPE);
                            final int decimationLevelsIndex = input
                                    .getColumnDefinitions()
                                    .getIndexOf(COLUMN_DECIMATION_LEVELS);
                            final int serverIdIndex = input
                                    .getColumnDefinitions()
                                    .getIndexOf(COLUMN_SERVER_ID);
                            return Iterables.transform(input,
                                    new Function<Row, ChannelInformation>() {
                                        @Override
                                        public ChannelInformation apply(
                                                Row input) {
                                            return new ChannelInformation(
                                                    input.getUUID(
                                                            channelDataIdIndex),
                                                    input.getString(
                                                            channelNameIndex),
                                                    input.getString(
                                                            controlSystemTypeIndex),
                                                    input.getSet(
                                                            decimationLevelsIndex,
                                                            Integer.class),
                                                    input.getUUID(
                                                            serverIdIndex));
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
    public ListenableFuture<? extends Iterable<ChannelConfiguration>> getChannelsByServer(
            UUID serverId) {
        // We wrap the whole method body in a try-catch block, because we want
        // to avoid this method throwing an exception directly. Instead, we
        // return a future that throws the exception. This way, the calling code
        // can deal with exceptions in a unified way instead of having to deal
        // with exceptions from both this method and the returned future.
        try {
            checkInitialized();
            return Futures.transform(
                    channelsByServerAccessor.getChannels(serverId),
                    new Function<ResultSet, Iterable<ChannelConfiguration>>() {
                        @Override
                        public Iterable<ChannelConfiguration> apply(
                                ResultSet input) {
                            final int channelDataIdIndex = input
                                    .getColumnDefinitions()
                                    .getIndexOf(COLUMN_CHANNEL_DATA_ID);
                            final int channelNameIndex = input
                                    .getColumnDefinitions()
                                    .getIndexOf(COLUMN_CHANNEL_NAME);
                            final int controlSystemTypeIndex = input
                                    .getColumnDefinitions()
                                    .getIndexOf(COLUMN_CONTROL_SYSTEM_TYPE);
                            final int decimationLevelToCurrentBucketStartTimeIndex = input
                                    .getColumnDefinitions().getIndexOf(
                                            COLUMN_DECIMATION_LEVEL_TO_CURRENT_BUCKET_START_TIME);
                            final int decimationLevelToRetentionPeriodIndex = input
                                    .getColumnDefinitions().getIndexOf(
                                            COLUMN_DECIMATION_LEVEL_TO_RETENTION_PERIOD);
                            final int enabledIndex = input
                                    .getColumnDefinitions()
                                    .getIndexOf(COLUMN_ENABLED);
                            final int optionsIndex = input
                                    .getColumnDefinitions()
                                    .getIndexOf(COLUMN_OPTIONS);
                            final int serverIdIndex = input
                                    .getColumnDefinitions()
                                    .getIndexOf(COLUMN_SERVER_ID);
                            return Iterables.transform(input,
                                    new Function<Row, ChannelConfiguration>() {
                                        @Override
                                        public ChannelConfiguration apply(
                                                Row input) {
                                            return new ChannelConfiguration(
                                                    input.getUUID(
                                                            channelDataIdIndex),
                                                    input.getString(
                                                            channelNameIndex),
                                                    input.getString(
                                                            controlSystemTypeIndex),
                                                    input.getMap(
                                                            decimationLevelToCurrentBucketStartTimeIndex,
                                                            Integer.class,
                                                            Long.class),
                                                    input.getMap(
                                                            decimationLevelToRetentionPeriodIndex,
                                                            Integer.class,
                                                            Integer.class),
                                                    input.getBool(enabledIndex),
                                                    input.getMap(optionsIndex,
                                                            String.class,
                                                            String.class),
                                                    input.getUUID(
                                                            serverIdIndex));
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
    public ListenableFuture<ChannelOperation> getPendingChannelOperation(
            UUID serverId, String channelName) {
        return getPendingChannelOperation(serverId, channelName, false);
    }

    @Override
    public ListenableFuture<? extends Iterable<ChannelOperation>> getPendingChannelOperations(
            UUID serverId) {
        // We wrap the whole method body in a try-catch block, because we want
        // to avoid this method throwing an exception directly. Instead, we
        // return a future that throws the exception. This way, the calling code
        // can deal with exceptions in a unified way instead of having to deal
        // with exceptions from both this method and the returned future.
        try {
            checkInitialized();
            return Futures.transform(
                    pendingChannelOperationsByServerAccessor
                            .getOperations(serverId),
                    new Function<ResultSet, Iterable<ChannelOperation>>() {
                        @Override
                        public Iterable<ChannelOperation> apply(
                                ResultSet input) {
                            final int channelNameIndex = input
                                    .getColumnDefinitions()
                                    .getIndexOf(COLUMN_CHANNEL_NAME);
                            final int operationDataIndex = input
                                    .getColumnDefinitions()
                                    .getIndexOf(COLUMN_OPERATION_DATA);
                            final int operationIdIndex = input
                                    .getColumnDefinitions()
                                    .getIndexOf(COLUMN_OPERATION_ID);
                            final int operationTypeIndex = input
                                    .getColumnDefinitions()
                                    .getIndexOf(COLUMN_OPERATION_TYPE);
                            final int serverIdIndex = input
                                    .getColumnDefinitions()
                                    .getIndexOf(COLUMN_SERVER_ID);
                            // We might get some null elements from stale rows
                            // (rows where the TTL of the operation ID has
                            // expired but the primary key has a longer TTL and
                            // thus still exists. We simply remove those
                            // elements from the iterable.
                            return Iterables.filter(Iterables.transform(input,
                                    new Function<Row, ChannelOperation>() {
                                        @Override
                                        public ChannelOperation apply(
                                                Row input) {
                                            UUID operationId = input
                                                    .getUUID(operationIdIndex);
                                            if (operationId == null) {
                                                return null;
                                            }
                                            return new ChannelOperation(
                                                    input.getString(
                                                            channelNameIndex),
                                                    input.getString(
                                                            operationDataIndex),
                                                    operationId,
                                                    input.getString(
                                                            operationTypeIndex),
                                                    input.getUUID(
                                                            serverIdIndex));
                                        }
                                    }), Predicates.notNull());
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
    public ListenableFuture<ObjectResultSet<SampleBucketInformation>> getSampleBuckets(
            String channelName) {
        // We wrap the whole method body in a try-catch block, because we want
        // to avoid this method throwing an exception directly. Instead, we
        // return a future that throws the exception. This way, the calling code
        // can deal with exceptions in a unified way instead of having to deal
        // with exceptions from both this method and the returned future.
        try {
            checkInitialized();
            return resultSetFutureToSampleBucketInformationSetFuture(
                    channelsAccessor.getSampleBuckets(channelName));
        } catch (Exception e) {
            // We do not catch errors, because they typically indicate a very
            // severe exception and it is possible that trying to wrap them in
            // a future would fail again.
            return Futures.immediateFailedFuture(e);
        }
    }

    @Override
    public ListenableFuture<ObjectResultSet<SampleBucketInformation>> getSampleBuckets(
            String channelName, int decimationLevel) {
        // We wrap the whole method body in a try-catch block, because we want
        // to avoid this method throwing an exception directly. Instead, we
        // return a future that throws the exception. This way, the calling code
        // can deal with exceptions in a unified way instead of having to deal
        // with exceptions from both this method and the returned future.
        try {
            checkInitialized();
            return resultSetFutureToSampleBucketInformationSetFuture(
                    channelsAccessor.getSampleBuckets(channelName,
                            decimationLevel));
        } catch (Exception e) {
            // We do not catch errors, because they typically indicate a very
            // severe exception and it is possible that trying to wrap them in
            // a future would fail again.
            return Futures.immediateFailedFuture(e);
        }
    }

    @Override
    public ListenableFuture<ObjectResultSet<SampleBucketInformation>> getSampleBucketsInInterval(
            String channelName, int decimationLevel,
            long startTimeGreaterThanOrEqualTo,
            long startTimeLessThanOrEqualTo) {
        // We wrap the whole method body in a try-catch block, because we want
        // to avoid this method throwing an exception directly. Instead, we
        // return a future that throws the exception. This way, the calling code
        // can deal with exceptions in a unified way instead of having to deal
        // with exceptions from both this method and the returned future.
        try {
            checkInitialized();
            ResultSetFuture resultSetFuture = channelsAccessor
                    .getSampleBucketsWithStartTimeGreaterThanOrEqualToAndLessThanOrEqualTo(
                            channelName, decimationLevel,
                            startTimeGreaterThanOrEqualTo,
                            startTimeLessThanOrEqualTo);
            return resultSetFutureToSampleBucketInformationSetFuture(
                    resultSetFuture);
        } catch (Exception e) {
            // We do not catch errors, because they typically indicate a very
            // severe exception and it is possible that trying to wrap them in
            // a future would fail again.
            return Futures.immediateFailedFuture(e);
        }
    }

    @Override
    public ListenableFuture<ObjectResultSet<SampleBucketInformation>> getSampleBucketsInReverseOrder(
            String channelName, int decimationLevel, int limit) {
        // We wrap the whole method body in a try-catch block, because we want
        // to avoid this method throwing an exception directly. Instead, we
        // return a future that throws the exception. This way, the calling code
        // can deal with exceptions in a unified way instead of having to deal
        // with exceptions from both this method and the returned future.
        try {
            checkInitialized();
            return resultSetFutureToSampleBucketInformationSetFuture(
                    channelsAccessor.getSampleBucketsReversed(channelName,
                            decimationLevel, limit));
        } catch (Exception e) {
            // We do not catch errors, because they typically indicate a very
            // severe exception and it is possible that trying to wrap them in
            // a future would fail again.
            return Futures.immediateFailedFuture(e);
        }
    }

    @Override
    public ListenableFuture<ObjectResultSet<SampleBucketInformation>> getSampleBucketsNewerThan(
            String channelName, int decimationLevel,
            long startTimeGreaterThanOrEqualTo, int limit) {
        // We wrap the whole method body in a try-catch block, because we want
        // to avoid this method throwing an exception directly. Instead, we
        // return a future that throws the exception. This way, the calling code
        // can deal with exceptions in a unified way instead of having to deal
        // with exceptions from both this method and the returned future.
        try {
            checkInitialized();
            ResultSetFuture resultSetFuture = channelsAccessor
                    .getSampleBucketsWithStartTimeGreaterThanOrEqualTo(
                            channelName, decimationLevel,
                            startTimeGreaterThanOrEqualTo, limit);
            return resultSetFutureToSampleBucketInformationSetFuture(
                    resultSetFuture);
        } catch (Exception e) {
            // We do not catch errors, because they typically indicate a very
            // severe exception and it is possible that trying to wrap them in
            // a future would fail again.
            return Futures.immediateFailedFuture(e);
        }
    }

    @Override
    public ListenableFuture<ObjectResultSet<SampleBucketInformation>> getSampleBucketsOlderThanInReverseOrder(
            String channelName, int decimationLevel,
            long startTimeLessThanOrEqualTo, int limit) {
        // We wrap the whole method body in a try-catch block, because we want
        // to avoid this method throwing an exception directly. Instead, we
        // return a future that throws the exception. This way, the calling code
        // can deal with exceptions in a unified way instead of having to deal
        // with exceptions from both this method and the returned future.
        try {
            checkInitialized();
            ResultSetFuture resultSetFuture = channelsAccessor
                    .getSampleBucketsReversedWithStartTimeLessThanOrEqualTo(
                            channelName, decimationLevel,
                            startTimeLessThanOrEqualTo, limit);
            return resultSetFutureToSampleBucketInformationSetFuture(
                    resultSetFuture);
        } catch (Exception e) {
            // We do not catch errors, because they typically indicate a very
            // severe exception and it is possible that trying to wrap them in
            // a future would fail again.
            return Futures.immediateFailedFuture(e);
        }
    }

    @Override
    public ListenableFuture<Void> moveChannel(final String channelName,
            final UUID oldServerId, final UUID newServerId) {
        // We wrap the whole method body in a try-catch block, because we want
        // to avoid this method throwing an exception directly. Instead, we
        // return a future that throws the exception. This way, the calling code
        // can deal with exceptions in a unified way instead of having to deal
        // with exceptions from both this method and the returned future.
        try {
            if (log.isDebugEnabled()) {
                log.debug("moveChannel(channelName=\""
                        + StringEscapeUtils.escapeJava(channelName)
                        + "\", oldServerId=" + oldServerId + ", newServerId="
                        + newServerId + ")");
            }
            checkInitialized();
            // First, we have to read the old channel meta-data. We do this
            // asynchronously and start the write operation once we have the
            // necessary data.
            return Futures.transform(
                    getChannelByServer(oldServerId, channelName),
                    new AsyncFunction<ChannelConfiguration, Void>() {
                        @Override
                        public ListenableFuture<Void> apply(
                                ChannelConfiguration input) {
                            // Now we have the data that we need. We do all
                            // three operation (inserting the new data, deleting
                            // the old data, and updating the server ID in a
                            // batch, so that we can ensure that either all or
                            // none of the changes is applied.
                            BatchStatement batchStatement = new BatchStatement();
                            batchStatement.add(channelsByServerAccessor
                                    .prepareCreateChannel(newServerId,
                                            channelName,
                                            input.getChannelDataId(),
                                            input.getControlSystemType(),
                                            input.getDecimationLevelToCurrentBucketStartTime(),
                                            input.getDecimationLevelToRetentionPeriod(),
                                            input.isEnabled(),
                                            input.getOptions()));
                            batchStatement.add(channelsByServerAccessor
                                    .prepareDeleteChannel(oldServerId,
                                            channelName));
                            batchStatement.add(channelsAccessor
                                    .prepareUpdateChannelServerId(newServerId,
                                            channelName));
                            return FutureUtils.transformAnyToVoid(
                                    session.executeAsync(batchStatement));
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
    public ListenableFuture<Void> renameChannel(final String oldChannelName,
            final String newChannelName, final UUID serverId) {
        // We wrap the whole method body in a try-catch block, because we want
        // to avoid this method throwing an exception directly. Instead, we
        // return a future that throws the exception. This way, the calling code
        // can deal with exceptions in a unified way instead of having to deal
        // with exceptions from both this method and the returned future.
        try {
            if (log.isDebugEnabled()) {
                log.debug("renameChannel(oldChannelName=\""
                        + StringEscapeUtils.escapeJava(oldChannelName)
                        + "\", newChannelName=\""
                        + StringEscapeUtils.escapeJava(newChannelName)
                        + "\", serverId=" + serverId + ")");
            }
            checkInitialized();
            // If the old and new name are the same, we should not do anything.
            if (oldChannelName.equals(newChannelName)) {
                return Futures.immediateFuture(null);
            }
            // In order to rename the channel, we first have to get all data
            // associated with the old name.
            final ListenableFuture<ChannelConfiguration> channelConfigurationFuture = getChannelByServer(
                    serverId, oldChannelName);
            final ListenableFuture<? extends Iterable<? extends SampleBucketInformation>> sampleBucketsFuture = resultSetFutureToSampleBucketInformationSetFuture(
                    channelsAccessor
                            .getChannelWithSampleBuckets(oldChannelName));
            @SuppressWarnings("unchecked")
            ListenableFuture<?> combinedFuture = Futures
                    .allAsList(channelConfigurationFuture, sampleBucketsFuture);
            // We are not really interested in the result of the combined
            // future, we just use it to know when both futures have finished.
            return Futures.transform(combinedFuture,
                    new AsyncFunction<Object, Void>() {
                        @Override
                        public ListenableFuture<Void> apply(Object input) {
                            // We know that both futures finishes successfully,
                            // because otherwise this method would not have been
                            // called.
                            final ChannelConfiguration channelConfiguration = FutureUtils
                                    .getUnchecked(channelConfigurationFuture);
                            final Iterable<? extends SampleBucketInformation> sampleBuckets = FutureUtils
                                    .getUnchecked(sampleBucketsFuture);
                            // Iterating over the sample buckets might result in
                            // additional network operation. Therefore, we do
                            // not want to process them in the callback but
                            // delegate this to a separate thread.
                            ListenableFutureTask<BatchStatement> futureTask = ListenableFutureTask
                                    .create(new Callable<BatchStatement>() {
                                        @Override
                                        public BatchStatement call()
                                                throws Exception {
                                            // We want to run all modifications
                                            // in a batch statement, so that we
                                            // can ensure that either all or
                                            // none of them are applied.
                                            BatchStatement batchStatement = new BatchStatement();
                                            // Create the new channel entries.
                                            batchStatement.add(channelsAccessor
                                                    .prepareCreateChannel(
                                                            newChannelName,
                                                            channelConfiguration
                                                                    .getChannelDataId(),
                                                            channelConfiguration
                                                                    .getControlSystemType(),
                                                            channelConfiguration
                                                                    .getDecimationLevelToCurrentBucketStartTime()
                                                                    .keySet(),
                                                            serverId));
                                            batchStatement
                                                    .add(channelsByServerAccessor
                                                            .prepareCreateChannel(
                                                                    serverId,
                                                                    newChannelName,
                                                                    channelConfiguration
                                                                            .getChannelDataId(),
                                                                    channelConfiguration
                                                                            .getControlSystemType(),
                                                                    channelConfiguration
                                                                            .getDecimationLevelToCurrentBucketStartTime(),
                                                                    channelConfiguration
                                                                            .getDecimationLevelToRetentionPeriod(),
                                                                    channelConfiguration
                                                                            .isEnabled(),
                                                                    channelConfiguration
                                                                            .getOptions()));
                                            // Copy all existing sample buckets.
                                            for (SampleBucketInformation sampleBucket : sampleBuckets) {
                                                batchStatement
                                                        .add(channelsAccessor
                                                                .prepareCreateSampleBucket(
                                                                        newChannelName,
                                                                        sampleBucket
                                                                                .getDecimationLevel(),
                                                                        sampleBucket
                                                                                .getBucketStartTime(),
                                                                        sampleBucket
                                                                                .getBucketEndTime()));
                                            }
                                            // Remove the old entries.
                                            batchStatement.add(channelsAccessor
                                                    .prepareDeleteChannel(
                                                            oldChannelName));
                                            batchStatement
                                                    .add(channelsByServerAccessor
                                                            .prepareDeleteChannel(
                                                                    serverId,
                                                                    oldChannelName));
                                            return batchStatement;
                                        }
                                    });
                            renameChannelExecutor.submit(futureTask);
                            // After creating the batch-statement, we have to
                            // execute it. The easiest way to do this
                            // asynchronously is by transforming the future.
                            return Futures.transform(futureTask,
                                    new AsyncFunction<BatchStatement, Void>() {
                                        @Override
                                        public ListenableFuture<Void> apply(
                                                BatchStatement input)
                                                throws Exception {
                                            return FutureUtils
                                                    .transformAnyToVoid(
                                                            session.executeAsync(
                                                                    input));
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
    public ListenableFuture<Void> updateChannelConfiguration(String channelName,
            UUID serverId,
            Map<Integer, Integer> decimationLevelToRetentionPeriod,
            boolean enabled, Map<String, String> options) {
        // We wrap the whole method body in a try-catch block, because we want
        // to avoid this method throwing an exception directly. Instead, we
        // return a future that throws the exception. This way, the calling code
        // can deal with exceptions in a unified way instead of having to deal
        // with exceptions from both this method and the returned future.
        try {
            if (log.isDebugEnabled()) {
                log.debug("updateChannelConfiguration(channelName=\""
                        + StringEscapeUtils.escapeJava(channelName)
                        + "\", serverId=" + serverId + ", ..., enabled="
                        + enabled + ", ...)");
            }
            checkInitialized();
            // Cassandra collections do not support null values and we want to
            // convert negative retention periods to zero, so that the data is
            // homogeneous. It would be best if we also filtered the map to not
            // contain decimation levels that have not been created for the
            // channel, but this would require a read before write. Therefore,
            // we rely on the calling code to do the right thing and only
            // present data for existing decimation levels.
            Map<Integer, Integer> decimationLevelToRetentionPeriodConverted = Maps
                    .transformValues(decimationLevelToRetentionPeriod,
                            new Function<Integer, Integer>() {
                                @Override
                                public Integer apply(Integer input) {
                                    if (input == null || input < 0) {
                                        return 0;
                                    } else {
                                        return input;
                                    }
                                }
                            });
            return FutureUtils.transformAnyToVoid(channelsByServerAccessor
                    .updateChannel(decimationLevelToRetentionPeriodConverted,
                            enabled, options, serverId, channelName));
        } catch (Exception e) {
            // We do not catch errors, because they typically indicate a very
            // severe exception and it is possible that trying to wrap them in
            // a future would fail again.
            return Futures.immediateFailedFuture(e);
        }
    }

    @Override
    public ListenableFuture<Pair<Boolean, UUID>> updatePendingChannelOperation(
            UUID serverId, String channelName, UUID oldOperationId,
            UUID newOperationId, String newOperationType,
            String newOperationData, int ttl) {
        // We wrap the whole method body in a try-catch block, because we want
        // to avoid this method throwing an exception directly. Instead, we
        // return a future that throws the exception. This way, the calling code
        // can deal with exceptions in a unified way instead of having to deal
        // with exceptions from both this method and the returned future.
        try {
            if (log.isDebugEnabled()) {
                log.debug("updatePendingChannelOperation(serverId=" + serverId
                        + ", channelName=\""
                        + StringEscapeUtils.escapeJava(channelName)
                        + "\", oldOperationId=" + oldOperationId
                        + ", newOperationId=" + newOperationId
                        + ", newOperationType=\"" + newOperationType
                        + "\", newOperationData=\""
                        + StringEscapeUtils.escapeJava(newOperationData)
                        + "\", ttl=" + ttl + ")");
            }
            checkInitialized();
            return Futures.transform(
                    pendingChannelOperationsByServerAccessor
                            .updateOperationIfIdMatches(ttl, newOperationData,
                                    newOperationId, newOperationType, serverId,
                                    channelName, oldOperationId),
                    new Function<ResultSet, Pair<Boolean, UUID>>() {
                        @Override
                        public Pair<Boolean, UUID> apply(ResultSet input) {
                            Row row = input.one();
                            boolean applied = input.wasApplied();
                            if (applied) {
                                return Pair.of(true, null);
                            }
                            // If the operation has already been deleted (or
                            // disappeared because of its TTL), applied is false
                            // but we do not have an existing row.
                            if (!row.getColumnDefinitions()
                                    .contains(COLUMN_OPERATION_ID)) {
                                return Pair.of(false, null);
                            }
                            // If we got an operation ID in the result, the
                            // update was rejected because the ID did not match
                            // and we want to return the information from the
                            // database.
                            return Pair.of(false,
                                    row.getUUID(COLUMN_OPERATION_ID));
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
    public ListenableFuture<Pair<Boolean, UUID>> updatePendingChannelOperationRelaxed(
            UUID serverId, String channelName, UUID oldOperationId,
            UUID newOperationId, String newOperationType,
            String newOperationData, int ttl) {
        return updatePendingChannelOperationRelaxed(serverId, channelName,
                oldOperationId, newOperationId, newOperationType,
                newOperationData, ttl, pendingChannelOperationMaxTries);
    }

    private void checkInitialized() {
        if (session == null) {
            throw new IllegalStateException(
                    "The ChannelMetaDataDAO has not been completely initialized yet.");
        }
    }

    private void createAccessors(Session session) {
        // The accessors should only be created once. We only check whether
        // there is a session, because we assign the session after creating the
        // accessors. Only the session field is volatile but this is okay
        // because it is read first and written last.
        if (this.session != null) {
            return;
        }
        MappingManager mappingManager = new MappingManager(session);
        // The session field is volatile. The accessor fields are not, but this
        // works because we only proceed if the session field is not null and
        // the Java memory model guarantees that reading a volatile field makes
        // all changes visible that have been written before writing the
        // respective value to this field.
        channelsAccessor = mappingManager
                .createAccessor(ChannelsAccessor.class);
        channelsByServerAccessor = mappingManager
                .createAccessor(ChannelsByServerAccessor.class);
        pendingChannelOperationsByServerAccessor = mappingManager
                .createAccessor(PendingChannelOperationsByServerAccessor.class);
    }

    private ListenableFuture<Pair<Boolean, UUID>> createPendingChannelOperationRelaxed(
            final UUID serverId, final String channelName,
            final UUID operationId, final String operationType,
            final String operationData, final int ttl,
            final int remainingTries) {
        return FutureUtils.transform(
                createPendingChannelOperation(serverId, channelName,
                        operationId, operationType, operationData, ttl),
                new AsyncFunction<Pair<Boolean, UUID>, Pair<Boolean, UUID>>() {
                    @Override
                    public ListenableFuture<Pair<Boolean, UUID>> apply(
                            Pair<Boolean, UUID> input) throws Exception {
                        // If the operation succeeded or it failed because there
                        // already is a pending operation with a different ID,
                        // we are done. If the result is false, but the existing
                        // pending operation has the same ID, a previous write
                        // operation that timed out might actually have been
                        // successful.
                        if (input.getLeft()
                                || !operationId.equals(input.getRight())) {
                            return Futures.immediateFuture(input);
                        }
                        // We read the complete record in order to verify
                        // whether the data in the existing record matches the
                        // data that we are trying to write. We do not have to
                        // do this with the serial consistency level because our
                        // previous write attempt already revealed the data.
                        final Pair<Boolean, UUID> originalResult = input;
                        return FutureUtils.transform(
                                getPendingChannelOperation(serverId,
                                        channelName, false),
                                new AsyncFunction<ChannelOperation, Pair<Boolean, UUID>>() {
                                    @Override
                                    public ListenableFuture<Pair<Boolean, UUID>> apply(
                                            ChannelOperation input)
                                            throws Exception {
                                        if (input == null) {
                                            // If there is no record any longer,
                                            // it could be that the last record
                                            // just expired. However, such a
                                            // scenario is unlikely and in order
                                            // to not make the logic more
                                            // complex than we already is, we
                                            // we still fail the operation.
                                            return Futures.immediateFuture(
                                                    originalResult);
                                        }
                                        if (new EqualsBuilder()
                                                .append(operationId,
                                                        input.getOperationId())
                                                .append(operationType,
                                                        input.getOperationType())
                                                .append(operationData,
                                                        input.getOperationData())
                                                .isEquals()) {
                                            return Futures.immediateFuture(
                                                    Pair.<Boolean, UUID> of(
                                                            true, null));
                                        } else {
                                            return Futures.immediateFuture(
                                                    originalResult);
                                        }
                                    }
                                },
                                new AsyncFunction<Throwable, Pair<Boolean, UUID>>() {
                                    @Override
                                    public ListenableFuture<Pair<Boolean, UUID>> apply(
                                            Throwable input) throws Exception {
                                        // If the read operation fails, we
                                        // simply return the original result.
                                        // We could try the read again if it
                                        // timed out, but we would need to keep
                                        // track of the number of attempts and
                                        // this would make the logic even more
                                        // complex.
                                        return Futures.immediateFuture(
                                                originalResult);
                                    }
                                });
                    }
                }, new AsyncFunction<Throwable, Pair<Boolean, UUID>>() {
                    @Override
                    public ListenableFuture<Pair<Boolean, UUID>> apply(
                            Throwable input) throws Exception {
                        // For LWTs, write timeouts are likely, even if the
                        // operation actually succeeded (CASSANDRA-9328).
                        // For this reason, we do a SELECT with the serial
                        // consistency level to check whether there actually
                        // still is a record.
                        rethrowIfNotWriteTimeoutException(input);
                        final Throwable originalException = input;
                        return FutureUtils.transform(
                                getPendingChannelOperation(serverId,
                                        channelName, true),
                                new AsyncFunction<ChannelOperation, Pair<Boolean, UUID>>() {
                                    @Override
                                    public ListenableFuture<Pair<Boolean, UUID>> apply(
                                            ChannelOperation input) {
                                        if (input == null) {
                                            // If there is no record, the INSERT
                                            // has actually failed and we
                                            // rethrow the original exception if
                                            // we have no more tries left.
                                            if (remainingTries > 0) {
                                                return retryCreatePendingChannelOperation(
                                                        serverId, channelName,
                                                        operationId,
                                                        operationType,
                                                        operationData, ttl,
                                                        remainingTries - 1);
                                            } else {
                                                rethrow(originalException);
                                                // This code is never reached.
                                                return null;
                                            }
                                        }
                                        if (new EqualsBuilder()
                                                .append(operationId,
                                                        input.getOperationId())
                                                .append(operationType,
                                                        input.getOperationType())
                                                .append(operationData,
                                                        input.getOperationData())
                                                .isEquals()) {
                                            return Futures.immediateFuture(
                                                    Pair.<Boolean, UUID> of(
                                                            true, null));
                                        } else {
                                            return Futures.immediateFuture(
                                                    Pair.of(false, input
                                                            .getOperationId()));
                                        }
                                    }
                                },
                                new AsyncFunction<Throwable, Pair<Boolean, UUID>>() {
                                    @Override
                                    public ListenableFuture<Pair<Boolean, UUID>> apply(
                                            Throwable input) {
                                        if (remainingTries > 0) {
                                            return retryCreatePendingChannelOperation(
                                                    serverId, channelName,
                                                    operationId, operationType,
                                                    operationData, ttl,
                                                    remainingTries - 1);
                                        } else {
                                            // If the read operation fails as
                                            // well, we rather throw the
                                            // original exception.
                                            rethrow(originalException);
                                            // This code is never reached.
                                            return null;
                                        }
                                    }
                                });
                    }
                });
    }

    private void createTables(Session session) {
        // The channels table stores the IDs of all sample buckets for a
        // channel, ordered by their start time. Separate decimation levels use
        // separate buckets. We make the decimation level a clustering column
        // (and not part of the partition key), because the information stored
        // in the static columns applies to all decimation levels. We store the
        // control-system type and the available decimation levels because we
        // need this information when reading samples. We could find the
        // available decimation levels by iterating over all rows for a channel,
        // but this would be very inefficient. We also store the server ID, so
        // that we can find the server responsible for a channel, when needed.
        session.execute(SchemaBuilder.createTable(TABLE_CHANNELS)
                .addPartitionKey(COLUMN_CHANNEL_NAME, DataType.text())
                .addClusteringColumn(COLUMN_DECIMATION_LEVEL, DataType.cint())
                .addClusteringColumn(COLUMN_BUCKET_START_TIME,
                        DataType.bigint())
                .addStaticColumn(COLUMN_CHANNEL_DATA_ID, DataType.uuid())
                .addStaticColumn(COLUMN_CONTROL_SYSTEM_TYPE, DataType.text())
                .addStaticColumn(COLUMN_DECIMATION_LEVELS,
                        DataType.set(DataType.cint()))
                .addStaticColumn(COLUMN_SERVER_ID, DataType.uuid())
                .addColumn(COLUMN_BUCKET_END_TIME, DataType.bigint())
                .ifNotExists());
        // Most information about a channel (except the sample buckets) is
        // stored in the channels_by_server table, because typically this
        // information is only needed when a server starts and in this case we
        // need the data for all channels it owns. We store the ID of the
        // current (most recent) raw-sample bucket, so that a lookup for the
        // most recent sample is cheaper (it is just one additional query
        // because we do not have to find the bucket ID first).
        session.execute(SchemaBuilder.createTable(TABLE_CHANNELS_BY_SERVER)
                .addPartitionKey(COLUMN_SERVER_ID, DataType.uuid())
                .addClusteringColumn(COLUMN_CHANNEL_NAME, DataType.text())
                .addColumn(COLUMN_CHANNEL_DATA_ID, DataType.uuid())
                .addColumn(COLUMN_CONTROL_SYSTEM_TYPE, DataType.text())
                .addColumn(COLUMN_DECIMATION_LEVEL_TO_CURRENT_BUCKET_START_TIME,
                        DataType.map(DataType.cint(), DataType.bigint()))
                .addColumn(COLUMN_DECIMATION_LEVEL_TO_RETENTION_PERIOD,
                        DataType.map(DataType.cint(), DataType.cint()))
                .addColumn(COLUMN_ENABLED, DataType.cboolean())
                .addColumn(COLUMN_OPTIONS,
                        DataType.map(DataType.text(), DataType.text()))
                .ifNotExists());
        // We use a separate table for storing information about pending
        // operations affecting channels. Typically, we need this information
        // together with the channel configuration, so it might seem logical to
        // store it in the same table. However, we need to be able to create
        // information about a pending operation atomically, which we cannot do
        // easily if it is stored in the same table: Depending on whether other
        // data for the same key exists or not, we would have to use an UPDATE
        // or INSERT operation. In addition to that, DELETEs would be
        // complicated because in general we would not be able to delete all
        // data for a channel (because we cannot know whether there might still
        // be data of the other type that we should not delete) and thus we
        // would end up with stale entries for channels that do not exist any
        // longer.
        session.execute(SchemaBuilder
                .createTable(TABLE_PENDING_CHANNEL_OPERATIONS_BY_SERVER)
                .addPartitionKey(COLUMN_SERVER_ID, DataType.uuid())
                .addClusteringColumn(COLUMN_CHANNEL_NAME, DataType.text())
                .addColumn(COLUMN_OPERATION_DATA, DataType.text())
                .addColumn(COLUMN_OPERATION_ID, DataType.uuid())
                .addColumn(COLUMN_OPERATION_TYPE, DataType.text())
                .ifNotExists());
    }

    private <V> ListenableFuture<V> delayedFuture(
            final Callable<ListenableFuture<V>> callable,
            long delayMilliseconds) {
        final SettableFuture<V> future = SettableFuture.create();
        TimerTask timerTask = new TimerTask() {
            @Override
            public void run() {
                try {
                    FutureUtils.forward(callable.call(), future);
                } catch (Throwable t) {
                    future.setException(t);
                }
            }
        };
        timer.schedule(timerTask, delayMilliseconds);
        return future;
    }

    private ListenableFuture<Pair<Boolean, UUID>> deletePendingChannelOperation(
            final UUID serverId, final String channelName,
            final UUID operationId, final int remainingTries) {
        // We wrap the whole method body in a try-catch block, because we want
        // to avoid this method throwing an exception directly. Instead, we
        // return a future that throws the exception. This way, the calling code
        // can deal with exceptions in a unified way instead of having to deal
        // with exceptions from both this method and the returned future.
        try {
            if (log.isDebugEnabled()) {
                log.debug("deletePendingChannelOperation(serverId=" + serverId
                        + ", channelName=\""
                        + StringEscapeUtils.escapeJava(channelName)
                        + "\", operationId=" + operationId + ")");
            }
            checkInitialized();
            return FutureUtils.transform(
                    pendingChannelOperationsByServerAccessor
                            .deleteOperationIfIdMatches(serverId, channelName,
                                    operationId),
                    new AsyncFunction<ResultSet, Pair<Boolean, UUID>>() {
                        @Override
                        public ListenableFuture<Pair<Boolean, UUID>> apply(
                                ResultSet input) {
                            Row row = input.one();
                            boolean applied = input.wasApplied();
                            // If the operation has already been deleted (or
                            // disappeared because of its TTL), applied is false
                            // even though the row does not exist any longer. We
                            // can detect this situation by checking whether the
                            // row contains additional information. If the
                            // operation has been declined because the operation
                            // ID did not match, there must be a different
                            // operation ID.
                            if (applied || !row.getColumnDefinitions()
                                    .contains(COLUMN_OPERATION_ID)) {
                                return Futures.immediateFuture(
                                        Pair.<Boolean, UUID> of(true, null));
                            }
                            // If we got an operation ID, the delete failed
                            // because it did not match and we want to return
                            // the ID stored in the database. However, due to
                            // stale rows, we might get a null ID. For all
                            // practical purposes, we treat such a rows as a
                            // non-existing one, so we still return true.
                            UUID storedOperationId = row
                                    .getUUID(COLUMN_OPERATION_ID);
                            return Futures.immediateFuture(
                                    Pair.of(storedOperationId == null,
                                            storedOperationId));
                        }
                    }, new AsyncFunction<Throwable, Pair<Boolean, UUID>>() {
                        @Override
                        public ListenableFuture<Pair<Boolean, UUID>> apply(
                                Throwable input) {
                            // For LWTs, write timeouts are likely, even if the
                            // operation actually succeeded (CASSANDRA-9328).
                            // For this reason, we do a SELECT with the serial
                            // consistency level to check whether there actually
                            // still is a record.
                            rethrowIfNotWriteTimeoutException(input);
                            final Throwable originalException = input;
                            return FutureUtils.transform(
                                    getPendingChannelOperation(serverId,
                                            channelName, true),
                                    new AsyncFunction<ChannelOperation, Pair<Boolean, UUID>>() {
                                        @Override
                                        public ListenableFuture<Pair<Boolean, UUID>> apply(
                                                ChannelOperation input) {
                                            if (input == null) {
                                                return Futures.immediateFuture(
                                                        Pair.<Boolean, UUID> of(
                                                                true, null));
                                            } else {
                                                if (input.getOperationId()
                                                        .equals(operationId)) {
                                                    if (remainingTries > 0) {
                                                        return retryDeletePendingChannelOperation(
                                                                serverId,
                                                                channelName,
                                                                operationId,
                                                                remainingTries
                                                                        - 1);
                                                    } else {
                                                        rethrow(originalException);
                                                        // Never reached.
                                                        return null;
                                                    }
                                                } else {
                                                    return Futures
                                                            .immediateFuture(
                                                                    Pair.of(false,
                                                                            input.getOperationId()));
                                                }
                                            }
                                        }
                                    },
                                    new AsyncFunction<Throwable, Pair<Boolean, UUID>>() {
                                        @Override
                                        public ListenableFuture<Pair<Boolean, UUID>> apply(
                                                Throwable input)
                                                throws Exception {
                                            // If the read operation fails as
                                            // well, we only retry if we have
                                            // not reached the max. number of
                                            // attempts.
                                            if (remainingTries > 0) {
                                                return retryDeletePendingChannelOperation(
                                                        serverId, channelName,
                                                        operationId,
                                                        remainingTries - 1);
                                            } else {
                                                rethrow(originalException);
                                                // Never reached.
                                                return null;
                                            }
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

    private ListenableFuture<ChannelOperation> getPendingChannelOperation(
            UUID serverId, String channelName,
            boolean useSerialConsistencyLevel) {
        // We wrap the whole method body in a try-catch block, because we want
        // to avoid this method throwing an exception directly. Instead, we
        // return a future that throws the exception. This way, the calling code
        // can deal with exceptions in a unified way instead of having to deal
        // with exceptions from both this method and the returned future.
        try {
            checkInitialized();
            Statement selectStatement = pendingChannelOperationsByServerAccessor
                    .getOperation(serverId, channelName);
            if (useSerialConsistencyLevel) {
                selectStatement.setConsistencyLevel(serialConsistencyLevel);
            }
            return Futures.transform(session.executeAsync(selectStatement),
                    new Function<ResultSet, ChannelOperation>() {
                        @Override
                        public ChannelOperation apply(ResultSet input) {
                            Row row = input.one();
                            if (row == null) {
                                return null;
                            }
                            UUID operationId = row.getUUID(COLUMN_OPERATION_ID);
                            // We might find an operation with a null ID because
                            // the operation was updated with a shorter TTL than
                            // the one that was used when inserting it. This
                            // way, the primary key is still present in the
                            // database (and generates a row), but there is no
                            // actual record. We simply want to ignore such
                            // records and treat them as non-existing.
                            // Eventually, the TTL of the primary key will
                            // expire and the row will vanish.
                            if (operationId == null) {
                                return null;
                            }
                            return new ChannelOperation(
                                    row.getString(COLUMN_CHANNEL_NAME),
                                    row.getString(COLUMN_OPERATION_DATA),
                                    row.getUUID(COLUMN_OPERATION_ID),
                                    row.getString(COLUMN_OPERATION_TYPE),
                                    row.getUUID(COLUMN_SERVER_ID));
                        }
                    });
        } catch (Exception e) {
            // We do not catch errors, because they typically indicate a very
            // severe exception and it is possible that trying to wrap them in
            // a future would fail again.
            return Futures.immediateFailedFuture(e);
        }
    }

    private void initialize() {
        // We want to avoid running to initializations threads in parallel
        // because this would generate warnings about preparing the same queries
        // again.
        if (!initializationInProgress.compareAndSet(false, true)) {
            return;
        }
        try {
            Session session = cassandraProvider.getSession();
            this.serialConsistencyLevel = session.getCluster()
                    .getConfiguration().getQueryOptions()
                    .getSerialConsistencyLevel();
            createTables(session);
            createAccessors(session);
            this.session = session;
        } catch (Throwable t) {
            log.error(
                    "The initialization of the ChannelMetaDataDAO failed. Most likely, this is caused by the database being temporarily unavailable. The next initialization attempt is going to be made the next time the connection to the database is reestablished.",
                    t);
        } finally {
            initializationInProgress.set(false);
        }
    }

    private ListenableFuture<Pair<Boolean, UUID>> retryCreatePendingChannelOperation(
            final UUID serverId, final String channelName,
            final UUID operationId, final String operationType,
            final String operationData, final int ttl,
            final int remainingTries) {
        return delayedFuture(
                new Callable<ListenableFuture<Pair<Boolean, UUID>>>() {
                    @Override
                    public ListenableFuture<Pair<Boolean, UUID>> call()
                            throws Exception {
                        return createPendingChannelOperationRelaxed(serverId,
                                channelName, operationId, operationType,
                                operationData, ttl, remainingTries);
                    }
                },
                ThreadLocalRandom.current().nextLong(
                        pendingChannelOperationMinSleepMilliseconds,
                        pendingChannelOperationMaxSleepMilliseconds));
    }

    private ListenableFuture<Pair<Boolean, UUID>> retryDeletePendingChannelOperation(
            final UUID serverId, final String channelName,
            final UUID operationId, final int remainingTries) {
        return delayedFuture(
                new Callable<ListenableFuture<Pair<Boolean, UUID>>>() {
                    @Override
                    public ListenableFuture<Pair<Boolean, UUID>> call()
                            throws Exception {
                        return deletePendingChannelOperation(serverId,
                                channelName, operationId, remainingTries);
                    }
                },
                ThreadLocalRandom.current().nextLong(
                        pendingChannelOperationMinSleepMilliseconds,
                        pendingChannelOperationMaxSleepMilliseconds));
    }

    private ListenableFuture<Pair<Boolean, UUID>> retryUpdatePendingChannelOperationRelaxed(
            final UUID serverId, final String channelName,
            final UUID oldOperationId, final UUID newOperationId,
            final String newOperationType, final String newOperationData,
            final int ttl, final int remainingTries) {
        return delayedFuture(
                new Callable<ListenableFuture<Pair<Boolean, UUID>>>() {
                    @Override
                    public ListenableFuture<Pair<Boolean, UUID>> call()
                            throws Exception {
                        return updatePendingChannelOperationRelaxed(serverId,
                                channelName, oldOperationId, newOperationId,
                                newOperationType, newOperationData, ttl,
                                remainingTries);
                    }
                },
                ThreadLocalRandom.current().nextLong(
                        pendingChannelOperationMinSleepMilliseconds,
                        pendingChannelOperationMaxSleepMilliseconds));
    }

    private ListenableFuture<Pair<Boolean, UUID>> updatePendingChannelOperationRelaxed(
            final UUID serverId, final String channelName,
            final UUID oldOperationId, final UUID newOperationId,
            final String newOperationType, final String newOperationData,
            final int ttl, final int remainingTries) {
        return FutureUtils.transform(
                updatePendingChannelOperation(serverId, channelName,
                        oldOperationId, newOperationId, newOperationType,
                        newOperationData, ttl),
                new AsyncFunction<Pair<Boolean, UUID>, Pair<Boolean, UUID>>() {
                    @Override
                    public ListenableFuture<Pair<Boolean, UUID>> apply(
                            Pair<Boolean, UUID> input) throws Exception {
                        // If the operation succeeded or it failed because there
                        // is a pending operation that neither matched the old
                        // nor the new ID, we are done. If the result is false,
                        // but the existing pending operation has the new ID, a
                        // previous write operation that timed out might
                        // actually have been successful.
                        if (input.getLeft()
                                || !newOperationId.equals(input.getRight())) {
                            return Futures.immediateFuture(input);
                        }
                        // We read the complete record in order to verify
                        // whether the data in the existing record matches the
                        // data that we are trying to write. We do not have to
                        // do this with the serial consistency level because our
                        // previous write attempt already revealed the data.
                        final Pair<Boolean, UUID> originalResult = input;
                        return FutureUtils.transform(
                                getPendingChannelOperation(serverId,
                                        channelName, false),
                                new AsyncFunction<ChannelOperation, Pair<Boolean, UUID>>() {
                                    @Override
                                    public ListenableFuture<Pair<Boolean, UUID>> apply(
                                            ChannelOperation input)
                                            throws Exception {
                                        if (input == null) {
                                            // If there is no record any longer,
                                            // it could be that the last record
                                            // just expired. However, such a
                                            // scenario is unlikely and in order
                                            // to not make the logic more
                                            // complex than we already is, we
                                            // we still fail the operation.
                                            return Futures.immediateFuture(
                                                    originalResult);
                                        }
                                        if (new EqualsBuilder()
                                                .append(newOperationId,
                                                        input.getOperationId())
                                                .append(newOperationType,
                                                        input.getOperationType())
                                                .append(newOperationData,
                                                        input.getOperationData())
                                                .isEquals()) {
                                            return Futures.immediateFuture(
                                                    Pair.<Boolean, UUID> of(
                                                            true, null));
                                        } else {
                                            return Futures.immediateFuture(
                                                    originalResult);
                                        }
                                    }
                                },
                                new AsyncFunction<Throwable, Pair<Boolean, UUID>>() {
                                    @Override
                                    public ListenableFuture<Pair<Boolean, UUID>> apply(
                                            Throwable input) throws Exception {
                                        // If the read operation fails, we
                                        // simply return the original result.
                                        // We could try the read again if it
                                        // timed out, but we would need to keep
                                        // track of the number of attempts and
                                        // this would make the logic even more
                                        // complex.
                                        return Futures.immediateFuture(
                                                originalResult);
                                    }
                                });
                    }
                }, new AsyncFunction<Throwable, Pair<Boolean, UUID>>() {
                    @Override
                    public ListenableFuture<Pair<Boolean, UUID>> apply(
                            Throwable input) throws Exception {
                        // For LWTs, write timeouts are likely, even if the
                        // operation actually succeeded (CASSANDRA-9328).
                        // For this reason, we do a SELECT with the serial
                        // consistency level to check whether there actually
                        // still is a record.
                        rethrowIfNotWriteTimeoutException(input);
                        final Throwable originalException = input;
                        return FutureUtils.transform(
                                getPendingChannelOperation(serverId,
                                        channelName, true),
                                new AsyncFunction<ChannelOperation, Pair<Boolean, UUID>>() {
                                    @Override
                                    public ListenableFuture<Pair<Boolean, UUID>> apply(
                                            ChannelOperation input) {
                                        if (input == null) {
                                            // If there is no record, the UPDATE
                                            // would have failed anyway.
                                            return Futures.immediateFuture(
                                                    Pair.<Boolean, UUID> of(
                                                            false, null));
                                        }
                                        if (new EqualsBuilder()
                                                .append(newOperationId,
                                                        input.getOperationId())
                                                .append(newOperationType,
                                                        input.getOperationType())
                                                .append(newOperationData,
                                                        input.getOperationData())
                                                .isEquals()) {
                                            return Futures.immediateFuture(
                                                    Pair.<Boolean, UUID> of(
                                                            true, null));
                                        } else if (input.getOperationId()
                                                .equals(oldOperationId)
                                                && remainingTries > 0) {
                                            return retryUpdatePendingChannelOperationRelaxed(
                                                    serverId, channelName,
                                                    oldOperationId,
                                                    newOperationId,
                                                    newOperationType,
                                                    newOperationData, ttl,
                                                    remainingTries - 1);
                                        } else {
                                            return Futures.immediateFuture(
                                                    Pair.of(false, input
                                                            .getOperationId()));
                                        }
                                    }
                                },
                                new AsyncFunction<Throwable, Pair<Boolean, UUID>>() {
                                    @Override
                                    public ListenableFuture<Pair<Boolean, UUID>> apply(
                                            Throwable input) {
                                        // If the read operation fails as well,
                                        // we can only try again after some
                                        // delay.
                                        if (remainingTries > 0) {
                                            return retryUpdatePendingChannelOperationRelaxed(
                                                    serverId, channelName,
                                                    oldOperationId,
                                                    newOperationId,
                                                    newOperationType,
                                                    newOperationData, ttl,
                                                    remainingTries - 1);
                                        } else {
                                            rethrow(originalException);
                                            // This code is never reached.
                                            return null;
                                        }
                                    }
                                });
                    }
                });
    }

}
