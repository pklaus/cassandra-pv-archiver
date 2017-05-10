/*
 * Copyright 2016-2017 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.controlsystem.channelaccess.internal;

import java.util.UUID;

import com.aquenos.cassandra.pvarchiver.common.ObjectResultSet;
import com.aquenos.cassandra.pvarchiver.controlsystem.SampleBucketId;
import com.aquenos.cassandra.pvarchiver.controlsystem.SampleBucketState;
import com.aquenos.cassandra.pvarchiver.controlsystem.channelaccess.ChannelAccessControlSystemSupport;
import com.aquenos.cassandra.pvarchiver.controlsystem.channelaccess.ChannelAccessSample;
import com.aquenos.cassandra.pvarchiver.controlsystem.channelaccess.ChannelAccessSampleType;
import com.aquenos.cassandra.pvarchiver.controlsystem.util.ResultSetBasedObjectResultSet;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.annotations.Accessor;
import com.datastax.driver.mapping.annotations.Query;
import com.datastax.driver.mapping.annotations.QueryParameters;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * <p>
 * Database access layer for the Channel Access samples table. This class wraps
 * a {@link Session}, providing access to the
 * <code>channel_access_samples</code> table. It also takes care of creating
 * this table and the user-defined types it needs if they do not exist yet.
 * </p>
 * 
 * <p>
 * This class is intended for use by {@link ChannelAccessControlSystemSupport}
 * and its associated classes only.
 * </p>
 * 
 * @author Sebastian Marsching
 */
public class ChannelAccessDatabaseAccess {

    /**
     * Function that converts a result set into an object result-set containing
     * samples. This function is used when reading samples from the database.
     * This function can be operated in two modes: For original samples
     * (decimation level zero) and for decimated samples (all other decimation
     * levels).
     * 
     * @author Sebastian Marsching
     */
    private static class ResultSetToSampleSetFunction implements
            Function<ResultSet, ObjectResultSet<ChannelAccessSample>> {

        public ResultSetToSampleSetFunction(boolean convertsOriginalSamples) {
            this.convertsOriginalSamples = convertsOriginalSamples;
        }

        private boolean convertsOriginalSamples;

        @Override
        public ObjectResultSet<ChannelAccessSample> apply(ResultSet input) {
            ColumnDefinitions columnDefinitions = input.getColumnDefinitions();
            final String[] columnNames = new String[columnDefinitions.size()];
            int i = 0;
            for (Definition columnDefinition : columnDefinitions) {
                columnNames[i] = columnDefinition.getName();
                ++i;
            }
            return new ResultSetBasedObjectResultSet<ChannelAccessSample>(
                    input, new Function<Row, ChannelAccessSample>() {
                        @Override
                        public ChannelAccessSample apply(Row input) {
                            boolean gotSampleTime = false;
                            boolean gotSampleValue = false;
                            boolean isAggregatedSample = false;
                            boolean isDisabledSample = false;
                            boolean isDisconnectedSample = false;
                            boolean isRawSample = false;
                            long sampleTime = 0L;
                            ChannelAccessSampleType sampleType = null;
                            UDTValue sampleValue = null;
                            for (int i = 0; i < columnNames.length; ++i) {
                                if (!input.isNull(i)) {
                                    boolean isDisabledColumn = false;
                                    boolean isDisconnectedColumn = false;
                                    boolean isTimeColumn = false;
                                    String columnName = columnNames[i];
                                    switch (columnName) {
                                    case COLUMN_AGGREGATED_SCALAR_CHAR:
                                        sampleType = ChannelAccessSampleType.AGGREGATED_SCALAR_CHAR;
                                        isAggregatedSample = true;
                                        break;
                                    case COLUMN_AGGREGATED_SCALAR_DOUBLE:
                                        sampleType = ChannelAccessSampleType.AGGREGATED_SCALAR_DOUBLE;
                                        isAggregatedSample = true;
                                        break;
                                    case COLUMN_AGGREGATED_SCALAR_FLOAT:
                                        sampleType = ChannelAccessSampleType.AGGREGATED_SCALAR_FLOAT;
                                        isAggregatedSample = true;
                                        break;
                                    case COLUMN_AGGREGATED_SCALAR_LONG:
                                        sampleType = ChannelAccessSampleType.AGGREGATED_SCALAR_LONG;
                                        isAggregatedSample = true;
                                        break;
                                    case COLUMN_AGGREGATED_SCALAR_SHORT:
                                        sampleType = ChannelAccessSampleType.AGGREGATED_SCALAR_SHORT;
                                        isAggregatedSample = true;
                                        break;
                                    case COLUMN_ARRAY_CHAR:
                                        sampleType = ChannelAccessSampleType.ARRAY_CHAR;
                                        isRawSample = true;
                                        break;
                                    case COLUMN_ARRAY_DOUBLE:
                                        sampleType = ChannelAccessSampleType.ARRAY_DOUBLE;
                                        isRawSample = true;
                                        break;
                                    case COLUMN_ARRAY_ENUM:
                                        sampleType = ChannelAccessSampleType.ARRAY_ENUM;
                                        isRawSample = true;
                                        break;
                                    case COLUMN_ARRAY_FLOAT:
                                        sampleType = ChannelAccessSampleType.ARRAY_FLOAT;
                                        isRawSample = true;
                                        break;
                                    case COLUMN_ARRAY_LONG:
                                        sampleType = ChannelAccessSampleType.ARRAY_LONG;
                                        isRawSample = true;
                                        break;
                                    case COLUMN_ARRAY_SHORT:
                                        sampleType = ChannelAccessSampleType.ARRAY_SHORT;
                                        isRawSample = true;
                                        break;
                                    case COLUMN_ARRAY_STRING:
                                        sampleType = ChannelAccessSampleType.ARRAY_STRING;
                                        isRawSample = true;
                                        break;
                                    case COLUMN_DISABLED:
                                        isDisabledColumn = true;
                                        break;
                                    case COLUMN_DISCONNECTED:
                                        isDisconnectedColumn = true;
                                        break;
                                    case COLUMN_SAMPLE_TIME:
                                        isTimeColumn = true;
                                        break;
                                    case COLUMN_SCALAR_CHAR:
                                        sampleType = ChannelAccessSampleType.SCALAR_CHAR;
                                        isRawSample = true;
                                        break;
                                    case COLUMN_SCALAR_DOUBLE:
                                        sampleType = ChannelAccessSampleType.SCALAR_DOUBLE;
                                        isRawSample = true;
                                        break;
                                    case COLUMN_SCALAR_ENUM:
                                        sampleType = ChannelAccessSampleType.SCALAR_ENUM;
                                        isRawSample = true;
                                        break;
                                    case COLUMN_SCALAR_FLOAT:
                                        sampleType = ChannelAccessSampleType.SCALAR_FLOAT;
                                        isRawSample = true;
                                        break;
                                    case COLUMN_SCALAR_LONG:
                                        sampleType = ChannelAccessSampleType.SCALAR_LONG;
                                        isRawSample = true;
                                        break;
                                    case COLUMN_SCALAR_SHORT:
                                        sampleType = ChannelAccessSampleType.SCALAR_SHORT;
                                        isRawSample = true;
                                        break;
                                    case COLUMN_SCALAR_STRING:
                                        sampleType = ChannelAccessSampleType.SCALAR_STRING;
                                        isRawSample = true;
                                        break;
                                    default:
                                        continue;
                                    }
                                    if (isDisabledColumn) {
                                        if (input.getBool(i)) {
                                            isDisabledSample = true;
                                            gotSampleValue = true;
                                        }
                                    } else if (isDisconnectedColumn) {
                                        if (input.getBool(i)) {
                                            isDisconnectedSample = true;
                                            gotSampleValue = true;
                                        }
                                    } else if (isTimeColumn) {
                                        sampleTime = input.getLong(i);
                                        gotSampleTime = true;
                                    } else {
                                        sampleValue = input.getUDTValue(i);
                                        gotSampleValue = true;
                                    }
                                    if (gotSampleTime && gotSampleValue) {
                                        break;
                                    }
                                }
                            }
                            if (!gotSampleTime) {
                                throw new RuntimeException(
                                        "Encountered invalid sample row without a sample time.");
                            }
                            if (!gotSampleValue) {
                                throw new RuntimeException(
                                        "Encountered invalid sample row without a sample value.");
                            }
                            if (isDisabledSample) {
                                return new ChannelAccessDisabledSample(
                                        sampleTime, convertsOriginalSamples);
                            } else if (isDisconnectedSample) {
                                return new ChannelAccessDisconnectedSample(
                                        sampleTime, convertsOriginalSamples);
                            } else if (isRawSample) {
                                return new ChannelAccessRawSample(sampleTime,
                                        sampleType, sampleValue,
                                        convertsOriginalSamples);
                            } else if (isAggregatedSample) {
                                return new ChannelAccessAggregatedSample(
                                        sampleTime, sampleType, sampleValue);
                            } else {
                                throw new RuntimeException(
                                        "Invalid sample row: The sample is neither an aggregated, nor a disconnected, nor a raw sample.");
                            }
                        }
                    });
        }

    }

    /**
     * Accessor for the <code>channel_access_samples</code> table.
     * 
     * @author Sebastian Marsching
     */
    @Accessor
    private interface SamplesAccessor {

        @Query("DELETE FROM " + TABLE_SAMPLES + " WHERE "
                + COLUMN_CHANNEL_DATA_ID + " = ? AND "
                + COLUMN_DECIMATION_LEVEL + " = ? AND "
                + COLUMN_BUCKET_START_TIME + " = ?;")
        @QueryParameters(idempotent = true)
        ResultSetFuture deleteSampleBucket(UUID channelDataId,
                int decimationLevel, long bucketStartTime);

        @Query("SELECT " + COLUMN_CURRENT_BUCKET_SIZE + ", "
                + COLUMN_SAMPLE_TIME + " FROM " + TABLE_SAMPLES + " WHERE "
                + COLUMN_CHANNEL_DATA_ID + " = ? AND "
                + COLUMN_DECIMATION_LEVEL + " = ? AND "
                + COLUMN_BUCKET_START_TIME + " = ? ORDER BY "
                + COLUMN_SAMPLE_TIME + " DESC LIMIT 1;")
        @QueryParameters(idempotent = true)
        ResultSetFuture getCurrentBucketSizeAndLatestSampleTime(
                UUID channelDataId, int decimationLevel, long bucketStartTime);

        @Query("SELECT " + COLUMN_SAMPLE_TIME + ", " + COLUMN_ARRAY_CHAR + ", "
                + COLUMN_ARRAY_DOUBLE + ", " + COLUMN_ARRAY_ENUM + ", "
                + COLUMN_ARRAY_FLOAT + ", " + COLUMN_ARRAY_LONG + ", "
                + COLUMN_ARRAY_SHORT + ", " + COLUMN_ARRAY_STRING + ", "
                + COLUMN_DISABLED + ", " + COLUMN_DISCONNECTED + ", "
                + COLUMN_AGGREGATED_SCALAR_CHAR + ", "
                + COLUMN_AGGREGATED_SCALAR_DOUBLE + ", "
                + COLUMN_AGGREGATED_SCALAR_FLOAT + ", "
                + COLUMN_AGGREGATED_SCALAR_LONG + ", "
                + COLUMN_AGGREGATED_SCALAR_SHORT + ", " + COLUMN_SCALAR_CHAR
                + ", " + COLUMN_SCALAR_DOUBLE + ", " + COLUMN_SCALAR_ENUM
                + ", " + COLUMN_SCALAR_FLOAT + ", " + COLUMN_SCALAR_LONG + ", "
                + COLUMN_SCALAR_SHORT + ", " + COLUMN_SCALAR_STRING + " FROM "
                + TABLE_SAMPLES + " WHERE " + COLUMN_CHANNEL_DATA_ID
                + " = ? AND " + COLUMN_DECIMATION_LEVEL + " = ? AND "
                + COLUMN_BUCKET_START_TIME + " = ? AND " + COLUMN_SAMPLE_TIME
                + " >= ? AND " + COLUMN_SAMPLE_TIME + " <= ? ORDER BY "
                + COLUMN_SAMPLE_TIME + " ASC;")
        @QueryParameters(idempotent = true)
        ResultSetFuture getSamples(UUID channelDataId, int decimationLevel,
                long bucketStartTime, long minSampleTime, long maxSampleTime);

        @Query("SELECT " + COLUMN_SAMPLE_TIME + ", " + COLUMN_ARRAY_CHAR + ", "
                + COLUMN_ARRAY_DOUBLE + ", " + COLUMN_ARRAY_ENUM + ", "
                + COLUMN_ARRAY_FLOAT + ", " + COLUMN_ARRAY_LONG + ", "
                + COLUMN_ARRAY_SHORT + ", " + COLUMN_ARRAY_STRING + ", "
                + COLUMN_DISABLED + ", " + COLUMN_DISCONNECTED + ", "
                + COLUMN_AGGREGATED_SCALAR_CHAR + ", "
                + COLUMN_AGGREGATED_SCALAR_DOUBLE + ", "
                + COLUMN_AGGREGATED_SCALAR_FLOAT + ", "
                + COLUMN_AGGREGATED_SCALAR_LONG + ", "
                + COLUMN_AGGREGATED_SCALAR_SHORT + ", " + COLUMN_SCALAR_CHAR
                + ", " + COLUMN_SCALAR_DOUBLE + ", " + COLUMN_SCALAR_ENUM
                + ", " + COLUMN_SCALAR_FLOAT + ", " + COLUMN_SCALAR_LONG + ", "
                + COLUMN_SCALAR_SHORT + ", " + COLUMN_SCALAR_STRING + " FROM "
                + TABLE_SAMPLES + " WHERE " + COLUMN_CHANNEL_DATA_ID
                + " = ? AND " + COLUMN_DECIMATION_LEVEL + " = ? AND "
                + COLUMN_BUCKET_START_TIME + " = ? AND " + COLUMN_SAMPLE_TIME
                + " >= ? AND " + COLUMN_SAMPLE_TIME + " <= ? ORDER BY "
                + COLUMN_SAMPLE_TIME + " ASC LIMIT ?;")
        @QueryParameters(idempotent = true)
        ResultSetFuture getSamples(UUID channelDataId, int decimationLevel,
                long bucketStartTime, long minSampleTime, long maxSampleTime,
                int limit);

        @Query("SELECT " + COLUMN_SAMPLE_TIME + ", " + COLUMN_ARRAY_CHAR + ", "
                + COLUMN_ARRAY_DOUBLE + ", " + COLUMN_ARRAY_ENUM + ", "
                + COLUMN_ARRAY_FLOAT + ", " + COLUMN_ARRAY_LONG + ", "
                + COLUMN_ARRAY_SHORT + ", " + COLUMN_ARRAY_STRING + ", "
                + COLUMN_DISABLED + ", " + COLUMN_DISCONNECTED + ", "
                + COLUMN_AGGREGATED_SCALAR_CHAR + ", "
                + COLUMN_AGGREGATED_SCALAR_DOUBLE + ", "
                + COLUMN_AGGREGATED_SCALAR_FLOAT + ", "
                + COLUMN_AGGREGATED_SCALAR_LONG + ", "
                + COLUMN_AGGREGATED_SCALAR_SHORT + ", " + COLUMN_SCALAR_CHAR
                + ", " + COLUMN_SCALAR_DOUBLE + ", " + COLUMN_SCALAR_ENUM
                + ", " + COLUMN_SCALAR_FLOAT + ", " + COLUMN_SCALAR_LONG + ", "
                + COLUMN_SCALAR_SHORT + ", " + COLUMN_SCALAR_STRING + " FROM "
                + TABLE_SAMPLES + " WHERE " + COLUMN_CHANNEL_DATA_ID
                + " = ? AND " + COLUMN_DECIMATION_LEVEL + " = ? AND "
                + COLUMN_BUCKET_START_TIME + " = ? AND " + COLUMN_SAMPLE_TIME
                + " >= ? AND " + COLUMN_SAMPLE_TIME + " <= ? ORDER BY "
                + COLUMN_SAMPLE_TIME + " DESC;")
        @QueryParameters(idempotent = true)
        ResultSetFuture getSamplesInReverseOrder(UUID channelDataId,
                int decimationLevel, long bucketStartTime, long minSampleTime,
                long maxSampleTime);

        @Query("SELECT " + COLUMN_SAMPLE_TIME + ", " + COLUMN_ARRAY_CHAR + ", "
                + COLUMN_ARRAY_DOUBLE + ", " + COLUMN_ARRAY_ENUM + ", "
                + COLUMN_ARRAY_FLOAT + ", " + COLUMN_ARRAY_LONG + ", "
                + COLUMN_ARRAY_SHORT + ", " + COLUMN_ARRAY_STRING + ", "
                + COLUMN_DISABLED + ", " + COLUMN_DISCONNECTED + ", "
                + COLUMN_AGGREGATED_SCALAR_CHAR + ", "
                + COLUMN_AGGREGATED_SCALAR_DOUBLE + ", "
                + COLUMN_AGGREGATED_SCALAR_FLOAT + ", "
                + COLUMN_AGGREGATED_SCALAR_LONG + ", "
                + COLUMN_AGGREGATED_SCALAR_SHORT + ", " + COLUMN_SCALAR_CHAR
                + ", " + COLUMN_SCALAR_DOUBLE + ", " + COLUMN_SCALAR_ENUM
                + ", " + COLUMN_SCALAR_FLOAT + ", " + COLUMN_SCALAR_LONG + ", "
                + COLUMN_SCALAR_SHORT + ", " + COLUMN_SCALAR_STRING + " FROM "
                + TABLE_SAMPLES + " WHERE " + COLUMN_CHANNEL_DATA_ID
                + " = ? AND " + COLUMN_DECIMATION_LEVEL + " = ? AND "
                + COLUMN_BUCKET_START_TIME + " = ? AND " + COLUMN_SAMPLE_TIME
                + " >= ? AND " + COLUMN_SAMPLE_TIME + " <= ? ORDER BY "
                + COLUMN_SAMPLE_TIME + " DESC LIMIT ?;")
        @QueryParameters(idempotent = true)
        ResultSetFuture getSamplesInReverseOrder(UUID channelDataId,
                int decimationLevel, long bucketStartTime, long minSampleTime,
                long maxSampleTime, int limit);

        @Query("INSERT INTO " + TABLE_SAMPLES + " (" + COLUMN_CHANNEL_DATA_ID
                + ", " + COLUMN_DECIMATION_LEVEL + ", "
                + COLUMN_BUCKET_START_TIME + ", " + COLUMN_SAMPLE_TIME + ", "
                + COLUMN_CURRENT_BUCKET_SIZE + ", "
                + COLUMN_AGGREGATED_SCALAR_CHAR
                + ") VALUES (?, ?, ?, ?, ?, ?);")
        @QueryParameters(idempotent = true)
        ResultSetFuture insertAggregatedScalarCharSampleAndCurrentBucketSize(
                UUID channelDataId, int decimationLevel, long bucketStartTime,
                long sampleTime, int currentBucketSize, UDTValue sampleValue);

        @Query("INSERT INTO " + TABLE_SAMPLES + " (" + COLUMN_CHANNEL_DATA_ID
                + ", " + COLUMN_DECIMATION_LEVEL + ", "
                + COLUMN_BUCKET_START_TIME + ", " + COLUMN_SAMPLE_TIME + ", "
                + COLUMN_CURRENT_BUCKET_SIZE + ", "
                + COLUMN_AGGREGATED_SCALAR_DOUBLE
                + ") VALUES (?, ?, ?, ?, ?, ?);")
        @QueryParameters(idempotent = true)
        ResultSetFuture insertAggregatedScalarDoubleSampleAndCurrentBucketSize(
                UUID channelDataId, int decimationLevel, long bucketStartTime,
                long sampleTime, int currentBucketSize, UDTValue sampleValue);

        @Query("INSERT INTO " + TABLE_SAMPLES + " (" + COLUMN_CHANNEL_DATA_ID
                + ", " + COLUMN_DECIMATION_LEVEL + ", "
                + COLUMN_BUCKET_START_TIME + ", " + COLUMN_SAMPLE_TIME + ", "
                + COLUMN_CURRENT_BUCKET_SIZE + ", "
                + COLUMN_AGGREGATED_SCALAR_FLOAT
                + ") VALUES (?, ?, ?, ?, ?, ?);")
        @QueryParameters(idempotent = true)
        ResultSetFuture insertAggregatedScalarFloatSampleAndCurrentBucketSize(
                UUID channelDataId, int decimationLevel, long bucketStartTime,
                long sampleTime, int currentBucketSize, UDTValue sampleValue);

        @Query("INSERT INTO " + TABLE_SAMPLES + " (" + COLUMN_CHANNEL_DATA_ID
                + ", " + COLUMN_DECIMATION_LEVEL + ", "
                + COLUMN_BUCKET_START_TIME + ", " + COLUMN_SAMPLE_TIME + ", "
                + COLUMN_CURRENT_BUCKET_SIZE + ", "
                + COLUMN_AGGREGATED_SCALAR_LONG
                + ") VALUES (?, ?, ?, ?, ?, ?);")
        @QueryParameters(idempotent = true)
        ResultSetFuture insertAggregatedScalarLongSampleAndCurrentBucketSize(
                UUID channelDataId, int decimationLevel, long bucketStartTime,
                long sampleTime, int currentBucketSize, UDTValue sampleValue);

        @Query("INSERT INTO " + TABLE_SAMPLES + " (" + COLUMN_CHANNEL_DATA_ID
                + ", " + COLUMN_DECIMATION_LEVEL + ", "
                + COLUMN_BUCKET_START_TIME + ", " + COLUMN_SAMPLE_TIME + ", "
                + COLUMN_CURRENT_BUCKET_SIZE + ", "
                + COLUMN_AGGREGATED_SCALAR_SHORT
                + ") VALUES (?, ?, ?, ?, ?, ?);")
        @QueryParameters(idempotent = true)
        ResultSetFuture insertAggregatedScalarShortSampleAndCurrentBucketSize(
                UUID channelDataId, int decimationLevel, long bucketStartTime,
                long sampleTime, int currentBucketSize, UDTValue sampleValue);

        @Query("INSERT INTO " + TABLE_SAMPLES + " (" + COLUMN_CHANNEL_DATA_ID
                + ", " + COLUMN_DECIMATION_LEVEL + ", "
                + COLUMN_BUCKET_START_TIME + ", " + COLUMN_SAMPLE_TIME + ", "
                + COLUMN_CURRENT_BUCKET_SIZE + ", " + COLUMN_ARRAY_CHAR
                + ") VALUES (?, ?, ?, ?, ?, ?);")
        @QueryParameters(idempotent = true)
        ResultSetFuture insertArrayCharSampleAndCurrentBucketSize(
                UUID channelDataId, int decimationLevel, long bucketStartTime,
                long sampleTime, int currentBucketSize, UDTValue sampleValue);

        @Query("INSERT INTO " + TABLE_SAMPLES + " (" + COLUMN_CHANNEL_DATA_ID
                + ", " + COLUMN_DECIMATION_LEVEL + ", "
                + COLUMN_BUCKET_START_TIME + ", " + COLUMN_SAMPLE_TIME + ", "
                + COLUMN_CURRENT_BUCKET_SIZE + ", " + COLUMN_ARRAY_DOUBLE
                + ") VALUES (?, ?, ?, ?, ?, ?);")
        @QueryParameters(idempotent = true)
        ResultSetFuture insertArrayDoubleSampleAndCurrentBucketSize(
                UUID channelDataId, int decimationLevel, long bucketStartTime,
                long sampleTime, int currentBucketSize, UDTValue sampleValue);

        @Query("INSERT INTO " + TABLE_SAMPLES + " (" + COLUMN_CHANNEL_DATA_ID
                + ", " + COLUMN_DECIMATION_LEVEL + ", "
                + COLUMN_BUCKET_START_TIME + ", " + COLUMN_SAMPLE_TIME + ", "
                + COLUMN_CURRENT_BUCKET_SIZE + ", " + COLUMN_ARRAY_ENUM
                + ") VALUES (?, ?, ?, ?, ?, ?);")
        @QueryParameters(idempotent = true)
        ResultSetFuture insertArrayEnumSampleAndCurrentBucketSize(
                UUID channelDataId, int decimationLevel, long bucketStartTime,
                long sampleTime, int currentBucketSize, UDTValue sampleValue);

        @Query("INSERT INTO " + TABLE_SAMPLES + " (" + COLUMN_CHANNEL_DATA_ID
                + ", " + COLUMN_DECIMATION_LEVEL + ", "
                + COLUMN_BUCKET_START_TIME + ", " + COLUMN_SAMPLE_TIME + ", "
                + COLUMN_CURRENT_BUCKET_SIZE + ", " + COLUMN_ARRAY_FLOAT
                + ") VALUES (?, ?, ?, ?, ?, ?);")
        @QueryParameters(idempotent = true)
        ResultSetFuture insertArrayFloatSampleAndCurrentBucketSize(
                UUID channelDataId, int decimationLevel, long bucketStartTime,
                long sampleTime, int currentBucketSize, UDTValue sampleValue);

        @Query("INSERT INTO " + TABLE_SAMPLES + " (" + COLUMN_CHANNEL_DATA_ID
                + ", " + COLUMN_DECIMATION_LEVEL + ", "
                + COLUMN_BUCKET_START_TIME + ", " + COLUMN_SAMPLE_TIME + ", "
                + COLUMN_CURRENT_BUCKET_SIZE + ", " + COLUMN_ARRAY_LONG
                + ") VALUES (?, ?, ?, ?, ?, ?);")
        @QueryParameters(idempotent = true)
        ResultSetFuture insertArrayLongSampleAndCurrentBucketSize(
                UUID channelDataId, int decimationLevel, long bucketStartTime,
                long sampleTime, int currentBucketSize, UDTValue sampleValue);

        @Query("INSERT INTO " + TABLE_SAMPLES + " (" + COLUMN_CHANNEL_DATA_ID
                + ", " + COLUMN_DECIMATION_LEVEL + ", "
                + COLUMN_BUCKET_START_TIME + ", " + COLUMN_SAMPLE_TIME + ", "
                + COLUMN_CURRENT_BUCKET_SIZE + ", " + COLUMN_ARRAY_SHORT
                + ") VALUES (?, ?, ?, ?, ?, ?);")
        @QueryParameters(idempotent = true)
        ResultSetFuture insertArrayShortSampleAndCurrentBucketSize(
                UUID channelDataId, int decimationLevel, long bucketStartTime,
                long sampleTime, int currentBucketSize, UDTValue sampleValue);

        @Query("INSERT INTO " + TABLE_SAMPLES + " (" + COLUMN_CHANNEL_DATA_ID
                + ", " + COLUMN_DECIMATION_LEVEL + ", "
                + COLUMN_BUCKET_START_TIME + ", " + COLUMN_SAMPLE_TIME + ", "
                + COLUMN_CURRENT_BUCKET_SIZE + ", " + COLUMN_ARRAY_STRING
                + ") VALUES (?, ?, ?, ?, ?, ?);")
        @QueryParameters(idempotent = true)
        ResultSetFuture insertArrayStringSampleAndCurrentBucketSize(
                UUID channelDataId, int decimationLevel, long bucketStartTime,
                long sampleTime, int currentBucketSize, UDTValue sampleValue);

        @Query("INSERT INTO " + TABLE_SAMPLES + " (" + COLUMN_CHANNEL_DATA_ID
                + ", " + COLUMN_DECIMATION_LEVEL + ", "
                + COLUMN_BUCKET_START_TIME + ", " + COLUMN_SAMPLE_TIME + ", "
                + COLUMN_CURRENT_BUCKET_SIZE + ", " + COLUMN_DISABLED
                + ") VALUES (?, ?, ?, ?, ?, true);")
        @QueryParameters(idempotent = true)
        ResultSetFuture insertDisabledSampleAndCurrentBucketSize(
                UUID channelDataId, int decimationLevel, long bucketStartTime,
                long sampleTime, int currentBucketSize);

        @Query("INSERT INTO " + TABLE_SAMPLES + " (" + COLUMN_CHANNEL_DATA_ID
                + ", " + COLUMN_DECIMATION_LEVEL + ", "
                + COLUMN_BUCKET_START_TIME + ", " + COLUMN_SAMPLE_TIME + ", "
                + COLUMN_CURRENT_BUCKET_SIZE + ", " + COLUMN_DISCONNECTED
                + ") VALUES (?, ?, ?, ?, ?, true);")
        @QueryParameters(idempotent = true)
        ResultSetFuture insertDisconnectedSampleAndCurrentBucketSize(
                UUID channelDataId, int decimationLevel, long bucketStartTime,
                long sampleTime, int currentBucketSize);

        @Query("INSERT INTO " + TABLE_SAMPLES + " (" + COLUMN_CHANNEL_DATA_ID
                + ", " + COLUMN_DECIMATION_LEVEL + ", "
                + COLUMN_BUCKET_START_TIME + ", " + COLUMN_SAMPLE_TIME + ", "
                + COLUMN_CURRENT_BUCKET_SIZE + ", " + COLUMN_SCALAR_CHAR
                + ") VALUES (?, ?, ?, ?, ?, ?);")
        @QueryParameters(idempotent = true)
        ResultSetFuture insertScalarCharSampleAndCurrentBucketSize(
                UUID channelDataId, int decimationLevel, long bucketStartTime,
                long sampleTime, int currentBucketSize, UDTValue sampleValue);

        @Query("INSERT INTO " + TABLE_SAMPLES + " (" + COLUMN_CHANNEL_DATA_ID
                + ", " + COLUMN_DECIMATION_LEVEL + ", "
                + COLUMN_BUCKET_START_TIME + ", " + COLUMN_SAMPLE_TIME + ", "
                + COLUMN_CURRENT_BUCKET_SIZE + ", " + COLUMN_SCALAR_DOUBLE
                + ") VALUES (?, ?, ?, ?, ?, ?);")
        @QueryParameters(idempotent = true)
        ResultSetFuture insertScalarDoubleSampleAndCurrentBucketSize(
                UUID channelDataId, int decimationLevel, long bucketStartTime,
                long sampleTime, int currentBucketSize, UDTValue sampleValue);

        @Query("INSERT INTO " + TABLE_SAMPLES + " (" + COLUMN_CHANNEL_DATA_ID
                + ", " + COLUMN_DECIMATION_LEVEL + ", "
                + COLUMN_BUCKET_START_TIME + ", " + COLUMN_SAMPLE_TIME + ", "
                + COLUMN_CURRENT_BUCKET_SIZE + ", " + COLUMN_SCALAR_ENUM
                + ") VALUES (?, ?, ?, ?, ?, ?);")
        @QueryParameters(idempotent = true)
        ResultSetFuture insertScalarEnumSampleAndCurrentBucketSize(
                UUID channelDataId, int decimationLevel, long bucketStartTime,
                long sampleTime, int currentBucketSize, UDTValue sampleValue);

        @Query("INSERT INTO " + TABLE_SAMPLES + " (" + COLUMN_CHANNEL_DATA_ID
                + ", " + COLUMN_DECIMATION_LEVEL + ", "
                + COLUMN_BUCKET_START_TIME + ", " + COLUMN_SAMPLE_TIME + ", "
                + COLUMN_CURRENT_BUCKET_SIZE + ", " + COLUMN_SCALAR_FLOAT
                + ") VALUES (?, ?, ?, ?, ?, ?);")
        @QueryParameters(idempotent = true)
        ResultSetFuture insertScalarFloatSampleAndCurrentBucketSize(
                UUID channelDataId, int decimationLevel, long bucketStartTime,
                long sampleTime, int currentBucketSize, UDTValue sampleValue);

        @Query("INSERT INTO " + TABLE_SAMPLES + " (" + COLUMN_CHANNEL_DATA_ID
                + ", " + COLUMN_DECIMATION_LEVEL + ", "
                + COLUMN_BUCKET_START_TIME + ", " + COLUMN_SAMPLE_TIME + ", "
                + COLUMN_CURRENT_BUCKET_SIZE + ", " + COLUMN_SCALAR_LONG
                + ") VALUES (?, ?, ?, ?, ?, ?);")
        @QueryParameters(idempotent = true)
        ResultSetFuture insertScalarLongSampleAndCurrentBucketSize(
                UUID channelDataId, int decimationLevel, long bucketStartTime,
                long sampleTime, int currentBucketSize, UDTValue sampleValue);

        @Query("INSERT INTO " + TABLE_SAMPLES + " (" + COLUMN_CHANNEL_DATA_ID
                + ", " + COLUMN_DECIMATION_LEVEL + ", "
                + COLUMN_BUCKET_START_TIME + ", " + COLUMN_SAMPLE_TIME + ", "
                + COLUMN_CURRENT_BUCKET_SIZE + ", " + COLUMN_SCALAR_SHORT
                + ") VALUES (?, ?, ?, ?, ?, ?);")
        @QueryParameters(idempotent = true)
        ResultSetFuture insertScalarShortSampleAndCurrentBucketSize(
                UUID channelDataId, int decimationLevel, long bucketStartTime,
                long sampleTime, int currentBucketSize, UDTValue sampleValue);

        @Query("INSERT INTO " + TABLE_SAMPLES + " (" + COLUMN_CHANNEL_DATA_ID
                + ", " + COLUMN_DECIMATION_LEVEL + ", "
                + COLUMN_BUCKET_START_TIME + ", " + COLUMN_SAMPLE_TIME + ", "
                + COLUMN_CURRENT_BUCKET_SIZE + ", " + COLUMN_SCALAR_STRING
                + ") VALUES (?, ?, ?, ?, ?, ?);")
        @QueryParameters(idempotent = true)
        ResultSetFuture insertScalarStringSampleAndCurrentBucketSize(
                UUID channelDataId, int decimationLevel, long bucketStartTime,
                long sampleTime, int currentBucketSize, UDTValue sampleValue);

    }

    /**
     * Name of the column that stores an aggregated, scalar "char" value. This
     * field is package private because it is also needed by the
     * {@link SampleSizeEstimator}.
     */
    static final String COLUMN_AGGREGATED_SCALAR_CHAR = "gs_char";

    /**
     * Name of the column that stores an aggregated, scalar "double" value. This
     * field is package private because it is also needed by the
     * {@link SampleSizeEstimator}.
     */
    static final String COLUMN_AGGREGATED_SCALAR_DOUBLE = "gs_double";

    /**
     * Name of the column that stores an aggregated, scalar "float" value. This
     * field is package private because it is also needed by the
     * {@link SampleSizeEstimator}.
     */
    static final String COLUMN_AGGREGATED_SCALAR_FLOAT = "gs_float";

    /**
     * Name of the column that stores an aggregated, scalar "long" value. This
     * field is package private because it is also needed by the
     * {@link SampleSizeEstimator}.
     */
    static final String COLUMN_AGGREGATED_SCALAR_LONG = "gs_long";

    /**
     * Name of the column that stores an aggregated, scalar "short" value. This
     * field is package private because it is also needed by the
     * {@link SampleSizeEstimator}.
     */
    static final String COLUMN_AGGREGATED_SCALAR_SHORT = "gs_short";

    /**
     * Name of the column that stores an array "char" value. This field is
     * package private because it is also needed by the
     * {@link SampleSizeEstimator}.
     */
    static final String COLUMN_ARRAY_CHAR = "a_char";

    /**
     * Name of the column that stores an array "double" value. This field is
     * package private because it is also needed by the
     * {@link SampleSizeEstimator}.
     */
    static final String COLUMN_ARRAY_DOUBLE = "a_double";

    /**
     * Name of the column that stores an array "enum" value. This field is
     * package private because it is also needed by the
     * {@link SampleSizeEstimator}.
     */
    static final String COLUMN_ARRAY_ENUM = "a_enum";

    /**
     * Name of the column that stores an array "float" value. This field is
     * package private because it is also needed by the
     * {@link SampleSizeEstimator}.
     */
    static final String COLUMN_ARRAY_FLOAT = "a_float";

    /**
     * Name of the column that stores an array "long" value. This field is
     * package private because it is also needed by the
     * {@link SampleSizeEstimator}.
     */
    static final String COLUMN_ARRAY_LONG = "a_long";

    /**
     * Name of the column that stores an array "short" value. This field is
     * package private because it is also needed by the
     * {@link SampleSizeEstimator}.
     */
    static final String COLUMN_ARRAY_SHORT = "a_short";

    /**
     * Name of the column that stores an array "string" value. This field is
     * package private because it is also needed by the
     * {@link SampleSizeEstimator}.
     */
    static final String COLUMN_ARRAY_STRING = "a_string";

    /**
     * Name of the column that stores the "disabled" flag that marks a special
     * sample that is written when a channel is disabled. This field is package
     * private because it is also needed by the {@link SampleSizeEstimator}.
     */
    static final String COLUMN_DISABLED = "disabled";

    /**
     * Name of the column that stores the "disconnected" flag that marks a
     * special sample that is written when a channel is disconnected. This field
     * is package private because it is also needed by the
     * {@link SampleSizeEstimator}.
     */
    static final String COLUMN_DISCONNECTED = "disconnected";

    /**
     * Name of the column that stores a scalar "char" value. This field is
     * package private because it is also needed by the
     * {@link SampleSizeEstimator}.
     */
    static final String COLUMN_SCALAR_CHAR = "s_char";

    /**
     * Name of the column that stores a scalar "double" value. This field is
     * package private because it is also needed by the
     * {@link SampleSizeEstimator}.
     */
    static final String COLUMN_SCALAR_DOUBLE = "s_double";

    /**
     * Name of the column that stores a scalar "enum" value. This field is
     * package private because it is also needed by the
     * {@link SampleSizeEstimator}.
     */
    static final String COLUMN_SCALAR_ENUM = "s_enum";

    /**
     * Name of the column that stores a scalar "float" value. This field is
     * package private because it is also needed by the
     * {@link SampleSizeEstimator}.
     */
    static final String COLUMN_SCALAR_FLOAT = "s_float";

    /**
     * Name of the column that stores a scalar "long" value. This field is
     * package private because it is also needed by the
     * {@link SampleSizeEstimator}.
     */
    static final String COLUMN_SCALAR_LONG = "s_long";

    /**
     * Name of the column that stores a scalar "short" value. This field is
     * package private because it is also needed by the
     * {@link SampleSizeEstimator}.
     */
    static final String COLUMN_SCALAR_SHORT = "s_short";

    /**
     * Name of the column that stores a scalar "string" value. This field is
     * package private because it is also needed by the
     * {@link SampleSizeEstimator}.
     */
    static final String COLUMN_SCALAR_STRING = "s_string";

    private static final String COLUMN_CHANNEL_DATA_ID = "channel_data_id";
    private static final String COLUMN_DECIMATION_LEVEL = "decimation_level";
    private static final String COLUMN_BUCKET_START_TIME = "bucket_start_time";
    private static final String COLUMN_CURRENT_BUCKET_SIZE = "current_bucket_size";
    private static final String COLUMN_SAMPLE_TIME = "sample_time";
    private static final String TABLE_SAMPLES = "channel_access_samples";

    private SamplesAccessor accessor;
    private final Function<Object, Void> anyToVoid = Functions.constant(null);
    private final Function<ResultSet, SampleBucketState> resultSetToSampleBucketState = new Function<ResultSet, SampleBucketState>() {
        @Override
        public SampleBucketState apply(ResultSet input) {
            Row row = input.one();
            int currentBucketSize;
            long latestSampleTimeStamp;
            if (row == null) {
                currentBucketSize = 0;
                latestSampleTimeStamp = 0L;
            } else {
                currentBucketSize = row.getInt(COLUMN_CURRENT_BUCKET_SIZE);
                latestSampleTimeStamp = row.getLong(COLUMN_SAMPLE_TIME);
            }
            return new SampleBucketState(currentBucketSize,
                    latestSampleTimeStamp);
        }
    };
    private final ResultSetToSampleSetFunction resultSetToDecimatedSampleSet = new ResultSetToSampleSetFunction(
            false);
    private final ResultSetToSampleSetFunction resultSetToOriginalSampleSet = new ResultSetToSampleSetFunction(
            true);
    private ChannelAccessSampleValueAccess sampleValueAccess;
    private Session session;

    /**
     * Creates the database access for the specified session. This implicitly
     * creates the tables needed by this database access if they do not exist
     * yet.
     * 
     * @param session
     *            session for accessing the database.
     * @param sampleValueAccess
     *            utility used by this database access for handling sample
     *            values.
     * @throws NullPointerException
     *             if <code>session</code> is <code>null</code>.
     * @throws RuntimeException
     *             if there is a problem with the database connection (e.g. a
     *             table cannot be created).
     */
    public ChannelAccessDatabaseAccess(Session session,
            ChannelAccessSampleValueAccess sampleValueAccess) {
        this.session = session;
        this.sampleValueAccess = sampleValueAccess;
        createTable();
        createAccessor();
    }

    /**
     * Deletes all samples associated with the specified sample bucket and the
     * stored bucket size. The operation finishes asynchronously. The returned
     * future completes when the operation completes and fails if the operation
     * fails.
     * 
     * @param bucketId
     *            unique identifier of the sample bucket that shall be deleted.
     * @return future that completes when the delete operation has finished or
     *         finally failed.
     */
    public ListenableFuture<Void> deleteSamples(SampleBucketId bucketId) {
        try {
            return Futures.transform(accessor.deleteSampleBucket(
                    bucketId.getChannelDataId(), bucketId.getDecimationLevel(),
                    bucketId.getBucketStartTime()), anyToVoid);
        } catch (Throwable t) {
            return Futures.immediateFailedFuture(t);
        }
    }

    /**
     * Returns the state of the specified sample bucket. The state stores
     * information about the total size of the samples currently stored in the
     * bucket and about the time stamp of the latest sample stored in the
     * bucket. The operation finishes asynchronously. The returned future
     * completes when the operation completes and fails if the operation fails.
     * 
     * @param bucketId
     *            unique identifier of the bucket for which the meta data shall
     *            be received.
     * @return future that eventually provides the meta data for the specified
     *         sample bucket and throws an exception when the retrieval
     *         operation has finally failed. If no meta-data is stored for the
     *         specified bucket, the returned bucket state specifies a bucket
     *         size of zero and a time stamp of zero for the latest sample.
     */
    public ListenableFuture<SampleBucketState> getSampleBucketState(
            SampleBucketId bucketId) {
        try {
            return Futures.transform(
                    accessor.getCurrentBucketSizeAndLatestSampleTime(
                            bucketId.getChannelDataId(),
                            bucketId.getDecimationLevel(),
                            bucketId.getBucketStartTime()),
                    resultSetToSampleBucketState);
        } catch (Throwable t) {
            return Futures.immediateFailedFuture(t);
        }
    }

    /**
     * <p>
     * Retrieves samples from the specified sample bucket. The result set
     * containing the samples matching the query is returned through a future.
     * If the query fails, the future throws an exception. However, due to the
     * iterative nature of the retrieval process, the result set returned by the
     * future may also throw an exception when part of the retrieval process
     * fails at a later time.
     * </p>
     * 
     * <p>
     * The samples in the bucket that have a time-stamp between
     * <code>minimumSampleTime</code> and <code>maximumSampleTime</code> (both
     * inclusive) are returned. Depending on the <code>reverseOrder</code>
     * parameter, the returned samples are ordered by the natural order of their
     * time-stamps (ascending, older samples first) or by the inverse of the
     * natural order of their time stamps (descending, newer samples first). If
     * there are more samples in the specified interval than the specified
     * <code>limit</code>, only the first <code>limit</code> samples are
     * returned. If <code>limit</code> is negative, the number of samples
     * returned is unbounded.
     * </p>
     * 
     * @param bucketId
     *            identifer of the sample bucket from which samples shall be
     *            retrieved.
     * @param minimumSampleTime
     *            lower limit (inclusive) of the time interval for which samples
     *            shall be retrieved.
     * @param maximumSampleTime
     *            upper limit (inclusive) of the time interveral for which
     *            samples shall be retrieved.
     * @param limit
     *            maximum number of samples to be retrieved. If negative, the
     *            number of samples is unbounded.
     * @param reverseOrder
     *            <code>true</code> if the order of the samples shall be
     *            reversed (newest samples first). <code>false</code> if the
     *            samples shall be returned in the natural order of their
     *            time-stamps (oldest samples first).
     * @return future that provides a result set containing the samples matching
     *         the specified criteria.
     */
    public ListenableFuture<ObjectResultSet<ChannelAccessSample>> getSamples(
            SampleBucketId bucketId, long minimumSampleTime,
            long maximumSampleTime, int limit, boolean reverseOrder) {
        try {
            ResultSetFuture resultSetFuture;
            if (reverseOrder) {
                if (limit < 0) {
                    resultSetFuture = accessor.getSamplesInReverseOrder(
                            bucketId.getChannelDataId(),
                            bucketId.getDecimationLevel(),
                            bucketId.getBucketStartTime(), minimumSampleTime,
                            maximumSampleTime);
                } else {
                    resultSetFuture = accessor.getSamplesInReverseOrder(
                            bucketId.getChannelDataId(),
                            bucketId.getDecimationLevel(),
                            bucketId.getBucketStartTime(), minimumSampleTime,
                            maximumSampleTime, limit);
                }

            } else {
                if (limit < 0) {
                    resultSetFuture = accessor.getSamples(
                            bucketId.getChannelDataId(),
                            bucketId.getDecimationLevel(),
                            bucketId.getBucketStartTime(), minimumSampleTime,
                            maximumSampleTime);
                } else {
                    resultSetFuture = accessor.getSamples(
                            bucketId.getChannelDataId(),
                            bucketId.getDecimationLevel(),
                            bucketId.getBucketStartTime(), minimumSampleTime,
                            maximumSampleTime, limit);
                }
            }
            return Futures.transform(resultSetFuture, bucketId
                    .getDecimationLevel() == 0 ? resultSetToOriginalSampleSet
                    : resultSetToDecimatedSampleSet);
        } catch (Throwable t) {
            return Futures.immediateFailedFuture(t);
        }
    }

    /**
     * Writes a sample to the database and updates the current bucket size. The
     * operation finishes asynchronously. The returned future completes when the
     * operation completes and fails if the operation fails.
     * 
     * @param sample
     *            sample that shall be written.
     * @param bucketId
     *            unique identifier of the bucket to which the
     *            <code>sample</code> shall be appended.
     * @param newBucketSize
     *            new size of the sample bucket.
     * @return future that completes when the sample has been written and that
     *         throws an exception when the write operation has finally failed.
     */
    public ListenableFuture<Void> writeSample(ChannelAccessSample sample,
            SampleBucketId bucketId, int newBucketSize) {
        try {
            if (sample instanceof ChannelAccessRawSample) {
                ChannelAccessRawSample rawSample = (ChannelAccessRawSample) sample;
                UDTValue sampleValue = rawSample.getValue();
                switch (rawSample.getType()) {
                case ARRAY_CHAR:
                    return Futures.transform(accessor
                            .insertArrayCharSampleAndCurrentBucketSize(
                                    bucketId.getChannelDataId(),
                                    bucketId.getDecimationLevel(),
                                    bucketId.getBucketStartTime(),
                                    rawSample.getTimeStamp(), newBucketSize,
                                    sampleValue), anyToVoid);
                case ARRAY_DOUBLE:
                    return Futures.transform(accessor
                            .insertArrayDoubleSampleAndCurrentBucketSize(
                                    bucketId.getChannelDataId(),
                                    bucketId.getDecimationLevel(),
                                    bucketId.getBucketStartTime(),
                                    rawSample.getTimeStamp(), newBucketSize,
                                    sampleValue), anyToVoid);
                case ARRAY_ENUM:
                    return Futures.transform(accessor
                            .insertArrayEnumSampleAndCurrentBucketSize(
                                    bucketId.getChannelDataId(),
                                    bucketId.getDecimationLevel(),
                                    bucketId.getBucketStartTime(),
                                    rawSample.getTimeStamp(), newBucketSize,
                                    sampleValue), anyToVoid);
                case ARRAY_FLOAT:
                    return Futures.transform(accessor
                            .insertArrayFloatSampleAndCurrentBucketSize(
                                    bucketId.getChannelDataId(),
                                    bucketId.getDecimationLevel(),
                                    bucketId.getBucketStartTime(),
                                    rawSample.getTimeStamp(), newBucketSize,
                                    sampleValue), anyToVoid);
                case ARRAY_LONG:
                    return Futures.transform(accessor
                            .insertArrayLongSampleAndCurrentBucketSize(
                                    bucketId.getChannelDataId(),
                                    bucketId.getDecimationLevel(),
                                    bucketId.getBucketStartTime(),
                                    rawSample.getTimeStamp(), newBucketSize,
                                    sampleValue), anyToVoid);
                case ARRAY_SHORT:
                    return Futures.transform(accessor
                            .insertArrayShortSampleAndCurrentBucketSize(
                                    bucketId.getChannelDataId(),
                                    bucketId.getDecimationLevel(),
                                    bucketId.getBucketStartTime(),
                                    rawSample.getTimeStamp(), newBucketSize,
                                    sampleValue), anyToVoid);
                case ARRAY_STRING:
                    return Futures.transform(accessor
                            .insertArrayStringSampleAndCurrentBucketSize(
                                    bucketId.getChannelDataId(),
                                    bucketId.getDecimationLevel(),
                                    bucketId.getBucketStartTime(),
                                    rawSample.getTimeStamp(), newBucketSize,
                                    sampleValue), anyToVoid);
                case SCALAR_CHAR:
                    return Futures.transform(accessor
                            .insertScalarCharSampleAndCurrentBucketSize(
                                    bucketId.getChannelDataId(),
                                    bucketId.getDecimationLevel(),
                                    bucketId.getBucketStartTime(),
                                    rawSample.getTimeStamp(), newBucketSize,
                                    sampleValue), anyToVoid);
                case SCALAR_DOUBLE:
                    return Futures.transform(accessor
                            .insertScalarDoubleSampleAndCurrentBucketSize(
                                    bucketId.getChannelDataId(),
                                    bucketId.getDecimationLevel(),
                                    bucketId.getBucketStartTime(),
                                    rawSample.getTimeStamp(), newBucketSize,
                                    sampleValue), anyToVoid);
                case SCALAR_ENUM:
                    return Futures.transform(accessor
                            .insertScalarEnumSampleAndCurrentBucketSize(
                                    bucketId.getChannelDataId(),
                                    bucketId.getDecimationLevel(),
                                    bucketId.getBucketStartTime(),
                                    rawSample.getTimeStamp(), newBucketSize,
                                    sampleValue), anyToVoid);
                case SCALAR_FLOAT:
                    return Futures.transform(accessor
                            .insertScalarFloatSampleAndCurrentBucketSize(
                                    bucketId.getChannelDataId(),
                                    bucketId.getDecimationLevel(),
                                    bucketId.getBucketStartTime(),
                                    rawSample.getTimeStamp(), newBucketSize,
                                    sampleValue), anyToVoid);
                case SCALAR_LONG:
                    return Futures.transform(accessor
                            .insertScalarLongSampleAndCurrentBucketSize(
                                    bucketId.getChannelDataId(),
                                    bucketId.getDecimationLevel(),
                                    bucketId.getBucketStartTime(),
                                    rawSample.getTimeStamp(), newBucketSize,
                                    sampleValue), anyToVoid);
                case SCALAR_SHORT:
                    return Futures.transform(accessor
                            .insertScalarShortSampleAndCurrentBucketSize(
                                    bucketId.getChannelDataId(),
                                    bucketId.getDecimationLevel(),
                                    bucketId.getBucketStartTime(),
                                    rawSample.getTimeStamp(), newBucketSize,
                                    sampleValue), anyToVoid);
                case SCALAR_STRING:
                    return Futures.transform(accessor
                            .insertScalarStringSampleAndCurrentBucketSize(
                                    bucketId.getChannelDataId(),
                                    bucketId.getDecimationLevel(),
                                    bucketId.getBucketStartTime(),
                                    rawSample.getTimeStamp(), newBucketSize,
                                    sampleValue), anyToVoid);
                default:
                    throw new RuntimeException(
                            "Unexpected type for raw sample: "
                                    + rawSample.getType());
                }
            } else if (sample instanceof ChannelAccessAggregatedSample) {
                ChannelAccessAggregatedSample aggregatedSample = (ChannelAccessAggregatedSample) sample;
                UDTValue sampleValue = aggregatedSample.getValue();
                switch (aggregatedSample.getType()) {
                case AGGREGATED_SCALAR_CHAR:
                    return Futures
                            .transform(
                                    accessor.insertAggregatedScalarCharSampleAndCurrentBucketSize(
                                            bucketId.getChannelDataId(),
                                            bucketId.getDecimationLevel(),
                                            bucketId.getBucketStartTime(),
                                            aggregatedSample.getTimeStamp(),
                                            newBucketSize, sampleValue),
                                    anyToVoid);
                case AGGREGATED_SCALAR_DOUBLE:
                    return Futures
                            .transform(
                                    accessor.insertAggregatedScalarDoubleSampleAndCurrentBucketSize(
                                            bucketId.getChannelDataId(),
                                            bucketId.getDecimationLevel(),
                                            bucketId.getBucketStartTime(),
                                            aggregatedSample.getTimeStamp(),
                                            newBucketSize, sampleValue),
                                    anyToVoid);
                case AGGREGATED_SCALAR_FLOAT:
                    return Futures
                            .transform(
                                    accessor.insertAggregatedScalarFloatSampleAndCurrentBucketSize(
                                            bucketId.getChannelDataId(),
                                            bucketId.getDecimationLevel(),
                                            bucketId.getBucketStartTime(),
                                            aggregatedSample.getTimeStamp(),
                                            newBucketSize, sampleValue),
                                    anyToVoid);
                case AGGREGATED_SCALAR_LONG:
                    return Futures
                            .transform(
                                    accessor.insertAggregatedScalarLongSampleAndCurrentBucketSize(
                                            bucketId.getChannelDataId(),
                                            bucketId.getDecimationLevel(),
                                            bucketId.getBucketStartTime(),
                                            aggregatedSample.getTimeStamp(),
                                            newBucketSize, sampleValue),
                                    anyToVoid);
                case AGGREGATED_SCALAR_SHORT:
                    return Futures
                            .transform(
                                    accessor.insertAggregatedScalarShortSampleAndCurrentBucketSize(
                                            bucketId.getChannelDataId(),
                                            bucketId.getDecimationLevel(),
                                            bucketId.getBucketStartTime(),
                                            aggregatedSample.getTimeStamp(),
                                            newBucketSize, sampleValue),
                                    anyToVoid);
                default:
                    throw new RuntimeException(
                            "Unexpected type for aggregated sample: "
                                    + aggregatedSample.getType());
                }
            } else if (sample instanceof ChannelAccessDisabledSample) {
                return Futures.transform(accessor
                        .insertDisabledSampleAndCurrentBucketSize(
                                bucketId.getChannelDataId(),
                                bucketId.getDecimationLevel(),
                                bucketId.getBucketStartTime(),
                                sample.getTimeStamp(), newBucketSize),
                        anyToVoid);
            } else if (sample instanceof ChannelAccessDisconnectedSample) {
                return Futures.transform(accessor
                        .insertDisconnectedSampleAndCurrentBucketSize(
                                bucketId.getChannelDataId(),
                                bucketId.getDecimationLevel(),
                                bucketId.getBucketStartTime(),
                                sample.getTimeStamp(), newBucketSize),
                        anyToVoid);
            } else {
                throw new IllegalArgumentException(
                        "Got sample of unsupported type: "
                                + sample.getClass().getName());
            }
        } catch (Throwable t) {
            return Futures.immediateFailedFuture(t);
        }
    }

    private void createAccessor() {
        MappingManager mappingManager = new MappingManager(session);
        accessor = mappingManager.createAccessor(SamplesAccessor.class);
    }

    private void createTable() {
        session.execute(SchemaBuilder.createTable(TABLE_SAMPLES)
                .addPartitionKey(COLUMN_CHANNEL_DATA_ID, DataType.uuid())
                .addPartitionKey(COLUMN_DECIMATION_LEVEL, DataType.cint())
                .addPartitionKey(COLUMN_BUCKET_START_TIME, DataType.bigint())
                .addClusteringColumn(COLUMN_SAMPLE_TIME, DataType.bigint())
                .addStaticColumn(COLUMN_CURRENT_BUCKET_SIZE, DataType.cint())
                .addUDTColumn(COLUMN_AGGREGATED_SCALAR_CHAR,
                        sampleValueAccess.getColumnType(
                                ChannelAccessSampleType.AGGREGATED_SCALAR_CHAR))
                .addUDTColumn(COLUMN_AGGREGATED_SCALAR_DOUBLE,
                        sampleValueAccess.getColumnType(
                                ChannelAccessSampleType.AGGREGATED_SCALAR_DOUBLE))
                .addUDTColumn(COLUMN_AGGREGATED_SCALAR_FLOAT,
                        sampleValueAccess.getColumnType(
                                ChannelAccessSampleType.AGGREGATED_SCALAR_FLOAT))
                .addUDTColumn(COLUMN_AGGREGATED_SCALAR_LONG,
                        sampleValueAccess.getColumnType(
                                ChannelAccessSampleType.AGGREGATED_SCALAR_LONG))
                .addUDTColumn(COLUMN_AGGREGATED_SCALAR_SHORT,
                        sampleValueAccess.getColumnType(
                                ChannelAccessSampleType.AGGREGATED_SCALAR_SHORT))
                .addUDTColumn(COLUMN_ARRAY_CHAR,
                        sampleValueAccess.getColumnType(
                                ChannelAccessSampleType.ARRAY_CHAR))
                .addUDTColumn(COLUMN_ARRAY_DOUBLE,
                        sampleValueAccess.getColumnType(
                                ChannelAccessSampleType.ARRAY_DOUBLE))
                .addUDTColumn(COLUMN_ARRAY_ENUM,
                        sampleValueAccess.getColumnType(
                                ChannelAccessSampleType.ARRAY_ENUM))
                .addUDTColumn(COLUMN_ARRAY_FLOAT,
                        sampleValueAccess.getColumnType(
                                ChannelAccessSampleType.ARRAY_FLOAT))
                .addUDTColumn(COLUMN_ARRAY_LONG,
                        sampleValueAccess.getColumnType(
                                ChannelAccessSampleType.ARRAY_LONG))
                .addUDTColumn(COLUMN_ARRAY_SHORT,
                        sampleValueAccess.getColumnType(
                                ChannelAccessSampleType.ARRAY_SHORT))
                .addUDTColumn(COLUMN_ARRAY_STRING,
                        sampleValueAccess.getColumnType(
                                ChannelAccessSampleType.ARRAY_STRING))
                .addColumn(COLUMN_DISABLED, DataType.cboolean())
                .addColumn(COLUMN_DISCONNECTED, DataType.cboolean())
                .addUDTColumn(COLUMN_SCALAR_CHAR,
                        sampleValueAccess.getColumnType(
                                ChannelAccessSampleType.SCALAR_CHAR))
                .addUDTColumn(COLUMN_SCALAR_DOUBLE,
                        sampleValueAccess.getColumnType(
                                ChannelAccessSampleType.SCALAR_DOUBLE))
                .addUDTColumn(COLUMN_SCALAR_ENUM,
                        sampleValueAccess.getColumnType(
                                ChannelAccessSampleType.SCALAR_ENUM))
                .addUDTColumn(COLUMN_SCALAR_FLOAT,
                        sampleValueAccess.getColumnType(
                                ChannelAccessSampleType.SCALAR_FLOAT))
                .addUDTColumn(COLUMN_SCALAR_LONG,
                        sampleValueAccess.getColumnType(
                                ChannelAccessSampleType.SCALAR_LONG))
                .addUDTColumn(COLUMN_SCALAR_SHORT,
                        sampleValueAccess.getColumnType(
                                ChannelAccessSampleType.SCALAR_SHORT))
                .addUDTColumn(COLUMN_SCALAR_STRING,
                        sampleValueAccess.getColumnType(
                                ChannelAccessSampleType.SCALAR_STRING))
                .ifNotExists());
    }

}
