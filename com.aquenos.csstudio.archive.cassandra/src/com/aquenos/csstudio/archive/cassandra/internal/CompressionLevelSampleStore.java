/*
 * Copyright 2013 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.csstudio.archive.cassandra.internal;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.csstudio.archive.config.ChannelConfig;
import org.csstudio.data.values.ITimestamp;
import org.csstudio.data.values.IValue;
import org.csstudio.data.values.IValue.Quality;
import org.csstudio.data.values.TimestampFactory;

import com.aquenos.csstudio.archive.cassandra.Sample;
import com.aquenos.csstudio.archive.cassandra.util.TimestampArithmetics;
import com.aquenos.csstudio.archive.cassandra.util.TimestampSerializer;
import com.aquenos.csstudio.archive.cassandra.util.astyanax.NotifyingMutationBatch;
import com.aquenos.csstudio.archive.cassandra.util.astyanax.NotifyingMutationBatch.MutationBatchListener;
import com.aquenos.csstudio.archive.config.cassandra.CassandraArchiveConfig;
import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.query.RowQuery;
import com.netflix.astyanax.serializers.BigIntegerSerializer;
import com.netflix.astyanax.util.RangeBuilder;

/**
 * Stores samples for a single compression level. This class is only intended
 * for inernal use by other classes in the same bundle.
 * 
 * @author Sebastian Marsching
 */
public class CompressionLevelSampleStore {
    private final static BigInteger DESIRED_SAMPLES_PER_BUCKET = BigInteger
            .valueOf(1000000L);
    private final static BigDecimal DESIRED_SAMPLES_PER_BUCKET_DECIMAL = new BigDecimal(
            DESIRED_SAMPLES_PER_BUCKET);
    private final static BigInteger ONE_BILLION = BigInteger
            .valueOf(1000000000L);
    private final static BigDecimal ONE_BILLION_DECIMAL = new BigDecimal(
            ONE_BILLION);
    private final static ITimestamp FOUR_HOURS = TimestampFactory
            .createTimestamp(14400L, 0L);
    private final static ITimestamp ONE_NANOSECOND = TimestampFactory
            .createTimestamp(0L, 1L);
    private final static ITimestamp ZERO_TIMESTAMP = TimestampFactory
            .createTimestamp(0L, 0L);

    private final static Iterable<Sample> EMPTY_SAMPLES = new Iterable<Sample>() {
        private final Iterator<Sample> emptyIterator = new Iterator<Sample>() {

            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public Sample next() {
                throw new NoSuchElementException();
            }

            @Override
            public void remove() {
                throw new IllegalStateException(
                        "The iterator's next() method must be called first.");
            }
        };

        @Override
        public Iterator<Sample> iterator() {
            return emptyIterator;
        }
    };

    private Keyspace keyspace;
    private ConsistencyLevel readDataConsistencyLevel;
    private ConsistencyLevel writeDataConsistencyLevel;
    private ConsistencyLevel readMetaDataConsistencyLevel;
    private ConsistencyLevel writeMetaDataConsistencyLevel;
    private long compressionPeriod;
    private Quality quality;
    private CassandraArchiveConfig archiveConfig;
    private boolean enableCache;
    private boolean enableWrites;

    private final Object bucketSizeLock = new Object();

    private ColumnFamilySamples cfSamples;
    private ColumnFamilySamplesBucketSize cfSamplesBucketSize;
    // For this map writes typically occur mainly at startup time. After some
    // time, effectively only reads happen. Therefore we prefer a hash map with
    // a relatively high initial capacity and normal concurrency and load
    // factors.
    private ConcurrentHashMap<String, BigInteger> channelToBucketSize = new ConcurrentHashMap<String, BigInteger>(
            1000);
    // For this map writes are typically more common than reads and the number
    // of concurrent processes is hard to predict. Therefore we use a skip list
    // instead of a hash map.
    private ConcurrentSkipListMap<String, ITimestamp> channelToLastTimestamp = new ConcurrentSkipListMap<String, ITimestamp>();

    public CompressionLevelSampleStore(Cluster cluster, Keyspace keyspace,
            ConsistencyLevel readDataConsistencyLevel,
            ConsistencyLevel writeDataConsistencyLevel,
            ConsistencyLevel readMetaDataConsistencyLevel,
            ConsistencyLevel writeMetaDataConsistencyLevel,
            CassandraArchiveConfig archiveConfig, long compressionPeriod,
            boolean enableWrites, boolean enableCache) {
        this.keyspace = keyspace;
        this.readDataConsistencyLevel = readDataConsistencyLevel;
        this.writeDataConsistencyLevel = writeDataConsistencyLevel;
        this.readMetaDataConsistencyLevel = readMetaDataConsistencyLevel;
        this.writeMetaDataConsistencyLevel = writeMetaDataConsistencyLevel;
        this.archiveConfig = archiveConfig;
        this.compressionPeriod = compressionPeriod;
        if (compressionPeriod == 0L) {
            this.quality = Quality.Original;
        } else {
            this.quality = Quality.Interpolated;
        }
        this.cfSamples = new ColumnFamilySamples(this.compressionPeriod);
        this.cfSamplesBucketSize = new ColumnFamilySamplesBucketSize(
                this.compressionPeriod);
        this.enableWrites = enableWrites;
        this.enableCache = enableCache;
        // Create missing column families and check that configuration of
        // existing column families is right.
        this.cfSamples.createOrCheckColumnFamily(cluster, this.keyspace);
        this.cfSamplesBucketSize.createOrCheckColumnFamily(cluster,
                this.keyspace);
    }

    public void insertSample(String channelName, IValue value,
            NotifyingMutationBatch mutationBatch) throws ConnectionException {
        if (!enableWrites) {
            throw new IllegalStateException("Writes are disabled.");
        }
        // Check that timestamp of value is not too far in the future. We do not
        // check for the past, because the first sample after start might be old
        // (the value might not have changed on the server for some time). The
        // archive engine code already checks that the sample is not older than
        // the most recent known sample, thus we do not have to check this here.
        long currentTime = System.currentTimeMillis();
        ITimestamp timestamp = value.getTime();
        if (timestamp.seconds() * 1000L > currentTime + 7200000L) {
            // Timestamp is more than two hours in the future. Either the clock
            // skew is extremely big, or there is something wrong.
            SimpleDateFormat dateFormat = new SimpleDateFormat(
                    "yyyy/MM/dd HH:mm:ss");
            throw new IllegalArgumentException(
                    "Timestamp of sample for channel \""
                            + channelName
                            + "\" is "
                            + timestamp
                                    .format(ITimestamp.Format.DateTimeSeconds)
                            + " while current system time is "
                            + dateFormat.format(new Date(currentTime))
                            + ". Please check that the clocks are set correctly.");
        }
        SampleBucketKey bucketKey = getBucketKeyForCurrentBucketSize(
                channelName, timestamp);
        mutationBatch.withRow(cfSamples.getCF(), bucketKey).putColumn(
                timestamp, ValueSerializer.toByteBuffer(value));
        if (enableCache) {
            // We use a mutation-batch listener, so that we only update the
            // timestamp if the sample has been successfully inserted.
            final String finalChannelName = channelName;
            final ITimestamp finalTimestamp = timestamp;
            mutationBatch.addListener(new MutationBatchListener() {
                @Override
                public void afterExecute(NotifyingMutationBatch source,
                        boolean success) {
                    if (!success) {
                        // If the mutation batch was not successful the sample
                        // has possibly not been inserted, thus we do not update
                        // the timestamp.
                        return;
                    }
                    boolean timestampUpdated = false;
                    while (!timestampUpdated) {
                        ITimestamp previousTimestamp = channelToLastTimestamp
                                .get(finalChannelName);
                        if (previousTimestamp == null) {
                            // This is the first time we insert a sample for
                            // this channel.
                            if (channelToLastTimestamp.putIfAbsent(
                                    finalChannelName, finalTimestamp) == null) {
                                // If putIfAbsent returns null, the put was
                                // successful, because the channel had not been
                                // present in the map yet.
                                timestampUpdated = true;
                            }
                        } else if (previousTimestamp
                                .isGreaterOrEqual(finalTimestamp)) {
                            // Timestamp in map is already greater than
                            // timestamp inserted (this can happen because of
                            // concurrent inserts), thus there is no need to
                            // update the map.
                            timestampUpdated = true;
                        } else {
                            // Try to update timestamp. If it fails (because it
                            // has been updated by another thread in the
                            // meantime, we just try again.
                            timestampUpdated = channelToLastTimestamp.replace(
                                    finalChannelName, previousTimestamp,
                                    finalTimestamp);
                        }
                    }
                }

                @Override
                public void afterDiscard(NotifyingMutationBatch source) {
                    // We are not interested in this event.
                }
            }, false);
        }
    }

    public void verifyZeroBucketSize(String channelName)
            throws ConnectionException {
        BigInteger bucketSize = channelToBucketSize.get(channelName);
        if (bucketSize != null) {
            // getBucketKeyForCurrentBucketSize has already been called for this
            // channel, which means that a zero bucket size has already been
            // inserted, if needed.
            return;
        }
        // We synchronize to a lock, so that we do not interfere with another
        // thread running getBucketKeyForCurrentBucketSize.
        synchronized (bucketSizeLock) {
            bucketSize = channelToBucketSize.get(channelName);
            if (bucketSize != null) {
                // getBucketKeyForCurrentBucketSize has already been called in
                // the meantime, which means that a zero bucket size has already
                // been inserted, if needed.
                return;
            }
            // This method does not only return the latest bucket size but also
            // inserts a zero bucket size if appropriate.
            getStoredBucketSizeSynchronized(channelName);
        }
    }

    private SampleBucketKey getBucketKeyForCurrentBucketSize(
            String channelName, ITimestamp timestamp)
            throws ConnectionException {
        BigInteger bucketSize = channelToBucketSize.get(channelName);
        if (bucketSize != null) {
            return getBucketKey(channelName, timestamp, bucketSize);
        }
        // We did not have the bucket size cached yet. This means that we have
        // to ensure that the bucket size is also written to the database,
        // if it has changed. In order to make sure that we do not interfere
        // with another thread doing the same thing (for the same channel), we
        // use a lock. This is okay, because we only have to do this once per
        // channel.
        // Although this might look like doubled checked locking (which is known
        // not to work correctly in Java, it is not, because channelToBucketSize
        // is a concurrent map.
        synchronized (bucketSizeLock) {
            bucketSize = channelToBucketSize.get(channelName);
            if (bucketSize != null) {
                return getBucketKey(channelName, timestamp, bucketSize);
            }
            return getBucketKeyForCurrentBucketSizeSynchronized(channelName,
                    timestamp);
        }
    }

    private SampleBucketKey getBucketKeyForCurrentBucketSizeSynchronized(
            String channelName, ITimestamp timestamp)
            throws ConnectionException {
        BigInteger bucketSize = getBucketSizeFromConfig(channelName);
        BigInteger storedBucketSize = getStoredBucketSizeSynchronized(channelName);
        if (storedBucketSize != null && bucketSize.equals(storedBucketSize)) {
            // Bucket size has not changed.
            BigInteger previousBucketSize = channelToBucketSize.putIfAbsent(
                    channelName, bucketSize);
            if (previousBucketSize == null) {
                return getBucketKey(channelName, timestamp, bucketSize);
            } else {
                return getBucketKey(channelName, timestamp, previousBucketSize);
            }
        } else {
            // Either the bucket size has changed or this is the first time a
            // sample is written for this channel. Anyway we have to write the
            // bucket size to the database.
            MutationBatch mutationBatch = keyspace.prepareMutationBatch()
                    .withConsistencyLevel(writeMetaDataConsistencyLevel);
            mutationBatch.withRow(cfSamplesBucketSize.getCF(), channelName)
                    .putColumn(timestamp, bucketSize,
                            BigIntegerSerializer.get(), null);
            mutationBatch.execute();
            BigInteger previousBucketSize = channelToBucketSize.putIfAbsent(
                    channelName, bucketSize);
            if (previousBucketSize == null) {
                return getBucketKey(channelName, timestamp, bucketSize);
            } else {
                return getBucketKey(channelName, timestamp, previousBucketSize);
            }
        }
    }

    private BigInteger getStoredBucketSizeSynchronized(String channelName)
            throws ConnectionException {
        // We ask for the most recent bucket size by not specifying any limits
        // for the column and requesting them in reverse order.
        ColumnList<ITimestamp> columnList = keyspace
                .prepareQuery(cfSamplesBucketSize.getCF())
                .setConsistencyLevel(readMetaDataConsistencyLevel)
                .getKey(channelName)
                .withColumnRange(
                        new RangeBuilder().setLimit(1).setReversed(true)
                                .build()).execute().getResult();
        if (columnList.size() > 0) {
            BigInteger storedBucketSize = columnList.getColumnByIndex(0)
                    .getValue(BigIntegerSerializer.get());
            if (storedBucketSize.equals(BigInteger.ZERO)) {
                // There is no sense in checking how old the latest sample is,
                // if the current bucket size is already zero.
                return storedBucketSize;
            }
            // Check how old the last sample is. If there must be several empty
            // buckets between the last sample time and the current time, we
            // want to insert a zero bucket size, in order to avoid long search
            // times by having to look for a huge number of empty buckets.
            // We assume that the last sample is "old", when the time that
            // passed since the last sample was written is at least three bucket
            // sizes (this means that there are at least two completely empty
            // buckets.
            ITimestamp lastSampleTimestamp = getLastSampleTimestamp(channelName);
            ITimestamp now = TimestampFactory.now();
            if (lastSampleTimestamp.isLessThan(now)
                    && TimestampArithmetics
                            .substract(now, lastSampleTimestamp)
                            .isGreaterThan(
                                    TimestampArithmetics.multiply(
                                            TimestampArithmetics
                                                    .bigIntegerToTimestamp(storedBucketSize),
                                            3))) {
                MutationBatch mutationBatch = keyspace.prepareMutationBatch()
                        .withConsistencyLevel(writeMetaDataConsistencyLevel);
                // We insert the zero bucket size with a timestamp that is
                // exactly one more than the
                mutationBatch.withRow(cfSamplesBucketSize.getCF(), channelName)
                        .putColumn(
                                TimestampArithmetics.add(lastSampleTimestamp,
                                        ONE_NANOSECOND), BigInteger.ZERO,
                                BigIntegerSerializer.get(), null);
                mutationBatch.execute();
                storedBucketSize = BigInteger.ZERO;
            }
            return storedBucketSize;
        } else {
            return null;
        }

    }

    private SampleBucketKey getBucketKey(String channelName,
            ITimestamp timestamp, BigInteger bucketSize) {
        BigInteger timestampInNanoseconds = TimestampArithmetics
                .timestampToBigInteger(timestamp);
        ITimestamp remainder = TimestampArithmetics
                .bigIntegerToTimestamp(timestampInNanoseconds
                        .remainder(bucketSize));
        ITimestamp bucketStartTime = TimestampArithmetics.substract(timestamp,
                remainder);
        return new SampleBucketKey(channelName, bucketSize, bucketStartTime);
    }

    private BigInteger getBucketSizeFromConfig(String channelName)
            throws ConnectionException {
        if (compressionPeriod != 0L) {
            // For compressed samples we know exactly how often we expect a
            // sample (although there might be less, when the value does not
            // change for an extend period of time).
            return DESIRED_SAMPLES_PER_BUCKET.multiply(
                    BigInteger.valueOf(compressionPeriod))
                    .multiply(ONE_BILLION);
        }
        // For raw samples we have to check the expected change rate in the
        // channel configuration.
        ChannelConfig channelConfig = archiveConfig.findChannel(channelName);
        if (channelConfig == null) {
            throw new IllegalStateException(
                    "Could not find configuration for channel \"" + channelName
                            + "\".");
        }
        double scanRate = channelConfig.getSampleMode().getPeriod();
        return BigDecimal.valueOf(scanRate)
                .multiply(DESIRED_SAMPLES_PER_BUCKET_DECIMAL)
                .multiply(ONE_BILLION_DECIMAL).toBigInteger();
    }

    public ITimestamp getLastSampleTimestamp(String channelName)
            throws ConnectionException {
        ITimestamp timestamp;
        if (enableCache) {
            // We first check whether the timestamp is already cached. In this
            // case we can skip the whole find process.
            timestamp = channelToLastTimestamp.get(channelName);
            if (timestamp != null) {
                return timestamp;
            }
        }
        timestamp = getLastSampleTimestampFromDatabase(channelName);
        if (enableCache && timestamp != null) {
            // Save timestamp in cache for future requests. If the timestamp is
            // already in the cache, because it has been cached by a concurrent
            // thread, we just return the timestamp from the cache.
            ITimestamp storedTimestamp = channelToLastTimestamp.putIfAbsent(
                    channelName, timestamp);
            if (storedTimestamp != null) {
                return storedTimestamp;
            }
        }
        return timestamp;
    }

    private ITimestamp getLastSampleTimestampFromDatabase(String channelName)
            throws ConnectionException {
        // We iterate over all bucket sizes for this channel in reverse order
        // until we find a sample or hit the start of the first bucket with a
        // non-zero bucket size.
        RowQuery<String, ITimestamp> query = keyspace
                .prepareQuery(cfSamplesBucketSize.getCF())
                .setConsistencyLevel(readMetaDataConsistencyLevel)
                .getKey(channelName)
                .autoPaginate(true)
                .withColumnRange(
                        new RangeBuilder().setLimit(10).setReversed(true)
                                .build());
        // The newest sample we look for is in the bucket with the current
        // timestamp plus four hours (to accommodate for clock skews). If there
        // is a bucket size, which is even newer, we use the timestamp of this
        // bucket size.
        ITimestamp maxTime = TimestampArithmetics.add(TimestampFactory.now(),
                FOUR_HOURS);
        ColumnList<ITimestamp> bucketSizeColumns;
        while (!(bucketSizeColumns = query.execute().getResult()).isEmpty()) {
            for (Column<ITimestamp> column : bucketSizeColumns) {
                ITimestamp bucketSizeTime = column.getName();
                BigInteger bucketSize = column.getValue(BigIntegerSerializer
                        .get());
                if (bucketSize.equals(BigInteger.ZERO)) {
                    // A bucket size of zero is special because it indicates
                    // that there are no samples in this range.
                    if (bucketSizeTime.equals(ZERO_TIMESTAMP)) {
                        // There cannot be an older bucket size, thus we do not
                        // have to continue our search.
                        return null;
                    }
                    maxTime = TimestampArithmetics.substract(bucketSizeTime,
                            ONE_NANOSECOND);
                    continue;
                }
                SampleBucketKey minBucketKey = getBucketKey(channelName,
                        bucketSizeTime, bucketSize);
                ITimestamp bucketSizeAsTimestamp = TimestampArithmetics
                        .bigIntegerToTimestamp(bucketSize);
                if (bucketSizeTime.isGreaterThan(maxTime)) {
                    // The bucket size is more than four hours in the future,
                    // so use this timestamp.
                    return bucketSizeTime;
                }
                // We look for all buckets with this bucket size in reverse
                // order. The last bucket (with the lowest timestamp) is the
                // bucket for the timestamp of the bucket size.
                SampleBucketKey nextBucketKey = getBucketKey(channelName,
                        maxTime, bucketSize);
                while (nextBucketKey.getBucketStartTime().isGreaterOrEqual(
                        minBucketKey.getBucketStartTime())) {
                    ColumnList<ITimestamp> sampleColumns = keyspace
                            .prepareQuery(cfSamples.getCF())
                            .setConsistencyLevel(readDataConsistencyLevel)
                            .getKey(nextBucketKey)
                            .withColumnRange(
                                    new RangeBuilder()
                                            .setStart(maxTime,
                                                    TimestampSerializer.get())
                                            .setEnd(bucketSizeTime,
                                                    TimestampSerializer.get())
                                            .setLimit(1).setReversed(true)
                                            .build()).execute().getResult();
                    if (!sampleColumns.isEmpty()) {
                        return sampleColumns.getColumnByIndex(0).getName();
                    }
                    // We can take a shortcut for generating the next key here,
                    // because we known that the old key is already aligned to
                    // the bucket size.
                    if (nextBucketKey.getBucketStartTime().equals(
                            ZERO_TIMESTAMP)) {
                        // There is no older bucket, thus we can stop the
                        // search.
                        break;
                    }
                    nextBucketKey = new SampleBucketKey(channelName,
                            bucketSize, TimestampArithmetics.substract(
                                    nextBucketKey.getBucketStartTime(),
                                    bucketSizeAsTimestamp));
                }
                // If we are here, we have not found any samples for the current
                // bucket size. In this case (by definition) the latest
                // timestamp is the timestamp of the bucket size itself, because
                // we checked earlier that it is non-zero.
                return bucketSizeTime;
            }
        }
        // If we are here, we did not find any non-zero bucket sizes (most
        // likely because this is a new channel and no data has been written to
        // it yet).
        return null;
    }

    public Iterable<Sample> findSamples(String channelName, ITimestamp start,
            ITimestamp end, int limit, boolean reverse)
            throws ConnectionException {
        return findSamples(channelName, start, end, limit, reverse, null);
    }

    private Iterable<Sample> findSamples(String channelName, ITimestamp start,
            ITimestamp end, int limit, boolean reverse,
            MutationBatch mutationBatch) throws ConnectionException {
        // The first bucket size we have to look for is the one with a timestamp
        // less than or equal to the requested start timestamp. If the start
        // timestamp is null, it is just the first bucket size at all.
        // The last bucket size we have to look for is the one with a timestamp
        // less than or equal to the requested end timestamp. If the end
        // timestamp is null, we look up to four hours into the future to
        // accommodate for clock skew.
        // For reverse order the logic is nearly the same, only that we set the
        // start timestamp in the future and the end timestamp is limited by the
        // first bucket.
        if (start != null && end != null) {
            if (reverse) {
                if (end.isGreaterThan(start)) {
                    throw new IllegalArgumentException("Start timestamp ("
                            + start.format(ITimestamp.Format.Full)
                            + ") must be greater than end timestamp ("
                            + end.format(ITimestamp.Format.Full) + ").");
                }
            } else {
                if (start.isGreaterThan(end)) {
                    throw new IllegalArgumentException("Start timestamp ("
                            + start.format(ITimestamp.Format.Full)
                            + ") most not be greater than end timestamp ("
                            + end.format(ITimestamp.Format.Full) + ").");
                }
            }
        }

        ITimestamp smallestTimestamp = reverse ? end : start;
        ITimestamp greatestTimestamp = reverse ? start : end;
        // If the cache is enabled, we know the timestamp of the newest sample.
        if (greatestTimestamp == null && enableCache) {
            greatestTimestamp = getLastSampleTimestamp(channelName);
        }
        // If the timestamp is still not set (either because the cache is
        // disabled or because there is no sample and thus no latest timestamp),
        // we use a timestamp four hours into the future.
        if (greatestTimestamp == null) {
            greatestTimestamp = TimestampArithmetics.add(
                    TimestampFactory.now(), FOUR_HOURS);
        }
        if (reverse) {
            start = greatestTimestamp;
        } else {
            end = greatestTimestamp;
        }

        // Because we might have changed the start or end timestamp, we have to
        // check, that they are still in the right order. If the order is wrong
        // now, we do not treat this as an error (the calling code supplied
        // correct timestamps, which we checked above), but just return an empty
        // iterable.
        if (start != null && end != null && start.isGreaterThan(end)) {
            return EMPTY_SAMPLES;
        }

        ITimestamp bucketSizeSmallestTimestamp = smallestTimestamp;
        if (bucketSizeSmallestTimestamp != null) {
            ColumnList<ITimestamp> bucketSizeColumns = keyspace
                    .prepareQuery(cfSamplesBucketSize.getCF())
                    .setConsistencyLevel(readMetaDataConsistencyLevel)
                    .getKey(channelName)
                    .withColumnRange(
                            new RangeBuilder()
                                    .setStart(reverse ? end : start,
                                            TimestampSerializer.get())
                                    .setLimit(1).setReversed(true).build())
                    .execute().getResult();
            if (!bucketSizeColumns.isEmpty()) {
                Column<ITimestamp> column = bucketSizeColumns
                        .getColumnByIndex(0);
                ITimestamp timestamp = column.getName();
                if (timestamp.isLessThan(reverse ? end : start)) {
                    bucketSizeSmallestTimestamp = timestamp;
                }
            }
        }

        final ITimestamp finalStart = start;
        final ITimestamp finalEnd = end;
        final int finalLimit = limit;
        final boolean finalReverse = reverse;
        final String finalChannelName = channelName;
        final MutationBatch finalMutationBatch = mutationBatch;
        final ITimestamp finalBucketSizeSmallestTimestamp = bucketSizeSmallestTimestamp;

        return new Iterable<Sample>() {
            private String channelName = finalChannelName;
            private ITimestamp start = finalStart;
            private ITimestamp end = finalEnd;
            private int limit = finalLimit;
            private boolean reverse = finalReverse;
            private MutationBatch mutationBatch = finalMutationBatch;
            private ITimestamp bucketSizeSmallestTimestamp = finalBucketSizeSmallestTimestamp;

            @Override
            public Iterator<Sample> iterator() {
                return new Iterator<Sample>() {

                    private RowQuery<String, ITimestamp> bucketSizeQuery = keyspace
                            .prepareQuery(cfSamplesBucketSize.getCF())
                            .setConsistencyLevel(readMetaDataConsistencyLevel)
                            .getKey(channelName)
                            .autoPaginate(true)
                            .withColumnRange(
                                    reverse ? start
                                            : bucketSizeSmallestTimestamp,
                                    reverse ? bucketSizeSmallestTimestamp : end,
                                    reverse, 50);
                    private Iterator<Column<ITimestamp>> bucketSizeIterator = null;
                    private SampleBucketKey nextBucketKey = null;
                    private ITimestamp currentBucketSizeStart = null;
                    private ITimestamp currentBucketSizeEnd = null;
                    private BigInteger currentBucketSize = null;
                    private ITimestamp currentBucketSizeAsTimestamp = null;
                    private ITimestamp nextBucketSizeTimestamp = null;
                    private BigInteger nextBucketSize = null;
                    private SampleBucketKey sampleQueryBucketKey = null;
                    private RowQuery<SampleBucketKey, ITimestamp> sampleQuery = null;
                    private Iterator<Column<ITimestamp>> sampleIterator = null;
                    private Sample nextSample = null;
                    private SampleBucketKey bucketKeyForNextSample = null;
                    private Sample lastReturnedSample = null;
                    private SampleBucketKey bucketKeyForLastReturnedSample = null;
                    private int count = 0;

                    @Override
                    public boolean hasNext() {
                        try {
                            // If a limit is set, we return at most the number
                            // of samples defined by the limit.
                            if (limit >= 0 && count >= limit) {
                                return false;
                            }
                            if (nextSample != null) {
                                // We already have the next sample, so there is
                                // nothing left to be done.
                                return true;
                            }
                            // Try to find the next sample.
                            if (sampleIterator != null
                                    && sampleIterator.hasNext()) {
                                Column<ITimestamp> sampleColumn = sampleIterator
                                        .next();
                                ITimestamp sampleTimestamp = sampleColumn
                                        .getName();
                                IValue sampleValue = ValueSerializer
                                        .fromByteBuffer(sampleColumn
                                                .getByteBufferValue(),
                                                sampleTimestamp, quality);
                                nextSample = new Sample(compressionPeriod,
                                        channelName, sampleValue);
                                bucketKeyForNextSample = sampleQueryBucketKey;
                                return true;
                            }
                            // Get next page of samples in current bucket.
                            if (sampleQuery != null) {
                                ColumnList<ITimestamp> sampleColumns = sampleQuery
                                        .execute().getResult();
                                if (!sampleColumns.isEmpty()) {
                                    sampleIterator = sampleColumns.iterator();
                                    return hasNext();
                                } else {
                                    sampleIterator = null;
                                }
                            }
                            // Get the next sample bucket. Skip parts with a
                            // zero bucket size (we use this to indicate periods
                            // without any samples).
                            if (nextBucketKey != null
                                    && !nextBucketKey.getBucketSize().equals(
                                            BigInteger.ZERO)) {
                                sampleQueryBucketKey = nextBucketKey;
                                sampleQuery = keyspace
                                        .prepareQuery(cfSamples.getCF())
                                        .setConsistencyLevel(
                                                readDataConsistencyLevel)
                                        .getKey(sampleQueryBucketKey)
                                        .autoPaginate(true)
                                        .withColumnRange(
                                                currentBucketSizeStart,
                                                currentBucketSizeEnd,
                                                reverse,
                                                (limit >= 0) ? (limit - count + 1)
                                                        : 5000);
                                ITimestamp nextBucketTimestamp;
                                if (reverse) {
                                    if (nextBucketKey.getBucketStartTime()
                                            .equals(ZERO_TIMESTAMP)) {
                                        // We already have the oldest possible
                                        // bucket.
                                        nextBucketKey = null;
                                        return hasNext();
                                    }
                                    nextBucketTimestamp = TimestampArithmetics
                                            .substract(nextBucketKey
                                                    .getBucketStartTime(),
                                                    currentBucketSizeAsTimestamp);
                                } else {
                                    nextBucketTimestamp = TimestampArithmetics
                                            .add(nextBucketKey
                                                    .getBucketStartTime(),
                                                    currentBucketSizeAsTimestamp);
                                }
                                if ((!reverse && nextBucketTimestamp
                                        .isGreaterThan(currentBucketSizeEnd))
                                        || (reverse && nextBucketTimestamp
                                                .isLessOrEqual(TimestampArithmetics
                                                        .substract(
                                                                currentBucketSizeEnd,
                                                                currentBucketSizeAsTimestamp)))) {
                                    nextBucketKey = null;
                                } else {
                                    nextBucketKey = new SampleBucketKey(
                                            channelName, currentBucketSize,
                                            nextBucketTimestamp);
                                }
                                return hasNext();
                            }
                            // Get the next bucket size.
                            if (bucketSizeIterator != null
                                    && bucketSizeIterator.hasNext()) {
                                // If the next bucket is already known, we can
                                // initialize the information about the current
                                // bucket. Otherwise, we have to get the next
                                // bucket first, which will then become the
                                // current bucket in the next step.
                                Column<ITimestamp> bucketSizeColumn = bucketSizeIterator
                                        .next();
                                if (nextBucketSizeTimestamp != null) {
                                    if (reverse) {
                                        if (currentBucketSizeEnd != null) {
                                            // We know that there is a bucket
                                            // size with a smaller timestamp,
                                            // thus it is safe to substract from
                                            // the current timestamp.
                                            currentBucketSizeStart = TimestampArithmetics
                                                    .substract(
                                                            currentBucketSizeEnd,
                                                            ONE_NANOSECOND);
                                        } else {
                                            currentBucketSizeStart = start;
                                        }
                                    } else {
                                        currentBucketSizeStart = nextBucketSizeTimestamp;
                                    }
                                    currentBucketSize = nextBucketSize;
                                    currentBucketSizeAsTimestamp = TimestampArithmetics
                                            .bigIntegerToTimestamp(currentBucketSize);
                                    if (reverse) {
                                        currentBucketSizeEnd = nextBucketSizeTimestamp;
                                    }
                                    nextBucketSizeTimestamp = bucketSizeColumn
                                            .getName();
                                    nextBucketSize = bucketSizeColumn
                                            .getValue(BigIntegerSerializer
                                                    .get());
                                    if (!reverse) {
                                        currentBucketSizeEnd = TimestampArithmetics
                                                .substract(
                                                        nextBucketSizeTimestamp,
                                                        ONE_NANOSECOND);
                                    }
                                    // Initialize the next bucket key, so that
                                    // the next sample bucket can be read.
                                    if (currentBucketSize
                                            .equals(BigInteger.ZERO)) {
                                        // A bucket size of zero is special and
                                        // means that this period of time does
                                        // not contain any samples.
                                        nextBucketKey = null;
                                    } else {
                                        nextBucketKey = getBucketKey(
                                                channelName,
                                                currentBucketSizeStart,
                                                currentBucketSize);
                                    }
                                } else {
                                    nextBucketSizeTimestamp = bucketSizeColumn
                                            .getName();
                                    if (!reverse) {
                                        // Only the first bucket size that we
                                        // get can have a timestamp that is less
                                        // than the requested start timestamp.
                                        // Thus it is sufficient if we perform
                                        // this check here.
                                        if (start != null
                                                && nextBucketSizeTimestamp
                                                        .isLessThan(start)) {
                                            nextBucketSizeTimestamp = start;
                                        }
                                    }
                                    nextBucketSize = bucketSizeColumn
                                            .getValue(BigIntegerSerializer
                                                    .get());
                                }
                                return hasNext();
                            }
                            // If the bucket size iterator has run out of
                            // columns, there are two possible causes: Either
                            // we have to request the next page, or we have hit
                            // the last bucket size.
                            ColumnList<ITimestamp> bucketSizeColumns = bucketSizeQuery
                                    .execute().getResult();
                            if (!bucketSizeColumns.isEmpty()) {
                                bucketSizeIterator = bucketSizeColumns
                                        .iterator();
                                return hasNext();
                            } else {
                                if (nextBucketSizeTimestamp != null) {
                                    // The next bucket size is the last bucket
                                    // size. Thus we set it for the current
                                    // bucket size and use the end timestamp
                                    // of the sample query as the end timestamp
                                    // of the bucket size.
                                    if (reverse) {
                                        if (currentBucketSizeEnd != null) {
                                            // We know that there is a bucket
                                            // size with a smaller timestamp,
                                            // thus it is safe to substract from
                                            // the current timestamp.
                                            currentBucketSizeStart = TimestampArithmetics
                                                    .substract(
                                                            currentBucketSizeEnd,
                                                            ONE_NANOSECOND);
                                        } else {
                                            currentBucketSizeStart = start;
                                        }
                                    } else {
                                        currentBucketSizeStart = nextBucketSizeTimestamp;
                                    }
                                    currentBucketSize = nextBucketSize;
                                    currentBucketSizeAsTimestamp = TimestampArithmetics
                                            .bigIntegerToTimestamp(currentBucketSize);
                                    if (reverse) {
                                        // If the end of the requested range is
                                        // before the end of the bucket, we
                                        // limit the queries to this end.
                                        // Otherwise we use the end of the last
                                        // bucket as the limit. This can only
                                        // happen for the last bucket size, thus
                                        // is is sufficient to make this check
                                        // here.
                                        if (end != null
                                                && end.isGreaterThan(nextBucketSizeTimestamp)) {
                                            currentBucketSizeEnd = end;
                                        } else {
                                            currentBucketSizeEnd = nextBucketSizeTimestamp;
                                        }
                                    } else {
                                        currentBucketSizeEnd = end;
                                    }
                                    nextBucketSizeTimestamp = null;
                                    nextBucketSize = null;
                                    // Initialize the next bucket key, so that
                                    // the next sample bucket can be read.
                                    if (currentBucketSize
                                            .equals(BigInteger.ZERO)) {
                                        // A bucket size of zero is special and
                                        // means that this period of time does
                                        // not contain any samples.
                                        nextBucketKey = null;
                                    } else {
                                        nextBucketKey = getBucketKey(
                                                channelName,
                                                currentBucketSizeStart,
                                                currentBucketSize);
                                    }
                                    return hasNext();
                                } else {
                                    // We have no more bucket sizes to read, we
                                    // have no next bucket size stored and we
                                    // have read everything for the current
                                    // bucket size. Thus there are no more
                                    // samples.
                                    return false;
                                }
                            }
                        } catch (ConnectionException e) {
                            throw new RuntimeException(
                                    "Error while trying to retrieve data for channel \""
                                            + channelName + "\": "
                                            + e.getMessage(), e);
                        }
                    }

                    @Override
                    public Sample next() {
                        if (!hasNext()) {
                            throw new NoSuchElementException();
                        }
                        lastReturnedSample = nextSample;
                        bucketKeyForLastReturnedSample = bucketKeyForNextSample;
                        nextSample = null;
                        bucketKeyForNextSample = null;
                        if (limit >= 0) {
                            count++;
                        }
                        return lastReturnedSample;
                    }

                    @Override
                    public void remove() {
                        if (mutationBatch == null) {
                            throw new UnsupportedOperationException(
                                    "Samples are read-only.");
                        } else {
                            if (lastReturnedSample == null) {
                                throw new IllegalStateException(
                                        "Either no element has been requested using the next() method yet, or the current element has already been deleted.");
                            }
                            mutationBatch.withRow(cfSamples.getCF(),
                                    bucketKeyForLastReturnedSample)
                                    .deleteColumn(
                                            lastReturnedSample.getValue()
                                                    .getTime());
                            lastReturnedSample = null;
                            bucketKeyForLastReturnedSample = null;
                        }
                    }

                };
            }
        };
    }

    public void deleteSamples(String channelName, ITimestamp start,
            ITimestamp end) throws ConnectionException {
        if (!enableWrites) {
            throw new IllegalStateException("Writes are disabled.");
        }
        // Limit end to the timestamp of the last sample in order to avoid
        // undesirable effects, when we are inserting samples in parallel.
        ITimestamp lastSampleTimestamp = getLastSampleTimestamp(channelName);
        if (lastSampleTimestamp == null
                || (start != null && start.isGreaterThan(lastSampleTimestamp))) {
            // There are no samples, that we can delete.
            return;
        }
        if (end == null
                || (lastSampleTimestamp != null && lastSampleTimestamp
                        .isLessThan(end))) {
            end = lastSampleTimestamp;
        }
        // Find and delete samples.
        MutationBatch mutationBatch = keyspace.prepareMutationBatch()
                .withConsistencyLevel(writeDataConsistencyLevel);
        for (Iterator<Sample> i = findSamples(channelName, start, end, -1,
                false, mutationBatch).iterator(); i.hasNext();) {
            i.next();
            i.remove();
        }
        mutationBatch.execute();

        // We use mutation batch with a different consistency level for writing
        // to the bucket-size column-family.
        mutationBatch = keyspace.prepareMutationBatch().withConsistencyLevel(
                writeMetaDataConsistencyLevel);
        ColumnListMutation<ITimestamp> cfSamplesBucketSizeMutation = mutationBatch
                .withRow(cfSamplesBucketSize.getCF(), channelName);
        // Now we check whether there is a bucket size that starts before the
        // start of the period we just deleted. If no such bucket size exists,
        // we do not need to insert a zero bucket size.
        ITimestamp startDeleteBucketSizes = null;
        if (start != null && start.isGreaterThan(ZERO_TIMESTAMP)) {
            ColumnList<ITimestamp> bucketSizeColumns = keyspace
                    .prepareQuery(cfSamplesBucketSize.getCF())
                    .getKey(channelName)
                    .withColumnRange(
                            TimestampArithmetics.substract(start,
                                    ONE_NANOSECOND), null, true, 1).execute()
                    .getResult();
            if (!bucketSizeColumns.isEmpty()) {
                // We insert a zero bucket size with the start timestamp in
                // order to indicate that no samples are available in this
                // period of time.
                cfSamplesBucketSizeMutation.putColumn(start, BigInteger.ZERO,
                        BigIntegerSerializer.get(), null);
                // Set start timestamp for deleting bucket sizes the start
                // timestamp plus one, so that we do not delete the zero bucket
                // size that we just inserted.
                startDeleteBucketSizes = TimestampArithmetics.add(start,
                        ONE_NANOSECOND);
            }
        }
        // Now we insert the bucket size that is valid at the end timestamp
        // using the end timestamp. Subsequently we can delete all bucket sizes
        // between the start and the end timestamp. If there is no bucket size
        // preceding the end timestamp, we do not have to do anything.
        if (end.equals(ZERO_TIMESTAMP)) {
            return;
        }
        ITimestamp endMinusOne = TimestampArithmetics.substract(end,
                ONE_NANOSECOND);
        ColumnList<ITimestamp> bucketSizeColumns = keyspace
                .prepareQuery(cfSamplesBucketSize.getCF()).getKey(channelName)
                .withColumnRange(endMinusOne, null, true, 1).execute()
                .getResult();
        if (!bucketSizeColumns.isEmpty()) {
            BigInteger lastBucketSize = bucketSizeColumns.getColumnByIndex(0)
                    .getValue(BigIntegerSerializer.get());
            cfSamplesBucketSizeMutation.putColumn(end, lastBucketSize,
                    BigIntegerSerializer.get(), null);
            // Finally we delete all bucket sizes between the start timestamp
            // (inclusive) and the end timestamp (exclusive).
            RowQuery<String, ITimestamp> bucketSizeQuery = keyspace
                    .prepareQuery(cfSamplesBucketSize.getCF())
                    .getKey(channelName)
                    .autoPaginate(true)
                    .withColumnRange(startDeleteBucketSizes, endMinusOne,
                            false, 100);
            while (!(bucketSizeColumns = bucketSizeQuery.execute().getResult())
                    .isEmpty()) {
                for (Column<ITimestamp> column : bucketSizeColumns) {
                    cfSamplesBucketSizeMutation.deleteColumn(column.getName());
                }
            }
        }

    }

    public void performCleanUp(HashSet<String> channelNames, boolean printStatus)
            throws ConnectionException {
        if (!enableWrites) {
            throw new IllegalStateException("Writes are disabled.");
        }
        // First we clean up the samples column-family.
        MutationBatch mutationBatch = keyspace.prepareMutationBatch()
                .withConsistencyLevel(writeDataConsistencyLevel);
        Rows<SampleBucketKey, ITimestamp> sampleRows = keyspace
                .prepareQuery(cfSamples.getCF())
                .setConsistencyLevel(readDataConsistencyLevel).getAllRows()
                .setRowLimit(5000)
                .withColumnRange(new RangeBuilder().setLimit(1).build())
                .execute().getResult();
        for (Row<SampleBucketKey, ITimestamp> row : sampleRows) {
            SampleBucketKey rowKey = row.getKey();
            String channelName = rowKey.getChannelName();
            if (!channelNames.contains(channelName)) {
                if (printStatus) {
                    System.out
                            .println("Removing sample bucket for non-existent channel \""
                                    + channelName
                                    + "\""
                                    + (compressionPeriod != 0 ? " (compression period "
                                            + compressionPeriod + " seconds)"
                                            : "") + ".");
                }
                mutationBatch.withRow(cfSamples.getCF(), row.getKey()).delete();
            } else {
                // Check that bucket fits to known bucket sizes.
                boolean foundBucketSize = false;
                ColumnList<ITimestamp> bucketSizeColumns = keyspace
                        .prepareQuery(cfSamplesBucketSize.getCF())
                        .setConsistencyLevel(readMetaDataConsistencyLevel)
                        .getKey(channelName)
                        .withColumnRange(rowKey.getBucketStartTime(), null,
                                true, 1).execute().getResult();
                if (!bucketSizeColumns.isEmpty()) {
                    BigInteger bucketSize = bucketSizeColumns.getColumnByIndex(
                            0).getValue(BigIntegerSerializer.get());
                    if (bucketSize.equals(rowKey.getBucketSize())) {
                        foundBucketSize = true;
                    }
                }
                bucketSizeColumns = keyspace
                        .prepareQuery(cfSamplesBucketSize.getCF())
                        .setConsistencyLevel(readMetaDataConsistencyLevel)
                        .getKey(channelName)
                        .withColumnRange(
                                rowKey.getBucketStartTime(),
                                TimestampArithmetics.add(
                                        rowKey.getBucketStartTime(),
                                        TimestampArithmetics
                                                .bigIntegerToTimestamp(rowKey
                                                        .getBucketSize()
                                                        .subtract(
                                                                BigInteger.ONE))),
                                false, Integer.MAX_VALUE).execute().getResult();
                for (Column<ITimestamp> bucketSizeColumn : bucketSizeColumns) {
                    ITimestamp bucketSizeStart = bucketSizeColumn.getName();
                    BigInteger bucketSize = bucketSizeColumn
                            .getValue(BigIntegerSerializer.get());
                    if (bucketSize.equals(rowKey.getBucketSize())) {
                        if (getBucketKey(channelName, bucketSizeStart,
                                bucketSize).getBucketStartTime().equals(
                                rowKey.getBucketStartTime())) {
                            foundBucketSize = true;
                            break;
                        }
                    }
                }
                if (!foundBucketSize) {
                    if (printStatus) {
                        System.out
                                .println("Removing sample bucket with start timestamp "
                                        + rowKey.getBucketStartTime().format(
                                                ITimestamp.Format.Full)
                                        + " and bucket size of "
                                        + rowKey.getBucketSize()
                                        + " nanoseconds for channel \""
                                        + channelName
                                        + (compressionPeriod != 0 ? "\" (compression period "
                                                + compressionPeriod
                                                + " seconds)"
                                                : "")
                                        + " because it cannot be reached from a reader.");
                    }
                    mutationBatch.withRow(cfSamples.getCF(), row.getKey())
                            .delete();
                }
            }
        }
        mutationBatch.execute();

        // Next we clean-up the samples bucket-size column-family.
        mutationBatch = keyspace.prepareMutationBatch().withConsistencyLevel(
                writeMetaDataConsistencyLevel);
        Rows<String, ITimestamp> sampleBucketSizeRows = keyspace
                .prepareQuery(cfSamplesBucketSize.getCF())
                .setConsistencyLevel(readMetaDataConsistencyLevel).getAllRows()
                .setRowLimit(5000)
                .withColumnRange(new RangeBuilder().setLimit(1).build())
                .execute().getResult();
        for (Row<String, ITimestamp> row : sampleBucketSizeRows) {
            String channelName = row.getKey();
            if (!channelNames.contains(channelName)) {
                if (printStatus) {
                    System.out
                            .println("Removing sample bucket-sizes for non-existent channel \""
                                    + channelName
                                    + (compressionPeriod != 0 ? "\" (compression period "
                                            + compressionPeriod + " seconds)"
                                            : "") + ".");
                }
                mutationBatch
                        .withRow(cfSamplesBucketSize.getCF(), row.getKey())
                        .delete();

            }
        }
        mutationBatch.execute();

        // Finally we process all bucket sizes for the known channel names and
        // ensure that the oldest bucket size is not older than the oldest
        // sample.
        for (String channelName : channelNames) {
            Iterator<Sample> samplesIterator = findSamples(channelName, null,
                    null, 1, false).iterator();
            if (samplesIterator.hasNext()) {
                Sample firstSample = samplesIterator.next();
                ITimestamp firstTimestamp = firstSample.getValue().getTime();
                ColumnList<ITimestamp> bucketSizeColumns = keyspace
                        .prepareQuery(cfSamplesBucketSize.getCF())
                        .setConsistencyLevel(readMetaDataConsistencyLevel)
                        .getKey(channelName)
                        .withColumnRange(firstTimestamp, null, true,
                                Integer.MAX_VALUE).execute().getResult();
                BigInteger lastBucketSize = null;
                for (Column<ITimestamp> column : bucketSizeColumns) {
                    boolean doDelete = true;
                    ITimestamp timestamp = column.getName();
                    if (lastBucketSize == null) {
                        lastBucketSize = column.getValue(BigIntegerSerializer
                                .get());
                        if (timestamp.equals(firstTimestamp)) {
                            doDelete = false;
                        } else {
                            mutationBatch.withRow(cfSamplesBucketSize.getCF(),
                                    channelName).putColumn(firstTimestamp,
                                    lastBucketSize, BigIntegerSerializer.get(),
                                    null);
                        }
                    }
                    if (doDelete) {
                        mutationBatch.withRow(cfSamplesBucketSize.getCF(),
                                channelName).deleteColumn(timestamp);
                    }
                }
            }
        }
        mutationBatch.execute();
    }

}
