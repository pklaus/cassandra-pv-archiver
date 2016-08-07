/*
 * Copyright 2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.database.test;

import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang3.tuple.Pair;

import com.aquenos.cassandra.pvarchiver.common.ObjectResultSet;
import com.aquenos.cassandra.pvarchiver.common.SimpleObjectResultSet;
import com.aquenos.cassandra.pvarchiver.server.database.ChannelMetaDataDAO;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.collect.Maps.EntryTransformer;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * Stub implementation of the {@link ChannelMetaDataDAO}. This implementation
 * only implements read-only methods related to retrieving information about
 * sample buckets. All other methods throw an
 * {@link UnsupportedOperationException}. Besides, this implementation can only
 * provide data for a single channel.
 * 
 * @author Sebastian Marsching
 */
public class ChannelMetaDataDAOSampleBucketStubImpl implements
        ChannelMetaDataDAO {

    private ObjectResultSet<SampleBucketInformation> emptySampleBucketInformationResultSet() {
        return SimpleObjectResultSet.fromIterator(null);
    }

    private ObjectResultSet<SampleBucketInformation> singleIteratorSampleBucketInformationResultSet(
            Iterator<SampleBucketInformation> iterator) {
        return SimpleObjectResultSet.fromIterator(iterator);
    }

    private UUID channelDataId;
    private String channelName;
    private Map<Integer, ? extends NavigableMap<Long, SampleBucketInformation>> sampleBuckets;

    /**
     * Creates a {@link ChannelMetaDataDAO} stub that provides information about
     * the sample buckets for the specified channel.
     * 
     * @param channelDataId
     *            data ID used for the channel.
     * @param channelName
     *            name of the channel.
     * @param sampleBuckets
     *            sampleBuckets that exist for the channel. This two-level map
     *            uses the decimation level as the key of the first layer and
     *            the bucket start-time as the key of the second layer. The
     *            value of the second layer is the bucket end-time.
     */
    public ChannelMetaDataDAOSampleBucketStubImpl(UUID channelDataId,
            String channelName,
            Map<Integer, ? extends NavigableMap<Long, Long>> sampleBuckets) {
        this.channelDataId = channelDataId;
        this.channelName = channelName;
        this.sampleBuckets = Maps
                .transformEntries(
                        sampleBuckets,
                        new EntryTransformer<Integer, NavigableMap<Long, Long>, NavigableMap<Long, SampleBucketInformation>>() {
                            @Override
                            public NavigableMap<Long, SampleBucketInformation> transformEntry(
                                    Integer key, NavigableMap<Long, Long> value) {
                                final int decimationLevel = key;
                                return Maps
                                        .transformEntries(
                                                value,
                                                new EntryTransformer<Long, Long, SampleBucketInformation>() {
                                                    @Override
                                                    public SampleBucketInformation transformEntry(
                                                            Long key, Long value) {
                                                        return new SampleBucketInformation(
                                                                value,
                                                                key,
                                                                ChannelMetaDataDAOSampleBucketStubImpl.this.channelDataId,
                                                                ChannelMetaDataDAOSampleBucketStubImpl.this.channelName,
                                                                decimationLevel);
                                                    }
                                                });
                            }
                        });
    }

    @Override
    public ListenableFuture<Void> createChannel(String channelName,
            UUID channelDataId, String controlSystemType,
            Set<Integer> decimationLevels,
            Map<Integer, Integer> decimationLevelToRetentionPeriod,
            boolean enabled, Map<String, String> options, UUID serverId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListenableFuture<Void> createChannelDecimationLevels(
            String channelName, UUID serverId, Set<Integer> decimationLevels,
            Map<Integer, Integer> decimationLevelToRetentionPeriod) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListenableFuture<Pair<Boolean, UUID>> createPendingChannelOperation(
            UUID serverId, String channelName, UUID operationId,
            String operationType, String operationData, int ttl) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListenableFuture<Void> createSampleBucket(String channelName,
            int decimationLevel, long bucketStartTime, long bucketEndTime,
            Long precedingBucketStartTime, boolean isNewCurrentBucket,
            UUID serverId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListenableFuture<Void> deleteChannel(String channelName,
            UUID serverId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListenableFuture<Void> deleteChannelDecimationLevels(
            String channelName, UUID serverId, Set<Integer> decimationLevels) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListenableFuture<Pair<Boolean, UUID>> deletePendingChannelOperation(
            UUID serverId, String channelName, UUID operationId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListenableFuture<Void> deleteSampleBucket(String channelName,
            int decimationLevel, long bucketStartTime, boolean isCurrentBucket,
            UUID serverId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListenableFuture<ChannelInformation> getChannel(String channelName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListenableFuture<ChannelConfiguration> getChannelByServer(
            UUID serverId, String channelName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListenableFuture<? extends Iterable<ChannelInformation>> getChannels() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListenableFuture<? extends Iterable<ChannelConfiguration>> getChannelsByServer(
            UUID serverId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListenableFuture<ChannelOperation> getPendingChannelOperation(
            UUID serverId, String channelName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListenableFuture<? extends Iterable<ChannelOperation>> getPendingChannelOperations(
            UUID serverId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListenableFuture<ObjectResultSet<SampleBucketInformation>> getSampleBuckets(
            String channelName) {
        if (!this.channelName.equals(channelName)) {
            return Futures
                    .immediateFuture(emptySampleBucketInformationResultSet());
        }
        return Futures
                .immediateFuture(singleIteratorSampleBucketInformationResultSet(Iterables
                        .concat(Iterables.transform(
                                sampleBuckets.values(),
                                new Function<NavigableMap<Long, SampleBucketInformation>, Iterable<SampleBucketInformation>>() {
                                    @Override
                                    public Iterable<SampleBucketInformation> apply(
                                            NavigableMap<Long, SampleBucketInformation> input) {
                                        return input.values();
                                    }
                                })).iterator()));
    }

    @Override
    public ListenableFuture<ObjectResultSet<SampleBucketInformation>> getSampleBuckets(
            String channelName, int decimationLevel) {
        if (!this.channelName.equals(channelName)) {
            return Futures
                    .immediateFuture(emptySampleBucketInformationResultSet());
        }
        if (!this.sampleBuckets.containsKey(decimationLevel)) {
            return Futures
                    .immediateFuture(emptySampleBucketInformationResultSet());
        }
        return Futures
                .immediateFuture(singleIteratorSampleBucketInformationResultSet(sampleBuckets
                        .get(decimationLevel).values().iterator()));
    }

    @Override
    public ListenableFuture<ObjectResultSet<SampleBucketInformation>> getSampleBucketsInInterval(
            final String channelName, final int decimationLevel,
            long startTimeGreaterThanOrEqualTo, long startTimeLessThanOrEqualTo) {
        if (!this.channelName.equals(channelName)) {
            return Futures
                    .immediateFuture(emptySampleBucketInformationResultSet());
        }
        NavigableMap<Long, SampleBucketInformation> allBuckets = sampleBuckets
                .get(decimationLevel);
        if (allBuckets == null) {
            return Futures
                    .immediateFuture(emptySampleBucketInformationResultSet());
        }
        NavigableMap<Long, SampleBucketInformation> selectedBuckets = allBuckets
                .subMap(startTimeGreaterThanOrEqualTo, true,
                        startTimeLessThanOrEqualTo, true);
        return Futures
                .<ObjectResultSet<SampleBucketInformation>> immediateFuture(singleIteratorSampleBucketInformationResultSet(selectedBuckets
                        .values().iterator()));
    }

    @Override
    public ListenableFuture<ObjectResultSet<SampleBucketInformation>> getSampleBucketsInReverseOrder(
            String channelName, int decimationLevel, int limit) {
        if (!this.channelName.equals(channelName)) {
            return Futures
                    .immediateFuture(emptySampleBucketInformationResultSet());
        }
        if (!this.sampleBuckets.containsKey(decimationLevel)) {
            return Futures
                    .immediateFuture(emptySampleBucketInformationResultSet());
        }
        return Futures
                .immediateFuture(singleIteratorSampleBucketInformationResultSet(Iterators
                        .limit(sampleBuckets.get(decimationLevel)
                                .descendingMap().values().iterator(), limit)));
    }

    @Override
    public ListenableFuture<ObjectResultSet<SampleBucketInformation>> getSampleBucketsNewerThan(
            String channelName, int decimationLevel,
            long startTimeGreaterThanOrEqualTo, int limit) {
        NavigableMap<Long, SampleBucketInformation> allBuckets = sampleBuckets
                .get(decimationLevel);
        if (allBuckets == null) {
            return Futures
                    .immediateFuture(emptySampleBucketInformationResultSet());
        }
        NavigableMap<Long, SampleBucketInformation> selectedBuckets = allBuckets
                .tailMap(startTimeGreaterThanOrEqualTo, true);
        return Futures
                .<ObjectResultSet<SampleBucketInformation>> immediateFuture(singleIteratorSampleBucketInformationResultSet(Iterators
                        .limit(selectedBuckets.values().iterator(), limit)));
    }

    @Override
    public ListenableFuture<ObjectResultSet<SampleBucketInformation>> getSampleBucketsOlderThanInReverseOrder(
            String channelName, int decimationLevel,
            long startTimeLessThanOrEqualTo, int limit) {
        NavigableMap<Long, SampleBucketInformation> allBuckets = sampleBuckets
                .get(decimationLevel);
        if (allBuckets == null) {
            return Futures
                    .immediateFuture(emptySampleBucketInformationResultSet());
        }
        NavigableMap<Long, SampleBucketInformation> selectedBuckets = allBuckets
                .headMap(startTimeLessThanOrEqualTo, true).descendingMap();
        return Futures
                .<ObjectResultSet<SampleBucketInformation>> immediateFuture(singleIteratorSampleBucketInformationResultSet(Iterators
                        .limit(selectedBuckets.values().iterator(), limit)));
    }

    @Override
    public ListenableFuture<Void> moveChannel(String channelName,
            UUID oldServerId, UUID newServerId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListenableFuture<Void> renameChannel(String oldChannelName,
            String newChannelName, UUID serverId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListenableFuture<Void> updateChannelConfiguration(
            String channelName, UUID serverId,
            Map<Integer, Integer> decimationLevelToRetentionPeriod,
            boolean enabled, Map<String, String> options) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListenableFuture<Pair<Boolean, UUID>> updatePendingChannelOperation(
            UUID serverId, String channelName, UUID oldOperationId,
            UUID newOperationId, String newOperationType,
            String newOperationData, int ttl) {
        throw new UnsupportedOperationException();
    }

}
