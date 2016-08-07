/*
 * Copyright 2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.archiving.test;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;

import org.apache.commons.lang3.builder.ToStringBuilder;

import com.aquenos.cassandra.pvarchiver.controlsystem.ControlSystemSupport;
import com.aquenos.cassandra.pvarchiver.controlsystem.Sample;
import com.aquenos.cassandra.pvarchiver.controlsystem.SampleBucketId;
import com.aquenos.cassandra.pvarchiver.server.archiving.ArchiveAccessServiceImpl;
import com.aquenos.cassandra.pvarchiver.server.archiving.ChannelInformationCache;
import com.aquenos.cassandra.pvarchiver.server.controlsystem.ControlSystemSupportRegistry;
import com.aquenos.cassandra.pvarchiver.server.controlsystem.test.ControlSystemSupportRegistryStubImpl;
import com.aquenos.cassandra.pvarchiver.server.controlsystem.test.ControlSystemSupportStubImpl;
import com.aquenos.cassandra.pvarchiver.server.database.ChannelMetaDataDAO;
import com.aquenos.cassandra.pvarchiver.server.database.ChannelMetaDataDAO.ChannelInformation;
import com.aquenos.cassandra.pvarchiver.server.database.test.ChannelMetaDataDAOSampleBucketStubImpl;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

/**
 * Utility for building an {@link ArchiveAccessServiceImpl}. This utility
 * provides an easy way to create an {@link ArchiveAccessServiceImpl} that is
 * wired to stub implementations of its dependencies. This is intended so that
 * code can easily test the {@link ArchiveAccessServiceImpl} with a fixed set of
 * samples.
 * 
 * @author Sebastian Marsching
 */
public class ArchiveAccessServiceImplBuilder {

    /**
     * Builds a decimation level containing sample buckets.
     * 
     * @author Sebastian Marsching
     * 
     * @see ArchiveAccessServiceImplBuilder
     */
    public class DecimationLevelBuilder {

        private int decimationLevel;
        private TreeMap<Long, SampleBucketBuilder> sampleBucketBuilders = new TreeMap<Long, SampleBucketBuilder>();

        private DecimationLevelBuilder(int decimationLevel) {
            this.decimationLevel = decimationLevel;
        }

        /**
         * Returns the builder to which this decimation-level builder belongs.
         * 
         * @return parent builder.
         */
        public ArchiveAccessServiceImplBuilder done() {
            return ArchiveAccessServiceImplBuilder.this;
        }

        /**
         * Creates a sample bucket, returning the builder that can be used to
         * add samples to this bucket.
         * 
         * @param startTime
         *            start time of the bucket. The start time is counted in
         *            nanoseconds since epoch (January 1st, 1970, 00:00:00 UTC).
         * @param endTime
         *            end time of the bucket. The end time is counted in
         *            nanoseconds since epoch (January 1st, 1970, 00:00:00 UTC).
         * @return builder for the newly created sample bucket.
         */
        public SampleBucketBuilder sampleBucket(long startTime, long endTime) {
            if (endTime < startTime) {
                throw new IllegalArgumentException(
                        "End time must not be before start time.");
            }
            Map.Entry<Long, SampleBucketBuilder> following = sampleBucketBuilders
                    .ceilingEntry(startTime);
            Map.Entry<Long, SampleBucketBuilder> preceding = sampleBucketBuilders
                    .floorEntry(startTime);
            if (following != null && following.getValue().startTime <= endTime) {
                throw new IllegalStateException(
                        "Sample buckets must not overlap.");
            }
            if (preceding != null && preceding.getValue().endTime >= startTime) {
                throw new IllegalStateException(
                        "Sample buckets must not overlap.");
            }
            SampleBucketBuilder sampleBucketBuilder = new SampleBucketBuilder(
                    this, startTime, endTime);
            sampleBucketBuilders.put(startTime, sampleBucketBuilder);
            return sampleBucketBuilder;
        }

    }

    /**
     * Builds a sample bucket containing samples.
     * 
     * @author Sebastian Marsching
     * 
     * @see ArchiveAccessServiceImplBuilder
     * @see DecimationLevelBuilder
     */
    public class SampleBucketBuilder {

        private DecimationLevelBuilder decimationLevelBuilder;
        private long endTime;
        private TreeSet<Sample> samples = new TreeSet<Sample>(
                new Comparator<Sample>() {
                    @Override
                    public int compare(Sample o1, Sample o2) {
                        long ts1 = o1.getTimeStamp();
                        long ts2 = o2.getTimeStamp();
                        return (ts1 == ts2) ? 0 : (ts1 > ts2) ? 1 : -1;
                    }
                });
        private long startTime;

        private SampleBucketBuilder(
                DecimationLevelBuilder decimationLevelBuilder, long startTime,
                long endTime) {
            this.decimationLevelBuilder = decimationLevelBuilder;
            this.endTime = endTime;
            this.startTime = startTime;
        }

        /**
         * Returns the builder to which this sample-bucket builder belongs.
         * 
         * @return parent builder.
         */
        public DecimationLevelBuilder done() {
            return decimationLevelBuilder;
        }

        /**
         * Adds a sample to this sample bucket. There always is only one sample
         * with the same time-stamp, even if this method is called multiple
         * times with the same time-stamp.
         * 
         * @param timeStamp
         *            time stamp of the sample to be added. The time-stamp is
         *            counted in nanoseconds since epoch (January 1st, 1970,
         *            00:00:00 UTC).
         * @return this builder.
         */
        public SampleBucketBuilder sample(final long timeStamp) {
            // We could verify that the time stamp is actually within the limits
            // of the sample bucket. However, we do not do this because some
            // tests might be interested in testing what happens when a sample
            // is stored in the wrong sample bucket.
            this.samples.add(new Sample() {
                @Override
                public long getTimeStamp() {
                    return timeStamp;
                }

                @Override
                public String toString() {
                    return new ToStringBuilder(this).append("timeStamp",
                            timeStamp).toString();
                }
            });
            return this;
        }

        /**
         * Adds samples to this sample bucket. This is a convenience method that
         * calls {@link #sample(long)} for each of the elements of the supplied
         * iterable.
         * 
         * @param timeStamps
         *            list of time-stamps for which samples are to be created.
         *            The time-stamps are counted in nanoseconds since epoch
         *            (January 1st, 1970, 00:00:00 UTC).
         * @return this builder.
         */
        public SampleBucketBuilder samples(Iterable<Long> timeStamps) {
            for (long timeStamp : timeStamps) {
                sample(timeStamp);
            }
            return this;
        }

        /**
         * Adds samples to this sample bucket. This is a convenience method that
         * calls {@link #sample(long)} for each of the supplied time-stamps.
         * 
         * @param timeStamps
         *            time-stamps for which samples are to be created. The
         *            time-stamps are counted in nanoseconds since epoch
         *            (January 1st, 1970, 00:00:00 UTC).
         * @return this builder.
         */
        public SampleBucketBuilder samples(long... timeStamps) {
            for (long timeStamp : timeStamps) {
                sample(timeStamp);
            }
            return this;
        }

    }

    private UUID channelDataId = UUID.randomUUID();
    private String channelName = "testChannel";
    private Map<Integer, DecimationLevelBuilder> decimationLevelBuilders = new HashMap<Integer, DecimationLevelBuilder>();
    private UUID serverId = UUID.randomUUID();

    /**
     * Creates a decimation level. If the specified decimation level already
     * exists, the existing builder instance is returned instead of creating a
     * new one.
     * 
     * @param decimationLevel
     *            decimation period of the decimation level to be created (in
     *            seconds).
     * @return builder for the specified decimation level.
     */
    public DecimationLevelBuilder decimationLevel(int decimationLevel) {
        DecimationLevelBuilder decimationLevelBuilder = decimationLevelBuilders
                .get(decimationLevel);
        if (decimationLevelBuilder == null) {
            decimationLevelBuilder = new DecimationLevelBuilder(decimationLevel);
            decimationLevelBuilders
                    .put(decimationLevel, decimationLevelBuilder);
        }
        return decimationLevelBuilder;
    }

    /**
     * Builds an instance of the {@link ArchiveAccessServiceImpl} that uses stub
     * implementations of {@link ControlSystemSupportRegistry},
     * {@link ChannelInformationCache} and {@link ChannelMetaDataDAO}. These
     * stub implementations are initialized in a way that they provide the
     * decimation levels, sample buckets, and samples that have been added to
     * this builder.
     * 
     * @return {@link ArchiveAccessServiceImpl} instance initialized from the
     *         configuration of this builder.
     */
    public ArchiveAccessServiceImpl build() {
        Map<SampleBucketId, Collection<Sample>> sampleBuckets = new HashMap<SampleBucketId, Collection<Sample>>();
        Map<Integer, NavigableMap<Long, Long>> sampleBucketInformationMap = new HashMap<Integer, NavigableMap<Long, Long>>();
        for (DecimationLevelBuilder decimationLevelBuilder : decimationLevelBuilders
                .values()) {
            TreeMap<Long, Long> sampleBucketInformationForDecimationLevel = new TreeMap<Long, Long>();
            for (SampleBucketBuilder sampleBucketBuilder : decimationLevelBuilder.sampleBucketBuilders
                    .values()) {
                SampleBucketId bucketId = new SampleBucketId(channelDataId,
                        decimationLevelBuilder.decimationLevel,
                        sampleBucketBuilder.startTime);
                Collection<Sample> samples = ImmutableList
                        .copyOf(sampleBucketBuilder.samples);
                sampleBuckets.put(bucketId, samples);
                sampleBucketInformationForDecimationLevel.put(
                        sampleBucketBuilder.startTime,
                        sampleBucketBuilder.endTime);
            }
            sampleBucketInformationMap.put(
                    decimationLevelBuilder.decimationLevel,
                    sampleBucketInformationForDecimationLevel);
        }

        ControlSystemSupport<Sample> controlSystemSupport = new ControlSystemSupportStubImpl<Sample>(
                sampleBuckets);
        ChannelInformation channelInformation = new ChannelInformation(
                channelDataId, channelName, controlSystemSupport.getId(),
                ImmutableSet.copyOf(decimationLevelBuilders.keySet()), serverId);
        ChannelInformationCache channelInformationCache = new ChannelInformationCacheStubImpl(
                channelInformation);
        ChannelMetaDataDAO channelMetaDataDAO = new ChannelMetaDataDAOSampleBucketStubImpl(
                channelDataId, channelName, sampleBucketInformationMap);
        ControlSystemSupportRegistry controlSystemSupportRegistry = new ControlSystemSupportRegistryStubImpl(
                controlSystemSupport);
        ArchiveAccessServiceImpl archiveAccessService = new ArchiveAccessServiceImpl();
        archiveAccessService
                .setChannelInformationCache(channelInformationCache);
        archiveAccessService.setChannelMetaDataDAO(channelMetaDataDAO);
        archiveAccessService
                .setControlSystemSupportRegistry(controlSystemSupportRegistry);
        return archiveAccessService;
    }

    /**
     * Sets the name of the channel that will have the samples. The stub
     * implementations provided to the {@link ArchiveAccessServiceImpl} by this
     * builder only support storing data for a single channel. If not set
     * explicitly, the default value "testChannel" is used.
     * 
     * @param channelName
     *            name of the channel for which the samples are being made
     *            available.
     * @return this builder.
     */
    public ArchiveAccessServiceImplBuilder channelName(String channelName) {
        this.channelName = channelName;
        return this;
    }

}
