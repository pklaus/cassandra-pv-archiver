/*
 * Copyright 2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.archiving;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.Callable;

import org.apache.commons.lang3.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.aquenos.cassandra.pvarchiver.common.AbstractObjectResultSet;
import com.aquenos.cassandra.pvarchiver.common.ObjectResultSet;
import com.aquenos.cassandra.pvarchiver.controlsystem.ControlSystemSupport;
import com.aquenos.cassandra.pvarchiver.controlsystem.Sample;
import com.aquenos.cassandra.pvarchiver.server.controlsystem.ControlSystemSupportRegistry;
import com.aquenos.cassandra.pvarchiver.server.database.ChannelMetaDataDAO;
import com.aquenos.cassandra.pvarchiver.server.database.ChannelMetaDataDAO.ChannelConfiguration;
import com.aquenos.cassandra.pvarchiver.server.database.ChannelMetaDataDAO.ChannelInformation;
import com.aquenos.cassandra.pvarchiver.server.database.ChannelMetaDataDAO.SampleBucketInformation;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * Standard implementation of the {@link ArchiveAccessService}.
 * 
 * @author Sebastian Marsching
 */
public class ArchiveAccessServiceImpl implements ArchiveAccessService {

    /**
     * Result set containing samples. This implementation is internally used
     * when a query for samples is made. It takes care of dispatching the right
     * queries to the underlying database and coordinating between the layer
     * managing sample buckets and the control-system adapter in charge of
     * storing the actual samples.
     * 
     * @author Sebastian Marsching
     *
     * @param <SampleType>
     *            type of the samples that are contained in this sample set.
     */
    private class SampleResultSet<SampleType extends Sample> extends
            AbstractObjectResultSet<SampleType> {

        /**
         * Logger for this object.
         */
        protected final Logger log = LoggerFactory.getLogger(getClass());

        private String channelName;
        private ControlSystemSupport<SampleType> controlSystemSupport;
        private int decimationLevel;
        private SampleBucketInformation firstSampleBucket;
        private ObjectResultSet<SampleBucketInformation> firstSampleBucketResultSet;
        private ObjectResultSet<SampleType> firstSampleResultSet;
        private SampleBucketInformation lastSampleBucket;
        private ObjectResultSet<SampleBucketInformation> lastSampleBucketResultSet;
        private ObjectResultSet<SampleType> lastSampleResultSet;
        private long lastSampleTimeStamp = -1L;
        private long lowerTimeStampLimit;
        private TimeStampLimitMode lowerTimeStampLimitMode;
        private SampleSetPhase phase = SampleSetPhase.FIRST_BUCKET_FIND_BUCKET;
        private ObjectResultSet<SampleType> regularBucketSampleResultSet;
        private SampleBucketInformation regularSampleBucket;
        private ObjectResultSet<SampleBucketInformation> regularSampleBucketResultSet;
        private Callable<ListenableFuture<Iterator<SampleType>>> runOnNextCall;
        private long upperTimeStampLimit;
        private TimeStampLimitMode upperTimeStampLimitMode;

        public SampleResultSet(String channelName, int decimationLevel,
                long lowerTimeStampLimit,
                TimeStampLimitMode lowerTimeStampLimitMode,
                long upperTimeStampLimit,
                TimeStampLimitMode upperTimeStampLimitMode,
                ControlSystemSupport<SampleType> controlSystemSupport) {
            this.channelName = channelName;
            this.controlSystemSupport = controlSystemSupport;
            this.decimationLevel = decimationLevel;
            this.lowerTimeStampLimit = lowerTimeStampLimit;
            this.lowerTimeStampLimitMode = lowerTimeStampLimitMode;
            this.upperTimeStampLimit = upperTimeStampLimit;
            this.upperTimeStampLimitMode = upperTimeStampLimitMode;
        }

        @Override
        protected ListenableFuture<Iterator<SampleType>> fetchNextPage() {
            // The parent class guarantees that this method is only called when
            // the future returned by a previous call has completed. Therefore,
            // we can trigger the whole read logic from here. We do not need
            // explicit synchronization because the synchronization through the
            // futures (before calling this method the calling code will call
            // the get() method of the future returned by an earlier execution
            // of this method).
            if (runOnNextCall != null) {
                Callable<ListenableFuture<Iterator<SampleType>>> callable = runOnNextCall;
                runOnNextCall = null;
                try {
                    return callable.call();
                } catch (Exception e) {
                    throw new RuntimeException("Unexpected exception: "
                            + e.getMessage(), e);
                }
            }
            switch (phase) {
            case FIRST_BUCKET_FIND_BUCKET:
                return firstBucketFindBucket();
            case FIRST_BUCKET_FIND_FIRST_SAMPLE:
                return firstBucketFindFirstSample();
            case REGULAR_BUCKETS_FIND_BUCKETS:
                return regularBucketsFindBuckets();
            case REGULAR_BUCKETS_FIND_SAMPLES:
                return regularBucketsFindSamples();
            case LAST_BUCKET_FIND_BUCKET:
                return lastBucketFindBucket();
            case LAST_BUCKET_FIND_LAST_SAMPLE:
                return lastBucketFindLastSample();
            default:
                throw new RuntimeException("Unhandled phase: " + phase);
            }
        }

        private ListenableFuture<Iterator<SampleType>> firstBucketFindBucket() {
            phase = SampleSetPhase.FIRST_BUCKET_FIND_BUCKET;
            if (firstSampleBucketResultSet == null) {
                return Futures
                        .transform(
                                channelMetaDataDAO
                                        .getSampleBucketsOlderThanInReverseOrder(
                                                channelName, decimationLevel,
                                                lowerTimeStampLimit, 1),
                                new Function<ObjectResultSet<SampleBucketInformation>, Iterator<SampleType>>() {
                                    @Override
                                    public Iterator<SampleType> apply(
                                            ObjectResultSet<SampleBucketInformation> input) {
                                        firstSampleBucketResultSet = input;
                                        return Collections.emptyIterator();
                                    }
                                });
            }
            // We might have to wait for more data being available.
            if (firstSampleBucketResultSet.getAvailableWithoutFetching() == 0
                    && !firstSampleBucketResultSet.isFullyFetched()) {
                return Futures.transform(
                        firstSampleBucketResultSet.fetchMoreResults(),
                        new Function<Void, Iterator<SampleType>>() {
                            @Override
                            public Iterator<SampleType> apply(Void input) {
                                return Collections.emptyIterator();
                            }
                        });
            }
            firstSampleBucket = firstSampleBucketResultSet.one();
            // If we still could not find a sample bucket, the only
            // explanation is that there is no sample bucket. Therefore, we
            // have to look for a sample bucket that starts after the lower
            // limit.
            if (firstSampleBucket == null) {
                return proceedWithRegularBucketsFindSamples(null);
            }
            if (lowerTimeStampLimitMode.equals(TimeStampLimitMode.AT_OR_BEFORE)) {
                return proceedWithFirstBucketFindFirstSample();
            }
            return proceedWithRegularBucketsFindSamples(firstSampleBucket);
        }

        private ListenableFuture<Iterator<SampleType>> firstBucketFindFirstSample() {
            phase = SampleSetPhase.FIRST_BUCKET_FIND_FIRST_SAMPLE;
            if (firstSampleResultSet == null) {
                ListenableFuture<ObjectResultSet<SampleType>> future;
                try {
                    future = controlSystemSupport.getSamplesInReverseOrder(
                            firstSampleBucket.getBucketId(),
                            firstSampleBucket.getBucketStartTime(),
                            lowerTimeStampLimit, 1);
                } catch (Throwable t) {
                    // If getSamplesInReverseOrder throws an exception (it
                    // should not) we can only fail the whole operation.
                    log.error(
                            "The \""
                                    + controlSystemSupport.getId()
                                    + "\" control-system support's getSamples method threw an exception.",
                            t);
                    return Futures.immediateFailedFuture(t);
                }
                return Futures
                        .transform(
                                future,
                                new Function<ObjectResultSet<SampleType>, Iterator<SampleType>>() {
                                    @Override
                                    public Iterator<SampleType> apply(
                                            ObjectResultSet<SampleType> input) {
                                        if (input == null) {
                                            log.error("The \""
                                                    + controlSystemSupport
                                                            .getId()
                                                    + "\" control-system support's getSamplesInReverseOrder returned a future that provided a null result-set.");
                                            // Throwing an exception is okay
                                            // because transform will translate
                                            // this to a failed future.
                                            throw new NullPointerException(
                                                    "The control-system support's getSamplesInReverseOrder method provided a null result-set.");
                                        }
                                        firstSampleResultSet = input;
                                        return Collections.emptyIterator();
                                    }
                                });
            }
            // We might have to wait for more data being available.
            if (firstSampleResultSet.getAvailableWithoutFetching() == 0
                    && !firstSampleResultSet.isFullyFetched()) {
                return Futures.transform(
                        firstSampleResultSet.fetchMoreResults(),
                        new Function<Void, Iterator<SampleType>>() {
                            @Override
                            public Iterator<SampleType> apply(Void input) {
                                return Collections.emptyIterator();
                            }
                        });
            }
            if (firstSampleResultSet.getAvailableWithoutFetching() == 0) {
                // It is possible that there is an older bucket that contains a
                // sample. We look for this bucket by decreasing the lower limit
                // just before the current bucket. We know that we will not find
                // any other samples because if there were such samples, we
                // would have found them with the last query.
                long currentBucketStartTime = firstSampleBucket
                        .getBucketStartTime();
                if (currentBucketStartTime <= 0L) {
                    // There cannot be an older bucket, therefore we can
                    // continue with the bucket that we have.
                    return proceedWithRegularBucketsFindSamples(firstSampleBucket);
                } else {
                    return proceedWithFirstBucketFindBucket(currentBucketStartTime - 1L);
                }
            }
            return returnSamples(firstSampleResultSet,
                    new Callable<ListenableFuture<Iterator<SampleType>>>() {
                        @Override
                        public ListenableFuture<Iterator<SampleType>> call()
                                throws Exception {
                            return proceedWithRegularBucketsFindSamples(firstSampleBucket);
                        }
                    });
        }

        private ListenableFuture<Iterator<SampleType>> regularBucketsFindBuckets() {
            phase = SampleSetPhase.REGULAR_BUCKETS_FIND_BUCKETS;
            if (regularSampleBucketResultSet == null) {
                long lowerLimit = Math.max(lowerTimeStampLimit,
                        lastSampleTimeStamp + 1L);
                if (regularSampleBucket != null) {
                    // We are only interested in buckets that are newer than the
                    // last bucket that we already used.
                    if (regularSampleBucket.getBucketEndTime() == Long.MAX_VALUE) {
                        // There cannot be any more buckets, thus we are done.
                        return Futures.immediateFuture(null);
                    }
                    lowerLimit = Math.max(lowerLimit,
                            regularSampleBucket.getBucketEndTime() + 1L);
                }
                if (lowerLimit > upperTimeStampLimit) {
                    return proceedWithLastBucketFindLastSample(null);
                }
                return Futures
                        .transform(
                                channelMetaDataDAO.getSampleBucketsInInterval(
                                        channelName, decimationLevel,
                                        lowerLimit, upperTimeStampLimit),
                                new Function<ObjectResultSet<SampleBucketInformation>, Iterator<SampleType>>() {
                                    @Override
                                    public Iterator<SampleType> apply(
                                            ObjectResultSet<SampleBucketInformation> input) {
                                        regularSampleBucketResultSet = input;
                                        return Collections.emptyIterator();
                                    }
                                });
            }
            // We might have to wait for more data being available.
            if (regularSampleBucketResultSet.getAvailableWithoutFetching() == 0
                    && !regularSampleBucketResultSet.isFullyFetched()) {
                return Futures.transform(
                        regularSampleBucketResultSet.fetchMoreResults(),
                        new Function<Void, Iterator<SampleType>>() {
                            @Override
                            public Iterator<SampleType> apply(Void input) {
                                return Collections.emptyIterator();
                            }
                        });
            }
            if (regularSampleBucketResultSet.getAvailableWithoutFetching() == 0) {
                // There are no more regular sample buckets. Therefore, we can
                // continue with finding the last sample.
                return proceedWithLastBucketFindLastSample(regularSampleBucket);
            }
            regularSampleBucket = regularSampleBucketResultSet.one();
            return proceedWithRegularBucketsFindSamples(regularSampleBucket);
        }

        private ListenableFuture<Iterator<SampleType>> regularBucketsFindSamples() {
            phase = SampleSetPhase.REGULAR_BUCKETS_FIND_SAMPLES;
            if (regularBucketSampleResultSet == null) {
                long lowerLimit = Math.max(lowerTimeStampLimit,
                        lastSampleTimeStamp + 1L);
                lowerLimit = Math.max(lowerLimit,
                        regularSampleBucket.getBucketStartTime());
                if (lowerLimit > upperTimeStampLimit) {
                    // There is no sense in looking for samples because we will
                    // not be able to find any. Therefore, we can directly
                    // proceed with finding the last sample.
                    return proceedWithLastBucketFindLastSample(regularSampleBucket);
                }
                if (lowerLimit > regularSampleBucket.getBucketEndTime()) {
                    // The current bucket does not contain any more samples,
                    // thus we have to proceed with the next sample bucket. We
                    // do not want to reset the state but continue with the list
                    // of sample buckets that we already have, thus we call
                    // regularBucketsFindBuckets() directly.
                    return regularBucketsFindBuckets();
                }
                long upperLimit = Math.min(upperTimeStampLimit,
                        regularSampleBucket.getBucketEndTime());
                ListenableFuture<ObjectResultSet<SampleType>> future;
                try {
                    future = controlSystemSupport.getSamples(
                            regularSampleBucket.getBucketId(), lowerLimit,
                            upperLimit,
                            ControlSystemSupport.SAMPLES_LIMIT_UNBOUNDED);
                } catch (Throwable t) {
                    // If getSamples throws an exception (it should not) we can
                    // only fail the whole operation.
                    log.error(
                            "The \""
                                    + controlSystemSupport.getId()
                                    + "\" control-system support's getSamples method threw an exception.",
                            t);
                    return Futures.immediateFailedFuture(t);
                }
                return Futures
                        .transform(
                                future,
                                new Function<ObjectResultSet<SampleType>, Iterator<SampleType>>() {
                                    @Override
                                    public Iterator<SampleType> apply(
                                            ObjectResultSet<SampleType> input) {
                                        if (input == null) {
                                            log.error("The \""
                                                    + controlSystemSupport
                                                            .getId()
                                                    + "\" control-system support's getSamples returned a future that provided a null result-set.");
                                            // Throwing an exception is okay
                                            // because transform will translate
                                            // this to a failed future.
                                            throw new NullPointerException(
                                                    "The control-system support's getSamples method provided a null result-set.");
                                        }
                                        regularBucketSampleResultSet = input;
                                        return Collections.emptyIterator();
                                    }
                                });
            }
            // We might have to wait for more data being available.
            if (regularBucketSampleResultSet.getAvailableWithoutFetching() == 0
                    && !regularBucketSampleResultSet.isFullyFetched()) {
                return Futures.transform(
                        regularBucketSampleResultSet.fetchMoreResults(),
                        new Function<Void, Iterator<SampleType>>() {
                            @Override
                            public Iterator<SampleType> apply(Void input) {
                                return Collections.emptyIterator();
                            }
                        });
            }
            if (regularBucketSampleResultSet.getAvailableWithoutFetching() == 0) {
                // There are no more samples in the bucket, thus we have to
                // continue with the next bucket.
                if (regularSampleBucket.getBucketEndTime() >= upperTimeStampLimit) {
                    // There are no more regular samples to be found, thus we
                    // can directly continue with finding the last sample.
                    return proceedWithLastBucketFindLastSample(regularSampleBucket);
                }
                // We do not want to reset the state but continue with the list
                // of sample buckets that we already have, thus we call
                // regularBucketsFindBuckets() directly.
                return regularBucketsFindBuckets();
            }
            return returnSamples(regularBucketSampleResultSet, null);
        }

        private ListenableFuture<Iterator<SampleType>> lastBucketFindBucket() {
            phase = SampleSetPhase.LAST_BUCKET_FIND_BUCKET;
            // Quit immediately if we do not need a last sample.
            if (upperTimeStampLimitMode.equals(TimeStampLimitMode.AT_OR_BEFORE)
                    || lastSampleTimeStamp >= upperTimeStampLimit) {
                return Futures.immediateFuture(null);
            }
            if (lastSampleBucketResultSet == null) {
                long lowerLimit = Math.max(lowerTimeStampLimit,
                        lastSampleTimeStamp + 1L);
                if (lastSampleBucket != null) {
                    // If we already tried a bucket for finding the last sample,
                    // we are only interested in buckets that are newer.
                    if (lastSampleBucket.getBucketEndTime() == Long.MAX_VALUE) {
                        // There cannot be any more buckets, thus we are done.
                        return Futures.immediateFuture(null);
                    }
                    lowerLimit = Math.max(lowerLimit,
                            lastSampleBucket.getBucketEndTime() + 1L);
                }
                return Futures
                        .transform(
                                channelMetaDataDAO.getSampleBucketsNewerThan(
                                        channelName, decimationLevel,
                                        lowerLimit, 1),
                                new Function<ObjectResultSet<SampleBucketInformation>, Iterator<SampleType>>() {
                                    @Override
                                    public Iterator<SampleType> apply(
                                            ObjectResultSet<SampleBucketInformation> input) {
                                        lastSampleBucketResultSet = input;
                                        return Collections.emptyIterator();
                                    }
                                });
            }
            // We might have to wait for more data being available.
            if (lastSampleBucketResultSet.getAvailableWithoutFetching() == 0
                    && !lastSampleBucketResultSet.isFullyFetched()) {
                return Futures.transform(
                        lastSampleBucketResultSet.fetchMoreResults(),
                        new Function<Void, Iterator<SampleType>>() {
                            @Override
                            public Iterator<SampleType> apply(Void input) {
                                return Collections.emptyIterator();
                            }
                        });
            }
            if (lastSampleBucketResultSet.getAvailableWithoutFetching() == 0) {
                // There are no more sample buckets, thus we are done.
                return Futures.immediateFuture(null);
            }
            lastSampleBucket = lastSampleBucketResultSet.one();
            return proceedWithLastBucketFindLastSample(lastSampleBucket);
        }

        private ListenableFuture<Iterator<SampleType>> lastBucketFindLastSample() {
            phase = SampleSetPhase.LAST_BUCKET_FIND_LAST_SAMPLE;
            // Quit immediately if we do not need a last sample.
            if (upperTimeStampLimitMode.equals(TimeStampLimitMode.AT_OR_BEFORE)
                    || lastSampleTimeStamp >= upperTimeStampLimit) {
                return Futures.immediateFuture(null);
            }
            if (upperTimeStampLimit > lastSampleBucket.getBucketEndTime()) {
                // There is no sense in looking in the current bucket. However,
                // there might be a following bucket.
                return proceedWithLastBucketFindBucket();
            }
            if (lastSampleResultSet == null) {
                long lowerLimit = Math.max(upperTimeStampLimit,
                        lastSampleBucket.getBucketStartTime());
                ListenableFuture<ObjectResultSet<SampleType>> future;
                try {
                    future = controlSystemSupport.getSamples(
                            lastSampleBucket.getBucketId(), lowerLimit,
                            lastSampleBucket.getBucketEndTime(), 1);
                    // getSamples should not return null.
                    if (future == null) {
                        throw new NullPointerException(
                                "The control-system support's getSamples method returned null.");
                    }
                } catch (Throwable t) {
                    // If getSamples throws an exception (it should not) we can
                    // only fail the whole operation.
                    log.error(
                            "The \""
                                    + controlSystemSupport.getId()
                                    + "\" control-system support's getSamples method threw an exception.",
                            t);
                    return Futures.immediateFailedFuture(t);
                }
                return Futures
                        .transform(
                                future,
                                new Function<ObjectResultSet<SampleType>, Iterator<SampleType>>() {
                                    @Override
                                    public Iterator<SampleType> apply(
                                            ObjectResultSet<SampleType> input) {
                                        if (input == null) {
                                            log.error("The \""
                                                    + controlSystemSupport
                                                            .getId()
                                                    + "\" control-system support's getSamples returned a future that provided a null result-set.");
                                            // Throwing an exception is okay
                                            // because transform will translate
                                            // this to a failed future.
                                            throw new NullPointerException(
                                                    "The control-system support's getSamples method provided a null result-set.");
                                        }
                                        lastSampleResultSet = input;
                                        return Collections.emptyIterator();
                                    }
                                });
            }
            // We might have to wait for more data being available.
            if (lastSampleResultSet.getAvailableWithoutFetching() == 0
                    && !lastSampleResultSet.isFullyFetched()) {
                return Futures.transform(
                        lastSampleResultSet.fetchMoreResults(),
                        new Function<Void, Iterator<SampleType>>() {
                            @Override
                            public Iterator<SampleType> apply(Void input) {
                                return Collections.emptyIterator();
                            }
                        });
            }
            if (lastSampleResultSet.getAvailableWithoutFetching() == 0) {
                // It is possible that there is a newer bucket that contains a
                // sample. This can happen if the last sample in the bucket is
                // just before the upper limit or if there is an empty bucket.
                // Anyway, this should be infrequent so that we do not have an
                // optimized path for this case but simply look for exactly one
                // bucket that comes after the current one.
                return proceedWithLastBucketFindBucket();
            }
            // We return the last sample. After returning that sample, we are
            // done. Therefore, we return null on the next call.
            return returnSamples(lastSampleResultSet,
                    new Callable<ListenableFuture<Iterator<SampleType>>>() {
                        @Override
                        public ListenableFuture<Iterator<SampleType>> call()
                                throws Exception {
                            return Futures.immediateFuture(null);
                        }
                    });
        }

        private ListenableFuture<Iterator<SampleType>> proceedWithFirstBucketFindFirstSample() {
            firstSampleResultSet = null;
            return firstBucketFindFirstSample();
        }

        private ListenableFuture<Iterator<SampleType>> proceedWithFirstBucketFindBucket(
                long newLowerTimeStampLimit) {
            lowerTimeStampLimit = newLowerTimeStampLimit;
            firstSampleBucket = null;
            firstSampleBucketResultSet = null;
            firstSampleResultSet = null;
            return firstBucketFindBucket();
        }

        private ListenableFuture<Iterator<SampleType>> proceedWithLastBucketFindBucket() {
            // We do not reset lastSampleBucket because we want to preserve it
            // so that we only look for a newer bucket.
            lastSampleBucketResultSet = null;
            lastSampleResultSet = null;
            return lastBucketFindBucket();
        }

        private ListenableFuture<Iterator<SampleType>> proceedWithLastBucketFindLastSample(
                SampleBucketInformation currentSampleBucket) {
            lastSampleBucket = currentSampleBucket;
            lastSampleBucketResultSet = null;
            lastSampleResultSet = null;
            if (lastSampleBucket == null) {
                return lastBucketFindBucket();
            } else {
                return lastBucketFindLastSample();
            }

        }

        private ListenableFuture<Iterator<SampleType>> proceedWithRegularBucketsFindSamples(
                SampleBucketInformation currentSampleBucket) {
            regularBucketSampleResultSet = null;
            regularSampleBucket = currentSampleBucket;
            regularSampleBucketResultSet = null;
            if (regularSampleBucket == null) {
                return regularBucketsFindBuckets();
            } else {
                return regularBucketsFindSamples();
            }
        }

        private ListenableFuture<Iterator<SampleType>> returnSamples(
                ObjectResultSet<SampleType> sampleResultSet,
                Callable<ListenableFuture<Iterator<SampleType>>> nextAction) {
            int available = sampleResultSet.getAvailableWithoutFetching();
            if (available == 0) {
                runOnNextCall = nextAction;
                return Futures.<Iterator<SampleType>> immediateFuture(Iterators
                        .<SampleType> emptyIterator());
            } else {
                ArrayList<SampleType> samples = new ArrayList<SampleType>(
                        available);
                for (int i = 0; i < available; ++i) {
                    samples.add(sampleResultSet.one());
                }
                lastSampleTimeStamp = samples.get(samples.size() - 1)
                        .getTimeStamp();
                // If the last sample had the largest possible time stamp, we
                // can be sure that there are no more samples. Therefore, we are
                // done after returning this list of samples. We handle this
                // here so that we can assume everywhere else the the
                // lastSampleTimeStamp is less than Long.MAX_VALUE.
                if (lastSampleTimeStamp == Long.MAX_VALUE) {
                    runOnNextCall = new Callable<ListenableFuture<Iterator<SampleType>>>() {
                        @Override
                        public ListenableFuture<Iterator<SampleType>> call()
                                throws Exception {
                            // A return value of null indicates that there are
                            // no more samples.
                            return Futures.immediateFuture(null);
                        }
                    };
                } else {
                    runOnNextCall = nextAction;
                }
                return Futures.immediateFuture(samples.iterator());
            }
        }

    }

    /**
     * Phases internally used by the {@link SampleResultSet}. Logically, this
     * enum type is part of the {@link SampleResultSet} implementation. However,
     * it cannot be inside this class because an enum type cannot be a member of
     * a non-static member type.
     * 
     * @author Sebastian Marsching
     */
    private static enum SampleSetPhase {
        FIRST_BUCKET_FIND_BUCKET, FIRST_BUCKET_FIND_FIRST_SAMPLE, REGULAR_BUCKETS_FIND_BUCKETS, REGULAR_BUCKETS_FIND_SAMPLES, LAST_BUCKET_FIND_BUCKET, LAST_BUCKET_FIND_LAST_SAMPLE;
    }

    private ChannelInformationCache channelInformationCache;
    private ChannelMetaDataDAO channelMetaDataDAO;
    private ControlSystemSupportRegistry controlSystemSupportRegistry;

    /**
     * Sets the channel information cache. The cache is used for reading
     * {@link ChannelInformation} objects from the database, using an in-memory
     * copy (if available) instead of having to read from the database every
     * time. Typically, this method is called automatically by the Spring
     * container.
     * 
     * @param channelInformationCache
     *            channel information cache for getting
     *            {@link ChannelInformation} objects.
     */
    @Autowired
    public void setChannelInformationCache(
            ChannelInformationCache channelInformationCache) {
        this.channelInformationCache = channelInformationCache;
    }

    /**
     * Sets the DAO for reading meta-data related to channels. Typically, this
     * method is called automatically by the Spring container.
     * 
     * @param channelMetaDataDAO
     *            channel meta-data DAO to be used by this object.
     */
    @Autowired
    public void setChannelMetaDataDAO(ChannelMetaDataDAO channelMetaDataDAO) {
        this.channelMetaDataDAO = channelMetaDataDAO;
    }

    /**
     * Sets the control-system support registry. The registry is used to gain
     * access to the control-system supports available on this archiving server.
     * Typically, this method is called automatically by the Spring container.
     * 
     * @param controlSystemSupportRegistry
     *            control-system support registry for this archiving server.
     */
    @Autowired
    public void setControlSystemSupportRegistry(
            ControlSystemSupportRegistry controlSystemSupportRegistry) {
        this.controlSystemSupportRegistry = controlSystemSupportRegistry;
    }

    @Override
    public ListenableFuture<? extends ObjectResultSet<? extends Sample>> getSamples(
            String channelName, final int decimationLevel,
            long lowerTimeStampLimit,
            TimeStampLimitMode lowerTimeStampLimitMode,
            long upperTimeStampLimit, TimeStampLimitMode upperTimeStampLimitMode) {
        try {
            Preconditions.checkNotNull(channelName,
                    "The channelName must not be null.");
            Preconditions.checkNotNull(lowerTimeStampLimitMode,
                    "The lowerTimeStampLimitMode must not be null.");
            Preconditions.checkNotNull(upperTimeStampLimitMode,
                    "The upperTimeStampLimitMode must not be null.");
            Preconditions.checkArgument(lowerTimeStampLimit >= 0L,
                    "The lowerTimeStampLimit must not be negative.");
            Preconditions
                    .checkArgument(upperTimeStampLimit >= lowerTimeStampLimit,
                            "The upper time-stamp limit must be greater than or equal to the lower limit.");
            ChannelInformation channelInformation = channelInformationCache
                    .getChannel(channelName);
            if (channelInformation == null) {
                throw new IllegalArgumentException("The channel \""
                        + StringEscapeUtils.escapeJava(channelName)
                        + "\" does not exist.");
            }
            String controlSystemType = channelInformation
                    .getControlSystemType();
            ControlSystemSupport<?> controlSystemSupport = controlSystemSupportRegistry
                    .getControlSystemSupport(controlSystemType);
            if (controlSystemSupport == null) {
                throw new IllegalArgumentException("The channel \""
                        + StringEscapeUtils.escapeJava(channelName)
                        + "\" uses the control-system \""
                        + StringEscapeUtils.escapeJava(channelInformation
                                .getControlSystemType())
                        + "\" which is not available.");
            }
            if (!channelInformation.getDecimationLevels().contains(
                    decimationLevel)) {
                // The specified decimation level does not exist, so there is no
                // sense in checking for samples.
                return Futures
                        .<ObjectResultSet<Sample>> immediateFuture(new AbstractObjectResultSet<Sample>() {
                            @Override
                            protected ListenableFuture<Iterator<Sample>> fetchNextPage() {
                                return Futures.immediateFuture(null);
                            }
                        });
            }
            return getSamplesInternal(channelName, decimationLevel,
                    lowerTimeStampLimit, lowerTimeStampLimitMode,
                    upperTimeStampLimit, upperTimeStampLimitMode,
                    controlSystemSupport);
        } catch (Throwable t) {
            return Futures.immediateFailedFuture(t);
        }
    }

    @Override
    public <SampleType extends Sample> ListenableFuture<ObjectResultSet<SampleType>> getSamples(
            ChannelConfiguration channelConfiguration, int decimationLevel,
            long lowerTimeStampLimit,
            TimeStampLimitMode lowerTimeStampLimitMode,
            long upperTimeStampLimit,
            TimeStampLimitMode upperTimeStampLimitMode,
            ControlSystemSupport<SampleType> controlSystemSupport) {
        try {
            Preconditions.checkNotNull(channelConfiguration,
                    "The channelConfiguration must not be null.");
            Preconditions.checkNotNull(lowerTimeStampLimitMode,
                    "The lowerTimeStampLimitMode must not be null.");
            Preconditions.checkNotNull(upperTimeStampLimitMode,
                    "The upperTimeStampLimitMode must not be null.");
            Preconditions.checkNotNull(controlSystemSupport,
                    "The controlSystemSupport must not be null.");
            Preconditions.checkArgument(lowerTimeStampLimit >= 0L,
                    "The lowerTimeStampLimit must not be negative.");
            Preconditions
                    .checkArgument(upperTimeStampLimit >= lowerTimeStampLimit,
                            "The upper time-stamp limit must be greater than or equal to the lower limit.");
            Preconditions
                    .checkArgument(
                            channelConfiguration.getControlSystemType().equals(
                                    controlSystemSupport.getId()),
                            "The channel's control-system type (\""
                                    + StringEscapeUtils
                                            .escapeJava(channelConfiguration
                                                    .getControlSystemType())
                                    + "\") must match the control-system support's type (\""
                                    + StringEscapeUtils
                                            .escapeJava(controlSystemSupport
                                                    .getId()) + "\").");
            if (!channelConfiguration
                    .getDecimationLevelToCurrentBucketStartTime().containsKey(
                            decimationLevel)) {
                // The specified decimation level does not exist, so there is no
                // sense in checking for samples.
                return Futures
                        .<ObjectResultSet<SampleType>> immediateFuture(new AbstractObjectResultSet<SampleType>() {
                            @Override
                            protected ListenableFuture<Iterator<SampleType>> fetchNextPage() {
                                return Futures.immediateFuture(null);
                            }
                        });
            }
            return getSamplesInternal(channelConfiguration.getChannelName(),
                    decimationLevel, lowerTimeStampLimit,
                    lowerTimeStampLimitMode, upperTimeStampLimit,
                    upperTimeStampLimitMode, controlSystemSupport);
        } catch (Throwable t) {
            return Futures.immediateFailedFuture(t);
        }
    }

    @Override
    public <SampleType extends Sample> ListenableFuture<ObjectResultSet<SampleType>> getSamples(
            ChannelInformation channelInformation, final int decimationLevel,
            long lowerTimeStampLimit,
            TimeStampLimitMode lowerTimeStampLimitMode,
            long upperTimeStampLimit,
            TimeStampLimitMode upperTimeStampLimitMode,
            ControlSystemSupport<SampleType> controlSystemSupport) {
        try {
            Preconditions.checkNotNull(channelInformation,
                    "The channelInformation must not be null.");
            Preconditions.checkNotNull(lowerTimeStampLimitMode,
                    "The lowerTimeStampLimitMode must not be null.");
            Preconditions.checkNotNull(upperTimeStampLimitMode,
                    "The upperTimeStampLimitMode must not be null.");
            Preconditions.checkNotNull(controlSystemSupport,
                    "The controlSystemSupport must not be null.");
            Preconditions.checkArgument(lowerTimeStampLimit >= 0L,
                    "The lowerTimeStampLimit must not be negative.");
            Preconditions
                    .checkArgument(upperTimeStampLimit >= lowerTimeStampLimit,
                            "The upper time-stamp limit must be greater than or equal to the lower limit.");
            Preconditions
                    .checkArgument(
                            channelInformation.getControlSystemType().equals(
                                    controlSystemSupport.getId()),
                            "The channel's control-system type (\""
                                    + StringEscapeUtils
                                            .escapeJava(channelInformation
                                                    .getControlSystemType())
                                    + "\") must match the control-system support's type (\""
                                    + StringEscapeUtils
                                            .escapeJava(controlSystemSupport
                                                    .getId()) + "\").");
            if (!channelInformation.getDecimationLevels().contains(
                    decimationLevel)) {
                // The specified decimation level does not exist, so there is no
                // sense in checking for samples.
                return Futures
                        .<ObjectResultSet<SampleType>> immediateFuture(new AbstractObjectResultSet<SampleType>() {
                            @Override
                            protected ListenableFuture<Iterator<SampleType>> fetchNextPage() {
                                return Futures.immediateFuture(null);
                            }
                        });
            }
            return getSamplesInternal(channelInformation.getChannelName(),
                    decimationLevel, lowerTimeStampLimit,
                    lowerTimeStampLimitMode, upperTimeStampLimit,
                    upperTimeStampLimitMode, controlSystemSupport);
        } catch (Throwable t) {
            return Futures.immediateFailedFuture(t);
        }
    }

    private <SampleType extends Sample> ListenableFuture<ObjectResultSet<SampleType>> getSamplesInternal(
            final String channelName, final int decimationLevel,
            long lowerTimeStampLimit,
            TimeStampLimitMode lowerTimeStampLimitMode,
            long upperTimeStampLimit,
            TimeStampLimitMode upperTimeStampLimitMode,
            final ControlSystemSupport<SampleType> controlSystemSupport) {
        try {
            return Futures
                    .<ObjectResultSet<SampleType>> immediateFuture(new SampleResultSet<SampleType>(
                            channelName, decimationLevel, lowerTimeStampLimit,
                            lowerTimeStampLimitMode, upperTimeStampLimit,
                            upperTimeStampLimitMode, controlSystemSupport));
        } catch (Throwable t) {
            return Futures.immediateFailedFuture(t);
        }
    }

}
