/*
 * Copyright 2015-2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.archiving.internal;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aquenos.cassandra.pvarchiver.common.ObjectResultSet;
import com.aquenos.cassandra.pvarchiver.controlsystem.Sample;
import com.aquenos.cassandra.pvarchiver.controlsystem.SampleBucketId;
import com.aquenos.cassandra.pvarchiver.controlsystem.SampleBucketState;
import com.aquenos.cassandra.pvarchiver.server.database.ChannelMetaDataDAO;
import com.aquenos.cassandra.pvarchiver.server.database.ChannelMetaDataDAO.ChannelConfiguration;
import com.aquenos.cassandra.pvarchiver.server.database.ChannelMetaDataDAO.SampleBucketInformation;
import com.aquenos.cassandra.pvarchiver.server.util.FutureUtils;
import com.google.common.base.Function;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * <p>
 * Data associated with a specific decimation level of an
 * {@link ArchivedChannel}. This class is abstract because it only serves as a
 * base class for {@link ArchivedChannelRawSamplesDecimationLevel} and
 * {@link ArchivedChannelDecimatedSamplesDecimationLevel}.
 * </p>
 * 
 * <p>
 * This class is intended to be used by the {@link ArchivingServiceInternalImpl}
 * and {@link ArchivedChannel} classes only. For this reason, it has been marked
 * as package private.
 * </p>
 * 
 * <p>
 * As this class is designed for internal use only, most of its methods do not
 * verify that the state or the input parameters match the expectations. Some
 * verifications are made by assertions, but in general the calling code has to
 * ensure that the preconditions needed when calling the methods in this class
 * are met.
 * </p>
 * 
 * <p>
 * As a guideline, methods in this class that are <em>public</em> verify their
 * input parameters and the state while method that are <em>protected</em> or
 * <em>package private</em> do not. In particular, most <em>protected</em> and
 * <em>package-private</em> methods assume that the caller has synchronized on
 * the {@link ArchivedChannel} instance that is associated with this object.
 * However, there might be methods that do not adhere to this basic rules, so in
 * case of doubt one should always refer to the method's documentation.
 * </p>
 * 
 * @author Sebastian Marsching
 */
abstract class ArchivedChannelDecimationLevel<SampleType extends Sample> {

    /**
     * Decimation period of the decimation level storing raw samples. Raw
     * samples are stored in the decimation level with a decimation period of
     * zero, which acts as a special marker.
     */
    protected static final int RAW_SAMPLES_DECIMATION_PERIOD = 0;

    /**
     * Maximum time for which raw samples and source samples for generating
     * decimated samples shall be kept in their queue. When this time is
     * exceeded and more samples are added to the queue, the samples older than
     * this limit are dropped. This ensures that the memory usage does not
     * increase unboundedly when an asynchronous operation takes very long to
     * finish or when raw samples arrive at a higher rate than the rate at which
     * they can be written.
     */
    protected static final long MAX_QUEUE_TIME_MILLISECONDS = 30000L;

    // We aim for a bucket size of about 100 MB because larger partitions may
    // have a negative impact on the performance of Cassandra's compaction
    // process. Actually, we set the limit slightly below 100 MB so that we have
    // a safety margin.
    private static final int MAX_BUCKET_SIZE_BYTES = 96000000;

    private static final long QUEUE_CHANNEL_DELAY_ON_ERROR_MILLISECONDS = 5000L;

    /**
     * Archiving service. This is the archiving service to which the channel
     * belongs to which this decimation level belongs.
     */
    protected final ArchivingServiceInternalImpl archivingService;

    /**
     * Channel to which this decimation level belongs.
     */
    protected final ArchivedChannel<SampleType> channel;

    /**
     * Configuration for the channel to which this decimationLevel belongs. This
     * field exists just for convenience so that we do not have to use
     * <code>channel.getConfiguration()</code> every time.
     */
    protected final ChannelConfiguration channelConfiguration;

    /**
     * Decimation period of this decimation level (in seconds).
     */
    protected final int decimationPeriodSeconds;

    /**
     * Logger for this object. This logger is created using the actual class of
     * this object and not just the base class where this field is declared.
     */
    protected final Logger log = LoggerFactory.getLogger(getClass());

    /**
     * Executor used for executing asynchronous tasks. This executor can be
     * expected to provide several threads.
     */
    protected final ExecutorService poolExecutor;

    /**
     * Scheduled executor for executing asynchronous tasks that shall be
     * executed with a delay. This executor has to be expected to only have a
     * single thread, so it should not be used for operations that take a
     * significant amount of time. Such operations should be delegated to the
     * {@link #poolExecutor}, for example by using the
     * {@link #runWithPoolExecutor(Runnable)} method.
     */
    protected final ScheduledExecutorService scheduledExecutor;

    private final ChannelMetaDataDAO channelMetaDataDAO;
    // We initialize the current bucket size with minus one. This way, we
    // can determine that the control-system support has not been asked for
    // the bucket size yet. The control-system support returns zero if it is
    // asked for a bucket that does not exist.
    private int currentBucketSize = -1;
    private long currentBucketStartTime = -1L;
    // We initialize the last sample time-stamp with minus one. This way, we
    // can determine that the control-system support has not been asked for
    // the time stamp yet. The control-system support returns zero if it is
    // asked for a bucket that does not exist.
    private long lastSampleTimeStamp = -1L;
    private LinkedList<SampleBucketInformation> oldestSampleBuckets = new LinkedList<SampleBucketInformation>();
    private boolean removeOldSampleBucketsScheduled;
    private final long retentionPeriodNanoseconds;
    private Set<ArchivedChannelDecimatedSamplesDecimationLevel<SampleType>> targetDecimationLevels = new HashSet<ArchivedChannelDecimatedSamplesDecimationLevel<SampleType>>();
    private final UUID thisServerId;
    private boolean waitingForAsynchronousRemoveSampleBucketOperation;
    private boolean waitingForAsynchronousWriteSampleOperation;
    // The write queue stores pairs where the left element is the sample and the
    // right element is the sample's estimated size.
    private final Queue<Pair<SampleType, Integer>> writeQueue = new LinkedList<Pair<SampleType, Integer>>();

    /**
     * Creates an object representing a decimation level of the specified
     * <code>channel</code>. This constructor can safely be used
     * <em>without</em> synchronizing on the <code>channel</code>. It should
     * only be used by the {@link ArchivedChannel} instance to which the created
     * decimation level belongs.
     * 
     * @param decimationPeriodSeconds
     *            decimation period of this decimation level (in seconds). Must
     *            be zero if this object represents the raw decimation level.
     *            Must not be negative.
     * @param retentionPeriodSeconds
     *            retention period of this decimation level (in seconds). Must
     *            be zero if samples are supposed to be kept indefinitely. Must
     *            not be negative.
     * @param currentBucketStartTime
     *            start time of the most recent sample buckets (represented as
     *            the number of nanoseconds since epoch). If this decimation
     *            level does not have any sample buckets yet, this number must
     *            be minus one. Must not be less than minus one.
     * @param archivingService
     *            archiving service to which the <code>channel</code> belongs.
     * @param channel
     *            channel to which this decimation-level instance belongs. This
     *            should be the instance that is calling this constructor.
     * @param channelMetaDataDAO
     *            channel meta-data DAO that is used for creating sample
     *            buckets.
     * @param poolExecutor
     *            executor that is used for running asynchronous tasks. It is
     *            strongly suggested that this executor uses more than a single
     *            thread for running asynchronous tasks.
     * @param scheduledExecutor
     *            scheduled executor that is used for scheduling asynchronous
     *            tasks that are supposed to be run with a delay or This
     *            executor is not used for tasks that may run for an extended
     *            period of time and can thus be single-threaded. periodically.
     * @param thisServerId
     *            unique identifier identifying this archive server instance.
     */
    ArchivedChannelDecimationLevel(int decimationPeriodSeconds,
            int retentionPeriodSeconds, long currentBucketStartTime,
            ArchivingServiceInternalImpl archivingService,
            ArchivedChannel<SampleType> channel,
            ChannelMetaDataDAO channelMetaDataDAO,
            ExecutorService poolExecutor,
            ScheduledExecutorService scheduledExecutor, UUID thisServerId) {
        assert (decimationPeriodSeconds >= 0);
        assert (retentionPeriodSeconds >= 0);
        assert (currentBucketStartTime >= -1);
        assert (archivingService != null);
        assert (channel != null);
        assert (channelMetaDataDAO != null);
        assert (poolExecutor != null);
        assert (scheduledExecutor != null);
        assert (thisServerId != null);
        this.currentBucketStartTime = currentBucketStartTime;
        this.decimationPeriodSeconds = decimationPeriodSeconds;
        this.retentionPeriodNanoseconds = retentionPeriodSeconds * 1000000000L;
        this.archivingService = archivingService;
        this.channel = channel;
        this.channelMetaDataDAO = channelMetaDataDAO;
        this.poolExecutor = poolExecutor;
        this.scheduledExecutor = scheduledExecutor;
        this.thisServerId = thisServerId;
        this.channelConfiguration = this.channel.getConfiguration();
    }

    /**
     * <p>
     * Returns the decimation period of this decimation level (in seconds). If
     * this decimation level stores raw samples, this method returns zero.
     * </p>
     *
     * <p>
     * This method may be called without synchronizing on this
     * decimation-level's channel and does not synchronize on the channel either
     * because the field read is immutable.
     * </p>
     * 
     * @return decimation period of this decimation level.
     */
    public int getDecimationPeriodSeconds() {
        return decimationPeriodSeconds;
    }

    /**
     * <p>
     * Processes the sample write queue. Typically, this method is called by the
     * {@link ArchivingServiceInternalImpl} after this decimation level has been
     * registered for processing through the
     * {@link ArchivingServiceInternalImpl#addChannelDecimationLevelToWriteProcessingQueue(ArchivedChannelDecimationLevel)}
     * method.
     * </p>
     * 
     * <p>
     * This method may be called without synchronizing on this
     * decimation-level's channel because it takes care of synchronizing on the
     * channel.
     * </p>
     */
    public void processSampleWriteQueue() {
        synchronized (channel) {
            // If the channel has not been initialized yet or if it has been
            // destroyed, we do not process any further write requests. We do
            // the same if it has not been destroyed yet, but destruction has
            // been requested.
            if (!channel.getState().equals(ArchivedChannelState.INITIALIZED)
                    || channel.isDestructionRequested()) {
                return;
            }
            // If we are waiting for an asynchronous action for the same channel
            // and decimation level to finish, we cannot process any other
            // action because it might interfere with the asynchronous action.
            if (waitingForAsynchronousWriteSampleOperation) {
                return;
            }
            // We get but do not remove the request from the queue. We will
            // remove the request later when it got handled.
            final Pair<SampleType, Integer> nextRequest = writeQueue.peek();
            // If the queue is empty, there is nothing we have to do.
            if (nextRequest == null) {
                return;
            }
            if (currentBucketStartTime < 0L) {
                // When we do not have a bucket yet, this also means that we do
                // not have any samples. Therefore, we can use the time of the
                // current sample as the start time of the bucket.
                ListenableFuture<Void> future = channel.createSampleBucket(
                        this, nextRequest.getLeft().getTimeStamp());
                // We do not want to run any other action until the asynchronous
                // operation has completed. This protects us from trying to
                // create a sample bucket twice.
                waitingForAsynchronousWriteSampleOperation = true;
                // We do not use the same-thread executor for our callback
                // because we want to synchronize on the channel and depending
                // on implementation details outside our scope, this could lead
                // to a dead lock if it is executed by a thread that also holds
                // a mutex that we acquire while holding the channel mutex.
                Futures.addCallback(future, new FutureCallback<Void>() {
                    @Override
                    public void onSuccess(Void result) {
                        synchronized (channel) {
                            // The asynchronous operation has finished, so we
                            // can run other operations again. We also queue the
                            // channel so that operations that might have been
                            // queued while we were waiting for the asynchronous
                            // operation to finish can finally be processed.
                            waitingForAsynchronousWriteSampleOperation = false;
                            addToGlobalWriteProcessingQueueIfNeeded();
                        }
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        // Depending on the cause of the failure, the channel
                        // might be in an inconsistent state. For example, we
                        // cannot know whether the sample bucket has been
                        // created. Our best option is to put the channel in an
                        // error state. Eventually, it can awake from that state
                        // when the cause of the error has been resolved.
                        channel.destroyWithException(t);
                    }
                }, poolExecutor);
                return;
            }
            if (currentBucketSize < 0 || lastSampleTimeStamp < 0L) {
                // If this is the first time that we write data for the channel,
                // we first have to get some information about the current
                // bucket.
                final ListenableFuture<Void> future = initializeCurrentBucketSizeAndLastSampleTimeStamp();
                // We do not want to run any other action until the asynchronous
                // operation has completed. We would only start the same query
                // again, causing unnecessary load.
                waitingForAsynchronousWriteSampleOperation = true;
                // We do not use the same-thread executor for our callback
                // because we want to synchronize on the channel and depending
                // on implementation details outside our scope, this could lead
                // to a dead lock if it is executed by a thread that also holds
                // a mutex that we acquire while holding the channel mutex.
                Futures.addCallback(future, new FutureCallback<Void>() {
                    @Override
                    public void onSuccess(Void result) {
                        synchronized (channel) {
                            // The asynchronous operation has finished, so we
                            // can run other operations again.
                            waitingForAsynchronousWriteSampleOperation = false;
                            // We add the channel to the queue so that the write
                            // request that was not completed earlier due to
                            // missing information can be completed now.
                            addToGlobalWriteProcessingQueueIfNeeded();
                        }
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        // The asynchronous operation has finished, so we can
                        // run other operations again.
                        waitingForAsynchronousWriteSampleOperation = false;
                        // If there was a problem with getting the required
                        // information, we process the channel again. This will
                        // result in the action to be repeated which most likely
                        // will result in success if the cause of the problem
                        // was transient. The most likely cause of an error that
                        // persists for a longer period of time are connection
                        // problems which will typically lead to the server
                        // going offline and thus the operation to be stopped
                        // eventually. However, we use a small delay before we
                        // queue the channel again. This ensures that we will
                        // not cause a very high CPU load when there is an
                        // immediate failure after queuing it.
                        scheduledExecutor.schedule(
                                runWithPoolExecutor(new Runnable() {
                                    @Override
                                    public void run() {
                                        synchronized (channel) {
                                            addToGlobalWriteProcessingQueueIfNeeded();
                                        }
                                    }
                                }), QUEUE_CHANNEL_DELAY_ON_ERROR_MILLISECONDS,
                                TimeUnit.MILLISECONDS);
                    }
                }, poolExecutor);
                return;
            }
            // We discard any sample that is not newer than the newest already
            // stored sample. First of all, such a sample is most likely bogus
            // (or a repetition of an existing sample). Second, but not less
            // important, our logic for determining the bucket to which the
            // sample belongs relies on the fact that we never skip back in
            // time.
            // Even if the lastSampleTimeStamp is zero (because the bucket is
            // empty), we need to ensure that we only write samples that have a
            // time stamp that is equal to or greater than the start time of the
            // bucket.
            long sampleTimeStamp = nextRequest.getLeft().getTimeStamp();
            if ((lastSampleTimeStamp >= sampleTimeStamp)
                    || currentBucketStartTime > sampleTimeStamp) {
                writeQueue.poll();
                // We keep a counter for the number of samples that skipped back
                // in time. This might be interesting to administrators because
                // it could indicate a problem with clocks.
                channel.incrementSamplesSkippedBack(1L);
                addToGlobalWriteProcessingQueueIfNeeded();
                return;
            }
            // When adding the sample to the current bucket would cause the
            // maximum bucket size to be exceeded, we create a new bucket. Of
            // course, we do not do this when the current bucket is empty (if a
            // single sample is greater than the maximum bucket size, we have to
            // live with the fact that the bucket will exceed the limit).
            // This code is safe even in the event of a restarted server because
            // createSampleBucket registers a pending operation so that the
            // channel is locked until either the operation succeeds and the
            // bucket is visible or enough time has passed so that we can be
            // sure that the operation has finally failed and the bucket will
            // never turn up.
            if (currentBucketSize > MAX_BUCKET_SIZE_BYTES
                    - nextRequest.getRight()
                    && currentBucketSize != 0) {
                // We create the new bucket with a start time that is equal to
                // the time stamp of the new sample. This makes sense because
                // the existing bucket is guaranteed to contain data up to the
                // point in time just before our new sample.
                ListenableFuture<Void> future = channel.createSampleBucket(
                        this, sampleTimeStamp);
                // We do not want to run any other action until the asynchronous
                // operation has completed. This protects us from trying to
                // create a sample bucket twice.
                waitingForAsynchronousWriteSampleOperation = true;
                // We do not use the same-thread executor for our callback
                // because we want to synchronize on the channel and depending
                // on implementation details outside our scope, this could lead
                // to a dead lock if it is executed by a thread that also holds
                // a mutex that we acquire while holding the channel mutex.
                Futures.addCallback(future, new FutureCallback<Void>() {
                    @Override
                    public void onSuccess(Void result) {
                        synchronized (channel) {
                            // The asynchronous operation has finished, so we
                            // can run other operations again. We also queue the
                            // channel so that operations that might have been
                            // queued while we were waiting for the asynchronous
                            // operation to finish can finally be processed.
                            waitingForAsynchronousWriteSampleOperation = false;
                            addToGlobalWriteProcessingQueueIfNeeded();
                        }
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        // Depending on the cause of the failure, the channel
                        // might be in an inconsistent state. For example, we
                        // cannot know whether the sample bucket has been
                        // created. Our best option is to put the channel in an
                        // error state. Eventually, it can awake from that state
                        // when the cause of the error has been resolved.
                        channel.destroyWithException(t);
                    }
                }, poolExecutor);
                return;
            }
            final int newBucketSize;
            if (Integer.MAX_VALUE - nextRequest.getRight() < currentBucketSize) {
                newBucketSize = Integer.MAX_VALUE;
            } else {
                newBucketSize = currentBucketSize + nextRequest.getRight();
            }
            ListenableFuture<Void> future;
            try {
                future = channel.getControlSystemSupport().writeSample(
                        nextRequest.getLeft(),
                        new SampleBucketId(channelConfiguration
                                .getChannelDataId(), decimationPeriodSeconds,
                                currentBucketStartTime), newBucketSize);
                if (future == null) {
                    throw new NullPointerException(
                            "The control-system support's writeSample method returned null.");
                }
            } catch (Throwable t) {
                // If writeSample throws an exception (it should not), we want
                // to put this channel into an error state because this violates
                // the contract of the ControlSystemSupport interface. Then, we
                // simply return because this is the method of choice for
                // aborting execution with the least undesired side-effects.
                log.error(
                        "The \""
                                + channel.getControlSystemSupport().getId()
                                + "\" control-system support's writeSample method violated its contract: "
                                + t.getMessage(), t);
                RuntimeException e = new RuntimeException(
                        "The control-system support's createSampleDecimator method violated its contract: "
                                + t.getMessage());
                channel.destroyWithException(e);
                return;
            }
            final long newSampleTimeStamp = nextRequest.getLeft()
                    .getTimeStamp();
            // We do not want to run any other action until the asynchronous
            // operation has completed. This protects us from writing a second
            // (or making any other modifications) before the current sample has
            // been written.
            waitingForAsynchronousWriteSampleOperation = true;
            // We do not use the same-thread executor for our callback because
            // we want to synchronize on the channel and depending on
            // implementation details outside our scope, this could lead to a
            // dead lock if it is executed by a thread that also holds a mutex
            // that we acquire while holding the channel mutex.
            Futures.addCallback(future, new FutureCallback<Void>() {
                @Override
                public void onSuccess(Void result) {
                    synchronized (channel) {
                        // The asynchronous operation has finished, so we can
                        // run other operations again.
                        waitingForAsynchronousWriteSampleOperation = false;
                        // We actually handled the write request, so we can
                        // remove it from the queue.
                        writeQueue.poll();
                        channel.incrementSamplesWritten(1L);
                        currentBucketSize = newBucketSize;
                        lastSampleTimeStamp = newSampleTimeStamp;
                        addToGlobalWriteProcessingQueueIfNeeded();
                        // We want to send the written sample to all decimation
                        // levels that use this decimation level as their
                        // source. We do this directly from the write thread as
                        // this is much more efficient than scheduling a
                        // background task for each sample. Typically,
                        // processing a single source sample is a very quick
                        // process. If the decimation process has to wait
                        // because of some background operation, the called
                        // method will still return quickly.
                        for (ArchivedChannelDecimatedSamplesDecimationLevel<SampleType> targetDecimationLevel : targetDecimationLevels) {
                            targetDecimationLevel
                                    .processSourceSample(nextRequest.getLeft());
                        }
                        // We time-stamp of the latest sample increased, so it
                        // is possible that we can now delete a sample bucket
                        // which still was within the retention period before.
                        removeOldSampleBuckets();
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    // On failure, we do not remove the sample from the queue
                    // and queue the channel for processing. This will not allow
                    // the sample queue to grow indefinitely, because the sample
                    // that we added back will be removed when new samples are
                    // added and it is too old.
                    // However, we use a small delay before we queue the channel
                    // again. This ensures that we will not cause a very high
                    // CPU load when there is an immediate failure after queuing
                    // it.
                    synchronized (channel) {
                        // The asynchronous operation has finished, so we can
                        // run other operations again.
                        waitingForAsynchronousWriteSampleOperation = false;
                    }
                    scheduledExecutor.schedule(
                            runWithPoolExecutor(new Runnable() {
                                @Override
                                public void run() {
                                    synchronized (channel) {
                                        addToGlobalWriteProcessingQueueIfNeeded();
                                    }
                                }
                            }), QUEUE_CHANNEL_DELAY_ON_ERROR_MILLISECONDS,
                            TimeUnit.MILLISECONDS);
                }
            }, poolExecutor);
        }
    }

    /**
     * <p>
     * Schedules removal of old sample buckets that have passed the retention
     * period. This method should be called once by the {@link ArchivedChannel}
     * after the control-system support has been initialized. Later runs are
     * scheduled automatically (if needed).
     * </p>
     *
     * <p>
     * This method may be called without synchronizing on this
     * decimation-level's channel and does not synchronize on the channel either
     * because it does not modify any fields or read mutable fields.
     * </p>
     * 
     * @param delay
     *            delay (specified in <code>unit</code>s) after which the
     *            process to remove old sample buckets should be started. This
     *            is passed on to the
     *            {@link ScheduledExecutorService#schedule(Runnable, long, TimeUnit)}
     *            method.
     * @param unit
     *            time unit in which the <code>delay</code> is specified. This
     *            is passed on to the
     *            {@link ScheduledExecutorService#schedule(Runnable, long, TimeUnit)}
     *            method.
     */
    public void scheduleRemoveOldSampleBuckets(long delay, TimeUnit unit) {
        if (retentionPeriodNanoseconds <= 0L) {
            return;
        }
        scheduledExecutor.schedule(runWithPoolExecutor(new Runnable() {
            @Override
            public void run() {
                synchronized (channel) {
                    if (channel.getControlSystemSupport() == null) {
                        log.error("The removal of old sample buckets has been scheduled before setting the control-system support.");
                        return;
                    }
                    removeOldSampleBuckets();
                }
            }
        }), delay, unit);
    }

    /**
     * <p>
     * Adds the specified sample to the write queue. Calling this method does
     * not ensure that the queued sample is actually processed. The calling code
     * has to call {@link #processSampleWriteQueue()} directly or indirectly
     * (e.g. through
     * {@link ArchivingServiceInternalImpl#addChannelDecimationLevelToWriteProcessingQueue(ArchivedChannelDecimationLevel)}
     * in order to process the sample.
     * </p>
     * 
     * <p>
     * Code calling this method has to synchronize on this decimation-level's
     * channel.
     * </p>
     * 
     * @param sample
     *            sample that shall be written.
     * @param estimatedSampleSize
     *            estimated size of the <code>sample</code> (in bytes). Must not
     *            be negative.
     */
    protected void addSampleToWriteQueue(SampleType sample,
            int estimatedSampleSize) {
        assert (Thread.holdsLock(channel));
        assert (sample != null);
        assert (estimatedSampleSize >= 0);
        // We do not add this decimation level to the global processing queue
        // because there might be cases in which a child class wants to add a
        // sample without registering the decimation level in the global queue.
        writeQueue.add(Pair.of(sample, estimatedSampleSize));
    }

    /**
     * <p>
     * Returns the current size of the bucket to which samples are currently
     * written (in bytes). If this information has not been initialized yet,
     * this method returns a negative number. Call
     * {@link #initializeCurrentBucketSizeAndLastSampleTimeStamp()} in order to
     * initialize this field.
     * </p>
     * 
     * <p>
     * Code calling this method has to synchronize on this decimation-level's
     * channel.
     * </p>
     * 
     * @return current size of the current bucket or a negative number if this
     *         information is not available.
     */
    protected long getCurrentBucketSize() {
        assert (Thread.holdsLock(channel));
        return currentBucketSize;
    }

    /**
     * <p>
     * Returns the start time of the current sample-bucket. The current bucket
     * is the one to which new samples are being written. The start time is
     * specified as the number of nanoseconds since epoch. If there is no sample
     * bucket yet, this method returns a negative number.
     * </p>
     * 
     * <p>
     * Code calling this method has to synchronize on this decimation-level's
     * channel.
     * </p>
     * 
     * @return start time of the current sample-bucket or a negative number if
     *         there is no sample bucket.
     */
    protected long getCurrentBucketStartTime() {
        assert (Thread.holdsLock(channel));
        return currentBucketStartTime;
    }

    /**
     * <p>
     * Returns the time stamp of the last sample that has been written. The time
     * stamp is specified as the number of nanoseconds sicne epoch. If this
     * information has not been initialized yet, this method returns a negative
     * number. Call {@link #initializeCurrentBucketSizeAndLastSampleTimeStamp()}
     * in order to initialize this field.
     * </p>
     * 
     * <p>
     * Code calling this method has to synchronize on this decimation-level's
     * channel.
     * </p>
     * 
     * @return time stamp of the last written sample or a negative number if
     *         this information is not available yet.
     */
    protected long getLastSampleTimeStamp() {
        assert (Thread.holdsLock(channel));
        return lastSampleTimeStamp;
    }

    /**
     * <p>
     * Returns the number of samples that are currently stored in the write
     * queue. This is the queue to which elements are added by calling
     * {@link #addSampleToWriteQueue(Sample, int)}.
     * </p>
     * 
     * <p>
     * Code calling this method has to synchronize on this decimation-level's
     * channel.
     * </p>
     * 
     * @return number of samples that are waiting to be written.
     */
    protected int getWriteQueueSize() {
        assert (Thread.holdsLock(channel));
        return writeQueue.size();
    }

    /**
     * <p>
     * Initializes the current bucket size and last sample time-stamp.
     * Typically, this method is called when {@link #getCurrentBucketSize()} or
     * {@link #getLastSampleTimeStamp()} return a negative number. The
     * information is initialized asynchronously so that this method does not
     * block. Instead, it returns a future that completes when the information
     * has been initialized.
     * </p>
     * 
     * <p>
     * If the information has already been initialized, this method does nothing
     * and returns a future that completes immediately. If there is no sample
     * bucket for this decimation level, the future returned by this method
     * completes immediately as well, but the information stays uninitialized.
     * </p>
     * 
     * <p>
     * Code calling this method has to synchronize on this decimation-level's
     * channel.
     * </p>
     * 
     * @return future that completes when the current bucket size and the last
     *         sample time-stamp have been initialized. If initializing this
     *         information fails, the returned future fails.
     */
    protected ListenableFuture<Void> initializeCurrentBucketSizeAndLastSampleTimeStamp() {
        assert (Thread.holdsLock(channel));
        // If the currentBucketSize and lastSampleTimeStamp have already been
        // initialized, we can return immediately. If there is no sample bucket
        // yet, we can return as well.
        if (currentBucketSize >= 0 && lastSampleTimeStamp >= 0L
                || currentBucketStartTime < 0L) {
            return Futures.immediateFuture(null);
        }
        ListenableFuture<SampleBucketState> future;
        try {
            future = channel.getControlSystemSupport().getSampleBucketState(
                    new SampleBucketId(channelConfiguration.getChannelDataId(),
                            decimationPeriodSeconds,
                            getCurrentBucketStartTime()));
            // getSampleBucketState should not return null.
            if (future == null) {
                throw new NullPointerException(
                        "The control-system support's getSampleBucketState method returned null.");
            }
        } catch (Throwable t) {
            // If getSampleBucketState throws an exception (it should not), we
            // want to put this channel into an error state because this
            // violates the contract of the ControlSystemSupport interface.
            // Then, we return a failed future because this is the way of choice
            // for aborting execution with the least side-effects.
            log.error(
                    "The \""
                            + channel.getControlSystemSupport().getId()
                            + "\" control-system support's getSampleBucketState method violated its contract: "
                            + t.getMessage(), t);
            RuntimeException e = new RuntimeException(
                    "The control-system support's getSampleBucketState method violated its contract: "
                            + t.getMessage(), t);
            channel.destroyWithException(e);
            return Futures.immediateFailedFuture(e);
        }
        Function<SampleBucketState, Void> initializeCurrentBucketSizeAndLastSampleTimeStamp = new Function<SampleBucketState, Void>() {
            @Override
            public Void apply(SampleBucketState input) {
                synchronized (channel) {
                    // If the future returns null, this is a violation of the
                    // ControlSystemSupport interface. We put this channel into
                    // an error mode and throw an exception (which will be
                    // translated into a failed future by transform) because
                    // this the way of choice for aborting the execution with
                    // the least possible side-effects.
                    if (input == null) {
                        NullPointerException e = new NullPointerException(
                                "The control-system support's getSampleBucketState method returned a future that provided a null value.");
                        log.error(
                                "The \""
                                        + channel.getControlSystemSupport()
                                                .getId()
                                        + "\" control-system support violated its contract.",
                                e);
                        throw e;
                    }
                    // Typically, this method will only be called when the
                    // bucket size and time stamp have not been initialized yet.
                    // However, we still only overwrite them if they have not
                    // been initialized so that we are safe in case of a race
                    // condition that we have not anticipated.
                    // We do not have to worry about negative values in the
                    // sample-bucket state because the constructor does not
                    // allow such values.
                    if (currentBucketSize < 0) {
                        currentBucketSize = input.getCurrentBucketSize();
                    }
                    if (lastSampleTimeStamp < 0L) {
                        lastSampleTimeStamp = input.getLatestSampleTimeStamp();
                    }
                    return null;
                }
            }
        };
        // We use the poolExecutor for running our callback because acquiring a
        // mutex in a callback processed by the same-thread executor is not a
        // good idea. We use Futures.transform because we want an error to be
        // propagated.
        return Futures.transform(future,
                initializeCurrentBucketSizeAndLastSampleTimeStamp);
    }

    /**
     * <p>
     * Tells whether there is an asynchronous remove-sample-bucket operation
     * that has been started, but has not finished yet. This information is used
     * by other decimation levels for the same channel that use this decimation
     * level as a source for generating decimated samples. If an old sample
     * bucket is currently being removed, they do not request samples from the
     * database because the data read might be inconsistent.
     * </p>
     * 
     * <p>
     * Code calling this method has to synchronize on this decimation-level's
     * channel.
     * </p>
     * 
     * @return <code>true</code> when an asynchronous remove-sample-bucket
     *         operation is currently running, <code>false</code> when no such
     *         operation has been started or when it has already finished.
     */
    protected boolean isAsynchronousRemoveSampleBucketOperationInProgress() {
        assert (Thread.holdsLock(channel));
        return waitingForAsynchronousRemoveSampleBucketOperation;
    }

    /**
     * <p>
     * Tells whether there is an asynchronous write-sample operation that has
     * been started, but has not finished yet. This information can be used by
     * child classes in order to determine whether queuing this decimation level
     * with the
     * {@link ArchivingServiceInternalImpl#addChannelDecimationLevelToWriteProcessingQueue(ArchivedChannelDecimationLevel)}
     * method makes sense at all. When an asynchronous write-sample operation is
     * in progress, queuing the decimation level does not make sense because the
     * {@link #processSampleWriteQueue()} method will return immediately anyway.
     * </p>
     * 
     * <p>
     * Code calling this method has to synchronize on this decimation-level's
     * channel.
     * </p>
     * 
     * @return <code>true</code> when an asynchronous write-sample operation is
     *         currently running, <code>false</code> when no such operation has
     *         been started or when it has already finished.
     */
    protected boolean isAsynchronousWriteSampleOperationInProgress() {
        assert (Thread.holdsLock(channel));
        return waitingForAsynchronousWriteSampleOperation;
    }

    /**
     * <p>
     * Creates a runnable that runs the specified runnable asynchronously, using
     * the {@link #poolExecutor}. This is useful when a runnable's execution is
     * supposed to be started by another executor, but it should not run within
     * this executor, e.g. because it might acquire a mutex or might run for a
     * significant period of time. In particular, this method is useful when
     * scheduling an asynchronos operation with the {@link #scheduledExecutor}.
     * </p>
     * 
     * <p>
     * This method can be called without having to synchronize on this
     * decimation-level's channel and it does not synchronize on the channel
     * either.
     * </p>
     * 
     * @param runnable
     *            runnable that shall be asynchronously executed by the
     *            {@link #poolExecutor} when the returned runnable is run. The
     *            returned runnable does not wait for the specified runnable to
     *            be executed but returns immediately after submitting the
     *            specified runnable.
     * @return runnable that executes the specified <code>runnable</code> with
     *         the {@link #poolExecutor} when being run.
     */
    protected Runnable runWithPoolExecutor(final Runnable runnable) {
        assert (runnable != null);
        // If a runnable might block (because it might wait for a lock) we do
        // not want to run it directly using the scheduledExecutor. This
        // executor only has a single thread and we might thus block other tasks
        // (even if it is only for a short amount of time). For this reason, we
        // submit tasks that acquire a lock to the poolExecutor. However, we
        // still might want to use the scheduledExecutor for such tasks in order
        // to delay them. This method generates a runnable that can be scheduled
        // with the scheduledExecutor and that executes the specified runnable
        // using the poolExecutor when run by the scheduledExecutor.
        return new Runnable() {
            @Override
            public void run() {
                poolExecutor.execute(runnable);
            }
        };
    }

    /**
     * <p>
     * Adds the specified decimation level to the list of target decimation
     * levels. The target decimation levels are the ones that use this
     * decimation level as the source for generating decimated samples. This
     * method is only intended for the {@link ArchivedChannel} to which this
     * decimation level belongs, so that it can create links between the
     * decimation levels that depend on each other.
     * </p>
     * 
     * <p>
     * Code calling this method has to synchronize on this decimation-level's
     * channel.
     * </p>
     * 
     * @param targetDecimationLevel
     *            decimation level for the same channel that uses this
     *            decimation level as its source for generating decimated
     *            samples.
     */
    void addTargetDecimationLevel(
            ArchivedChannelDecimatedSamplesDecimationLevel<SampleType> targetDecimationLevel) {
        assert (Thread.holdsLock(channel));
        assert (targetDecimationLevel != null);
        targetDecimationLevels.add(targetDecimationLevel);
    }

    /**
     * <p>
     * Creates a new sample bucket for this decimation level. This method must
     * only be called by this decimation-level's channel's
     * {@link ArchivedChannel#createSampleBucket(ArchivedChannelDecimationLevel, long)
     * createSampleBucket} method because that method is responsible for
     * ensuring that such operations are not run concurrently and a consistent
     * state of the channel's meta-data is ensured.
     * </p>
     * 
     * <p>
     * Code calling this method has to synchronize on this decimation-level's
     * channel.
     * </p>
     * 
     * @param newBucketStartTime
     *            start time of the sample bucket to be created. This must be
     *            strictly greater than the current's sample-bucket start time.
     * @return future that completes when the sample bucket has been created in
     *         the database or fails when the database operation has failed.
     */
    ListenableFuture<Void> createSampleBucket(final long newBucketStartTime) {
        assert (Thread.holdsLock(channel));
        assert (newBucketStartTime >= 0L);
        assert (newBucketStartTime > currentBucketStartTime);
        ListenableFuture<Void> createSampleBucketFuture = channelMetaDataDAO
                .createSampleBucket(channelConfiguration.getChannelName(),
                        decimationPeriodSeconds, newBucketStartTime,
                        Long.MAX_VALUE,
                        currentBucketStartTime >= 0L ? currentBucketStartTime
                                : null, true, thisServerId);
        Function<Void, Void> updateDecimationLevelState = new Function<Void, Void>() {
            @Override
            public Void apply(Void input) {
                synchronized (channel) {
                    // If this is the first sample bucket that was created, we
                    // should also add it to the list of "oldest" sample
                    // buckets. This saves us the trouble of making a query
                    // later. If this is not the first sample bucket, we have to
                    // check whether the preceding sample bucket is in the list
                    // of the "oldest" sample buckets and update its end time.
                    if (oldestSampleBuckets.isEmpty()) {
                        if (currentBucketStartTime < 0L) {
                            oldestSampleBuckets
                                    .add(new SampleBucketInformation(
                                            Long.MAX_VALUE, newBucketStartTime,
                                            channelConfiguration
                                                    .getChannelDataId(),
                                            channelConfiguration
                                                    .getChannelName(),
                                            decimationPeriodSeconds));
                        }
                    } else {
                        SampleBucketInformation newestOldestSampleBucket = oldestSampleBuckets
                                .getLast();
                        if (newestOldestSampleBucket.getBucketStartTime() == currentBucketStartTime) {
                            oldestSampleBuckets.removeLast();
                            oldestSampleBuckets
                                    .add(new SampleBucketInformation(
                                            newBucketStartTime - 1L,
                                            currentBucketStartTime,
                                            channelConfiguration
                                                    .getChannelDataId(),
                                            channelConfiguration
                                                    .getChannelName(),
                                            decimationPeriodSeconds));
                        }
                    }
                    currentBucketSize = 0;
                    currentBucketStartTime = newBucketStartTime;
                }
                return null;
            }
        };
        return Futures.transform(createSampleBucketFuture,
                updateDecimationLevelState);
    }

    private void addToGlobalWriteProcessingQueueIfNeeded() {
        assert (Thread.holdsLock(channel));
        // We only queue the channel if there is a write request and there is no
        // pending asynchronous operation. We could queue the channel when there
        // is an asynchronous operation, but the write requests would not be
        // processed anyway before the asynchronous operation has finished, so
        // we can save the overhead.
        if (!writeQueue.isEmpty()
                && !waitingForAsynchronousWriteSampleOperation) {
            archivingService
                    .addChannelDecimationLevelToWriteProcessingQueue(this);
        }
    }

    private void removeOldSampleBuckets() {
        assert (Thread.holdsLock(channel));
        // When the retention period is zero, samples are supposed to be kept
        // indefinitely, so we do not have to do anything.
        if (retentionPeriodNanoseconds == 0L) {
            return;
        }
        // If we are waiting for an asynchronous remove operation, we do not
        // want to run this method again until this operation has finished.
        if (waitingForAsynchronousRemoveSampleBucketOperation) {
            return;
        }
        // When the channel has been destroyed, we do not want to run any
        // more tasks for it.
        if (channel.getState().equals(ArchivedChannelState.DESTROYED)) {
            return;
        }
        // We do not want to remove a sample bucket while a decimation process
        // is reading samples from this decimation level. This could lead to the
        // read samples being inconsistent (some still being read while others
        // are already missing), which is something we want to avoid.
        // If the target decimation level did not complete the first query yet,
        // we do not want to start removing samples either. will happen if the
        // decimation process has not started yet for some reason. If we removed
        // samples now, the decimation process would not be able to use them
        // later. We can still remove these samples when the decimation process
        // has finished its first run.
        // We can do this check here because the
        // waitingForAsynchronousRemoveSampleBucketOperation flag is only set
        // directly by this method. When this flag is set, the decimation
        // process is not started.
        for (ArchivedChannelDecimatedSamplesDecimationLevel<SampleType> targetDecimationLevel : targetDecimationLevels) {
            // We use the regular (long) delay before making the next attempt.
            // We could use a shorter delay, but then we would need to keep
            // track of whether we already scheduled the next run in order to
            // avoid scheduling many runs. As removing old samples is not really
            // time-critical, always using a long delay seems appropriate.
            if (!targetDecimationLevel.isSourceSampleQueryComplete()) {
                removeOldSampleBucketsReschedule();
                return;
            }
        }
        // If the list storing the oldest sample buckets is empty, we first have
        // to fill it.
        if (oldestSampleBuckets.isEmpty()) {
            // If there is not even a current sample-bucket, there is no sense
            // in checking for older buckets.
            if (currentBucketStartTime < 0L) {
                return;
            }
            final ListenableFuture<ObjectResultSet<SampleBucketInformation>> future = channelMetaDataDAO
                    .getSampleBucketsNewerThan(
                            channelConfiguration.getChannelName(),
                            decimationPeriodSeconds, 0L, 5);
            // We do not want to make this query again until we got a response.
            waitingForAsynchronousRemoveSampleBucketOperation = true;
            // Our callback acquires the channel mutex, so we use our pool
            // executor instead of the same-thread executor.
            future.addListener(new Runnable() {
                @Override
                public void run() {
                    synchronized (channel) {
                        try {
                            ObjectResultSet<SampleBucketInformation> resultSet = FutureUtils
                                    .getUnchecked(future);
                            removeOldSampleBucketsProcessQueryResults(resultSet);
                        } catch (Throwable t) {
                            waitingForAsynchronousRemoveSampleBucketOperation = false;
                            removeOldSampleBucketsReschedule();
                        }
                    }
                }
            }, poolExecutor);
            return;
        }
        // If we do not know the time-stamp of the latest sample yet, we have to
        // determine it. When this method is called from the write thread, we
        // will have a time stamp. However, if it is called at some point in
        // time after the channel has been initialized, but no sample has been
        // written yet, we will not know it yet. Unlike the write thread, we do
        // not take any special precautions against this code being run twice:
        // This method is only scheduled to be run once or it is called by the
        // write thread. When it is called by the write thread, we already have
        // the necessary information, so we will never query the sample-bucket
        // state more than once. It is possible that a concurrent second query
        // is made by the write thread. For this reason, we only use the result
        // if the last sample time-stamp (and current bucket size) has not been
        // set by a different thread yet.
        if (lastSampleTimeStamp < 0L) {
            final ListenableFuture<Void> future = initializeCurrentBucketSizeAndLastSampleTimeStamp();
            // We acquire the channel mutex from the callback, so we cannot
            // safely execute it using the same-thread executor.
            future.addListener(new Runnable() {
                @Override
                public void run() {
                    synchronized (channel) {
                        try {
                            FutureUtils.getUnchecked(future);
                            // If we were successfull we can proceed right away.
                            removeOldSampleBuckets();
                        } catch (Throwable t) {
                            // If there was an error, we try again after some
                            // time has passed.
                            removeOldSampleBucketsReschedule();
                        }
                    }
                }
            }, poolExecutor);
            return;
        }
        // If we got down here, we know that the list of oldest sample buckets
        // is not empty and that we have the lastSampleTimeStamp (but it might
        // still be zero).
        // If the lastSampleTimeStamp is zero, we have to use the
        // currentBucketStartTime. Such a situation can happen if the current
        // bucket does not contain any samples. This situation is unlikely, but
        // it can appear if the server is stopped after a new sample bucket has
        // been created, but before the first sample has been written. Using the
        // time-stamp of the sample bucket makes sense for two reasons: First,
        // any sample that is going to be written will have a time-stamp greater
        // than or equal to the start time of the sample bucket. Second, it
        // means that enough time has passed that there has been one sample with
        // the time-stamp of the sample bucket and thus it should be okay to
        // delete the old data.
        long mostRecentTimeStamp = (lastSampleTimeStamp > 0L) ? lastSampleTimeStamp
                : currentBucketStartTime;
        // If the time for deleting the sample bucket has not come yet, we are
        // done. We use subtraction here because this is safer when the time
        // stamps get close to Long.MAX_VALUE.
        if (mostRecentTimeStamp - retentionPeriodNanoseconds <= oldestSampleBuckets
                .getFirst().getBucketEndTime()) {
            return;
        }
        final SampleBucketInformation oldestSampleBucket = oldestSampleBuckets
                .getFirst();
        // We never delete the current sample bucket. This should not happen
        // anyway because the current sample bucket should have an end time that
        // is the maximum possible long value, but this extra safety check makes
        // sense because deleting the current sample bucket could wrack havoc.
        if (oldestSampleBucket.getBucketStartTime() == currentBucketStartTime) {
            return;
        }
        // We first delete the actual samples and then delete the bucket
        // meta-data. As we run these operations asynchronously, there is a
        // small chance that the same bucket is deleted twice when the channel
        // is destroyed and recreated. However, unlike creating a bucket twice,
        // this is not dangerous, so we do not take any precautions.
        // We can safely execute the transformation using the same-thread
        // executor because the transformation function does not acquire the
        // channel mutex.
        final ListenableFuture<Void> future;
        try {
            future = Futures.transform(channel.getControlSystemSupport()
                    .deleteSamples(oldestSampleBucket.getBucketId()),
                    new AsyncFunction<Void, Void>() {
                        @Override
                        public ListenableFuture<Void> apply(Void input)
                                throws Exception {
                            // We only read immutable fields, so we do not have
                            // to acquire the channel mutex.
                            return channelMetaDataDAO.deleteSampleBucket(
                                    channelConfiguration.getChannelName(),
                                    decimationPeriodSeconds,
                                    oldestSampleBucket.getBucketStartTime(),
                                    false, null);
                        }
                    });
            // deleteSampleBuckets should not return null either.
            if (future == null) {
                throw new NullPointerException(
                        "The control-system support's deleteSamples method returned null.");
            }
        } catch (Throwable t) {
            // If deleteSamples throws an exception (it should not), we want to
            // put this channel into an error state because this violates the
            // contract of the ControlSystemSupport interface. Then, we throw an
            // exception because this is the method of choice for aborting
            // execution with the least undesired side-effects.
            RuntimeException e = new RuntimeException(
                    "The control-system support's deleteSamples method violated its contract: "
                            + t.getMessage());
            channel.destroyWithException(e);
            throw e;
        }
        // We want to remember that we are waiting for a sample bucket to be
        // deleted. This protects from running the same code again and again
        // when new samples are written and thus this method is called again and
        // again.
        waitingForAsynchronousRemoveSampleBucketOperation = true;
        // When the sample bucket has been deleted, we want to remove it from
        // the list and check whether the next bucket can be deleted. If the
        // deletion failed for some reason, we want to try the operation again.
        // We cannot use the same-thread executor because we have to acquire the
        // channel mutex from the callback.
        future.addListener(new Runnable() {
            @Override
            public void run() {
                synchronized (channel) {
                    // The asynchronous operation has finished (either because
                    // it failed or because it succeeded). Anyway, we now want
                    // the surrounding method to run again.
                    waitingForAsynchronousRemoveSampleBucketOperation = false;
                    try {
                        FutureUtils.getUnchecked(future);
                        // We deleted the first sample bucket, so we should
                        // remove it from the list. We also reset the
                        // deleteNextBucketAfterSampleTimeStamp so that it is
                        // calculated again based on the next oldest bucket.
                        oldestSampleBuckets.removeFirst();
                        // We also want to call the surrounding method again
                        // because there might be more sample buckets that we
                        // can remove.
                        removeOldSampleBuckets();
                    } catch (Throwable t) {
                        removeOldSampleBucketsReschedule();
                    }
                }
            }
        }, poolExecutor);
    }

    private void removeOldSampleBucketsProcessQueryResults(
            final ObjectResultSet<SampleBucketInformation> resultSet) {
        assert (Thread.holdsLock(channel));
        assert (resultSet != null);
        // This method is called when we have received the information about the
        // oldest sample buckets. Due to paging, the result might be incomplete
        // and we might have to call this method again until we fetch all
        // records. Typically, this will already have happened because the
        // number of records that we requested is very small, but we still have
        // to handle the case where there is more network I/O and we do not want
        // to block in this case.
        if (!resultSet.isFullyFetched()) {
            final ListenableFuture<Void> future = resultSet.fetchMoreResults();
            // Our callback might acquire a mutex, so we use our pool executor
            // instead of the same-thread executor.
            future.addListener(new Runnable() {
                @Override
                public void run() {
                    synchronized (channel) {
                        try {
                            FutureUtils.getUnchecked(future);
                        } catch (Throwable t) {
                            // We have to reset the waiting flag so that
                            // removeOldSampleBuckets can actually run when its
                            // scheduled time has come.
                            waitingForAsynchronousRemoveSampleBucketOperation = false;
                            removeOldSampleBucketsReschedule();
                            return;
                        }
                        removeOldSampleBucketsProcessQueryResults(resultSet);
                    }
                }
            }, poolExecutor);
            return;
        }
        // The asynchronous operation has finished, so removeOldSampleBuckets
        // should be able to run again.
        waitingForAsynchronousRemoveSampleBucketOperation = false;
        // When the channel has been destroyed, we do not want to run any
        // more tasks for it.
        if (channel.getState().equals(ArchivedChannelState.DESTROYED)) {
            return;
        }
        // Before adding the sample buckets, the list should be empty. When
        // initially there were no sample buckets, the write thread might have
        // added the first sample bucket to the list and our query might have
        // returned the same sample bucket asynchronously. In this case, we
        // simply disregard the results from the query.
        if (oldestSampleBuckets.isEmpty()) {
            oldestSampleBuckets.addAll(resultSet.all());
            // If the list is still empty, there are no sample buckets and we
            // are done. Typically, this should not happen because we only
            // search for sample buckets when the currentBucketStartTime is set
            // which means there should be at least one sample bucket. We still
            // check for this condition because not doing so would cause an
            // infinite loop when for some reason the currentBucketStartTime is
            // set, but there query still returns no sample buckets.
            if (oldestSampleBuckets.isEmpty()) {
                return;
            }
        }
        removeOldSampleBuckets();
    }

    private void removeOldSampleBucketsReschedule() {
        assert (Thread.holdsLock(channel));
        // This method is called when an asynchronous operation that is part of
        // the removal process for old sample buckets failed. If this failure
        // was just caused by a temporary hiccup, running the operation again
        // (after a short delay) should succeed. If there is a persistent
        // problem, it is most likely caused by the database being unavailable.
        // In this case, the server will go offline and the channel is going to
        // be destroyed, so that this will break the retry cycle.
        // There might be situations in which this method is called repeatedly
        // before the scheduled run has actually happened. In these situations,
        // we do not want to schedule more runs because this would only make the
        // executor's queue grow bigger and bigger.
        if (removeOldSampleBucketsScheduled) {
            return;
        }
        removeOldSampleBucketsScheduled = true;
        scheduledExecutor.schedule(runWithPoolExecutor(new Runnable() {
            @Override
            public void run() {
                synchronized (channel) {
                    removeOldSampleBucketsScheduled = false;
                    removeOldSampleBuckets();
                }
            }
        }), 30000L, TimeUnit.MILLISECONDS);
    }

}
