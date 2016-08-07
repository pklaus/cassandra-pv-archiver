/*
 * Copyright 2015-2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.archiving.internal;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.commons.lang3.StringEscapeUtils;

import com.aquenos.cassandra.pvarchiver.controlsystem.ControlSystemChannel;
import com.aquenos.cassandra.pvarchiver.controlsystem.Sample;
import com.aquenos.cassandra.pvarchiver.controlsystem.SampleListener;
import com.aquenos.cassandra.pvarchiver.server.archiving.ArchivingService;
import com.aquenos.cassandra.pvarchiver.server.database.ChannelMetaDataDAO;
import com.google.common.base.Preconditions;

/**
 * <p>
 * Decimation level storing the raw samples of an {@link ArchivedChannel}. This
 * class is intended to be used by the {@link ArchivingServiceInternalImpl} and
 * {@link ArchivedChannel} classes only. For this reason, it has been marked as
 * package private.
 * </p>
 * 
 * <p>
 * It is expected that there is only one instance of this class for each
 * {@link ArchivedChannel}, because there is only one decimation level storing
 * raw samples.
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
class ArchivedChannelRawSamplesDecimationLevel<SampleType extends Sample>
        extends ArchivedChannelDecimationLevel<SampleType> {

    /**
     * Encapsulates the request for writing a sample. Every time the
     * {@link SampleListenerImpl} is notified of a new sample, it creates an
     * object of this type and adds it to the queue of the respective channel so
     * that it can later be processed by
     * {@link ArchivingService#processSampleWriteQueues()}.
     * 
     * @author Sebastian Marsching
     */
    private static class WriteRawSampleRequest<SampleType extends Sample> {
        public ControlSystemChannel controlSystemChannel;
        public int estimatedSampleSize;
        public SampleType sample;
    }

    private final TimeBoundedQueue<WriteRawSampleRequest<SampleType>> rawSampleQueue;
    // This field must only be read or modified while synchronizing on the
    // rawSampleQueue.
    private long rawSamplesDropped;
    private final SampleListener<SampleType> sampleListener = new SampleListener<SampleType>() {
        @Override
        public void onSampleReceived(ControlSystemChannel csChannel,
                SampleType sample, int estimatedSampleSize) {
            // The called method checks that the preconditions hold, so we do
            // not have to do this here.
            addSampleToRawSampleQueue(sample, estimatedSampleSize, csChannel);
        }
    };

    /**
     * Creates an object representing the decimation level storing raw samples
     * for the specified <code>channel</code>. This constructor can safely be
     * used <em>without</em> synchronizing on the <code>channel</code>. It
     * should only be used by the {@link ArchivedChannel} instance to which the
     * created decimation level belongs.
     * 
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
    ArchivedChannelRawSamplesDecimationLevel(int retentionPeriodSeconds,
            long currentBucketStartTime,
            ArchivingServiceInternalImpl archivingService,
            ArchivedChannel<SampleType> channel,
            ChannelMetaDataDAO channelMetaDataDAO,
            ExecutorService poolExecutor,
            ScheduledExecutorService scheduledExecutor, UUID thisServerId) {
        super(0, retentionPeriodSeconds, currentBucketStartTime,
                archivingService, channel, channelMetaDataDAO, poolExecutor,
                scheduledExecutor, thisServerId);
        this.rawSampleQueue = new TimeBoundedQueue<WriteRawSampleRequest<SampleType>>(
                MAX_QUEUE_TIME_MILLISECONDS);
    }

    /**
     * <p>
     * Returns the sample listener that accepts raw samples for this channel.
     * The sample listener takes care of adding these samples to the queue that
     * is going to be processed by this decimation level's
     * {@link #processSampleWriteQueue()} method and making sure that this
     * method is going to be called by the {@link ArchivingServiceInternalImpl}.
     * </p>
     * 
     * <p>
     * This method can safely be called without synchronizing on this decimation
     * level's channel and does not synchronize on the channel either.
     * </p>
     * 
     * @return sample listener that accepts samples for this decimation level.
     */
    public SampleListener<SampleType> getSampleListener() {
        return sampleListener;
    }

    @Override
    public void processSampleWriteQueue() {
        synchronized (channel) {
            switch (channel.getState()) {
            case DESTROYED:
                // If the channel has been destroyed, we do not want to write
                // any more samples. However, we still want to drain the queue.
                synchronized (rawSampleQueue) {
                    rawSampleQueue.clear();
                }
                return;
            case INITIALIZED:
                // If the write queue is empty, we take a sample from the raw
                // queue and move it to the write queue. We also update the
                // counter that keeps track of the number of samples dropped.
                synchronized (rawSampleQueue) {
                    // If the write queue has less than two elements, we add a
                    // sample from the raw samples queue (if there is one).
                    // We add two samples because the processSampleWriteQueue
                    // method of the parent class will never process more than
                    // one sample at once. However, adding one sample is not
                    // sufficient because after processing that sample, the
                    // write queue would be empty and thus this decimation level
                    // would not be queued again.
                    // We do not simply queue all raw samples because the
                    // writeQueue is unbounded and thus it would grow larger and
                    // larger if samples were queued more quickly than they can
                    // be written.
                    while (getWriteQueueSize() < 2) {
                        WriteRawSampleRequest<SampleType> rawWriteRequest = rawSampleQueue
                                .poll();
                        // If there is a raw sample, we process it. Otherwise,
                        // we are done here and can delegate to the
                        // implementation in the super class.
                        if (rawWriteRequest != null) {
                            // The control-system channel should always match
                            // the expected one because we give each channel a
                            // separate SampleListener. If they do not match
                            // this indicates an error in the control-system
                            // support. In such a case, we log the error and
                            // skip the sample.
                            if (rawWriteRequest.controlSystemChannel != channel
                                    .getControlSystemChannel()) {
                                log.error("The \""
                                        + channel.getControlSystemSupport()
                                                .getId()
                                        + "\" control-system support sent a sample for channel \""
                                        + StringEscapeUtils.escapeJava(rawWriteRequest.controlSystemChannel
                                                .getChannelName()
                                                + "\" to the sample listener for channel \""
                                                + channel.getConfiguration()
                                                        .getChannelName()
                                                + "\"."));
                                continue;
                            }
                            addSampleToWriteQueue(rawWriteRequest.sample,
                                    rawWriteRequest.estimatedSampleSize);
                        } else {
                            break;
                        }
                    }
                    channel.incrementSamplesDropped(rawSamplesDropped);
                    rawSamplesDropped = 0;
                }
                super.processSampleWriteQueue();
                return;
            case INITIALIZING:
                // If the channel is still initializing, we cannot write any
                // samples yet. However, we can use this chance to update the
                // counter that takes track of the number of dropped samples.
                synchronized (rawSampleQueue) {
                    channel.incrementSamplesDropped(rawSamplesDropped);
                    rawSamplesDropped = 0;
                }
                return;
            default:
                throw new RuntimeException("Unhandled channel state: "
                        + channel.getState());

            }
        }
    }

    private void addSampleToRawSampleQueue(SampleType sample,
            int estimatedSampleSize, ControlSystemChannel csChannel) {
        // This code is called by the control-system support, so using
        // preconditions instead of assertions seems in order.
        Preconditions.checkNotNull(csChannel,
                "The control-system channel must not be null.");
        Preconditions.checkNotNull(sample, "The sample must not be null.");
        Preconditions.checkArgument(estimatedSampleSize >= 0,
                "The estimated sample size must not be negative.");
        // This method is only called by the sample listener, so it is only used
        // for queuing raw samples. We do not want to acquire the channel mutex
        // because doing so might lead to a dead lock if the
        // control-system-specific code that notifies the sample listener (and
        // thus calls this method) holds a mutex that is also acquired when we
        // call one of the control-system support's methods while holding the
        // channel mutex.
        // For this reason, we do not synchronize on the channel, but rather on
        // the rawSampleQueue. This lock is never hold while calling any of the
        // control-system support's methods, so there is no risk of a dead lock.
        synchronized (rawSampleQueue) {
            // It would be nice if we could check here whether the channel has
            // been destroyed and skip queuing the sample. However, we would
            // have to acquire the channel mutex and this would undermine the
            // goal of avoiding a dead lock.
            WriteRawSampleRequest<SampleType> newWriteRequest = new WriteRawSampleRequest<SampleType>();
            newWriteRequest.controlSystemChannel = csChannel;
            newWriteRequest.estimatedSampleSize = estimatedSampleSize;
            newWriteRequest.sample = sample;
            this.rawSampleQueue.add(newWriteRequest);
            int numberOfSamplesDropped = this.rawSampleQueue
                    .getAndResetOverflowCount();
            // We cannot increment the number of samples dropped directly
            // because we would have to acquire the channel mutex. Instead, we
            // remember the number of samples dropped and increment the counter
            // when we acquire the channel mutex in order to process the
            // samples.
            this.rawSamplesDropped += numberOfSamplesDropped;
            // It would be optimal if we only queued this decimation level for
            // processing if the channel was initialized. However, we would
            // again need to acquire the channel mutex in order to check this.
            // For this reason, we always queue the decimation level and check
            // the channel's state when it is processed.
            archivingService
                    .addChannelDecimationLevelToWriteProcessingQueue(this);
        }
    }

}
