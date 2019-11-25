/*
 * Copyright 2015-2019 aquenos GmbH.
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
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringEscapeUtils;

import com.aquenos.cassandra.pvarchiver.controlsystem.Sample;
import com.aquenos.cassandra.pvarchiver.controlsystem.SampleDecimator;
import com.aquenos.cassandra.pvarchiver.server.archiving.TimeStampLimitMode;
import com.aquenos.cassandra.pvarchiver.server.archiving.internal.ThrottledArchiveAccessService.SkippableObjectResultSet;
import com.aquenos.cassandra.pvarchiver.server.database.ChannelMetaDataDAO;
import com.aquenos.cassandra.pvarchiver.server.util.FutureUtils;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

/**
 * <p>
 * Decimation level storing decimated samples for a certain decimation period of
 * an {@link ArchivedChannel}. This class is intended to be used by the
 * {@link ArchivingServiceInternalImpl} and {@link ArchivedChannel} classes
 * only. For this reason, it has been marked as package private.
 * </p>
 * 
 * <p>
 * It is expected that there is exactly one instance of this class for each
 * decimation level of the {@link ArchivedChannel}, except for the ones storing
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
class ArchivedChannelDecimatedSamplesDecimationLevel<SampleType extends Sample>
        extends ArchivedChannelDecimationLevel<SampleType> {

    private final long decimationPeriodNanoseconds;
    private boolean fatalErrorOccurred;
    private SettableFuture<Void> generateDecimatedSamplesProcessSourceSampleFuture;
    private boolean generateDecimatedSamplesScheduled;
    private boolean initialProcessingOfTargetDecimationLevelsFinished;
    private SampleType lastSourceSample;
    private Runnable onWriteQueueEmptyRunnable;
    private SampleDecimator<SampleType> sampleDecimator;
    private ArchivedChannelDecimationLevel<SampleType> sourceDecimationLevel;
    private final int sourceDecimationPeriodSeconds;
    private boolean sourceSampleQueryComplete;
    private boolean sourceSampleQueryStarted;
    private TimeBoundedQueue<SampleType> sourceSampleQueue = new TimeBoundedQueue<SampleType>(
            MAX_QUEUE_TIME_MILLISECONDS);
    private final ThrottledArchiveAccessService throttledArchiveAccessService;
    private boolean waitingForAsynchronousDecimationOperation;

    /**
     * Creates an object representing the decimation level for the specified
     * decimation period and <code>channel</code>. This constructor can safely
     * be used <em>without</em> synchronizing on the <code>channel</code>. It
     * should only be used by the {@link ArchivedChannel} instance to which the
     * created decimation level belongs.
     * 
     * @param decimationPeriodSeconds
     *            decimation period of this decimation level (in seconds). Must
     *            be strictly positive.
     * @param retentionPeriodSeconds
     *            retention period of this decimation level (in seconds). Must
     *            be zero if samples are supposed to be kept indefinitely. Must
     *            not be negative.
     * @param sourceDecimationPeriodSeconds
     *            decimation period (in seconds) of the decimation level that
     *            acts as the source when generating samples for this decimation
     *            level. This must be less than
     *            <code>decimationPeriodSeconds</code> and must be an integer
     *            fraction of <code>decimationPeriodSeconds</code> or zero if
     *            raw samples are supposed to be used.
     * @param currentBucketStartTime
     *            start time of the most recent sample buckets (represented as
     *            the number of nanoseconds since epoch). If this decimation
     *            level does not have any sample buckets yet, this number must
     *            be minus one. Must not be less than minus one.
     * @param throttledArchiveAccessService
     *            archive access service that is used for reading already
     *            written samples when generating decimated samples.
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
    ArchivedChannelDecimatedSamplesDecimationLevel(int decimationPeriodSeconds,
            int retentionPeriodSeconds, int sourceDecimationPeriodSeconds,
            long currentBucketStartTime,
            ThrottledArchiveAccessService throttledArchiveAccessService,
            ArchivingServiceInternalImpl archivingService,
            ArchivedChannel<SampleType> channel,
            ChannelMetaDataDAO channelMetaDataDAO, ExecutorService poolExecutor,
            ScheduledExecutorService scheduledExecutor, UUID thisServerId) {
        super(decimationPeriodSeconds, retentionPeriodSeconds,
                currentBucketStartTime, archivingService, channel,
                channelMetaDataDAO, poolExecutor, scheduledExecutor,
                thisServerId);
        // This class must not be used for raw samples.
        assert (decimationPeriodSeconds > 0);
        // The source decimation period might be zero or positive.
        assert (sourceDecimationPeriodSeconds >= 0);
        // The source decimation period must be less than this decimation
        // level's decimation period.
        assert (sourceDecimationPeriodSeconds < decimationPeriodSeconds);
        // The source decimation period must be well-aligned with this
        // decimation level's decimation period, unless the source decimation
        // period is zero because raw samples are used as the source.
        assert (sourceDecimationPeriodSeconds == 0 || (decimationPeriodSeconds
                % sourceDecimationPeriodSeconds == 0));
        assert (throttledArchiveAccessService != null);
        this.decimationPeriodNanoseconds = decimationPeriodSeconds
                * 1000000000L;
        this.sourceDecimationPeriodSeconds = sourceDecimationPeriodSeconds;
        this.throttledArchiveAccessService = throttledArchiveAccessService;
    }

    /**
     * <p>
     * Schedules generation of decimated samples. This method should be called
     * once by the {@link ArchivedChannel} after the control-system support has
     * been initialized. Later runs are scheduled automatically (if needed).
     * </p>
     *
     * <p>
     * This method may be called without synchronizing on this
     * decimation-level's channel and does not synchronize on the channel either
     * because it does not modify any fields or read mutable fields.
     * </p>
     */
    public void scheduleGenerateDecimatedSamples() {
        poolExecutor.execute(new Runnable() {
            @Override
            public void run() {
                synchronized (channel) {
                    generateDecimatedSamples();
                }
            }
        });
    }

    /**
     * <p>
     * Called when a sample has been written and as a result the write queue is
     * now empty. This event can be used by child classes in order to schedule
     * operations that were postponed because the write queue was growing too
     * large.
     * </p>
     * <p>
     * This method is called while holding the channel mutex, so implementations
     * must not perform any blocking operations.
     * </p>
     * <p>
     * This implementation schedules an invocation of
     * {@link #generateDecimatedSamplesProcessSourceSample(Sample)} if a
     * previous invocation of that method has been interrupted because the write
     * queue was growing too large.
     * </p>
     */
    @Override
    protected void onWriteQueueEmpty() {
        assert (Thread.holdsLock(channel));
        // If the runnable has been registered, we schedule it for execution and
        // unregister it so that it will not be run again. If it needs to be run
        // again, it will automatically be registered again when it is run.
        Runnable runnable;
        runnable = onWriteQueueEmptyRunnable;
        onWriteQueueEmptyRunnable = null;
        if (runnable != null) {
            poolExecutor.execute(runnable);
        }
    }

    /**
     * <p>
     * Processes the specified source sample. This method is called by the
     * {@link ArchivedChannelDecimationLevel} that acts as the source for this
     * decimation level when it has successfully written a sample, so that this
     * decimation level can process this sample without having to query the
     * database. This method does not block and can thus be called directly from
     * the write callback.
     * </p>
     *
     * <p>
     * Code calling this method has to synchronize on this decimation-level's
     * channel.
     * </p>
     * 
     * @param sample
     *            sample that has been successfully written to the source
     *            decimation-level and should now be processed.
     */
    protected void processSourceSample(SampleType sample) {
        assert (Thread.holdsLock(channel));
        assert (sample != null);
        // If the query for source samples has not even been started yet, there
        // is no need to start queuing samples from the source decimation level.
        // These samples will be returned as part of the query. Not actually
        // queuing them saves memory in a situation where starting the query
        // is delayed significantly (typically because we are waiting for one of
        // the target decimation levels to be finished with its source sample
        // query).
        if (!sourceSampleQueryStarted) {
            return;
        }
        sourceSampleQueue.add(sample);
        generateDecimatedSamples();
    }

    /**
     * <p>
     * Returns the decimation period of this decimation level (in seconds).
     * </p>
     * 
     * <p>
     * This method may be called without synchronizing on this
     * decimation-level's channel and does not synchronize on the channel either
     * because it does not modify any fields or read mutable fields.
     * </p>
     * 
     * @return decimation period of this decimation level.
     */
    int getSourceDecimationPeriodSeconds() {
        return sourceDecimationPeriodSeconds;
    }

    /**
     * Tells whether the query for source samples has completed. This method
     * also returns <code>true</code> if the query did not complete, but there
     * has been a fatal error and thus it will never complete. Basically, this
     * method tells whether the generation process for this decimation level is
     * still processing data from the database or has finished this processing
     * (either successfully or because of a permanent error).
     * 
     * @return <code>true</code> if the processing of source samples from the
     *         database is still running, <code>false</code> if it has finished.
     */
    boolean isSourceSampleQueryComplete() {
        assert (Thread.holdsLock(channel));
        return sourceSampleQueryComplete;
    }

    /**
     * <p>
     * Sets the source decimation level for this decimation period. This method
     * is called by the {@link ArchivedChannel} to which this decimation level
     * belongs after creating all decimation levels (but before initializing
     * them). This decimation level uses this information to check on the status
     * of the source decimation level when performing certain operations (e.g.
     * looking for source samples).
     * </p>
     * 
     * <p>
     * Code calling this method has to synchronize on this decimation-level's
     * channel.
     * </p>
     * 
     * @param sourceDecimationLevel
     *            decimation level that acts as a source for generating
     *            decimated samples for this decimation level.
     */
    void setSourceDecimationLevel(
            ArchivedChannelDecimationLevel<SampleType> sourceDecimationLevel) {
        assert (Thread.holdsLock(channel));
        assert (sourceDecimationLevel != null);
        // The source decimation Level should only be set once.
        assert (this.sourceDecimationLevel == null);
        // The decimation period of the source decimation level should match the
        // expected decimation period.
        assert (this.sourceDecimationPeriodSeconds == sourceDecimationLevel
                .getDecimationPeriodSeconds());
        this.sourceDecimationLevel = sourceDecimationLevel;
    }

    private void generateDecimatedSamples() {
        assert (Thread.holdsLock(channel));
        // If we have exceeded the allowed range of the time-stamp of the new
        // decimated sample or there has been another permanent error, we do not
        // want to generate any decimated samples any longer.
        if (fatalErrorOccurred) {
            // We still want to drain the queue. If we did not drain the queue,
            // there would still not be a memory leak because the queue is
            // bounded. However by draining the queue, we can free this memory
            // earlier.
            sourceSampleQueue.clear();
            return;
        }
        // When we are waiting for an asynchronous operation to complete, we do
        // not want to run this method while we are waiting. This method will be
        // called again when the asynchronous operation has finished.
        if (waitingForAsynchronousDecimationOperation) {
            return;
        }
        // If the channel has been destroyed, there is no sense in generating
        // more decimated samples because they will not be written anyway.
        if (channel.getState().equals(ArchivedChannelState.DESTROYED)) {
            return;
        }
        // If this is the first time that we try to generate decimated samples
        // since the channel has been initialized, we first have to find out
        // with which source sample we should start.
        if (lastSourceSample == null) {
            // If we already made a query but this query did not return any
            // samples, we can directly continue with the samples from the
            // queue. We cannot do this if the queue has the overflow flag set
            // because we have to make another query in order to ensure that we
            // get the lost samples.
            if (sourceSampleQueryComplete
                    && sourceSampleQueue.getOverflowCount() == 0) {
                // If the queue does not have a sample either, there is nothing
                // left to do and we can return. Otherwise, we process the first
                // sample and call this method again to process more samples.
                if (sourceSampleQueue.isEmpty()) {
                    return;
                } else {
                    SampleType nextSourceSample = sourceSampleQueue.poll();
                    ListenableFuture<Void> processSourceSampleFuture;
                    try {
                        processSourceSampleFuture = generateDecimatedSamplesProcessSourceSample(
                                nextSourceSample);
                    } catch (Throwable t) {
                        generateDecimatedSamplesProcessSourceSampleHandleError(
                                t);
                        return;
                    }
                    // If the operation finished synchronously (the default),
                    // the return value is null. Otherwise, we call
                    // generateDecimatedSamples() again when the asynchronous
                    // operation has finished.
                    if (processSourceSampleFuture == null) {
                        generateDecimatedSamples();
                    } else {
                        // The generateDecimatedSamples() method must not run
                        // until the asynchronous operation has finished.
                        waitingForAsynchronousDecimationOperation = true;
                        processSourceSampleFuture.addListener(new Runnable() {
                            @Override
                            public void run() {
                                synchronized (channel) {
                                    waitingForAsynchronousDecimationOperation = false;
                                    generateDecimatedSamples();
                                }
                            }
                        }, poolExecutor);
                    }
                    return;
                }
            }
            // If no sample has been written so far, we read all samples from
            // the source decimation level, starting with the oldest available
            // sample.
            if (getCurrentBucketStartTime() < 0L) {
                generateDecimatedSamplesStartQuery(0L,
                        TimeStampLimitMode.AT_OR_AFTER, Long.MAX_VALUE,
                        TimeStampLimitMode.AT_OR_BEFORE);
                return;
            }
            // If we do not know the time stamp of the last sample yet, we first
            // have to find out this time stamp. The code writing the decimated
            // samples will need this information anyway, so it makes sense to
            // store it in the same place.
            if (getLastSampleTimeStamp() < 0L) {
                final ListenableFuture<Void> future = initializeCurrentBucketSizeAndLastSampleTimeStamp();
                // We do not want to run this method again until the
                // asynchronous operation has finished.
                waitingForAsynchronousDecimationOperation = true;
                // We use the poolExecutor for running our callback because
                // acquiring a mutex in a callback processed by the same-thread
                // executor is not a good idea.
                future.addListener(new Runnable() {
                    @Override
                    public void run() {
                        synchronized (channel) {
                            // The asynchronous operation has finished, so the
                            // surrounding method may be run again.
                            waitingForAsynchronousDecimationOperation = false;
                            try {
                                FutureUtils.getUnchecked(future);
                                // If we were successfull we can proceed right
                                // away.
                                generateDecimatedSamples();
                            } catch (Throwable t) {
                                // If there was an error, we try again after
                                // some time has passed.
                                generateDecimatedSamplesReschedule();
                            }
                        }
                    }
                }, poolExecutor);
                return;
            }
            // If the time stamp of the last sample is zero, but we have a
            // sample bucket, the time of the next sample must be the start time
            // of the sample bucket. We can only get an empty sample bucket when
            // the server was interrupted before the sample could be written and
            // in this case the sample bucket starts at the time stamp of the
            // lost sample. This means that the next sample that we need to
            // generate is just this sample.
            long nextDecimatedSampleTimeStamp;
            // If the time stamp of the last sample is zero, but we have a
            // sample bucket, the time of the next sample must be the start time
            // of the sample bucket. We can only get an empty sample bucket when
            // the server was interrupted before the sample could be written and
            // in this case the sample bucket starts at the time stamp of the
            // lost sample. This means that the next sample that we need to
            // generate is just this sample.
            if (getLastSampleTimeStamp() == 0L) {
                nextDecimatedSampleTimeStamp = getCurrentBucketStartTime();
            } else {
                // If time stamps get very large (around the year 2262), the
                // time-stamp could become negative when incrementing it. We
                // rather stop generating decimated samples then using negative
                // time-stamps which would only mess up everything. Obviously,
                // such a situation will most likely occur because of a clock
                // going crazy.
                // We keep a safety margin of two times the decimation period so
                // that the end time of the interval still fits into the limits
                // of the 64-bit time-stamp.
                if (getLastSampleTimeStamp() > Long.MAX_VALUE
                        - 2L * decimationPeriodNanoseconds) {
                    // We do not want to generate decimated samples any longer.
                    fatalErrorOccurred = true;
                    // When we stop the decimation process, we want the removal
                    // process for the source decimation level to be able to
                    // run. For this reason, we clear/set the flags that are
                    // important for this process.
                    sourceSampleQueryComplete = true;
                    waitingForAsynchronousDecimationOperation = false;
                    // We log a warning to inform the user that we are not going
                    // to generate any more decimated samples.
                    logTimeStampWarning();
                    return;
                }
                nextDecimatedSampleTimeStamp = getLastSampleTimeStamp()
                        + decimationPeriodNanoseconds;
            }
            // We have not used a source sample yet, so we first have to query
            // the database in order to get a sample that has a time-stamp at or
            // before the start of the decimation interval. Actually, we also
            // want to get all samples that come later because we need all
            // samples that have been stored earlier and that we will not get
            // via the queue.
            generateDecimatedSamplesStartQuery(nextDecimatedSampleTimeStamp,
                    TimeStampLimitMode.AT_OR_BEFORE, Long.MAX_VALUE,
                    TimeStampLimitMode.AT_OR_BEFORE);
            return;
        }
        // We have used a source sample before. Typically, this means that we
        // can simply proceed with the next sample from the queue. However,
        // there might have been an overflow of the queue and in this case we
        // have to query the database again.
        SampleType nextSourceSample;
        nextSourceSample = sourceSampleQueue.peek();
        // If we have at least one sample in the queue that we have already
        // seen, we can be sure that we did not miss any samples. We can thus
        // reset the overflow count.
        while (nextSourceSample != null && nextSourceSample
                .getTimeStamp() <= lastSourceSample.getTimeStamp()) {
            sourceSampleQueue.poll();
            sourceSampleQueue.resetOverflowCount();
            nextSourceSample = sourceSampleQueue.peek();
        }
        // If the source sample queue is empty, we have not missed any sample
        // and we do not have to do any processing either. We can wait until we
        // are notified that a source sample has been added to the queue.
        if (nextSourceSample == null) {
            return;
        }
        // If the overflow count of the source sample queue is not zero, we have
        // missed samples. We have to make a query so that we get these missed
        // samples. We also have to make another query if the last query was not
        // processed completely (most likely because of an error while trying to
        // fetch the samples).
        if (sourceSampleQueue.getOverflowCount() != 0
                || !sourceSampleQueryComplete) {
            // Before making the query, we drain the queue except for the last
            // element. We want to get all samples that are in the queue from
            // the database so that it is less likely that we will have to make
            // another query when we are finished with the first query. If we
            // only made a query for samples up to the first sample in the
            // queue, it is very likely that by the time that we have processed
            // the results from the query, more elements have been removed from
            // the queue and we have to make another query.
            while (sourceSampleQueue.size() > 1) {
                sourceSampleQueue.poll();
            }
            nextSourceSample = sourceSampleQueue.peek();
            generateDecimatedSamplesStartQuery(lastSourceSample.getTimeStamp(),
                    TimeStampLimitMode.AT_OR_AFTER,
                    nextSourceSample.getTimeStamp(),
                    TimeStampLimitMode.AT_OR_BEFORE);
            return;
        }
        // We verified that we can use the first source sample from the queue.
        // This implies that we can also use the following samples. We can
        // process the samples from the queue until we have either processed all
        // samples or a fatal error has occurred. We have to check this flag
        // after each iteration because it might be set by the called method.
        while (!sourceSampleQueue.isEmpty() && !fatalErrorOccurred) {
            nextSourceSample = sourceSampleQueue.poll();
            ListenableFuture<Void> processSourceSampleFuture;
            try {
                processSourceSampleFuture = generateDecimatedSamplesProcessSourceSample(
                        nextSourceSample);
            } catch (Throwable t) {
                generateDecimatedSamplesProcessSourceSampleHandleError(t);
                break;
            }
            // If the operation finished synchronously (the default), the return
            // value is null. Otherwise, we call generateDecimatedSamples()
            // again when the asynchronous operation has finished.
            if (processSourceSampleFuture != null) {
                // The generateDecimatedSamples() method must not run until the
                // asynchronous operation has finished.
                waitingForAsynchronousDecimationOperation = true;
                processSourceSampleFuture.addListener(new Runnable() {
                    @Override
                    public void run() {
                        synchronized (channel) {
                            waitingForAsynchronousDecimationOperation = false;
                            generateDecimatedSamples();
                        }
                    }
                }, poolExecutor);
                return;
            }
        }
    }

    private void generateDecimatedSamplesProcessQueryResults(
            final SkippableObjectResultSet<SampleType> resultSet) {
        // We need to know whether the first sample has already been processed.
        // If it has not, we have to check that the first sample returned by the
        // query is the last source sample (if there is a last source sample).
        // If it is not, there is a gap in the source samples and we have to
        // reset the sample decimator. We could use a field in the decimation
        // level for transporting this state, but it is really local to the
        // method processing the query results.
        generateDecimatedSamplesProcessQueryResults(resultSet, false);
    }

    private void generateDecimatedSamplesProcessQueryResults(
            final SkippableObjectResultSet<SampleType> resultSet,
            boolean processedFirstSample) {
        assert (Thread.holdsLock(channel));
        assert (resultSet != null);
        boolean retainResultSet = false;
        try {
            // Processing samples from the database might take a long time when
            // there are a lot of source samples to be processed. For this
            // reason we periodically check whether the channel has been
            // destroyed so that we do not waste resources on generating
            // decimated samples that are not going to be written anyway.
            if (channel.getState().equals(ArchivedChannelState.DESTROYED)) {
                return;
            }
            SampleType nextSourceSample = null;
            // When making the query, we choose the limits appropriately, so we
            // can expect that we will only get source samples that we can
            // actually use.
            // We process all samples from the result set until we either have
            // to fetch more samples (involving network I/O that we do not want
            // to perform while holding the lock) or the allowed time-stamp
            // range has been exceeded. We have to check this flag after each
            // iteration because it might be set by the called method.
            int samplesProcessed = 0;
            while (resultSet.getAvailableWithoutFetching() > 0
                    && !fatalErrorOccurred) {
                // After processing 100 samples, we return and schedule this
                // method for another execution. This way, the lock on the
                // channel is released for a moment and other threads waiting on
                // this lock (in particular the write thread) have a chance of
                // running.
                if (samplesProcessed == 100) {
                    // We simply schedule another run with the pool executor.
                    // This has the effect that the mutex is released for a
                    // short moment and thus other threads have a chance to
                    // acquire it.
                    poolExecutor.execute(new Runnable() {
                        @Override
                        public void run() {
                            synchronized (channel) {
                                // We can always use true for the
                                // processedFirstSample flag because this
                                // callback is only triggered after a certain
                                // number of samples have been written.
                                try {
                                    generateDecimatedSamplesProcessQueryResults(
                                            resultSet, true);
                                } catch (Throwable t) {
                                    generateDecimatedSamplesProcessQueryResultsHandleError(
                                            t);
                                }
                            }
                        }
                    });
                    // We are going to use the result set in the future, so we
                    // have to retain the samples remaining in the result set.
                    retainResultSet = true;
                    return;
                }
                ++samplesProcessed;
                nextSourceSample = resultSet.one();
                if (!processedFirstSample) {
                    if (lastSourceSample != null && nextSourceSample
                            .getTimeStamp() > lastSourceSample.getTimeStamp()) {
                        // There is a gap in the samples. This can only happen
                        // if source samples are not processed quickly enough
                        // and are deleted fast than we can process them. Such a
                        // situation can only occur if the source samples are
                        // written at an incredibly high rate and have a very
                        // short retention period. This is a very improbable
                        // scenario, but we still should handle it correctly. By
                        // resetting the sample decimator, we ensure that the we
                        // will not create a decimated sample for which we do
                        // not have data, incorrectly using a much older sample.
                        // Stopping the removal of sample buckets while we are
                        // reading source samples is not sufficient to mitigate
                        // this scenario because source samples might be deleted
                        // between processing a query from the queue and
                        // starting a query because the queue has overflowed.
                        sampleDecimator = null;
                    }
                    processedFirstSample = true;
                }
                ListenableFuture<Void> processSourceSampleFuture;
                try {
                    processSourceSampleFuture = generateDecimatedSamplesProcessSourceSample(
                            nextSourceSample);
                } catch (Throwable t) {
                    generateDecimatedSamplesProcessSourceSampleHandleError(t);
                    break;
                }
                // If the operation finished synchronously (the default), the
                // return value is null. Otherwise, we call this method again
                // when the asynchronous operation has finished.
                if (processSourceSampleFuture != null) {
                    processSourceSampleFuture.addListener(new Runnable() {
                        @Override
                        public void run() {
                            synchronized (channel) {
                                try {
                                    generateDecimatedSamplesProcessQueryResults(
                                            resultSet, true);
                                } catch (Throwable t) {
                                    generateDecimatedSamplesProcessQueryResultsHandleError(
                                            t);
                                }
                            }
                        }
                    }, poolExecutor);
                    // We are going to use the result set in the future, so we
                    // have to retain the samples remaining in the result set.
                    retainResultSet = true;
                    return;
                }
            }
            // If we broke the loop because the time-stamp range has been
            // exceeded or any other fatal error occurred, we do not want to do
            // anything else and simply return.
            if (fatalErrorOccurred) {
                return;
            }
            // There might be more samples that we can fetch.
            if (!resultSet.isFullyFetched()) {
                // We do not want to fetch more samples if the write queue
                // already contains a significant number of samples. In this
                // case, we rather wait for the write queue to be empty again so
                // that the memory consumption is limited.
                // This check looks redundant because
                // generateDecimatedSamplesProcessSourceSample already includes
                // a similar check, but that check is only triggered when a
                // single source sample causes the generation of more than one
                // decimated sample, while the check here will always apply
                // before fetching more data. This makes sense because usually
                // generated samples are less dense than decimated samples, so
                // we only want to wait until the generated samples have been
                // written when we have no more source samples in memory.
                if (getWriteQueueSize() >= 100) {
                    // We register a runnable that is going to be run when the
                    // queue is empty again.
                    final boolean finalProcessedFirstSample = processedFirstSample;
                    onWriteQueueEmptyRunnable = new Runnable() {
                        @Override
                        public void run() {
                            synchronized (channel) {
                                try {
                                    waitingForAsynchronousDecimationOperation = false;
                                    generateDecimatedSamplesProcessQueryResults(
                                            resultSet,
                                            finalProcessedFirstSample);
                                } catch (Throwable t) {
                                    generateDecimatedSamplesProcessQueryResultsHandleError(
                                            t);
                                }
                            }
                        }
                    };
                    // The generateDecimatedSamples() method must not run until
                    // the asynchronous operation has finished.
                    waitingForAsynchronousDecimationOperation = true;
                    // We are going to use the result set in the future, so we
                    // have to retain it (which does not cost much anyway,
                    // because there are currently no samples in memory).
                    retainResultSet = true;
                    return;
                }
                final ListenableFuture<Void> future = resultSet
                        .fetchMoreResults();
                // We use the poolExecutor for running our callback because
                // acquiring a mutex in a callback processed by the same-thread
                // executor is not a good idea.
                final boolean finalProcessedFirstSample = processedFirstSample;
                future.addListener(new Runnable() {
                    @Override
                    public void run() {
                        synchronized (channel) {
                            try {
                                FutureUtils.getUnchecked(future);
                                generateDecimatedSamplesProcessQueryResults(
                                        resultSet, finalProcessedFirstSample);
                            } catch (Throwable t) {
                                generateDecimatedSamplesProcessQueryResultsHandleError(
                                        t);
                            }
                        }
                    }
                }, poolExecutor);
                // We are going to use the result set in the future, so we have
                // to retain the samples remaining in the result set.
                retainResultSet = true;
                return;
            }
            // The asynchronous operation is finished, so we have to reset the
            // flag to indicate that decimation process may be started again.
            waitingForAsynchronousDecimationOperation = false;
            // We received all the data from the query. This means that we can
            // set the complete flag.
            sourceSampleQueryComplete = true;
            // After we finished processing the samples from the query, we want
            // to delegate back to the primary method because there might be
            // samples in the queue.
            generateDecimatedSamples();
        } finally {
            // If the result set is not needed any longer (because no
            // asynchronous operation using it has been scheduled), we want to
            // release any samples that might still be remaining. Eventually,
            // the garbage collection would take care of this, but as we limit
            // the number of samples that may be concurrently loaded into
            // memory, other operations might not be started until this happens
            // and garbage collection runs are inherently unpredictable.
            if (!retainResultSet) {
                resultSet.skipRemaining();
            }
        }
    }

    private void generateDecimatedSamplesProcessQueryResultsHandleError(
            Throwable t) {
        assert (Thread.holdsLock(channel));
        // The asynchronous operation is finished, so the decimation process can
        // be run again in the future.
        waitingForAsynchronousDecimationOperation = false;
        // We write the error message to the log. Most likely, the error is
        // transient, but it might still be good to know that it occurred.
        log.error("Error while trying to process source samples for channel \""
                + StringEscapeUtils
                        .escapeJava(channelConfiguration.getChannelName())
                + "\" and decimation level " + decimationPeriodSeconds + ".",
                t);
        // We schedule another run because the cause of the problem might be
        // transient (e.g. the database being temporarily overloaded).
        generateDecimatedSamplesReschedule();
    }

    private ListenableFuture<Void> generateDecimatedSamplesProcessSourceSample(
            final SampleType sourceSample) {
        assert (Thread.holdsLock(channel));
        assert (sourceSample != null);
        // This method should not be called after there has been a fatal error.
        // If it is, this is an error in the logic of the calling code.
        assert (!fatalErrorOccurred);
        // If we do not have a sample decimator yet, we have to create one. If
        // we already have a sample decimator but the next source sample is at
        // the start of or before the decimation interval of this decimator, we
        // also want to use a new decimator. Each decimator should see exactly
        // one sample before or at the start of its decimation interval.
        // If we have multiple samples before the start of the first decimation
        // interval, this means that we will create (and destroy) as many sample
        // decimators as there are samples, which seems a bit wasteful. However,
        // this case is very rare because usually we make our query so that we
        // get exactly one fitting sample. The only exception is the first
        // decimation interval that we process, because we cannot know a priori
        // which interval this is going to be and thus cannot specify the exact
        // interval when getting samples. This means that this will only happen
        // once when a decimation level is processed for the first time after
        // being created, which seems acceptable.
        // If this method has been invoked because a previous invocation has
        // been interrupted (we can determine this by the future being set),
        // we can skip this check and continue right to the loop where we
        // generate more decimated samples).
        if (generateDecimatedSamplesProcessSourceSampleFuture == null
                && (sampleDecimator == null
                        || sourceSample.getTimeStamp() <= sampleDecimator
                                .getIntervalStartTime())) {
            // The first sample fed to a sample decimator must be at or before
            // the start of the decimation interval. The calling code ensures
            // that we get the right source sample, so we can calculate the
            // start of the interval from the source sample's time-stamp.
            long nextDecimatedSampleTimeStamp;
            if (sourceSample.getTimeStamp()
                    % decimationPeriodNanoseconds == 0L) {
                nextDecimatedSampleTimeStamp = sourceSample.getTimeStamp();
            } else {
                // We first calculate the time-stamp of the start of the
                // decimation period that contains the given source sample. This
                // is safer because that time-stamp is always less than the
                // sample's time-stamp and thus we do not risk exceeding the
                // 64-bit integer range.
                long lastDecimatedSampleTimeStamp = (sourceSample.getTimeStamp()
                        / decimationPeriodNanoseconds)
                        * decimationPeriodNanoseconds;
                // The next decimation interval has to fit into the integer
                // range completely. If it does not, we cannot calculate any
                // more decimated samples.
                if (lastDecimatedSampleTimeStamp > Long.MAX_VALUE
                        - 2L * decimationPeriodNanoseconds) {
                    fatalErrorOccurred = true;
                    // When we stop the decimation process, we want the removal
                    // process for the source decimation level to be able to
                    // run. For this reason, we clear/set the flags that are
                    // important for this process.
                    sourceSampleQueryComplete = true;
                    waitingForAsynchronousDecimationOperation = false;
                    // We log a warning to inform the user that we are not going
                    // to generate any more decimated samples.
                    logTimeStampWarning();
                    // We return null to indicate that the operation has already
                    // finished. This is much more efficient that creating an
                    // immediate future.
                    return null;
                }
                nextDecimatedSampleTimeStamp = lastDecimatedSampleTimeStamp
                        + decimationPeriodNanoseconds;
            }
            // If the generation of decimated sampled was interrupted earlier,
            // it can be that the source sample is much older than the latest
            // decimated sample. In this case inferring the decimated sample
            // time-stamp from the source sample time-stamp is not optimal
            // because it will lead to already existing decimated samples being
            // generated again. Those samples are going to be dropped when
            // trying to write them, but it still is unnecessary work. For this
            // reason, we check that the time-stamp of the next decimated sample
            // is actually greater than the time-stamp of the last decimated
            // sample that has been written.
            // We can safely call getLastSampleTimeStamp() because the last
            // sample time-stamp has been initialized before calling this
            // method.
            if (nextDecimatedSampleTimeStamp <= getLastSampleTimeStamp()) {
                long lastDecimatedSampleTimeStamp = getLastSampleTimeStamp();
                // The next decimation interval has to fit into the integer
                // range completely. If it does not, we cannot calculate any
                // more decimated samples.
                if (lastDecimatedSampleTimeStamp > Long.MAX_VALUE
                        - 2L * decimationPeriodNanoseconds) {
                    fatalErrorOccurred = true;
                    // When we stop the decimation process, we want the removal
                    // process for the source decimation level to be able to
                    // run. For this reason, we clear/set the flags that are
                    // important for this process.
                    sourceSampleQueryComplete = true;
                    waitingForAsynchronousDecimationOperation = false;
                    // We log a warning to inform the user that we are not going
                    // to generate any more decimated samples.
                    logTimeStampWarning();
                    // We return null to indicate that the operation has already
                    // finished. This is much more efficient that creating an
                    // immediate future.
                    return null;
                }
                nextDecimatedSampleTimeStamp = lastDecimatedSampleTimeStamp
                        + decimationPeriodNanoseconds;
            }
            try {
                sampleDecimator = channel.getControlSystemSupport()
                        .createSampleDecimator(
                                channelConfiguration.getChannelName(),
                                channelConfiguration.getOptions(),
                                nextDecimatedSampleTimeStamp,
                                decimationPeriodNanoseconds);
                // createSampleDecimator should not return null.
                if (sampleDecimator == null) {
                    throw new NullPointerException(
                            "The control-system support's createSampleDecimator method returned null.");
                }
            } catch (Throwable t) {
                // If createSampleDecimator throws an exception (it should not),
                // we want to put this channel into an error state because this
                // violates the contract of the ControlSystemSupport interface.
                // Then, we throw an exception because this is the method of
                // choice for aborting execution with the least undesired
                // side-effects.
                RuntimeException e = new RuntimeException(
                        "The control-system support's createSampleDecimator method violated its contract: "
                                + t.getMessage());
                channel.destroyWithException(e);
                throw e;
            }
        }
        // If the next source sample comes after the current interval, we first
        // have to finish the calculations for the current interval. We have to
        // make this check until it fails because there might be gaps between
        // source samples that are greater than the decimation period.
        boolean firstIteration = true;
        while (sourceSample
                .getTimeStamp() >= sampleDecimator.getIntervalStartTime()
                        + decimationPeriodNanoseconds) {
            // When this is not the first iteration, we check whether the size
            // of the write queue exceeds a limit of 100 samples. If it does, we
            // suspend generating samples until the write queue is empty again.
            // When we generate more than one decimated sample for a source
            // sample, we occupy more and more memory. The write queue cannot be
            // processed while we are in this method because we hold the channel
            // mutex. When gaps between source samples are large, we would
            // occupy a lot of memory for the samples in the write queue before
            // actually being finished and thus being able to process the write
            // queue, potentially leading to memory exhaustion.
            // By waiting until the queue is empty again, we give it a chance to
            // be processed and also limit the amount of memory that is
            // occupied. While waiting, the input queue with the source samples
            // might grow, but when having large gaps this grow rate is
            // typically smaller than the potential grow rate for the output
            // queue.
            if (!firstIteration && getWriteQueueSize() >= 100) {
                // We register a runnable that is going to be run when the queue
                // is empty again.
                onWriteQueueEmptyRunnable = new Runnable() {
                    @Override
                    public void run() {
                        synchronized (channel) {
                            try {
                                // If the channel has been destroyed, there is
                                // no sense in generating more decimated samples
                                // because they will not be written anyway.
                                // We do this check here (instead of the start
                                // of this method) because this method is called
                                // quite often and checking the channel's state
                                // every time does not make sense.
                                if (channel.getState().equals(
                                        ArchivedChannelState.DESTROYED)) {
                                    // We complete the future with an exception.
                                    // At the moment, code using this future
                                    // will only use listeners that do not use
                                    // the result of the future, so that it does
                                    // not matter whether it completes with an
                                    // exception or successfully. On the other
                                    // hand, that code will also check whether
                                    // the channel has been destroyed and will
                                    // stop.
                                    // If we add some code in the future that
                                    // actually depends on the method having
                                    // finished successfully, completing with an
                                    // exception might help us discover a bug.
                                    SettableFuture<Void> future = generateDecimatedSamplesProcessSourceSampleFuture;
                                    generateDecimatedSamplesProcessSourceSampleFuture = null;
                                    future.setException(new RuntimeException(
                                            "The channel was destroyed before the processing of source samples could be finished."));
                                    return;
                                }
                                generateDecimatedSamplesProcessSourceSample(
                                        sourceSample);
                            } catch (Throwable t) {
                                generateDecimatedSamplesProcessSourceSampleHandleError(
                                        t);
                            }
                        }
                    }
                };
                // If this method has been called from a runnable that we
                // registered, there already is a future. Otherwise, we create
                // one.
                if (generateDecimatedSamplesProcessSourceSampleFuture == null) {
                    generateDecimatedSamplesProcessSourceSampleFuture = SettableFuture
                            .create();
                }
                return generateDecimatedSamplesProcessSourceSampleFuture;
            }
            firstIteration = false;
            SampleType nextDecimatedSample;
            int nextDecimatedSampleSize;
            try {
                sampleDecimator.buildDecimatedSample();
                nextDecimatedSample = sampleDecimator.getDecimatedSample();
                if (nextDecimatedSample == null) {
                    throw new NullPointerException(
                            "The control-system support's sample-decimator's getDecimatedSample method returned null.");
                }
                nextDecimatedSampleSize = sampleDecimator
                        .getDecimatedSampleEstimatedSize();
                if (nextDecimatedSampleSize < 0) {
                    throw new IllegalArgumentException(
                            "The control-system support's sample-decimator's getDecimatedSampleEstimatedSize method returned a negative value.");
                }
            } catch (Throwable t) {
                // If buildDecimatedSample (or one of the other methods throw an
                // exception or return null (they should not), we want to put
                // this channel into an error state because this violates the
                // contract of the SampleDecimator interface. Then, we throw an
                // exception because this is the method of choice for aborting
                // execution with the least undesired side-effects.
                RuntimeException e = new RuntimeException(
                        "The control-system support's sample-decimator violated its contract by throwing an exception or returning an invalid value: "
                                + t.getMessage());
                channel.destroyWithException(e);
                throw e;
            }
            addSampleToWriteQueue(nextDecimatedSample, nextDecimatedSampleSize);
            // We do not want to register with the global processing queue when
            // we are waiting for an asynchronous write operation to be
            // finished.
            if (!isAsynchronousWriteSampleOperationInProgress()) {
                archivingService
                        .addChannelDecimationLevelToWriteProcessingQueue(this);
            }
            // Now that the decimated sample has been queued for being written,
            // we can start generating the next sample.
            long nextDecimatedSampleTimeStamp = sampleDecimator
                    .getIntervalStartTime() + decimationPeriodNanoseconds;
            // If the end time of the next interval is outside the range covered
            // by the 64-bit integer, we do not want to proceed with generating
            // decimated samples. This situation happens when we see a time
            // stamp that is around the year 2262, so such a situation will
            // typically only occur if there is a clock problem.
            if (nextDecimatedSampleTimeStamp > Long.MAX_VALUE
                    - decimationPeriodNanoseconds) {
                // We do not want to generate decimated samples any longer.
                fatalErrorOccurred = true;
                // When we stop the decimation process, we want the removal
                // process for the source decimation level to be able to run.
                // For this reason, we clear/set the flags that are important
                // for this process.
                sourceSampleQueryComplete = true;
                waitingForAsynchronousDecimationOperation = false;
                // We log a warning to inform the user that we are not going
                // to generate any more decimated samples.
                logTimeStampWarning();
                // We return null to indicate that the operation has already
                // finished. This is much more efficient that creating an
                // immediate future. If we have an incomplete future, we have to
                // complete it first.
                SettableFuture<Void> future = generateDecimatedSamplesProcessSourceSampleFuture;
                generateDecimatedSamplesProcessSourceSampleFuture = null;
                if (future != null) {
                    future.set(null);
                }
                return null;
            }
            try {
                sampleDecimator = channel.getControlSystemSupport()
                        .createSampleDecimator(
                                channelConfiguration.getChannelName(),
                                channelConfiguration.getOptions(),
                                nextDecimatedSampleTimeStamp,
                                decimationPeriodNanoseconds);
                // createSampleDecimator should not return null.
                if (sampleDecimator == null) {
                    throw new NullPointerException(
                            "The control-system support's createSampleDecimator method returned null.");
                }
            } catch (Throwable t) {
                // If createSampleDecimator throws an exception (it should not),
                // we want to put this channel into an error state because this
                // violates the contract of the ControlSystemSupport interface.
                // Then, we throw an exception because this is the method of
                // choice for aborting execution with the least undesired
                // side-effects.
                RuntimeException e = new RuntimeException(
                        "The control-system support's createSampleDecimator method violated its contract: "
                                + t.getMessage());
                channel.destroyWithException(e);
                throw e;
            }
            // If the next source sample is not right at the start of the next
            // interval, we first have to feed the last sample from the last
            // interval to the sample decimator.
            if (sourceSample.getTimeStamp() > nextDecimatedSampleTimeStamp) {
                try {
                    sampleDecimator.processSample(lastSourceSample);
                } catch (Throwable t) {
                    // If processSample throws an exception (it should not), we
                    // want to put this channel into an error state because this
                    // violates the contract of the SampleDecimator interface.
                    // Then, we throw an exception because this is the method of
                    // choice for aborting execution with the least undesired
                    // side-effects.
                    RuntimeException e = new RuntimeException(
                            "The control-system support's sample-decimator's processSample method violated its contract: "
                                    + t.getMessage());
                    channel.destroyWithException(e);
                    throw e;
                }
            }
        }
        try {
            sampleDecimator.processSample(sourceSample);
        } catch (Throwable t) {
            // If processSample throws an exception (it should not), we want to
            // put this channel into an error state because this violates the
            // contract of the SampleDecimator interface. Then, we throw an
            // exception because this is the method of choice for aborting
            // execution with the least undesired side-effects.
            RuntimeException e = new RuntimeException(
                    "The control-system support's sample-decimator's processSample method violated its contract: "
                            + t.getMessage());
            channel.destroyWithException(e);
            throw e;
        }
        lastSourceSample = sourceSample;
        // We return null to indicate that the operation has already finished.
        // This is much more efficient that creating an immediate future. This
        // optimization makes sense because it is the most likely case (the
        // operation finishing asynchronously is the exception. If we have an
        // incomplete future, we have to complete it first.
        SettableFuture<Void> future = generateDecimatedSamplesProcessSourceSampleFuture;
        generateDecimatedSamplesProcessSourceSampleFuture = null;
        if (future != null) {
            future.set(null);
        }
        return null;
    }

    private void generateDecimatedSamplesProcessSourceSampleHandleError(
            Throwable t) {
        assert (Thread.holdsLock(channel));
        // When generateDecimatedSamplesProcessSourceSample(...) throws an
        // exception it is most likely fatal: This method does not depend on the
        // availability on any external system, so any error is most likely to
        // be caused by a bug in the logic.
        // More importantly, if this method throws an exception, a source sample
        // has been consumed, but not processed. This means that continuing
        // would result in this sample being ignored, leading to a decimated
        // sample that does not account for all source samples.
        // Instead of failing permanently, we could store the offending sample
        // and try again later, but it as the logic is essentially stateless,
        // the same source sample will most likely result in the same exception
        // until either the source sample or the logic is changed.
        // For these reasons, stopping the whole decimation process entirely
        // seems like the most reasonable option. We log a corresponding error
        // message, so that the user can know that there is a bug that needs
        // attention.
        fatalErrorOccurred = true;
        log.error(
                "Fatal error while trying to generate a decimated sample for channel \""
                        + StringEscapeUtils.escapeJava(
                                channelConfiguration.getChannelName())
                        + "\" and decimation level " + decimationPeriodSeconds
                        + ". No more decimated samples will be generated for this channel and decimation level.",
                t);
        // When we stop the decimation process, we want the removal process for
        // the source decimation level to be able to run. For this reason, we
        // clear/set the flags that are important for this process.
        sourceSampleQueryComplete = true;
        waitingForAsynchronousDecimationOperation = false;
    }

    private void generateDecimatedSamplesReschedule() {
        assert (Thread.holdsLock(channel));
        // This method is called when an asynchronous operation that is part of
        // the decimation process failed. If this failure was just caused by a
        // temporary hiccup, running the operation again (after a short delay)
        // should succeed. If there is a persistent problem, it is most likely
        // caused by the database being unavailable. In this case, the server
        // will go offline and the channel is going to be destroyed, so that
        // this will break the retry cycle.
        // There might be situations in which this method is called repeatedly
        // before the scheduled run has actually happened. In these situations,
        // we do not want to schedule more runs because this would only make the
        // executor's queue grow bigger and bigger.
        if (generateDecimatedSamplesScheduled) {
            return;
        }
        generateDecimatedSamplesScheduled = true;
        scheduledExecutor.schedule(runWithPoolExecutor(new Runnable() {
            @Override
            public void run() {
                synchronized (channel) {
                    generateDecimatedSamplesScheduled = false;
                    generateDecimatedSamples();
                }
            }
        }), 30000L, TimeUnit.MILLISECONDS);
    }

    private void generateDecimatedSamplesStartQuery(
            final long lowerTimeStampLimit,
            final TimeStampLimitMode lowerTimeStampLimitMode,
            final long upperTimeStampLimit,
            final TimeStampLimitMode upperTimeStampLimitMode) {
        assert (Thread.holdsLock(channel));
        assert (lowerTimeStampLimitMode != null);
        assert (upperTimeStampLimitMode != null);
        // This method might reschedule its execution when it is waiting for a
        // sample removal process to finish. However, the channel might have
        // been destroyed in the meantime. In this case, we do not want to
        // continue, but rather return directly. This helps in avoiding this
        // method to be scheduled again and again.
        if (channel.getState().equals(ArchivedChannelState.DESTROYED)) {
            return;
        }
        // We do not want to run the calling method again until the
        // asynchronous operation has finished.
        waitingForAsynchronousDecimationOperation = true;
        // If we currently deleting samples from the source decimation level, we
        // have to wait. Otherwise, we could see inconsistent samples (some
        // samples are still read while other are already deleted and can thus
        // not be read). Once we have set the
        // waitingForAsynchronousDecimationOperation flag, we do not have to
        // worry about a competing remove sample-bucket operation because such
        // an operation is not started while this flag is set.
        if (sourceDecimationLevel
                .isAsynchronousRemoveSampleBucketOperationInProgress()) {
            // We try again with a small delay. The small delay should be
            // sufficient because removing sample buckets is typically a quick
            // process.
            scheduledExecutor.schedule(runWithPoolExecutor(new Runnable() {
                @Override
                public void run() {
                    synchronized (channel) {
                        generateDecimatedSamplesStartQuery(lowerTimeStampLimit,
                                lowerTimeStampLimitMode, upperTimeStampLimit,
                                upperTimeStampLimitMode);
                    }
                }
            }), 1000L, TimeUnit.MILLISECONDS);
            return;
        }
        // If this is the first time that a source sample query is started, we
        // want to delay the query until all target decimation levels have
        // completed their source sample queries.
        // As soon as we start generating decimated samples, these will be
        // pushed into the target decimation levels. However, the target
        // decimation levels will not process them until they are finished with
        // the samples from their queries. For this reason, it makes sense to
        // process target decimation levels first and source decimation levels
        // later.
        if (!initialProcessingOfTargetDecimationLevelsFinished) {
            for (ArchivedChannelDecimatedSamplesDecimationLevel<SampleType> targetDecimationLevel : targetDecimationLevels) {
                // The sourceSampleQueryComplete flag might change back to false
                // after the initial processing has finished. However, this can
                // only happen when new source samples are sent to the
                // decimation level. We know that this will not happen before we
                // have set initialProcessingOfTargetDecimationLevelsFinished
                // because this is the source decimation level and we will not
                // generate any samples before
                // initialProcessingOfTargetDecimationLevelsFinished has been
                // set.
                if (!targetDecimationLevel.isSourceSampleQueryComplete()) {
                    // We try again after a delay. This delay is longer than
                    // when waiting for the deletion of samples to have finished
                    // because it might actually take some time until all target
                    // decimation levels have been processed.
                    scheduledExecutor
                            .schedule(runWithPoolExecutor(new Runnable() {
                                @Override
                                public void run() {
                                    synchronized (channel) {
                                        generateDecimatedSamplesStartQuery(
                                                lowerTimeStampLimit,
                                                lowerTimeStampLimitMode,
                                                upperTimeStampLimit,
                                                upperTimeStampLimitMode);
                                    }
                                }
                            }), 2500L, TimeUnit.MILLISECONDS);
                    return;
                }
            }
            initialProcessingOfTargetDecimationLevelsFinished = true;
        }
        final ListenableFuture<? extends SkippableObjectResultSet<SampleType>> future = throttledArchiveAccessService
                .getSamples(channelConfiguration, sourceDecimationPeriodSeconds,
                        lowerTimeStampLimit, lowerTimeStampLimitMode,
                        upperTimeStampLimit, upperTimeStampLimitMode,
                        channel.getControlSystemSupport());
        // From this point on, any sample written to the source decimation level
        // might not be present in the query results, so we have to start
        // storing them in the source sample queue. If this is not the first
        // query, this flag might already have been set, but setting it again
        // does not hurt.
        sourceSampleQueryStarted = true;
        // We clear the complete flag. This ensures that we will know when we
        // did not process all samples that would usually be returned by the
        // query. This flag is set when the last sample that is a result of the
        // query has been processed.
        sourceSampleQueryComplete = false;
        // We use the poolExecutor for running our callback because acquiring a
        // mutex in a callback processed by the same-thread executor is not a
        // good idea.
        future.addListener(new Runnable() {
            @Override
            public void run() {
                synchronized (channel) {
                    try {
                        generateDecimatedSamplesProcessQueryResults(
                                FutureUtils.getUnchecked(future));
                    } catch (Throwable t) {
                        generateDecimatedSamplesProcessQueryResultsHandleError(
                                t);
                    }
                }
            }
        }, poolExecutor);
    }

    private void logTimeStampWarning() {
        log.warn(
                "Max. time-stamp reached while generating decimated samples for channel \""
                        + StringEscapeUtils.escapeJava(
                                channelConfiguration.getChannelName())
                        + "\" and decimation level " + decimationPeriodSeconds
                        + ". No more decimated samples will be generated for this channel and decimation level.");
    }

}
