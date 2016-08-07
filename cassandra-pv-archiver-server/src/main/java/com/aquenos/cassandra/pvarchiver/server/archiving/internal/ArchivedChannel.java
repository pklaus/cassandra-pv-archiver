/*
 * Copyright 2015-2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.archiving.internal;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.aquenos.cassandra.pvarchiver.controlsystem.ControlSystemChannel;
import com.aquenos.cassandra.pvarchiver.controlsystem.ControlSystemSupport;
import com.aquenos.cassandra.pvarchiver.controlsystem.Sample;
import com.aquenos.cassandra.pvarchiver.controlsystem.SampleBucketId;
import com.aquenos.cassandra.pvarchiver.controlsystem.SampleWithSizeEstimate;
import com.aquenos.cassandra.pvarchiver.server.archiving.ArchiveAccessService;
import com.aquenos.cassandra.pvarchiver.server.controlsystem.ControlSystemSupportRegistry;
import com.aquenos.cassandra.pvarchiver.server.database.ChannelMetaDataDAO;
import com.aquenos.cassandra.pvarchiver.server.database.ChannelMetaDataDAO.ChannelConfiguration;
import com.aquenos.cassandra.pvarchiver.server.database.ChannelMetaDataDAO.ChannelOperation;
import com.aquenos.cassandra.pvarchiver.server.util.FutureUtils;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;

/**
 * <p>
 * Internal representation of a channel. This objects aggregates all information
 * associated with a certain channel so that it can be stored in a map
 * containing the information for all archived channels.
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
 * input parameters and the state while method that are <em>package private</em>
 * do not. In particular, most <em>package-private</em> methods assume that the
 * caller has synchronized on this object. However, there might be methods that
 * do not adhere to this basic rules, so in case of doubt one should always
 * refer to the method's documentation.
 * </p>
 * 
 * @author Sebastian Marsching
 */
class ArchivedChannel<SampleType extends Sample> {

    // Implementation note: We use channel objects in a way that ensures that
    // there is only one active object for each channel name at a time. This
    // makes many things much easier because there is a single synchronization
    // point. Code manipulating a channel object (or one of its associated
    // decimation levels) has to synchronize on that channel object. Code
    // reading from a channel object (or one of its associated decimation
    // levels) also has to synchronize on that channel object, except for final
    // fields storing immutable objects, which are never modified and thus safe
    // for concurrent read access.

    private static final int RAW_SAMPLES_DECIMATION_PERIOD = 0;
    private static final ListenableFuture<Void> VOID_SUCCESS = Futures
            .immediateFuture(null);

    /**
     * Logger for this class.
     */
    protected final Log log = LogFactory.getLog(getClass());

    private final ArchiveAccessService archiveAccessService;
    private final ArchivingServiceInternalImpl archivingService;
    private final ChannelMetaDataDAO channelMetaDataDAO;
    private final ChannelConfiguration configuration;
    private ControlSystemChannel controlSystemChannel;
    private ControlSystemSupport<SampleType> controlSystemSupport;
    private final ControlSystemSupportRegistry controlSystemSupportRegistry;
    // This future can be used to wait on a create sample buckets operation to
    // finish. Please note that this future will only finish when all requests
    // to create a sample bucket (for different decimation levels) have been
    // finished. This is slightly inefficient, but makes the logic simpler.
    // Creating sample buckets is a relatively rare operation, so typically
    // there will not be more than one request in parallel anyway. This future
    // is only non-null if at least one operation to create a sample bucket is
    // in progress.
    private SettableFuture<Void> createSampleBucketsFuture;
    private HashMap<Integer, ArchivedChannelDecimationLevel<SampleType>> decimationLevels = new HashMap<Integer, ArchivedChannelDecimationLevel<SampleType>>();
    // This future can be used to wait for the channel to be destroyed.
    private final SettableFuture<Void> destructionFuture = SettableFuture
            .create();
    // We set this flag when the channel shall be destroyed. If this flag is
    // true but the channel is not in the DESTROYED state yet, this is because
    // there are still asynchronous operations that need to be finished before
    // the channel can finally be destroyed.
    private boolean destructionRequested;
    private Throwable error;
    // This future can be used to wait on the channel to be initialized. It is
    // only non-null when the initialization has been started
    // (initializeChannel(...) has been called) but has not completed yet.
    private SettableFuture<Void> initializationFuture;
    private final ExecutorService poolExecutor;
    // This pending operation is only set when it is detected during
    // initialization of the channel.
    private final ChannelOperation pendingOperationOnInit;
    private long samplesDropped;
    private long samplesSkippedBack;
    private long samplesWritten;
    private final ScheduledExecutorService scheduledExecutor;
    private ArchivedChannelState state = ArchivedChannelState.INITIALIZING;
    private final UUID thisServerId;

    /**
     * <p>
     * Creates a new channel instance. This constructor is only intended for use
     * by {@link ArchivingServiceInternalImpl}. Before creating a new channel
     * instance, the calling code has to ensure that another channel instance
     * that might have existed for the same channel name has been destroyed.
     * </p>
     * 
     * <p>
     * This constructor only stores the references to the passed objects, but
     * does not do any actual initialization (e.g. creating decimation level
     * objects or creating the control-system channel). The
     * {@link #initialize()} method has to be called for these initialization
     * actions to be run.
     * </p>
     * 
     * <p>
     * For obvious reasons, this constructor can be used without synchronizing
     * on the channel instance to be created.
     * </p>
     * 
     * @param configuration
     *            configuration for this channel.
     * @param pendingOperation
     *            pending operation that was found in the database for this
     *            channel or <code>null</code> if there is no pending operation.
     * @param archiveAccessService
     *            archive access service used for reading samples. This service
     *            is used when generating decimated samples (if there is at
     *            least one decimation level storing decimated samples).
     * @param archivingService
     *            the archiving service that is creating this channel.
     * @param channelMetaDataDAO
     *            channel meta-data DAO used for manipulating the channel's
     *            meta-data. This DAO is used when creating new sample buckets.
     * @param controlSystemSupportRegistry
     *            control-system support registry. The registry is used for
     *            retrieving the control-system support for this channel as part
     *            of the initialization process when calling
     *            {@link #initialize()}.
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
    ArchivedChannel(ChannelConfiguration configuration,
            ChannelOperation pendingOperation,
            ArchiveAccessService archiveAccessService,
            ArchivingServiceInternalImpl archivingService,
            ChannelMetaDataDAO channelMetaDataDAO,
            ControlSystemSupportRegistry controlSystemSupportRegistry,
            ExecutorService poolExecutor,
            ScheduledExecutorService scheduledExecutor, UUID thisServerId) {
        assert (configuration != null);
        assert (archiveAccessService != null);
        assert (archivingService != null);
        assert (channelMetaDataDAO != null);
        assert (controlSystemSupportRegistry != null);
        assert (poolExecutor != null);
        assert (scheduledExecutor != null);
        assert (thisServerId != null);
        this.configuration = configuration;
        this.pendingOperationOnInit = pendingOperation;
        this.archiveAccessService = archiveAccessService;
        this.archivingService = archivingService;
        this.channelMetaDataDAO = channelMetaDataDAO;
        this.controlSystemSupportRegistry = controlSystemSupportRegistry;
        this.poolExecutor = poolExecutor;
        this.scheduledExecutor = scheduledExecutor;
        this.thisServerId = thisServerId;
    }

    /**
     * Returns the configuration for this channel. This method can be called
     * without synchronizing on this channel and does not synchronize on this
     * channel either.
     * 
     * @return configuration for this channel.
     */
    public ChannelConfiguration getConfiguration() {
        return configuration;
    }

    /**
     * <p>
     * Creates a new sample bucket starting at the specified point in time. The
     * sample bucket must start after the start of the newest already existing
     * sample bucket. This method is only intended to be called by
     * {@link ArchivedChannelDecimationLevel}. Creating a sample bucket is
     * handled by the channel instead of the decimation level because it
     * involves registering a pending operation and there can only be one
     * asynchronous create-sample-bucket operation in progress at any point in
     * time.
     * </p>
     * 
     * <p>
     * The code calling this method has to synchronize on this channel instance.
     * </p>
     * 
     * @param decimationLevel
     *            the decimation level that is requesting the sample bucket to
     *            be created. This decimation level must belong to this channel.
     * @param newBucketStartTime
     *            start time of the new sample bucket. This start time must be
     *            strictly greater than the start time of any existing sample
     *            bucket for the specified decimation level.
     * @return future that completes when the sample bucket has been created or
     *         fails when creating the sample bucket has failed.
     */
    ListenableFuture<Void> createSampleBucket(
            final ArchivedChannelDecimationLevel<SampleType> decimationLevel,
            final long newBucketStartTime) {
        // This is a helper function only intended for use by
        // processSampleWriteQueues. We expect that the calling code holds a
        // lock on the channel when calling this method. This is important,
        // because we rely on synchronized access to data structures in the
        // channel object. In callbacks, however, we have to take care of taking
        // the mutex.
        assert (Thread.holdsLock(this));
        assert (decimationLevel != null);
        assert (newBucketStartTime >= 0L);
        // We do not want to run the operation for a channel that has been
        // destroyed or for which destruction has been requested. We check this
        // here, because this method might be called recursively by one of the
        // callbacks that we use later in this method.
        if (state.equals(ArchivedChannelState.DESTROYED)
                || destructionRequested) {
            return Futures
                    .immediateFailedFuture(new IllegalStateException(
                            "Destruction of the channel was requested before the create sample-bucket operation could finish."));
        }
        // If one or more operations to create a new sample bucket is already in
        // progress, we wait for this operation to finish. In theory, we could
        // run multiple operations to create a sample bucket in parallel, but
        // this would make the whole handling much more complicated. Creating a
        // sample bucket is a rare operation, so this situation is very unlikely
        // to happen anyway.
        if (createSampleBucketsFuture != null) {
            final SettableFuture<Void> future = SettableFuture.create();
            // We do not use the same-thread executor for our listener, because
            // we want to synchronize on the channel and depending on
            // implementation details outside our scope, this could lead to a
            // dead lock if it is executed by a thread that also holds a mutex
            // that we acquire while holding the channel mutex.
            // We cannot use Futures.transform(...) here, because we want to run
            // our operation even if the pending create-sample operation fails.
            createSampleBucketsFuture.addListener(new Runnable() {
                @Override
                public void run() {
                    ListenableFuture<Void> newFuture;
                    synchronized (ArchivedChannel.this) {
                        newFuture = createSampleBucket(decimationLevel,
                                newBucketStartTime);
                    }
                    FutureUtils.forward(newFuture, future);
                }
            }, poolExecutor);
            return future;
        }
        createSampleBucketsFuture = SettableFuture.create();
        final UUID operationId = UUID.randomUUID();
        final String channelName = configuration.getChannelName();
        StringBuilder operationDataBuilder = new StringBuilder();
        operationDataBuilder.append("{ decimationLevel: \"");
        operationDataBuilder.append(decimationLevel
                .getDecimationPeriodSeconds());
        operationDataBuilder.append("\", bucketStartTime: \"");
        operationDataBuilder.append(newBucketStartTime);
        operationDataBuilder.append("\" }, ");
        ListenableFuture<Pair<Boolean, UUID>> createPendingOperationFuture = channelMetaDataDAO
                .createPendingChannelOperation(
                        thisServerId,
                        channelName,
                        operationId,
                        PendingChannelOperationConstants.OPERATION_CREATE_SAMPLE_BUCKET,
                        operationDataBuilder.toString(),
                        PendingChannelOperationConstants.PENDING_OPERATION_TTL_SECONDS);
        AsyncFunction<Pair<Boolean, UUID>, Void> checkResultAndcreateSampleBucket = new AsyncFunction<Pair<Boolean, UUID>, Void>() {
            @Override
            public ListenableFuture<Void> apply(Pair<Boolean, UUID> input)
                    throws Exception {
                if (!input.getLeft()) {
                    // We could not create the pending operation because there
                    // is another pending operation. In theory, this should
                    // never happen because a channel is not initialized if
                    // there is a pending operation. However, if another create
                    // sample-bucket operation failed due to a transient
                    // problem, it is possible that there is still a pending
                    // operation registered in the database. There is not much
                    // we can do about this at this point, so we fail with an
                    // error.
                    throw new IllegalStateException(
                            "A new sample bucket for the channel cannot be created because there is another pending operation.");
                }
                synchronized (ArchivedChannel.this) {
                    // It is possible that the channel is supposed to be
                    // destroyed. However, we still finish creating the current
                    // sample bucket. This should not take long and when the
                    // channel is started again, this would be one of the first
                    // operations anyway. Besides, it makes the logic much
                    // simpler, because we do not have to maintain two separate
                    // branches in the asynchronous execution.
                    return decimationLevel
                            .createSampleBucket(newBucketStartTime);
                }
            }
        };
        // We do not use the same-thread executor for our transformation because
        // we want to synchronize on the channel and depending on implementation
        // details outside our scope, this could lead to a dead lock if it is
        // executed by a thread that also holds a mutex that we acquire while
        // holding the channel mutex.
        ListenableFuture<Void> createSampleBucketFuture = Futures.transform(
                createPendingOperationFuture, checkResultAndcreateSampleBucket,
                poolExecutor);
        AsyncFunction<Void, Void> deletePendingOperation = new AsyncFunction<Void, Void>() {
            @Override
            public ListenableFuture<Void> apply(Void input) throws Exception {
                // Our next actions do not depend on whether we could remove the
                // pending operation successfully. Therefore, we can convert to
                // void.
                return FutureUtils.transformAnyToVoid(channelMetaDataDAO
                        .deletePendingChannelOperation(thisServerId,
                                channelName, operationId));
            }
        };
        // We do not use the same-thread executor for our transformation because
        // we want to synchronize on the channel and depending on implementation
        // details outside our scope, this could lead to a dead lock if it is
        // executed by a thread that also holds a mutex that we acquire while
        // holding the channel mutex.
        final ListenableFuture<Void> deletePendingOperationFuture = Futures
                .transform(createSampleBucketFuture, deletePendingOperation,
                        poolExecutor);
        // We have to reset the createSampleBucketFuture to null before
        // triggering the callbacks that have registered with it. Otherwise, we
        // could end up in an infinite loop if one of the callbacks immediately
        // tries to register with the future again.
        // We do not use the same-thread executor for our transformation because
        // we want to synchronize on the channel and depending on implementation
        // details outside our scope, this could lead to a dead lock if it is
        // executed by a thread that also holds a mutex that we acquire while
        // holding the channel mutex.
        // We have to remove the createSampleBucketFuture, even if the operation
        // failed, so we cannot use a regular transformation. However, we have
        // to remove it before notifying regular listeners, so we cannot use
        // FutureUtils.forward() early because the result might be forwarded
        // before our listener is called.
        deletePendingOperationFuture.addListener(new Runnable() {
            @Override
            public void run() {
                SettableFuture<Void> targetFuture;
                synchronized (ArchivedChannel.this) {
                    targetFuture = createSampleBucketsFuture;
                    createSampleBucketsFuture = null;
                }
                try {
                    deletePendingOperationFuture.get();
                    targetFuture.set(null);
                } catch (Throwable t) {
                    targetFuture.setException(t);
                }
            }
        }, poolExecutor);
        return createSampleBucketsFuture;
    }

    /**
     * <p>
     * Destroys this channel. The destruction completes asynchronously so that
     * this method does not block. The returned future completes when this
     * channel has been destroyed. The returned future never throws an
     * exception. Destruction might be delayed when the channel's initialization
     * has not finished yet or when a sample bucket is being created
     * concurrently.
     * </p>
     * 
     * <p>
     * This method synchronizes on this channel and can thus be called without
     * synchronizing on this channel explicitly.
     * </p>
     * 
     * @return future that complete when this channel has been destroyed. The
     *         returned future never throws an exception.
     * @see #destroyWithException(Throwable)
     */
    ListenableFuture<Void> destroy() {
        return destroyInternal(null, false);
    }

    /**
     * <p>
     * Destroys this channel, storing the specified exception. The destruction
     * completes asynchronously so that this method does not block. The returned
     * future completes when this channel has been destroyed. The returned
     * future never throws an exception. Destruction might be delayed when the
     * channel's initialization has not finished yet or when a sample bucket is
     * being created concurrently.
     * </p>
     * 
     * <p>
     * After the channel has been destroyed, the stored exception can be
     * retrieved with the {@link #getError()} method. If destruction of the
     * channel has started before calling this method, the specified exception
     * is not going to be stored.
     * </p>
     * 
     * <p>
     * This method synchronizes on this channel and can thus be called without
     * synchronizing on this channel explicitly.
     * </p>
     * 
     * @return future that complete when this channel has been destroyed. The
     *         returned future never throws an exception.
     * @see #destroy()
     */
    ListenableFuture<Void> destroyWithException(Throwable error) {
        return destroyInternal(error, false);
    }

    /**
     * <p>
     * Returns the control-system channel that belongs to this channel object.
     * The control-system channel might be <code>null</code> if this channel has
     * not finished initialization yet, is not enabled, initialization has
     * failed, or this channel has already been destroyed.
     * </p>
     * 
     * <p>
     * The code calling this method has to synchronize on this channel instance.
     * </p>
     * 
     * @return control-system channel for this channel object or
     *         <code>null</code> if there is no control-system channel.
     */
    ControlSystemChannel getControlSystemChannel() {
        assert (Thread.holdsLock(this));
        return controlSystemChannel;
    }

    /**
     * <p>
     * Returns the control-system support for this channel instance. The
     * control-system support might be <code>null</code> if this channel has not
     * finished initialization yet or initialization has failed.
     * </p>
     * 
     * <p>
     * The code calling this method has to synchronize on this channel instance.
     * </p>
     * 
     * @return control-system support for this channel or <code>null</code> if
     *         the control-system support is not available.
     */
    ControlSystemSupport<SampleType> getControlSystemSupport() {
        assert (Thread.holdsLock(this));
        return controlSystemSupport;
    }

    /**
     * <p>
     * Returns the exception that was the cause of this channel being destroyed.
     * This is the exception that has been specified when calling
     * {@link #destroyWithException(Throwable)}.
     * </p>
     * 
     * <p>
     * The code calling this method has to synchronize on this channel instance.
     * </p>
     * 
     * @return stored exception or <code>null</code> if no exception has been
     *         stored (yet).
     */
    Throwable getError() {
        assert (Thread.holdsLock(this));
        return error;
    }

    /**
     * <p>
     * Returns the number of raw samples that have been dropped. Samples are
     * dropped when they stay in the queue for too long before being written.
     * Typically, this happens when samples arrive at a higher rate than the
     * rate at which they can be written to the database.
     * </p>
     * 
     * <p>
     * The code calling this method has to synchronize on this channel instance.
     * </p>
     * 
     * @return number of raw samples that have been dropped since creating this
     *         channel instance.
     */
    long getSamplesDropped() {
        assert (Thread.holdsLock(this));
        return samplesDropped;
    }

    /**
     * <p>
     * Returns the number of raw samples that skipped back in time. Samples that
     * skip back in time (have a time stamp that is less than or equal to the
     * time stamp of a sample that has already been written) are not written. If
     * this number keeps growing, this might indicate that there is a problem
     * with the clock source used for the raw samples of this channel.
     * </p>
     * 
     * <p>
     * The code calling this method has to synchronize on this channel instance.
     * </p>
     * 
     * @return number of raw samples that skipped back in time and have thus not
     *         been written (counted since creating this channel instance).
     */
    long getSamplesSkippedBack() {
        assert (Thread.holdsLock(this));
        return samplesSkippedBack;
    }

    /**
     * <p>
     * Returns the number of samples that have been written. This number
     * includes both raw and decimated samples. Only samples that have actually
     * been written to the database (the write operation has finished
     * successfully) are counted.
     * </p>
     * 
     * <p>
     * The code calling this method has to synchronize on this channel instance.
     * </p>
     * 
     * @return number of samples that have been writen since creating this
     *         channel instance.
     */
    long getSamplesWritten() {
        assert (Thread.holdsLock(this));
        return samplesWritten;
    }

    /**
     * <p>
     * Returns this channel's state. The state indicates whether this channel
     * has just been created and not initialized yet, has finished
     * initialization, or has been destroyed.
     * </p>
     * 
     * <p>
     * The code calling this method has to synchronize on this channel instance.
     * </p>
     * 
     * @return this channel's state.
     */
    ArchivedChannelState getState() {
        assert (Thread.holdsLock(this));
        return state;
    }

    /**
     * <p>
     * Increments the number of samples that have been dropped by the specified
     * number. The counter increased by this method is the one that can be read
     * using the {@link #getSamplesDropped()} method. As a side effect, this
     * method also calls the
     * {@link ArchivingServiceInternalImpl#incrementNumberOfSamplesDropped(long)}
     * method. This method is only intended to be called by the
     * {@link ArchivedChannelDecimationLevel} instances belonging to this
     * channel.
     * </p>
     * 
     * <p>
     * The code calling this method has to synchronize on this channel instance.
     * </p>
     * 
     * @param increment
     *            number by which the counter tracking the number of dropped
     *            samples should be increased.
     */
    void incrementSamplesDropped(long increment) {
        assert (Thread.holdsLock(this));
        assert (increment >= 0L);
        samplesDropped += increment;
        archivingService.incrementNumberOfSamplesDropped(increment);
    }

    /**
     * <p>
     * Increments the number of samples that have been skipped by the specified
     * number. The counter increased by this method is the one that can be read
     * using the {@link #getSamplesSkippedBack()} method. This method is only
     * intended to be called by the {@link ArchivedChannelDecimationLevel}
     * instances belonging to this channel.
     * </p>
     * 
     * <p>
     * The code calling this method has to synchronize on this channel instance.
     * </p>
     * 
     * @param increment
     *            number by which the counter tracking the number of samples
     *            that skipped back in time should be increased.
     */
    void incrementSamplesSkippedBack(long increment) {
        assert (Thread.holdsLock(this));
        assert (increment >= 0L);
        samplesSkippedBack += increment;
    }

    /**
     * <p>
     * Increments the number of samples that have been written by the specified
     * number. The counter increased by this method is the one that can be read
     * using the {@link #getSamplesWritten()} method. As a side effect, this
     * method also calls the
     * {@link ArchivingServiceInternalImpl#incrementNumberOfSamplesWritten(long)}
     * method. This method is only intended to be called by the
     * {@link ArchivedChannelDecimationLevel} instances belonging to this
     * channel.
     * </p>
     * 
     * <p>
     * The code calling this method has to synchronize on this channel instance.
     * </p>
     * 
     * @param increment
     *            number by which the counter tracking the number of written
     *            samples should be increased.
     */
    void incrementSamplesWritten(long increment) {
        assert (Thread.holdsLock(this));
        assert (increment >= 0L);
        samplesWritten += increment;
        archivingService.incrementNumberOfSamplesWritten(increment);
    }

    /**
     * <p>
     * Initializes this channel. This means that the
     * {@link ArchivedChannelDecimationLevel} objects for this channel are
     * created and the {@link ControlSystemChannel} is initialized (the latter
     * only if the channel is enabled). The initialization process completes
     * asynchronously, so this method does not block. After the initialization
     * process has finished, the channel is in the <code>INITIALIZED</code>
     * state or, if initialization failed, in the <code>DESTROYED</code> state.
     * The returned future never throws, even if there is an error during
     * initialization.
     * </p>
     * 
     * <p>
     * This method must never be called more than once. The code calling this
     * method must synchronize on this channel object.
     * </p>
     * 
     * @return future that completes when initialization has finished and the
     *         channel is either in the <code>INITIALIZED</code> or
     *         <code>DESTROYED</code> state. The returned future never throws an
     *         exception.
     */
    ListenableFuture<Void> initialize() {
        assert (Thread.holdsLock(this));
        // We only initialize the channel if it needs to be initialized. If it
        // has already been destroyed, there is no need to continue. We do not
        // wait for the creation of the control-system channel to finish
        // because depending on the control system, this might take a
        // considerable amount of time and we do not want to hold the mutex for
        // the channel for an extended amount of time. Instead, we return a
        // future that completes when we finally got the control-system channel
        // and finishes the initialization.
        assert (!state.equals(ArchivedChannelState.INITIALIZED));
        // It might happen that the channel has been destroyed before this
        // method gets called, so we only continue if we are still in the
        // initializing state.
        if (!state.equals(ArchivedChannelState.INITIALIZING)) {
            return VOID_SUCCESS;
        }
        if (pendingOperationOnInit != null) {
            // If the pending operation is only protective, we want to try again
            // in a few seconds because most likely it will have expired by
            // then.
            if (pendingOperationOnInit.getOperationType().equals(
                    PendingChannelOperationConstants.OPERATION_PROTECTIVE)) {
                scheduledExecutor
                        .schedule(
                                new Runnable() {
                                    @Override
                                    public void run() {
                                        archivingService
                                                .refreshChannel(pendingOperationOnInit
                                                        .getChannelName());
                                    }
                                },
                                PendingChannelOperationConstants.PROTECTIVE_PENDING_OPERATION_TTL_SECONDS + 1L,
                                TimeUnit.SECONDS);
            }
            return destroyWithException(new IllegalStateException(
                    "The channel cannot be initialized because an operation of type \""
                            + StringEscapeUtils
                                    .escapeJava(pendingOperationOnInit
                                            .getOperationType())
                            + "\" is pending."));
        }
        // Even if the channel is not enabled, we need the control-system
        // support to access samples.
        // We can safely use an unchecked cast here because the actual type of
        // the control-system support does not matter as long as it is
        // consistent. We only use one control-system support for each channel,
        // so regardless of the type it actually has, we will always use samples
        // of the type supported by this control-system support.
        @SuppressWarnings("unchecked")
        ControlSystemSupport<SampleType> uncheckedControlSystemSupport = (ControlSystemSupport<SampleType>) controlSystemSupportRegistry
                .getControlSystemSupport(configuration.getControlSystemType());
        controlSystemSupport = uncheckedControlSystemSupport;
        if (controlSystemSupport == null) {
            return destroyWithException(new IllegalArgumentException(
                    "Control system support \""
                            + StringEscapeUtils.escapeJava(configuration
                                    .getControlSystemType())
                            + "\" could not be found."));
        }
        // For each decimation level that exists for the channel, we have to
        // create the corresponding object. We have to do this before creating
        // the control-system channel because the sample listener depends on the
        // raw decimation-level object being available.
        for (Map.Entry<Integer, Long> decimationLevelPeriodAndCurrentBucketStartTime : configuration
                .getDecimationLevelToCurrentBucketStartTime().entrySet()) {
            int decimationPeriod = decimationLevelPeriodAndCurrentBucketStartTime
                    .getKey();
            // We have to find the decimation period of the source decimation
            // level (unless this is the decimation level for raw samples). The
            // source decimation level is the decimation level which is used as
            // a source for samples when generating decimated samples. When
            // there is no suitable decimation level, we can always use raw
            // samples.
            int sourceDecimationPeriod = RAW_SAMPLES_DECIMATION_PERIOD;
            ArchivedChannelDecimationLevel<SampleType> decimationLevel;
            if (decimationPeriod == RAW_SAMPLES_DECIMATION_PERIOD) {
                // The current bucket start time is never null. If there is no
                // bucket yet, it is minus one.
                decimationLevel = new ArchivedChannelRawSamplesDecimationLevel<SampleType>(
                        configuration.getDecimationLevelToRetentionPeriod()
                                .get(decimationPeriod),
                        decimationLevelPeriodAndCurrentBucketStartTime
                                .getValue(), archivingService, this,
                        channelMetaDataDAO, poolExecutor, scheduledExecutor,
                        thisServerId);
            } else {
                for (int possibleSourceDecimationPeriod : configuration
                        .getDecimationLevelToCurrentBucketStartTime().keySet()) {
                    // We want to use the decimation level with the greatest
                    // decimation period that is less than the decimation period
                    // of the target decimation level and that is an integer
                    // fraction of the target decimation level (for alignment
                    // reasons).
                    if (possibleSourceDecimationPeriod > sourceDecimationPeriod
                            && possibleSourceDecimationPeriod < decimationPeriod
                            && decimationPeriod
                                    % possibleSourceDecimationPeriod == 0) {
                        sourceDecimationPeriod = possibleSourceDecimationPeriod;
                    }
                }
                // The current bucket start time is never null. If there is no
                // bucket yet, it is minus one.
                decimationLevel = new ArchivedChannelDecimatedSamplesDecimationLevel<SampleType>(
                        decimationPeriod, configuration
                                .getDecimationLevelToRetentionPeriod().get(
                                        decimationPeriod),
                        sourceDecimationPeriod,
                        decimationLevelPeriodAndCurrentBucketStartTime
                                .getValue(), archiveAccessService,
                        archivingService, this, channelMetaDataDAO,
                        poolExecutor, scheduledExecutor, thisServerId);
            }
            decimationLevels.put(
                    decimationLevelPeriodAndCurrentBucketStartTime.getKey(),
                    decimationLevel);
        }
        // After creating the data-structures for all decimation levels, we have
        // to create the links between them: Each decimation level needs to know
        // which decimation level it uses as a source for generating decimated
        // samples. At the same time, each decimation level (including the raw
        // samples) needs to know which decimation levels use it as a source
        // (the so called "target decimation-levels"). This means that there is
        // a circular dependency which is why we cannot resolve this before
        // constructing the objects.
        for (ArchivedChannelDecimationLevel<SampleType> decimationLevel : decimationLevels
                .values()) {
            // The raw decimation level does not have any source
            // decimation-levels, so we can skip it.
            if (decimationLevel.getDecimationPeriodSeconds() == RAW_SAMPLES_DECIMATION_PERIOD) {
                continue;
            }
            ArchivedChannelDecimatedSamplesDecimationLevel<SampleType> decimatedSamplesDecimationLevel = (ArchivedChannelDecimatedSamplesDecimationLevel<SampleType>) decimationLevel;
            ArchivedChannelDecimationLevel<SampleType> sourceDecimationLevel = decimationLevels
                    .get(decimatedSamplesDecimationLevel
                            .getSourceDecimationPeriodSeconds());
            decimatedSamplesDecimationLevel
                    .setSourceDecimationLevel(sourceDecimationLevel);
            sourceDecimationLevel
                    .addTargetDecimationLevel(decimatedSamplesDecimationLevel);
        }
        if (configuration.isEnabled()) {
            try {
                Long currentBucketStartTime = configuration
                        .getDecimationLevelToCurrentBucketStartTime().get(
                                RAW_SAMPLES_DECIMATION_PERIOD);
                // If createChannel throws an exception (it should not), the
                // surrounding try-catch block will catch this exception and put
                // the channel into an error state.
                final ListenableFuture<? extends ControlSystemChannel> controlSystemChannelFuture = controlSystemSupport
                        .createChannel(
                                configuration.getChannelName(),
                                configuration.getOptions(),
                                (currentBucketStartTime != null && currentBucketStartTime >= 0L) ? new SampleBucketId(
                                        configuration.getChannelDataId(),
                                        RAW_SAMPLES_DECIMATION_PERIOD,
                                        currentBucketStartTime) : null,
                                ((ArchivedChannelRawSamplesDecimationLevel<SampleType>) decimationLevels
                                        .get(RAW_SAMPLES_DECIMATION_PERIOD))
                                        .getSampleListener());
                // createChannel should not return null either.
                if (controlSystemChannelFuture == null) {
                    throw new IllegalArgumentException(
                            "The control-system support's createChannel method returned null.");
                }
                initializationFuture = SettableFuture.create();
                // We do not use the same-thread executor for our callback
                // because we want to synchronize on the channel and depending
                // on implementation details outside our scope, this could lead
                // to a dead lock if it is executed by a thread that also holds
                // a mutex that we acquire while holding the channel mutex.
                controlSystemChannelFuture.addListener(new Runnable() {
                    @Override
                    public void run() {
                        synchronized (ArchivedChannel.this) {
                            // We have to reset the initializationFuture to null
                            // because code that gets called when completing the
                            // future might check whether it is null.
                            SettableFuture<Void> localInitializationFuture = initializationFuture;
                            initializationFuture = null;
                            try {
                                ControlSystemChannel controlSystemChannel = FutureUtils
                                        .getUnchecked(controlSystemChannelFuture);
                                if (controlSystemChannel == null) {
                                    throw new IllegalArgumentException(
                                            "The control-system support returnd null when requested to create the channel.");
                                }
                                try {
                                    initializeWithControlSystemSupport(controlSystemChannel);
                                } finally {
                                    localInitializationFuture.set(null);
                                }
                            } catch (Throwable t) {
                                // We do not want the returned future to fail
                                // because the initialization completed even if
                                // the channel was not initialized completely.
                                // If destruction has started, the destroy
                                // process waits for the initializationFuture.
                                // If we chained it to the destruction future,
                                // we would effectively create a dead lock and
                                // neither future would ever complete.
                                if (destructionRequested) {
                                    destroyInternalDuringInitialization();
                                } else {
                                    FutureUtils.forward(
                                            destroyWithException(t),
                                            localInitializationFuture);
                                }
                            }
                        }
                    }
                }, poolExecutor);
                return initializationFuture;
            } catch (Throwable t) {
                return destroyWithException(t);
            }
        } else {
            try {
                Long currentBucketStartTime = configuration
                        .getDecimationLevelToCurrentBucketStartTime().get(
                                RAW_SAMPLES_DECIMATION_PERIOD);
                // If generateChannelDisabledSample throws an exception (it
                // should not), the surrounding try-catch block will catch this
                // exception and put the channel into an error state.
                final ListenableFuture<SampleWithSizeEstimate<SampleType>> disabledChannelSampleFuture = controlSystemSupport
                        .generateChannelDisabledSample(
                                configuration.getChannelName(),
                                configuration.getOptions(),
                                (currentBucketStartTime != null && currentBucketStartTime >= 0L) ? new SampleBucketId(
                                        configuration.getChannelDataId(),
                                        RAW_SAMPLES_DECIMATION_PERIOD,
                                        currentBucketStartTime) : null);
                // generateChannelDisabledSample should not return null either.
                if (disabledChannelSampleFuture == null) {
                    throw new IllegalArgumentException(
                            "The control-system support's generateDisabledChannelSample method returned null.");
                }
                initializationFuture = SettableFuture.create();
                // We do not use the same-thread executor for our callback
                // because we want to synchronize on the channel and depending
                // on implementation details outside our scope, this could lead
                // to a dead lock if it is executed by a thread that also holds
                // a mutex that we acquire while holding the channel mutex.
                disabledChannelSampleFuture.addListener(new Runnable() {
                    @Override
                    public void run() {
                        synchronized (ArchivedChannel.this) {
                            // We have to reset the initializationFuture to null
                            // because code that gets called when completing the
                            // future might check whether it is null.
                            SettableFuture<Void> localInitializationFuture = initializationFuture;
                            initializationFuture = null;
                            try {
                                SampleWithSizeEstimate<SampleType> sampleWithSizeEstimate = FutureUtils
                                        .getUnchecked(disabledChannelSampleFuture);
                                try {
                                    initializeWithControlSystemSupport(null);
                                    if (sampleWithSizeEstimate != null) {
                                        // We do not have a control-system
                                        // channel, so we cannot go the regular
                                        // way of using the sample listener. On
                                        // the other hand, we do not have to
                                        // worry about locks here (this code is
                                        // not called by the control-system
                                        // support), so we can directly add the
                                        // sample to the queue.
                                        ArchivedChannelRawSamplesDecimationLevel<SampleType> rawDecimationLevel = ((ArchivedChannelRawSamplesDecimationLevel<SampleType>) decimationLevels
                                                .get(RAW_SAMPLES_DECIMATION_PERIOD));
                                        rawDecimationLevel
                                                .addSampleToWriteQueue(
                                                        sampleWithSizeEstimate
                                                                .getSample(),
                                                        sampleWithSizeEstimate
                                                                .getEstimatedSampleSize());
                                        archivingService
                                                .addChannelDecimationLevelToWriteProcessingQueue(rawDecimationLevel);
                                    }
                                } finally {
                                    localInitializationFuture.set(null);
                                }
                            } catch (Throwable t) {
                                // We do not want the returned future to fail
                                // because the initialization completed even if
                                // the channel was not initialized completely.
                                // If destruction has started, the destroy
                                // process waits for the initializationFuture.
                                // If we chained it to the destruction future,
                                // we would effectively create a dead lock and
                                // neither future would ever complete.
                                if (destructionRequested) {
                                    destroyInternalDuringInitialization();
                                } else {
                                    FutureUtils.forward(
                                            destroyWithException(t),
                                            localInitializationFuture);
                                }
                            }
                        }
                    }
                }, poolExecutor);
                return initializationFuture;
            } catch (Throwable t) {
                return destroyWithException(t);
            }
        }
    }

    /**
     * <p>
     * Tells whether destruction has requested. This means that either
     * {@link #destroy()} or {@link #destroyWithException(Throwable)} has been
     * called. Please note that destruction completes asynchronously, so this
     * flag might be set, but the channel might not have been destroyed yet.
     * </p>
     * 
     * <p>
     * This method is mainly provided for the benefit of the
     * {@link ArchivedChannelDecimationLevel} objects that belong to this
     * channel. The objects might want to check whether destruction has started
     * so that they do not start certain asynchronous actions any longer.
     * </p>
     * 
     * <p>
     * The code calling this method has to synchronize on this channel instance.
     * </p>
     * 
     * @return <code>true</code> if destruction of this channel has started,
     *         <code>false</code> if destruction has not been requested yet.
     */
    boolean isDestructionRequested() {
        assert (Thread.holdsLock(this));
        return destructionRequested;
    }

    private ListenableFuture<Void> destroyInternal(final Throwable error,
            boolean runAgain) {
        synchronized (this) {
            if (destructionRequested && !runAgain) {
                return destructionFuture;
            }
            destructionRequested = true;
            if (state.equals(ArchivedChannelState.DESTROYED)) {
                return destructionFuture;
            }
            // When we are in the process of creating a new sample bucket (which
            // is an asynchronous operation and might thus run while we hold the
            // channel mutex), we have to wait for this operation to finish.
            // Creating a sample bucket modifies some sensitive data structures
            // in the database and reinitializing a channel while such an
            // operation is in progress could lead to serious inconsistencies in
            // the stored data. If creating the sample bucket fails and we can
            // thus not be sure about the current state of the database, this is
            // not too bad because the create-sample-bucket operation will leave
            // a pending operation that will stop this channel from being
            // started again until enough time has passed so that we can assume
            // that the data in the database is consistent again.
            if (createSampleBucketsFuture != null) {
                createSampleBucketsFuture.addListener(new Runnable() {
                    @Override
                    public void run() {
                        destroyInternal(error, true);
                    }
                }, MoreExecutors.sameThreadExecutor());
                return destructionFuture;
            }
            // The initialization is much less critical than creating a sample
            // bucket. There are two reasons, why we are still waiting for the
            // initialization to complete before destroying this channel. First,
            // it ensures that there is only one ControlSystemChannel for each
            // channel name at a time. This is not extremely critical, because
            // we have a few safe-guards for the case when there is more than
            // one, but it is still cleaner. Second, it ensures that the newly
            // created ControlSystemChannel is actually destroyed and we do not
            // leave a dangling ControlSystemChannel around that might never be
            // destroyed.
            if (initializationFuture != null) {
                initializationFuture.addListener(new Runnable() {
                    @Override
                    public void run() {
                        destroyInternal(error, true);
                    }
                }, MoreExecutors.sameThreadExecutor());
                return destructionFuture;
            }
            // Strictly speaking, writing a sample is similar to creating a
            // sample bucket, so in theory we would have to wait for all
            // asynchronous write operations to be finished before destroying
            // the channel. However, we do not do this for two reasons. First,
            // it would make both the code for writing sample and the code for
            // destroying the channel much more complex. For different
            // decimation levels, samples can be written in parallel, so we
            // would need a lot of logic for coordinating these concurrent
            // operations. More importantly, there would be no clear path if one
            // of the write operations failed: A write operation is not
            // protected by a "pending operation" in the database, so in case of
            // a server restart, we could see an inconsistent state anyway.
            // Luckily, the inconsistencies possibly caused by an incomplete or
            // asynchronously finishing write operation are much less severe
            // than for a create-sample-bucket operation. Basically, there are
            // three scenarios that we have to consider:
            // 1) After the reinitialization of the channel, when we get the
            // sample-bucket state, we already see the written samples. This is
            // the regular (expected) case and obviously there is no problem in
            // this case.
            // 2) The write operation has failed and the sample has not been
            // written. This means we do not see it when read the sample-bucket
            // state. Obviously, this is fine as well because the sample will
            // never be seen and thus we have the same situation as if it was
            // never written.
            // 3) The write operation completes asynchronously or finally
            // succeeds after it has failed initially. The later case is
            // possible because of how Cassandra achieves consistency. In this
            // case, the exact consequences depend on the next sample that is
            // written.
            // a) The next sample is the same sample (with the same time-stamp).
            // In this case, the sample will effectively be overwritten with the
            // same data and everything is consistent.
            // b) The next sample is a different sample with the same
            // time-stamp. In this case, the decimation process might use the
            // first sample as a source sample but the second one gets stored in
            // the database. This is clearly not optimal, but it should not be a
            // major problem.
            // c) The next sample is a different sample with an earlier
            // time-stamp. This is very similar to b) with the difference that
            // the original sample might become "invisible" when it is close the
            // edge of a sample bucket and the next sample bucket is created in
            // a way that the original sample bucket does not cover the
            // time-stamp of the asynchronously written sample any longer.
            // Regarding sample decimation, this can mean that the sample is
            // used for decimation, but not visible later or not used for
            // decimation, but visible later. This is virtually the same
            // situation as with b).
            // d) The next sample is a different sample with a later time-stamp.
            // In this case, the only consequence is that the decimation process
            // might not use the original sample while it is present in the
            // database. Again, this is very similar to b) and not a reason for
            // major concern.
            // Of course, all the scenarios b) to d) have the consequence that
            // the sample-bucket size might be off by the size of one sample,
            // but this should typically not be a problem.
            // In summary, waiting for the write-sample operations to be
            // completed would not fix all possible scenarios (in the case of a
            // failed operation or a server restart), but is not critical
            // either. For these reasons, we completely skip the check for
            // asynchronous write operations.
            state = ArchivedChannelState.DESTROYED;
            if (error != null) {
                this.error = error;
            }
            if (controlSystemChannel != null) {
                try {
                    controlSystemChannel.destroy();
                } catch (Throwable e) {
                    // If the destroy method throws an exception, this should
                    // not keep us from destroying other channels.
                    log.error(
                            "Destruction of channel \""
                                    + StringEscapeUtils
                                            .escapeJava(configuration
                                                    .getChannelName()
                                                    + "\" failed: "
                                                    + e.getMessage()), e);
                }
            }
            controlSystemChannel = null;
            destructionFuture.set(null);
            return destructionFuture;
        }
    }

    private void destroyInternalDuringInitialization() {
        assert (Thread.holdsLock(this));
        // This method should only be used when we are in the initializing state
        // and destruction has already been requested.
        assert (state.equals(ArchivedChannelState.INITIALIZING));
        assert (destructionRequested);
        state = ArchivedChannelState.DESTROYED;
        if (controlSystemChannel != null) {
            try {
                controlSystemChannel.destroy();
            } catch (Throwable e) {
                // If the destroy method throws an exception, this should
                // not keep us from destroying other channels.
                log.error(
                        "Destruction of channel \""
                                + StringEscapeUtils.escapeJava(configuration
                                        .getChannelName()
                                        + "\" failed: "
                                        + e.getMessage()), e);
            }
        }
        controlSystemChannel = null;
        // When this method is called, it implies that initialization has
        // finished (though it might not have finished successfully).
        initializationFuture.set(null);
        destructionFuture.set(null);
    }

    private void initializeWithControlSystemSupport(
            ControlSystemChannel controlSystemChannel) {
        // This method should only be called while holding the mutex for the
        // channel.
        assert (Thread.holdsLock(this));
        // The controlSystemChannel might be null if the channel is not enabled,
        // but in this case we will just replace null with null.
        this.controlSystemChannel = controlSystemChannel;
        state = ArchivedChannelState.INITIALIZED;
        // For each decimation level, we want to check whether there are sample
        // buckets that can be deleted. Usually, this will be done when the
        // first sample is written, but if no sample is written for a decimation
        // period, this might never happen. For this reason, we want to do this
        // once after the channel has been initialized. We use a random delay so
        // that all these actions do not run at the same time. A delay of up to
        // 15 minutes looks like a reasonable compromise between not waiting to
        // long and distributing the load.
        // We also have to check whether we need to generate decimated samples.
        // Unlike the removal process, this task should be run as soon as
        // possible because not having up-to-date decimated samples might be a
        // problem. Of course, we do not generate decimated samples for the
        // decimation level that stores raw samples.
        Random random = new Random();
        for (final ArchivedChannelDecimationLevel<SampleType> decimationLevel : decimationLevels
                .values()) {
            if (decimationLevel.getDecimationPeriodSeconds() != RAW_SAMPLES_DECIMATION_PERIOD) {
                ((ArchivedChannelDecimatedSamplesDecimationLevel<SampleType>) decimationLevel)
                        .scheduleGenerateDecimatedSamples();
            }
            decimationLevel.scheduleRemoveOldSampleBuckets(
                    random.nextInt(900000), TimeUnit.MILLISECONDS);
        }
    }

}
