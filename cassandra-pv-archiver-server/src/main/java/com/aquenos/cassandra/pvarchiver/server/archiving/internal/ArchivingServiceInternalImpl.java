/*
 * Copyright 2015-2017 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.archiving.internal;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.aquenos.cassandra.pvarchiver.controlsystem.ControlSystemChannel;
import com.aquenos.cassandra.pvarchiver.controlsystem.ControlSystemChannelStatus;
import com.aquenos.cassandra.pvarchiver.controlsystem.Sample;
import com.aquenos.cassandra.pvarchiver.server.archiving.ArchiveAccessService;
import com.aquenos.cassandra.pvarchiver.server.archiving.ArchivingService;
import com.aquenos.cassandra.pvarchiver.server.archiving.ChannelStatus;
import com.aquenos.cassandra.pvarchiver.server.cluster.ClusterManagementService;
import com.aquenos.cassandra.pvarchiver.server.controlsystem.ControlSystemSupportRegistry;
import com.aquenos.cassandra.pvarchiver.server.database.CassandraProvider;
import com.aquenos.cassandra.pvarchiver.server.database.ChannelMetaDataDAO;
import com.aquenos.cassandra.pvarchiver.server.database.ChannelMetaDataDAO.ChannelConfiguration;
import com.aquenos.cassandra.pvarchiver.server.database.ChannelMetaDataDAO.ChannelOperation;
import com.aquenos.cassandra.pvarchiver.server.spring.ThrottlingProperties;
import com.aquenos.cassandra.pvarchiver.server.util.FutureUtils;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.MoreFutures;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;

/**
 * <p>
 * Internal implementation used by the {@link ArchivingService}. This
 * implementation has been separated into a separate class so that this class
 * can be in the <code>internal</code> package and access the
 * {@link ArchivedChannel} and {@link ArchivedChannelDecimationLevel} classes
 * that are package private.
 * </p>
 * 
 * <p>
 * Instances of this class should only be created by the
 * {@link ArchivingService}. The package-private methods in this class are only
 * intended for the {@link ArchivedChannel} and
 * {@link ArchivedChannelDecimationLevel} objects created by this object.
 * </p>
 * 
 * <p>
 * Due to the fact this class is intended for internal use only, it generally
 * does not validate its input parameters or its state. It only uses assertions
 * to check that preconditions hold, but it is the task of the calling code to
 * ensure that they are not violated.
 * </p>
 * 
 * @author Sebastian Marsching
 */
public class ArchivingServiceInternalImpl {

    private static final ListenableFuture<Void> VOID_SUCCESS = Futures
            .immediateFuture(null);

    /**
     * Logger for this class.
     */
    protected final Log log = LogFactory.getLog(getClass());

    private CassandraProvider cassandraProvider;
    private ChannelMetaDataDAO channelMetaDataDAO;
    private ThrottledArchiveAccessService throttledArchiveAccessService;
    private LinkedBlockingUniqueQueue<ArchivedChannelDecimationLevel<?>> channelDecimationLevelsWithPendingWriteRequests = new LinkedBlockingUniqueQueue<ArchivedChannelDecimationLevel<?>>();
    private ConcurrentMap<String, SettableFuture<Void>> channelsNeedingRefresh = new ConcurrentHashMap<String, SettableFuture<Void>>();
    private AtomicLong numberOfSamplesDropped = new AtomicLong();
    private AtomicLong numberOfSamplesWritten = new AtomicLong();
    private ExecutorService poolExecutor;
    private ControlSystemSupportRegistry controlSystemSupportRegistry;
    private volatile boolean destroyed;
    private ScheduledExecutorService scheduledExecutor;
    // The session field is only accessed from the write thread, so we do not
    // have to make it volatile.
    private Session session;
    private AtomicReference<ArchivingServiceState> state = new AtomicReference<ArchivingServiceState>(
            new ArchivingServiceState(false, false,
                    ImmutableSortedMap.<String, ArchivedChannel<?>> of()));
    private UUID thisServerId;
    private ExecutorService writeSampleExecutor;

    /**
     * Creates an internal implementation of the archiving service. This
     * constructor should only be used by the {@link ArchivingService}.
     * 
     * @param archiveAccessService
     *            archive access service used for reading samples. This service
     *            is used when generating decimated samples (if there is at
     *            least one channel with a decimation level storing decimated
     *            samples).
     * @param cassandraProvider
     *            Cassandra provider that is used to get access to the database.
     *            In particular, the Cassandra provider is used to pause the
     *            write thread until the database connection is available.
     * @param channelMetaDataDAO
     *            channel meta-data DAO used for accessing channel meta-data. In
     *            particular, this DAO is used for accessing channel
     *            configurations, information about sample buckets, creating
     *            pending channel operations, and creating sample buckets.
     * @param controlSystemSupportRegistry
     *            control-system support registry that is used for retrieving
     *            the control-system supports needed by the channels that are
     *            managed by this service.
     * @param throttlingProperties
     *            configuration options that control throttling. This used for
     *            limiting source sample fetch operations when generating
     *            decimated samples.
     * @param thisServerId
     *            unique identifier identifying this archive server instance.
     */
    public ArchivingServiceInternalImpl(
            ArchiveAccessService archiveAccessService,
            CassandraProvider cassandraProvider,
            ChannelMetaDataDAO channelMetaDataDAO,
            ControlSystemSupportRegistry controlSystemSupportRegistry,
            ThrottlingProperties throttlingProperties, UUID thisServerId) {
        Preconditions.checkNotNull(archiveAccessService);
        Preconditions.checkNotNull(cassandraProvider);
        Preconditions.checkNotNull(channelMetaDataDAO);
        Preconditions.checkNotNull(controlSystemSupportRegistry);
        Preconditions.checkNotNull(thisServerId);
        this.cassandraProvider = cassandraProvider;
        this.channelMetaDataDAO = channelMetaDataDAO;
        this.controlSystemSupportRegistry = controlSystemSupportRegistry;
        this.thisServerId = thisServerId;
        // We use daemon threads because we do not want any of our threads to
        // stop the JVM from shutdown.
        ThreadFactory poolThreadFactory = new BasicThreadFactory.Builder()
                .daemon(true).namingPattern("archiving-service-pool-thread-%d")
                .build();
        ThreadFactory scheduledExecutorThreadFactory = new BasicThreadFactory.Builder()
                .daemon(true)
                .namingPattern("archiving-service-scheduled-executor-thread-%d")
                .build();
        ThreadFactory writeThreadFactory = new BasicThreadFactory.Builder()
                .daemon(true).namingPattern("archiving-service-write-thread-%d")
                .build();
        // We use the number of available processors as the number of threads.
        // This is somehow inaccurate (for example, we might create too many
        // threads for processors with hyper threading), but this should not
        // hurt us too much. Even if we detect only a single processor, we want
        // to create at least four threads so that in case of a blocking thread
        // some other tasks still have a chance to run. On the other hand, we
        // limit the pool size to 16 threads so that we do not create too many
        // threads on a machine with a very large number of cores. As the core
        // pool size is always zero, we will not create threads if we do not
        // need them.
        int numberOfPoolThreads = Runtime.getRuntime().availableProcessors();
        numberOfPoolThreads = Math.max(numberOfPoolThreads, 4);
        numberOfPoolThreads = Math.min(numberOfPoolThreads, 16);
        // We use a rejected execution handler that executes tasks in the
        // calling thread. As we have an unbounded queue, this handler will
        // typically not be used. When the executor is shutdown, however, this
        // handler will ensure that tasks that are queued later are still
        // executed. This is of advantage because we are using the executor for
        // processing future callbacks and not executing those could lead to
        // chained futures to never complete. This somehow defies our safeguards
        // for avoiding dead locks, but as this is only done during shutdown, we
        // are not that afraid of a dead lock (the application is going to be
        // killed anyway).
        poolExecutor = new ThreadPoolExecutor(0, numberOfPoolThreads, 60L,
                TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(),
                poolThreadFactory, new RejectedExecutionHandler() {
                    @Override
                    public void rejectedExecution(Runnable r,
                            ThreadPoolExecutor executor) {
                        try {
                            r.run();
                        } catch (Throwable t) {
                            // We ignore any exception thrown by a task because
                            // we do not want it to bubble up into the code that
                            // scheduled the task.
                        }
                    }
                }) {
            @Override
            protected void afterExecute(Runnable r, Throwable t) {
                super.afterExecute(r, t);
                // When some code submitted a runnable that also implements
                // future, there is no guarantee that this future is done, so we
                // can only call the get() method when it is actually done. If
                // we did not do this check, we could cause a deadlock if the
                // future is completed when the task is completed and the
                // completion of the task is blocked because we are waiting for
                // the future.
                if (t == null && r instanceof Future
                        && ((Future<?>) r).isDone()) {
                    try {
                        ((Future<?>) r).get();
                    } catch (CancellationException e) {
                        t = e;
                    } catch (ExecutionException e) {
                        t = e.getCause();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt(); // ignore/reset
                    }
                }
                if (t != null) {
                    String message = t.getMessage();
                    log.error("Task failed"
                            + (message == null ? "." : (": " + message)), t);
                }
            }
        };
        // We do not attempt any long running operations from our scheduled
        // executor (we have the poolExecutor for this). Therefore, a single
        // thread is sufficient.
        scheduledExecutor = new ScheduledThreadPoolExecutor(1,
                scheduledExecutorThreadFactory) {
            @Override
            protected void afterExecute(Runnable r, Throwable t) {
                super.afterExecute(r, t);
                // When some code submitted a runnable that also implements
                // future, there is no guarantee that this future is done, so we
                // can only call the get() method when it is actually done. If
                // we did not do this check, we could cause a deadlock if the
                // future is completed when the task is completed and the
                // completion of the task is blocked because we are waiting for
                // the future.
                if (t == null && r instanceof Future
                        && ((Future<?>) r).isDone()) {
                    try {
                        ((Future<?>) r).get();
                    } catch (CancellationException e) {
                        t = e;
                    } catch (ExecutionException e) {
                        t = e.getCause();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt(); // ignore/reset
                    }
                }
                if (t != null) {
                    String message = t.getMessage();
                    log.error("Task failed"
                            + (message == null ? "." : (": " + message)), t);
                }
            }
        };
        throttledArchiveAccessService = new ThrottledArchiveAccessService(
                archiveAccessService, this.poolExecutor,
                throttlingProperties.getSampleDecimation()
                        .getMaxRunningFetchOperations(),
                throttlingProperties.getSampleDecimation()
                        .getMaxFetchedSamplesInMemory());
        // We use a separate executor service for running the write-sample
        // thread. Unlike the other tasks, this thread will block waiting on new
        // samples until it is interrupted, so it would definitely block one of
        // the pool threads forever.
        writeSampleExecutor = Executors
                .newSingleThreadExecutor(writeThreadFactory);
    }

    /**
     * Destroys this archiving service. This method is called when the
     * {@link ArchivingService} that created this instance is destroyed. This
     * method blocks until the destruction has completed.
     */
    public void destroy() {
        // Typically, this method should not be called twice, but if it is this
        // should still not cause any problems.
        destroyed = true;
        // We want to stop the write-sample thread. We have to use shutdownNow()
        // so that the thread is interrupted.
        writeSampleExecutor.shutdownNow();
        scheduledExecutor.shutdownNow();
        switchOffline();
        // switchOffline() start the shutdown of all channels but does not wait
        // for it to finish. We want to destroy the thread pool, so we have to
        // wait for the shutdown to finish.
        try {
            FutureUtils.getUnchecked(shutdownChannels(state.get()));
        } catch (Throwable t) {
            // The future provided by shutdownChannels(...) should never throw.
            // If it does or if this thread is interrupted, we want to go on
            // with the destruction and shutdown the thread pool.
        }
        // Shutting down the poolExecutor has the consequence that chained
        // futures that rely on their callbacks being executed might never
        // finish. Even though this would not be too bad (if we are shutting
        // down all threads should get interrupted eventually), we prefer the
        // callbacks to run to make shutdown more graceful. We ensure this by
        // having a rejection handler that executes newly submitted tasks in the
        // calling thread and executing all tasks that have been scheduled but
        // not executed yet here.
        for (Runnable remainingTask : poolExecutor.shutdownNow()) {
            try {
                remainingTask.run();
            } catch (Throwable t) {
                // We ignore all exceptions thrown by tasks because we want to
                // execute the remaining tasks.
            }
        }
    }

    /**
     * Returns the status of an archived channel. If the channel is not known by
     * this archiving service, <code>null</code> is returned. Throws an
     * {@link IllegalStateException} if the archiving service has not finished
     * initialization of its channel list yet.
     * 
     * @param channelName
     *            name of the channel to be queried.
     * @return status of the specified channel or <code>null</code> if the
     *         specified channel is not known by this archiving service.
     * @throws IllegalStateException
     *             if the archiving service has not initialized its list of
     *             channels yet.
     */
    public ChannelStatus getChannelStatus(String channelName) {
        Preconditions.checkNotNull(channelName,
                "The channelName must not be null.");
        ArchivingServiceState state = this.state.get();
        if (!state.isInitialized()) {
            throw new IllegalStateException(
                    "The archiving service has not initialized the channels yet.");
        }
        ArchivedChannel<?> channel = state.getChannels().get(channelName);
        return (channel == null) ? null : statusForInternalChannel(channel);
    }

    /**
     * Returns a list of the status for all channels known by this archiving
     * service. Throws an {@link IllegalStateException} if the archiving service
     * has not finished initialization of its channel list yet. The channels are
     * returned in the natural order of their names. The returned list is never
     * <code>null</code>, but it might be empty if no channels belong this
     * server.
     * 
     * @return status of all channels known by this archiving service.
     * @throws IllegalStateException
     *             if the archiving service has not initialized its list f
     *             channels yet.
     */
    public List<ChannelStatus> getChannelStatusForAllChannels() {
        ArchivingServiceState state = this.state.get();
        if (!state.isInitialized()) {
            throw new IllegalStateException(
                    "The archiving service has not initialized the channels yet.");
        }
        LinkedList<ChannelStatus> statusList = new LinkedList<ChannelStatus>();
        for (ArchivedChannel<?> channel : state.getChannels().values()) {
            statusList.add(statusForInternalChannel(channel));
        }
        return statusList;
    }

    /**
     * Returns the total number of samples that have been dropped. A sample is
     * dropped when new samples for a channel arrive more quickly than they can
     * be written and consequently samples remain in the queue until a timeout
     * is reached. The returned number represents the total number of samples
     * that have been dropped for any channel since this archiving service was
     * started. When this number grows constantly, this can be an indication
     * that the server (or the Cassandra cluster) is overloaded and the workload
     * should be reduced (possibly by adding more servers).
     * 
     * @return number of samples that have been dropped since this archiving
     *         service was started.
     */
    public long getNumberOfSamplesDropped() {
        return numberOfSamplesDropped.get();
    }

    /**
     * Returns the total number of samples that have been written. A sample is
     * only counted as written when the databases acknowledges that the write
     * operation has been successful. Samples for all decimation levels and not
     * just raw samples contribute to this number. The returned number
     * represents the total number of samples that have been written since this
     * archiving service was started.
     * 
     * @return number of samples that have been written since this archiving
     *         service was started.
     */
    public long getNumberOfSamplesWritten() {
        return numberOfSamplesWritten.get();
    }

    /**
     * <p>
     * Returns the number of sample fetch operations that are running currently.
     * </p>
     * 
     * <p>
     * Usually, decimated samples are generated as new (raw) samples are
     * written. However, when adding a new decimation level or when restarting
     * the server, it might be necessary to read samples from the database in
     * order to generate decimated samples. In this case, the number of fetch
     * operations that may run concurrently is limited (to the number returned
     * by {@link #getSamplesDecimationMaxRunningFetchOperations()}).
     * </p>
     * 
     * @return number of sample fetch operations (to generate decimated samples)
     *         that are currently running.
     * @see #getSamplesDecimationCurrentSamplesInMemory()
     * @see #getSamplesDecimationMaxRunningFetchOperations()
     */
    public int getSamplesDecimationCurrentRunningFetchOperations() {
        return throttledArchiveAccessService.getCurrentRunningFetchOperations();
    }

    /**
     * <p>
     * Returns the number of samples that have been fetched (but not processed
     * yet). This is the number of samples that is currently kept in memory.
     * </p>
     * 
     * <p>
     * Usually, decimated samples are generated as new (raw) samples are
     * written. However, when adding a new decimation level or when restarting
     * the server, it might be necessary to read samples from the database in
     * order to generate decimated samples. For performance reasons, samples are
     * fetched in batches. These samples, that have been fetched, but not
     * processed yet, consume memory, so that no new fetch operations may be
     * started if a large number of samples has already been fetched, but not
     * processed yet. The actual limit is returned by
     * {@link #getSamplesDecimationMaxSamplesInMemory()}.
     * </p>
     * 
     * @return number of samples that have been fetched (to generate decimated
     *         samples), but not processed yet.
     * @see #getSamplesDecimationCurrentRunningFetchOperations()
     * @see #getSamplesDecimationMaxSamplesInMemory()
     */
    public int getSamplesDecimationCurrentSamplesInMemory() {
        return throttledArchiveAccessService.getCurrentSamplesInMemory();
    }

    /**
     * <p>
     * Returns the max. number of sample fetch operations that may run
     * concurrently.
     * </p>
     * 
     * <p>
     * Usually, decimated samples are generated as new (raw) samples are
     * written. However, when adding a new decimation level or when restarting
     * the server, it might be necessary to read samples from the database in
     * order to generate decimated samples. In this case, the number of fetch
     * operations that may run concurrently is limited to the number returned by
     * this method.
     * </p>
     * 
     * @return max. number of sample fetch operations (to generate decimated
     *         samples) that may run concurrently.
     * @see #getSamplesDecimationCurrentRunningFetchOperations()
     * @see #getSamplesDecimationMaxSamplesInMemory()
     */
    public int getSamplesDecimationMaxRunningFetchOperations() {
        return throttledArchiveAccessService.getMaxRunningFetchOperations();
    }

    /**
     * <p>
     * Returns the max. number of samples that may concurrently be kept in
     * memory.
     * </p>
     * 
     * <p>
     * Usually, decimated samples are generated as new (raw) samples are
     * written. However, when adding a new decimation level or when restarting
     * the server, it might be necessary to read samples from the database in
     * order to generate decimated samples. For performance reasons, samples are
     * fetched in batches. These samples, that have been fetched, but not
     * processed yet, consume memory, so that no new fetch operations may be
     * started if a large number of samples has already been fetched, but not
     * processed yet. This method returns the number of samples at which this
     * limit is enforced and no more samples are fetched.
     * </p>
     * 
     * @return max. number of samples that may be fetched (to generate decimated
     *         samples) into memory concurrently.
     * @see #getSamplesDecimationCurrentSamplesInMemory()
     * @see #getSamplesDecimationMaxRunningFetchOperations()
     */
    public int getSamplesDecimationMaxSamplesInMemory() {
        return throttledArchiveAccessService.getMaxSamplesInMemory();
    }

    /**
     * <p>
     * Reloads the configuration of the specified channel. This method should be
     * called whenever the configuration of a channel belonging to this server
     * has been changed. It also needs to be called after registering a pending
     * operation for a channel, so that archiving for the channel can be
     * disabled.
     * </p>
     * 
     * <p>
     * If the server is currently offline, calling this method will typically
     * have no effect because there is no need to reload the channel
     * configuration. In this case, the future returned by this method completed
     * immediately.
     * </p>
     * 
     * <p>
     * The future returned by this method completes when the refresh operation
     * has finished. It should never throw an exception.
     * </p>
     * 
     * @param channelName
     *            name of the channel to be refreshed.
     * @return future that completes when the refresh operation has finished.
     * @throws NullPointerException
     *             if <code>channelName</code> is <code>null</code>.
     */
    public ListenableFuture<Void> refreshChannel(final String channelName) {
        Preconditions.checkNotNull(channelName);
        // If the archiving service has already been destroyed, we do not have
        // to refresh the channel (and cannot because the poolExecutor is not
        // available any longer). However, the channel might still be active, so
        // we have to return a future that will only complete after the channel
        // has been shutdown.
        if (destroyed) {
            ArchivedChannel<?> channel = state.get().getChannels()
                    .get(channelName);
            if (channel != null) {
                // The channel can be considered "refreshed" when it has been
                // destroyed. Calling channel.shutdown() more than once is okay.
                return channel.destroy();
            } else {
                // The channel is not active, so it can be considered
                // "refreshed" immediately.
                return VOID_SUCCESS;
            }
        }
        // We need to save a reference to the channel instance that was valid at
        // the time of reading the configuration. By saving this reference
        // before getting the configuration, we can ensure that the
        // configuration is never older than the reference and thus there is no
        // race condition in which we replace a channel having a more recent
        // configuration with a channel having an older configuration.
        final ArchivedChannel<?> oldChannel = state.get().getChannels()
                .get(channelName);
        // We need both the configuration and the pending channel operation (if
        // there is one), but we can get both in parallel to reduce the latency.
        final ListenableFuture<ChannelConfiguration> getChannelFuture = channelMetaDataDAO
                .getChannelByServer(thisServerId, channelName);
        final ListenableFuture<ChannelOperation> getPendingChannelOperationFuture = channelMetaDataDAO
                .getPendingChannelOperation(thisServerId, channelName);
        @SuppressWarnings("unchecked")
        ListenableFuture<Void> combinedFuture = FutureUtils
                .transformAnyToVoid(Futures.allAsList(getChannelFuture,
                        getPendingChannelOperationFuture));
        // If one of the get operations fails, we should put the channel into
        // error mode, because it will not represent the right configuration any
        // longer. We execute the callback using the poolExecutor (and not in
        // the thread of the database access layer) because we might have to
        // shutdown the existing channel which could block.
        // We do not use the same-thread executor for our callback because we
        // want to synchronize on the channel and depending on implementation
        // details outside our scope, this could lead to a dead lock if it is
        // executed by a thread that also holds a mutex that we acquire while
        // holding the channel mutex.
        Futures.addCallback(combinedFuture, new FutureCallback<Void>() {
            @Override
            public void onSuccess(Void result) {
                // In case of success, the transformation will take care of
                // dealing with the result.
            }

            @Override
            public void onFailure(Throwable e) {
                // We want to switch the channel into the error state, because
                // it should not be used with an outdated configuration.
                // If the channel did not exist before, we cannot put it into an
                // error state. We could create a new channel instance and put
                // it into an error state, but we would need the channel
                // configuration for this. For this reason, we do not have much
                // choice and simply do nothing. This means that the channel
                // will not appear in the channel list, but there is not really
                // a better way to handle this situation at the moment.
                if (oldChannel == null) {
                    // TODO Allow creating a channel with a null configuration
                    // and handle this correctly in the archiving code, the
                    // archive configuration code, and the user interface.
                    return;
                }
                synchronized (oldChannel) {
                    switch (oldChannel.getState()) {
                    case INITIALIZING:
                        oldChannel.destroyWithException(e);
                        break;
                    case INITIALIZED:
                        // If the channel is initialized, we shut it down and
                        // set the error. There is no reason for us to wait on
                        // the shutdown operation to finish, because we do not
                        // have to run any code after it finishes.
                        oldChannel.destroyWithException(e);
                        break;
                    case DESTROYED:
                        // If the channel has been destroyed, we do not
                        // have to do anything - the object is not used
                        // any longer anyway.
                        break;
                    }
                }
                // TODO We should maybe try to recover from an error state
                // automatically. However, we have to think about how we would
                // recover from other situations causing an error state or how
                // we can distinguish recoverable from non-recoverable errors.
            }
        }, poolExecutor);
        // We have to destroy the old channel instance before creating a new
        // one. This way, we can ensure that there is never more than one active
        // instance.
        AsyncFunction<Void, Void> destroyOldChannel = new AsyncFunction<Void, Void>() {
            @Override
            public ListenableFuture<Void> apply(Void input) throws Exception {
                if (oldChannel == null) {
                    return VOID_SUCCESS;
                } else {
                    return oldChannel.destroy();
                }
            }
        };
        // We do not use the same-thread executor for our transformation because
        // we want to synchronize on the channel and depending on implementation
        // details outside our scope, this could lead to a dead lock if it is
        // executed by a thread that also holds a mutex that we acquire while
        // holding the channel mutex.
        ListenableFuture<Void> destroyOldChannelFuture = Futures
                .transform(combinedFuture, destroyOldChannel, poolExecutor);
        return Futures.transform(destroyOldChannelFuture,
                new AsyncFunction<Void, Void>() {
                    @Override
                    public ListenableFuture<Void> apply(Void input) {
                        ChannelConfiguration channelConfiguration = FutureUtils
                                .getUnchecked(getChannelFuture);
                        ChannelOperation pendingOperation = FutureUtils
                                .getUnchecked(getPendingChannelOperationFuture);
                        ArchivedChannel<?> newChannel = null;
                        // We do not have to check the server ID in the
                        // configuration because it will always be the ID of
                        // this server. The server ID has been specified when
                        // reading the configuration, so it is impossible that
                        // we get the configuration for a channel that belongs
                        // to a different server.
                        // We have to loop until we updated the state
                        // successfully or until we determined that we cannot
                        // update the state.
                        for (;;) {
                            ArchivingServiceState oldState = state.get();
                            if (!oldState.isOnline()) {
                                // If we are not online, the channel will be
                                // initialized when we go online.
                                return VOID_SUCCESS;
                            }
                            if (!oldState.isInitialized()) {
                                // If we are not in the initialized state, but
                                // online, there is a potential race condition.
                                // We have to queue the channel to be refreshed
                                // later and then check whether we are still not
                                // initialized.
                                final SettableFuture<Void> future = SettableFuture
                                        .create();
                                SettableFuture<Void> existingFuture = channelsNeedingRefresh
                                        .putIfAbsent(channelName, future);
                                // Now we check the state again. If it changed
                                // to initialized, the channel might not have
                                // been refreshed and we might have to trigger
                                // the refresh action. If we successfully added
                                // our own future, we remove it, but we have to
                                // make sure that the future gets notified when
                                // we are finished because another thread might
                                // wait on it to finish. If we got an existing
                                // future, we can simply use that future. The
                                // thread that registered it (or the
                                // initialization thread) will take care of
                                // finishing that future.
                                if (state.get().isInitialized()) {
                                    if (existingFuture != null) {
                                        return existingFuture;
                                    }
                                    if (channelsNeedingRefresh
                                            .remove(channelName, future)) {
                                        // We have removed the channel and
                                        // future and can try again. However, we
                                        // have to make sure that the future is
                                        // notified when we are finished because
                                        // another thread might depend on it.
                                        // The easiest way to do this is calling
                                        // this method again and registering a
                                        // listener with the future returned.
                                        // This will also make sure that we use
                                        // the most up-to-date configuration
                                        // (the other thread that requested the
                                        // refresh might have updated the
                                        // configuration after we read it).
                                        ListenableFuture<Void> newFuture = refreshChannel(
                                                channelName);
                                        Futures.addCallback(newFuture,
                                                new FutureCallback<Void>() {
                                                    @Override
                                                    public void onSuccess(
                                                            Void result) {
                                                        future.set(result);
                                                    }

                                                    @Override
                                                    public void onFailure(
                                                            Throwable t) {
                                                        future.setException(t);
                                                    }
                                                });
                                        return newFuture;
                                    } else {
                                        // Some other thread removed the channel
                                        // from the queue. This means that this
                                        // channel will be responsible for
                                        // handling the refresh.
                                        return VOID_SUCCESS;
                                    }
                                } else {
                                    // We are still not in the initialized
                                    // state, so our queued request is processed
                                    // when changing to the initialized state.
                                    return VOID_SUCCESS;
                                }
                            }
                            TreeMap<String, ArchivedChannel<?>> newChannels = new TreeMap<String, ArchivedChannel<?>>();
                            newChannels.putAll(oldState.getChannels());
                            // We can use channels.get(...) because this will
                            // work correctly in both cases: When we expect a
                            // null value (the channel was not registered
                            // previously) and when we expect a certain channel
                            // instance.
                            if (newChannels.get(channelName) != oldChannel) {
                                // Some other thread modified the channel in the
                                // meantime. There is a risk, that the
                                // configuration that we read is outdated,
                                // because this thread might have slept between
                                // reading the configuration and running this
                                // code. Therefore, we have to read the
                                // configuration again, in order to be sure that
                                // we have the most recent configuration.
                                return refreshChannel(channelName);
                            }
                            if (channelConfiguration != null) {
                                // Technically speaking, using Sample for the
                                // generic type parameter is not correct. This
                                // should be the same generic type parameter as
                                // used by the control-system type. However, we
                                // do not know this type (and there might not
                                // even be a compatible control-system support).
                                // In the end, it does not matter, because it is
                                // only important that we consistently use the
                                // same type of samples for a channel and that
                                // these samples are compatible with the
                                // control-system support. This is guaranteed
                                // because all samples used by a channel are
                                // created by its control-system support and
                                // only passed back to the same control-system
                                // support.
                                newChannel = new ArchivedChannel<Sample>(
                                        channelConfiguration, pendingOperation,
                                        throttledArchiveAccessService,
                                        ArchivingServiceInternalImpl.this,
                                        channelMetaDataDAO,
                                        controlSystemSupportRegistry,
                                        poolExecutor, scheduledExecutor,
                                        thisServerId);
                                // We have to add or update the channel.
                                newChannels.put(channelName, newChannel);
                            } else {
                                // If the channel configuration is null, the
                                // channel does not exist any longer. We only
                                // have to remove the old channel (if
                                // present).
                                newChannels.remove(channelName);
                            }
                            ArchivingServiceState newState = new ArchivingServiceState(
                                    oldState.isInitialized(),
                                    oldState.isOnline(),
                                    Collections.unmodifiableSortedMap(
                                            newChannels));
                            // If we could update the state, we are done. If
                            // some other thread modified the state in the
                            // meantime, we have to get the updated state and
                            // run all checks again.
                            if (state.compareAndSet(oldState, newState)) {
                                break;
                            }
                        }
                        // Finally, we have to initialize the new channel. We
                        // can run this operation in this thread, because this
                        // callback is already executed by the poolExecutor.
                        if (newChannel != null) {
                            synchronized (newChannel) {
                                return newChannel.initialize();
                            }
                        } else {
                            return VOID_SUCCESS;
                        }
                    }
                }, poolExecutor);
    }

    /**
     * Starts the background tasks. In particular, this method starts processing
     * in the write thread. This method is called by the
     * {@link ArchivingService} that created this instace when initialization of
     * the application context has completed and thus background tasks should be
     * started.
     * 
     * @throws IllegalArgumentException
     *             if this service has already been destroyed.
     */
    public void startBackgroundTasks() {
        if (destroyed) {
            throw new IllegalStateException(
                    "Cannot start the background tasks after the service has been destroyed.");
        }
        writeSampleExecutor.execute(new Runnable() {
            @Override
            public void run() {
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        processSampleWriteQueues();
                    } catch (Throwable e) {
                        // We log the exception and continue. If we did not
                        // catch the exception, this would stop the write-sample
                        // thread and no samples could be written any longer.
                        log.error("Processing the sample write queues failed: "
                                + e.getMessage(), e);
                    }
                }
            }
        });
    }

    /**
     * Switches this archiving service offline. This means that all channels are
     * destroyed and write operation is ceased. This method is called by the
     * {@link ArchivingService} that created this instance when it is informed
     * by the {@link ClusterManagementService} that the server should not remain
     * online. This method completes asynchronously and does not block.
     */
    public void switchOffline() {
        // We expect that switchOnline() and switchOffline() are not executed
        // concurrently. This should hold as long as notifications are only done
        // by one thread. If notifications are done by different unsynchronized
        // threads, that is no strict ordering of notifications anyway so any
        // effort to get a strict ordering is useless. If we are already in the
        // offline state, there is no need to start another shutdown.
        final ArchivingServiceState oldState = this.state.get();
        if (!oldState.isOnline()) {
            return;
        }
        ArchivingServiceState newState = new ArchivingServiceState(false, false,
                oldState.getChannels());
        // We set the newState so that concurrent methods see that the state has
        // changed. There will be another state update when the initialization
        // code has run. This later update is only done if we are still in the
        // same state, so the ordering of class to switchOnline() and
        // switchOffline() is kept, even though the initialization and
        // destruction code are executed asynchronously.
        if (!this.state.compareAndSet(oldState, newState)) {
            // If another thread has modified the state concurrently, we first
            // want to check it again (even though such a change is unlikely).
            switchOffline();
            return;
        }
        // We can run shutdownChannels within this thread because it operates
        // asynchronously. We do not wait for it to finish, because we can go
        // back online even if the shutdown has not finished yet.
        shutdownChannels(oldState);
    }

    /**
     * Switches the archiving service online. This starts the initialization of
     * all channels that are managed by this archiving service. This method is
     * called by the {@link ArchivingService} that created this instance when it
     * is notified by the {@link ClusterManagementService} that this server is
     * now online. It is also called once after the background jobs have been
     * started (see {@link #startBackgroundTasks()}) when the server is already
     * online at this point in time. This method completes asynchronously and
     * does not block.
     */
    public void switchOnline() {
        // If this manager has been destroyed, we never want to go online again.
        if (destroyed) {
            return;
        }
        // We expect that switchOnline() and switchOffline() are not executed
        // concurrently. This should hold as long as notifications are only done
        // by one thread. If notifications are done by different unsynchronized
        // threads, that is no strict ordering of notifications anyway so any
        // effort to get a strict ordering is useless. If we are already in the
        // online state, there is no need to start another initialization.
        ArchivingServiceState oldState = this.state.get();
        if (oldState.isOnline()) {
            return;
        }
        final ArchivingServiceState newState = new ArchivingServiceState(false,
                true, oldState.getChannels());
        // We set the newState so that concurrently running methods see that the
        // state has changed. There will be another state update when the
        // initialization has finished. This later update is only done if we are
        // still in the same state, so the ordering of calls to switchOnline()
        // and switchOffline() is kept, even though the initialization and
        // destruction code are executed asynchronously.
        if (!this.state.compareAndSet(oldState, newState)) {
            // If another thread has modified the state concurrently, we first
            // want to check it again (even though such a change is unlikely).
            switchOnline();
            return;
        }
        // initializeChannels(...) does not block, so it is okay to call it from
        // here. We do not wait for the operation to finish, because another
        // call to switchOnline() or switchOffline() is handled correctly by the
        // state-machine logic.
        // Strictly speaking, we should wait for the ChannelMetaDAO to be
        // initialized before calling initializeChannels. However, the archiving
        // service is only switched online when the server is switched online
        // which involves a delay of several seconds after the connection to the
        // database is established. For this reason, it is very likely that the
        // ChannelMetaDataDAO has been initialized completely, and we skip this
        // check. If we added this check, the logic would become much more
        // complex because we now need to handle different events.
        initializeChannels(newState);
    }

    /**
     * <p>
     * Adds the specified channel decimation-level to the global queue of
     * decimation levels that shall be processed. This means that the write
     * thread will eventually call the decimation level's
     * {@link ArchivedChannelDecimationLevel#processSampleWriteQueue()
     * processSampleWriteQueue()} method.
     * </p>
     * 
     * <p>
     * If the specified decimation level has already been queued for processing
     * but not been processed yet, calling this method has no effect.
     * </p>
     * 
     * <p>
     * This method should only be called by the
     * {@link ArchivedChannelDecimationLevel}s that belong to this archiving
     * service.
     * </p>
     * 
     * @param decimationLevel
     *            decimation level that shall be process by the write thread.
     */
    void addChannelDecimationLevelToWriteProcessingQueue(
            ArchivedChannelDecimationLevel<?> decimationLevel) {
        assert (decimationLevel != null);
        channelDecimationLevelsWithPendingWriteRequests.add(decimationLevel);
    }

    /**
     * Increments the counter that keeps track of the number of samples that
     * have been dropped. This method is called by the {@link ArchivedChannel}s
     * that belong to this archiving service when they increment their dropped
     * samples counters.
     * 
     * @param increment
     *            number by which the counter shall be incremented. Must not be
     *            negative.
     */
    void incrementNumberOfSamplesDropped(long increment) {
        assert (increment >= 0L);
        numberOfSamplesDropped.addAndGet(increment);
    }

    /**
     * Increments the counter that keeps track of the number of samples that
     * have been written. This method is called by the {@link ArchivedChannel}s
     * that belong to this archiving service when they increment their written
     * samples counters.
     * 
     * @param increment
     *            number by which the counter shall be incremented. Must not be
     *            negative.
     */
    void incrementNumberOfSamplesWritten(long increment) {
        assert (increment >= 0L);
        numberOfSamplesWritten.addAndGet(increment);
    }

    private ListenableFuture<Void> initializeChannels(
            final ArchivingServiceState expectedState) {
        assert (expectedState != null);
        final TreeMap<String, ArchivedChannel<?>> newChannels = new TreeMap<String, ArchivedChannel<?>>();
        // If there are any old channels instances that have not been destroyed
        // yet (e.g. because there still are some operations running), we do not
        // create a new instance for them. They will be refreshed when they
        // finally are destroyed.
        final HashSet<ArchivedChannel<?>> refreshOnDestroy = new HashSet<ArchivedChannel<?>>();
        for (ArchivedChannel<?> channel : expectedState.getChannels()
                .values()) {
            synchronized (channel) {
                if (!channel.getState()
                        .equals(ArchivedChannelState.DESTROYED)) {
                    newChannels.put(channel.getConfiguration().getChannelName(),
                            channel);
                    refreshOnDestroy.add(channel);
                }
            }
        }
        // We can start both queries in parallel and simply wait until both have
        // finished. This should reduce the latency.
        final ListenableFuture<? extends Iterable<? extends ChannelOperation>> getPendingOperationsFuture = channelMetaDataDAO
                .getPendingChannelOperations(thisServerId);
        final ListenableFuture<? extends Iterable<? extends ChannelConfiguration>> getChannelsFuture = channelMetaDataDAO
                .getChannelsByServer(thisServerId);
        @SuppressWarnings("unchecked")
        ListenableFuture<Void> combinedFuture = FutureUtils.transformAnyToVoid(
                Futures.allAsList(getPendingOperationsFuture,
                        getChannelsFuture));
        Function<Void, Void> initializeChannels = new Function<Void, Void>() {
            @Override
            public Void apply(Void input) {
                Map<String, ChannelOperation> pendingOperations = new HashMap<String, ChannelOperation>();
                for (ChannelOperation pendingOperation : FutureUtils
                        .getUnchecked(getPendingOperationsFuture)) {
                    pendingOperations.put(pendingOperation.getChannelName(),
                            pendingOperation);
                }
                for (ChannelConfiguration channelConfiguration : FutureUtils
                        .getUnchecked(getChannelsFuture)) {
                    if (newChannels.containsKey(
                            channelConfiguration.getChannelName())) {
                        // If there still is an old instance, we do not want to
                        // create a new one.
                        continue;
                    }
                    // We do not initialize the fields referencing the
                    // control-system channel yet. Getting the control-system
                    // channel is an asynchronous operation that might block, so
                    // we do this later.
                    // Technically speaking, using Sample for the generic type
                    // parameter is not correct. This should be the same generic
                    // type parameter as used by the control-system type.
                    // However, we do not know this type (and there might not
                    // even be a compatible control-system support). In the end,
                    // it does not matter, because it is only important that we
                    // consistently use the same type of samples for a channel
                    // and that these samples are compatible with the
                    // control-system support. This is guaranteed because all
                    // samples used by a channel are created by its
                    // control-system support and only passed back to the same
                    // control-system support.
                    final ArchivedChannel<?> channel = new ArchivedChannel<Sample>(
                            channelConfiguration,
                            pendingOperations
                                    .get(channelConfiguration.getChannelName()),
                            throttledArchiveAccessService,
                            ArchivingServiceInternalImpl.this,
                            channelMetaDataDAO, controlSystemSupportRegistry,
                            poolExecutor, scheduledExecutor, thisServerId);
                    newChannels.put(channelConfiguration.getChannelName(),
                            channel);
                }
                final ArchivingServiceState newState = new ArchivingServiceState(
                        true, true,
                        Collections.unmodifiableSortedMap(newChannels));
                if (!ArchivingServiceInternalImpl.this.state
                        .compareAndSet(expectedState, newState)) {
                    return null;
                }
                for (final ArchivedChannel<?> channel : newState.getChannels()
                        .values()) {
                    if (refreshOnDestroy.contains(channel)) {
                        // We only initialize new channels. Old instances need a
                        // different handling.
                        continue;
                    }
                    // The channel initialization completes asynchronously (if
                    // blocking operations are needed), so we can start it in
                    // this thread. We do not wait for the initialization to
                    // complete because the channels are completely independent.
                    synchronized (channel) {
                        channel.initialize();
                    }
                }
                // Channels that were not completely destroyed before we loaded
                // the configuration have to be refreshed when they finally are
                // destroyed. Simply creating a new channel and initializing it
                // with the configuration that we already have is not sufficient
                // because an active channel might have made changes to the
                // database and thus the configuration that we got might be
                // outdated.
                for (final ArchivedChannel<?> channel : refreshOnDestroy) {
                    // Calling channel.destroy() is safe because destruction
                    // must already have started and calling channel.destroy()
                    // more than once is okay.
                    ListenableFuture<Void> channelDestructionFuture = channel
                            .destroy();
                    if (channelDestructionFuture.isDone()) {
                        // The channel is now destroyed, so we can start the
                        // refresh right away.
                        refreshChannel(
                                channel.getConfiguration().getChannelName());
                    } else {
                        channelDestructionFuture.addListener(new Runnable() {
                            @Override
                            public void run() {
                                refreshChannel(channel.getConfiguration()
                                        .getChannelName());
                            }
                        }, MoreExecutors.sameThreadExecutor());
                    }
                }
                while (!channelsNeedingRefresh.isEmpty()) {
                    Map.Entry<String, SettableFuture<Void>> channelNameAndFuture;
                    try {
                        channelNameAndFuture = channelsNeedingRefresh.entrySet()
                                .iterator().next();
                    } catch (NoSuchElementException e) {
                        // If the map is modified concurrently, the map might
                        // suddenly be empty. We simply ignore such an
                        // exception. If a subsequent call to isEmpty() returns
                        // true, we are done. Otherwise, we process the next
                        // available element.
                        continue;
                    }
                    String channelName = channelNameAndFuture.getKey();
                    final SettableFuture<Void> future = channelNameAndFuture
                            .getValue();
                    if (!channelsNeedingRefresh.remove(channelName, future)) {
                        // If the channel has already been removed, some other
                        // thread is taking care of it and we can ignore it.
                        continue;
                    }
                    // We can run the refresh operation asynchronously because
                    // there is no reason for the refresh of one channel to wait
                    // for the refresh of another channel. However, we have to
                    // register a callback in order to notify other threads that
                    // might wait for the refresh operation to finish.
                    FutureUtils.forward(refreshChannel(channelName), future);
                }
                return null;
            }
        };
        // We do not want to execute the initializeChannels functions using the
        // same-thread executor because the function iterates over the results
        // from the database query and this iteration might block if the results
        // are spread over multiple pages.
        ListenableFuture<Void> initializationFuture = Futures
                .transform(combinedFuture, initializeChannels, poolExecutor);
        // If one of the two queries or the transformation that processes the
        // query results fails, we want to try again after some time. When the
        // failure is caused by the database being unavailable, the server will
        // switch back to the offline state. However, if the failure is caused
        // by a transient problem, the server might never switch offline and
        // thus the initialization would never be retried if we did not retry it
        // explicitly.
        // We use a callback on the future that is the result of the
        // transformation. This callback will be triggered if one of the queries
        // or the transformation fails, thus catching all errors.
        // The transformation should only fail if there is an error before it
        // changes the state. All actions that happen after the state change
        // should not result in an exception. In particular,
        // channel.initialize(), channel.destroy() and refreshChannel(...)
        // should never throw. Instead, a channel-specific error should result
        // in only this channel being put into an error state.
        // Even if some logic error actually caused an exception after the state
        // change, this would not result in incorrect behavior, because the
        // initialization logic will not make any changes if the current state
        // does not match the expected state. This also means that the
        // initialization logic will not run if the server has gone offline in
        // the meantime.
        // We can run in this callback in-thread because we only schedule an
        // operation with the scheduled executor and this action should not
        // block.
        Futures.addCallback(initializationFuture,
                new MoreFutures.FailureCallback<Void>() {
                    @Override
                    public void onFailure(Throwable t) {
                        // We log the problem. Maybe this information is helpful
                        // for finding the root of the problem.
                        log.error(
                                "Initialization of the archiving service failed. Another initialization attempt has been scheduled.",
                                t);
                        // We schedule another initialization attempt in 30
                        // seconds.
                        scheduledExecutor.schedule(new Runnable() {
                            @Override
                            public void run() {
                                // If the state has changed in the meantime, we
                                // stop the initialization attempt now. The
                                // logic that triggered the state change will
                                // take care of starting another initialization
                                // when it is time.
                                if (state.get() != expectedState) {
                                    return;
                                }
                                // We run the actual initialization logic using
                                // the poolExecutor. The initialization logic
                                // might block and we do not want the thread of
                                // the scheduledExector to be blocked.
                                poolExecutor.execute(new Runnable() {
                                    @Override
                                    public void run() {
                                        initializeChannels(expectedState);
                                    }
                                });

                            }
                        }, 30L, TimeUnit.SECONDS);
                    }
                });
        return initializationFuture;
    }

    private void processSampleWriteQueues() {
        try {
            if (session == null) {
                session = cassandraProvider.getSessionFuture().get();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        } catch (ExecutionException e) {
            // The session future only fails if the Cassandra provider is
            // destroyed before it can finish initialization. In this case,
            // there is no hope for recovery and we can stop this thread.
            log.error(
                    "Getting the Cassandra session finally failed, stopping the processSampleWriteQueues thread.",
                    e.getCause() != null ? e.getCause() : e);
            Thread.currentThread().interrupt();
            return;
        }
        try {
            channelDecimationLevelsWithPendingWriteRequests.take()
                    .processSampleWriteQueue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }
    }

    private ListenableFuture<Void> shutdownChannels(
            ArchivingServiceState oldState) {
        assert (oldState != null);
        // We request the shutdown of all channels, but we do not wait for it.
        // This is okay, because we can handle a situation in which we start up
        // again and there are still old channels which have not been destroyed.
        LinkedList<ListenableFuture<Void>> shutdownFutures = new LinkedList<ListenableFuture<Void>>();
        for (ArchivedChannel<?> channel : oldState.getChannels().values()) {
            shutdownFutures.add(channel.destroy());
        }
        return FutureUtils
                .transformAnyToVoid(Futures.allAsList(shutdownFutures));
    }

    private ChannelStatus statusForInternalChannel(ArchivedChannel<?> channel) {
        assert (channel != null);
        ChannelStatus.State channelState;
        ChannelConfiguration configuration;
        Throwable error = null;
        long totalSamplesDropped = 0L;
        long totalSamplesSkippedBack = 0L;
        long totalSamplesWritten = 0L;
        synchronized (channel) {
            configuration = channel.getConfiguration();
            switch (channel.getState()) {
            case DESTROYED:
                if (channel.getError() == null) {
                    channelState = ChannelStatus.State.DESTROYED;
                } else {
                    channelState = ChannelStatus.State.ERROR;
                    error = channel.getError();
                }
                break;
            case INITIALIZED:
                ControlSystemChannel controlSystemChannel = channel
                        .getControlSystemChannel();
                if (controlSystemChannel == null) {
                    channelState = ChannelStatus.State.DISABLED;
                } else {
                    ControlSystemChannelStatus controlSystemChannelStatus = controlSystemChannel
                            .getStatus();
                    // The ControlSystemChannel implementation might be broken
                    // and might return null.
                    if (controlSystemChannelStatus == null) {
                        channelState = ChannelStatus.State.ERROR;
                        error = new IllegalArgumentException(
                                "The control-system channel's getStatus() method returned null.");
                    }
                    // We do not have to worry about getState() returning null
                    // because only status objects with a non-null state can be
                    // constructed.
                    switch (controlSystemChannelStatus.getState()) {
                    case CONNECTED:
                        channelState = ChannelStatus.State.OK;
                        break;
                    case DISABLED:
                        channelState = ChannelStatus.State.DISABLED;
                        break;
                    case DISCONNECTED:
                        channelState = ChannelStatus.State.DISCONNECTED;
                        break;
                    case ERROR:
                        channelState = ChannelStatus.State.ERROR;
                        error = controlSystemChannel.getStatus().getError();
                        break;
                    default:
                        throw new RuntimeException("Unhandled case: "
                                + controlSystemChannel.getStatus().getState());
                    }
                    totalSamplesDropped = channel.getSamplesDropped();
                    totalSamplesSkippedBack = channel.getSamplesSkippedBack();
                    totalSamplesWritten = channel.getSamplesWritten();
                }
                break;
            case INITIALIZING:
                channelState = ChannelStatus.State.INITIALIZING;
                break;
            default:
                throw new RuntimeException(
                        "Unhandled case: " + channel.getState());
            }
        }
        String errorMessage = (error == null) ? null
                : ((error.getMessage() == null) ? error.getClass().getName()
                        : error.getMessage());
        return new ChannelStatus(configuration, errorMessage, channelState,
                totalSamplesDropped, totalSamplesSkippedBack,
                totalSamplesWritten);
    }

}
