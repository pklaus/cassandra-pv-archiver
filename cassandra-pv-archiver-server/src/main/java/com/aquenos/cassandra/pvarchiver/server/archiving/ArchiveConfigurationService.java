/*
 * Copyright 2015-2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.archiving;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;

import com.aquenos.cassandra.pvarchiver.server.archiving.internal.AddChannelOperation;
import com.aquenos.cassandra.pvarchiver.server.archiving.internal.MoveChannelOperation;
import com.aquenos.cassandra.pvarchiver.server.archiving.internal.RemoveChannelOperation;
import com.aquenos.cassandra.pvarchiver.server.archiving.internal.RenameChannelOperation;
import com.aquenos.cassandra.pvarchiver.server.archiving.internal.UpdateChannelOperation;
import com.aquenos.cassandra.pvarchiver.server.cluster.ClusterManagementService;
import com.aquenos.cassandra.pvarchiver.server.controlsystem.ControlSystemSupportRegistry;
import com.aquenos.cassandra.pvarchiver.server.database.CassandraProvider;
import com.aquenos.cassandra.pvarchiver.server.database.ChannelMetaDataDAO;
import com.aquenos.cassandra.pvarchiver.server.database.ChannelMetaDataDAO.ChannelInformation;
import com.aquenos.cassandra.pvarchiver.server.database.ThrottlingCassandraProvider;
import com.aquenos.cassandra.pvarchiver.server.internode.InterNodeCommunicationService;
import com.aquenos.cassandra.pvarchiver.server.spring.ServerProperties;
import com.aquenos.cassandra.pvarchiver.server.util.AsyncFunctionUtils;
import com.aquenos.cassandra.pvarchiver.server.util.FutureUtils;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

/**
 * Service providing access to the archive configuration. Updates of the
 * configuration must be performed through this service. This service takes care
 * of ensuring that updates are performed in a safe way, taking measures to
 * avoid concurrent operations in the cluster to interfere with each other.
 * 
 * @author Sebastian Marsching
 */
public class ArchiveConfigurationService implements DisposableBean,
        InitializingBean {

    /**
     * {@link ArchiveConfigurationCommand} that has been queued instead of
     * running it immediately because too many commands are already running. We
     * limit the number of commands that can run in parallel because running too
     * many in parallel might put too much load on the database servers.
     * 
     * @author Sebastian Marsching
     */
    private static class PendingConfigurationCommand {

        // The field channelsNeedingCacheUpdate must refer to a synchronized (or
        // otherwise thread-safe) list because we cannot know which thread will
        // update it.
        public final List<String> channelsNeedingCacheUpdate;
        public final ArchiveConfigurationCommand command;
        public final ChannelInformation prefetchedChannelInformation;
        public final SettableFuture<Pair<ArchiveConfigurationCommandResult, Throwable>> resultFuture;

        public PendingConfigurationCommand(ArchiveConfigurationCommand command,
                ChannelInformation prefetchedChannelInformation,
                List<String> channelsNeedingCacheUpdate) {
            this.channelsNeedingCacheUpdate = channelsNeedingCacheUpdate;
            this.command = command;
            this.prefetchedChannelInformation = prefetchedChannelInformation;
            this.resultFuture = SettableFuture.create();
        }

    }

    private static final Function<Object, Boolean> ALWAYS_FALSE = Functions
            .constant(false);
    private static final Function<Pair<ArchiveConfigurationCommandResult, Throwable>, ArchiveConfigurationCommandResult> CONVERT_PAIR_TO_RESULT = new Function<Pair<ArchiveConfigurationCommandResult, Throwable>, ArchiveConfigurationCommandResult>() {
        @Override
        public ArchiveConfigurationCommandResult apply(
                Pair<ArchiveConfigurationCommandResult, Throwable> input) {
            if (input == null) {
                return null;
            } else {
                return input.getLeft();
            }
        }
    };
    private static final Function<List<Pair<ArchiveConfigurationCommandResult, Throwable>>, List<ArchiveConfigurationCommandResult>> CONVERT_TO_RESULT_ONLY_LIST = new Function<List<Pair<ArchiveConfigurationCommandResult, Throwable>>, List<ArchiveConfigurationCommandResult>>() {
        @Override
        public List<ArchiveConfigurationCommandResult> apply(
                List<Pair<ArchiveConfigurationCommandResult, Throwable>> input) {
            return Lists.transform(input, CONVERT_PAIR_TO_RESULT);
        }
    };
    private static final Function<List<Pair<ArchiveConfigurationCommandResult, Throwable>>, Void> THROW_IF_NOT_SUCCESSFUL = new Function<List<Pair<ArchiveConfigurationCommandResult, Throwable>>, Void>() {
        @Override
        public Void apply(
                List<Pair<ArchiveConfigurationCommandResult, Throwable>> input) {
            Pair<ArchiveConfigurationCommandResult, Throwable> pair = input
                    .get(0);
            if (!pair.getLeft().isSuccess()) {
                Throwable t = pair.getRight();
                Throwables.propagateIfPossible(t);
                throw new RuntimeException("Unexpected checked exception: "
                        + t.getMessage(), t);
            } else {
                return null;
            }
        }
    };

    private static String extractChannelName(ArchiveConfigurationCommand command) {
        switch (command.getCommandType()) {
        case ADD_CHANNEL:
            return ((AddChannelCommand) command).getChannelName();
        case ADD_OR_UPDATE_CHANNEL:
            return ((AddOrUpdateChannelCommand) command).getChannelName();
        case MOVE_CHANNEL:
            return ((MoveChannelCommand) command).getChannelName();
        case REFRESH_CHANNEL:
            return ((RefreshChannelCommand) command).getChannelName();
        case REMOVE_CHANNEL:
            return ((RemoveChannelCommand) command).getChannelName();
        case RENAME_CHANNEL:
            return ((RenameChannelCommand) command).getOldChannelName();
        case UPDATE_CHANNEL:
            return ((UpdateChannelCommand) command).getChannelName();
        default:
            // This case should never happen as the commands built a closed
            // hierarchy.
            return null;
        }
    }

    /**
     * Logger for this class.
     */
    protected final Log log = LogFactory.getLog(getClass());

    private ArchivingService archivingService;
    private CassandraProvider cassandraProvider;
    private ChannelInformationCache channelInformationCache;
    private ChannelMetaDataDAO channelMetaDataDAO;
    private ClusterManagementService clusterManagementService;
    private InterNodeCommunicationService interNodeCommunicationService;
    private int maxConcurrentArchiveConfigurationCommands = 64;
    private LinkedList<PendingConfigurationCommand> pendingConfigurationCommands = new LinkedList<PendingConfigurationCommand>();
    private ExecutorService poolExecutor;
    private int runningConfigurationCommandsCount;
    private ControlSystemSupportRegistry controlSystemSupportRegistry;
    private ServerProperties serverProperties;
    private UUID thisServerId;

    /**
     * Sets the archiving service. The reference to the archive service is used
     * to make sure that a channel is refreshed (its configuration is reloaded),
     * when needed. Typically, this method is called automatically by the Spring
     * container.
     * 
     * @param archivingService
     *            archiving service for this server.
     */
    @Autowired
    public void setArchivingService(ArchivingService archivingService) {
        this.archivingService = archivingService;
    }

    /**
     * Sets the Cassandra provider that provides access to the Apache Cassandra
     * database. Typically, this method is called automatically by the Spring
     * container.
     * 
     * @param cassandraProvider
     *            provider that provides a connection to the Apache Cassandra
     *            database.
     */
    @Autowired
    public void setCassandraProvider(CassandraProvider cassandraProvider) {
        this.cassandraProvider = cassandraProvider;
    }

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
     * Sets the DAO for reading and modifying meta-data related to channels.
     * Typically, this method is called automatically by the Spring container.
     * 
     * @param channelMetaDataDAO
     *            channel meta-data DAO to be used by this object.
     */
    @Autowired
    public void setChannelMetaDataDAO(ChannelMetaDataDAO channelMetaDataDAO) {
        this.channelMetaDataDAO = channelMetaDataDAO;
    }

    /**
     * Sets the cluster management service. The cluster management service is
     * used to get information about the cluster status. In particular, it is
     * needed to determine the online state of servers in the cluster.
     * Typically, this method is called automatically by the Spring container.
     * 
     * @param clusterManagementService
     *            cluster manager used by the archiving server.
     */
    @Autowired
    public void setClusterManagementService(
            ClusterManagementService clusterManagementService) {
        this.clusterManagementService = clusterManagementService;
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

    /**
     * Sets the inter-node communication service. The inter-node communication
     * service is needed to communicate with other servers in the cluster when
     * updating the archive configuration.
     * 
     * @param interNodeCommunicationService
     *            inter-node communication service.
     */
    @Autowired
    public void setInterNodeCommunicationService(
            InterNodeCommunicationService interNodeCommunicationService) {
        this.interNodeCommunicationService = interNodeCommunicationService;
    }

    /**
     * <p>
     * Sets the maximum number of archive-configuration commands that are
     * processed in parallel.
     * </p>
     * 
     * <p>
     * When many configuration commands are submitted at the same time (e.g.
     * through the {@link #runConfigurationCommands(Iterable)} method), running
     * all these commands concurrently might not make sense because it would put
     * a lot of load on the database, slowing it down too a point where the
     * processing of each command takes so long that it fails with a timeout. By
     * limiting the number of commands that are run concurrently, this can be
     * avoided. When the limit set through this method is reached, further
     * commands that are submitted are queued until the processing of other
     * commands has completed. This effectively means, that at most the
     * specified number of configuration commands is processed in parallel.
     * </p>
     * 
     * <p>
     * When this object is configured to use a
     * {@link ThrottlingCassandraProvider}, this limit should be set to about
     * four times the limit this provider has for concurrent write statements.
     * </p>
     * 
     * <p>
     * The default value of this setting is 64.
     * </p>
     * 
     * @param maxConcurrentArchiveConfigurationCommands
     *            maximum number of archive-configuration commands that are
     *            processed in parallel. Must be greater than zero.
     * @throws IllegalArgumentException
     *             if a value less than one is specified.
     */
    public void setMaxConcurrentArchiveConfigurationCommands(
            int maxConcurrentArchiveConfigurationCommands) {
        Preconditions
                .checkArgument(
                        maxConcurrentArchiveConfigurationCommands > 0,
                        "The maxConcurrentArchiveConfigurationCommands parameter must be greater than zero.");
        this.maxConcurrentArchiveConfigurationCommands = maxConcurrentArchiveConfigurationCommands;
    }

    /**
     * Sets the server-specific configuration properties. These server
     * properties are used to get the ID of this archiving server.
     * 
     * @param serverProperties
     *            configuration properties used by this server.
     */
    @Autowired
    public void setServerProperties(ServerProperties serverProperties) {
        this.serverProperties = serverProperties;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        Preconditions.checkState(archivingService != null);
        Preconditions.checkState(cassandraProvider != null);
        Preconditions.checkState(channelMetaDataDAO != null);
        Preconditions.checkState(clusterManagementService != null);
        Preconditions.checkState(controlSystemSupportRegistry != null);
        Preconditions.checkState(interNodeCommunicationService != null);
        Preconditions.checkState(serverProperties != null);
        thisServerId = serverProperties.getUuid();
        // We use daemon threads because we do not want any of our threads to
        // stop the JVM from shutdown.
        ThreadFactory daemonThreadFactory = new BasicThreadFactory.Builder()
                .daemon(true).build();
        // We use the number of available processors divided by two as the
        // number of threads. This is somehow inaccurate (for example, we might
        // create too many threads for processors with hyper threading), but
        // this should not hurt us too much. Even if we detect only a single
        // processor, we want to create at least two threads so that in case of
        // a blocking thread some other tasks still have a chance to run. On the
        // other hand, we limit the pool size to 8 threads so that we do not
        // create too many threads on a machine with a very large number of
        // cores. As the core pool size is always zero, we will not create
        // threads if we do not need them.
        int numberOfPoolThreads = Runtime.getRuntime().availableProcessors() / 2;
        numberOfPoolThreads = Math.max(numberOfPoolThreads, 2);
        numberOfPoolThreads = Math.min(numberOfPoolThreads, 8);
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
                daemonThreadFactory, new RejectedExecutionHandler() {
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
                });
    }

    @Override
    public void destroy() throws Exception {
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
     * <p>
     * Adds a channel to the archive configuration. In general, the
     * {@link #runConfigurationCommands(Iterable)} method, passing an instance
     * of {@link AddChannelCommand}, should be preferred over this method
     * because it provides better performance when making configuration changes
     * that affect different servers. However, this method can be used for
     * convenience if only a single channel is to be added.
     * </p>
     * 
     * <p>
     * The future returned by this method might throw the following exceptions:
     * </p>
     * 
     * <ul>
     * <li>{@link ChannelAlreadyExistsException} if <code>channelName</code> is
     * already in use.</li>
     * <li>{@link IllegalArgumentException} if <code>channelName</code> is empty
     * or <code>decimationLevels</code> contains negative elements.</li>
     * <li>{@link NullPointerException} if <code>channelName</code>,
     * <code>controlSystemType</code>, or <code>serverId</code> is
     * <code>null</code> or <code>decimationLevels</code>,
     * <code>decimationLevelToRetentionPeriod</code>, or <code>options</code>
     * contains <code>null</code> keys or values.</li>
     * <li>{@link PendingChannelOperationException} if another operation for the
     * specified channel is pending and thus the add operation cannot be
     * performed.</li>
     * <li>{@link RuntimeException} if an unexpected error occurs (e.g. problems
     * when accessing the database).</li>
     * </ul>
     * 
     * @param serverId
     *            ID of the server to which the channel shall be added.
     * @param channelName
     *            name of the channel to be added.
     * @param controlSystemType
     *            string identifying the control-system support that is used for
     *            the channel.
     * @param decimationLevels
     *            set of decimation levels that shall be created for the
     *            channel. The number identifying a decimation level represents
     *            the period between two samples (in seconds). The decimation
     *            level zero (for raw samples) is always created, even if it is
     *            not contained in the specified set. If <code>null</code>, only
     *            the decimation level zero is created.
     * @param decimationLevelToRetentionPeriod
     *            map containing the mapping of decimation levels to the
     *            corresponding retention period (both in seconds). If an entry
     *            for a decimation level is missing, a retention period of zero
     *            (keep samples indefinitely) is assumed. Negative retention
     *            periods are silently converted to zero. A <code>null</code>
     *            reference has the same effect as an empty map (use a retention
     *            period of zero for all decimation levels).
     * @param enabled
     *            <code>true</code> if archiving for the channel shall be
     *            enabled, <code>false</code> if archiving shall be disabled.
     * @param options
     *            map storing the control-system specific options for the
     *            channel. A <code>null</code> reference has the same effect as
     *            an empty map.
     * @return future that completes when the operation has finished. If the
     *         operation fails, the future will throw an exception.
     */
    public ListenableFuture<Void> addChannel(final UUID serverId,
            final String channelName, final String controlSystemType,
            final Set<Integer> decimationLevels,
            final Map<Integer, Integer> decimationLevelToRetentionPeriod,
            final boolean enabled, final Map<String, String> options) {
        try {
            return runConfigurationCommandInternal(new AddChannelCommand(
                    channelName, controlSystemType, decimationLevels,
                    decimationLevelToRetentionPeriod, enabled, options,
                    serverId));
        } catch (Throwable t) {
            return Futures.immediateFailedFuture(t);
        }
    }

    /**
     * <p>
     * Adds a channel to the archive configuration or updates the channel's
     * configuration if it already exists. This method is very similar to
     * {@link #addChannel(UUID, String, String, Set, Map, boolean, Map)}, but
     * its future will never throw a {@link ChannelAlreadyExistsException}. In
     * general, the {@link #runConfigurationCommands(Iterable)} method, passing
     * an instance of {@link AddOrUpdateChannelCommand}, should be preferred
     * over this method because it provides better performance when making
     * configuration changes that affect different servers. However, this method
     * can be used for convenience if only a single channel is to be added or
     * updated.
     * </p>
     * 
     * <p>
     * The future returned by this method might throw the following exceptions:
     * </p>
     * 
     * <ul>
     * <li>{@link IllegalArgumentException} if <code>channelName</code> is
     * empty, <code>decimationLevels</code> contains negative elements, or the
     * channel exists and the specified <code>serverId</code> or
     * <code>controlSystemType</code> do not match the ones of the existing
     * channel.</li>
     * <li>{@link NullPointerException} if <code>channelName</code>,
     * <code>controlSystemType</code>, or <code>serverId</code> is
     * <code>null</code> or <code>decimationLevels</code>,
     * <code>decimationLevelToRetentionPeriod</code>, or <code>options</code>
     * contains <code>null</code> keys or values.</li>
     * <li>{@link PendingChannelOperationException} if another operation for the
     * specified channel is pending and thus the add-or-update operation cannot
     * be performed.</li>
     * <li>{@link RuntimeException} if an unexpected error occurs (e.g. problems
     * when accessing the database).</li>
     * </ul>
     * 
     * @param serverId
     *            ID of the server to which the channel shall be added. If the
     *            channel already exists and the server ID does not match the
     *            one of existing channel, the operation will fail.
     * @param channelName
     *            name of the channel to be added or updated.
     * @param controlSystemType
     *            string identifying the control-system support that is used for
     *            the channel. If the channel already exists and the specified
     *            control-system type does not match the one of the existing
     *            channel, the operation will fail.
     * @param decimationLevels
     *            set of decimation levels that shall be created for the
     *            channel. If the channel already exists, decimation levels that
     *            currently exist but are not listed in this set are deleted.
     *            The number identifying a decimation level represents the
     *            period between two samples (in seconds). The decimation level
     *            zero (for raw samples) is always created, even if it is not
     *            contained in the specified set. If <code>null</code>, only the
     *            decimation level zero is created.
     * @param decimationLevelToRetentionPeriod
     *            map containing the mapping of decimation levels to the
     *            corresponding retention period (both in seconds). If an entry
     *            for a decimation level is missing, a retention period of zero
     *            (keep samples indefinitely) is assumed. Negative retention
     *            periods are silently converted to zero. A <code>null</code>
     *            reference has the same effect as an empty map (use a retention
     *            period of zero for all decimation levels).
     * @param enabled
     *            <code>true</code> if archiving for the channel shall be
     *            enabled, <code>false</code> if archiving shall be disabled.
     * @param options
     *            map storing the control-system specific options for the
     *            channel. A <code>null</code> reference has the same effect as
     *            an empty map.
     * @return future that completes when the operation has finished. If the
     *         operation fails, the future will throw an exception.
     */
    public ListenableFuture<Void> addOrUpdateChannel(final UUID serverId,
            final String channelName, final String controlSystemType,
            final Set<Integer> decimationLevels,
            final Map<Integer, Integer> decimationLevelToRetentionPeriod,
            final boolean enabled, final Map<String, String> options) {
        try {
            return runConfigurationCommandInternal(new AddOrUpdateChannelCommand(
                    channelName, controlSystemType, decimationLevels,
                    decimationLevelToRetentionPeriod, enabled, options,
                    serverId));
        } catch (Throwable t) {
            return Futures.immediateFailedFuture(t);
        }
    }

    /**
     * <p>
     * Moves a channel from one server to another one. In general, the
     * {@link #runConfigurationCommands(Iterable)} method, passing an instance
     * of {@link MoveChannelCommand}, should be preferred over this method
     * because it provides better performance when making configuration changes
     * that affect different servers. However, this method can be used for
     * convenience if only a single channel is to be moved.
     * </p>
     * 
     * <p>
     * The future returned by this method might throw the following exceptions:
     * </p>
     * 
     * <ul>
     * <li>{@link IllegalArgumentException} if <code>channelName</code> is empty
     * or the channel is not currently registered with the server identified by
     * <code>expectedOldServerId</code>.</li>
     * <li>{@link NoSuchChannelException} if <code>channelName</code> does not
     * refer to an existing channel.</li>
     * <li>{@link NullPointerException} if <code>channelName</code> or
     * <code>newServerId</code> is <code>null</code>.</li>
     * <li>{@link PendingChannelOperationException} if another operation for the
     * specified channel is pending and thus the move operation cannot be
     * performed.</li>
     * <li>{@link RuntimeException} if an unexpected error occurs (e.g. problems
     * when accessing the database).</li>
     * </ul>
     * 
     * @param expectedOldServerId
     *            if not <code>null</code>, the channel is only moved if it is
     *            currently registered with the specified server.
     * @param newServerId
     *            ID of the server to which the channel shall be moved.
     * @param channelName
     *            name identifying the channel to be moved.
     * @return future that completes when the operation has finished. If the
     *         operation fails, the future will throw an exception.
     */
    public ListenableFuture<Void> moveChannel(UUID expectedOldServerId,
            UUID newServerId, String channelName) {
        try {
            return runConfigurationCommandInternal(new MoveChannelCommand(
                    channelName, expectedOldServerId, newServerId));
        } catch (Throwable t) {
            return Futures.immediateFailedFuture(t);
        }
    }

    /**
     * <p>
     * Refreshes the channel on the specified server. Refreshing a channel
     * results in the channel to be stopped, the channel configuration being
     * reloaded, and the channel being started again.
     * </p>
     * 
     * <p>
     * In general, the {@link #runConfigurationCommands(Iterable)} method,
     * passing an instance of {@link RefreshChannelCommand}, should be preferred
     * over this method because it provides better performance when making
     * configuration changes that affect different servers. However, this method
     * can be used for convenience if only a single channel is to be refreshed.
     * </p>
     * 
     * <p>
     * The future returned by this method might throw the following exceptions:
     * </p>
     * 
     * <ul>
     * <li>{@link IllegalArgumentException} if <code>channelName</code> is
     * empty.</li>
     * <li>{@link NullPointerException} if <code>channelName</code> or
     * <code>serverId</code> is <code>null</code>.</li>
     * <li>{@link RuntimeException} if an unexpected error occurs (e.g. problems
     * when accessing the database or communicating with remote servers).</li>
     * </ul>
     * 
     * @param serverId
     *            ID of the server on which the channel should be refreshed.
     *            That does not have to be the server that owns the channel. For
     *            example, after moving a channel it might still exist on the
     *            old server and refreshing will have the effect of removing it
     *            from the configuration set. If a channel is not in a servers
     *            current configuration set and the channel is not owned by the
     *            server, the server will not continue after loading the channel
     *            configuration, effectively making the refresh a no-op.
     * @param channelName
     *            name of the channel to be refreshed.
     * @return future that completes when the operation has finished. If the
     *         operation fails, the future will throw an exception.
     */
    public ListenableFuture<Void> refreshChannel(UUID serverId,
            String channelName) {
        try {
            return runConfigurationCommandInternal(new RefreshChannelCommand(
                    channelName, serverId));
        } catch (Throwable t) {
            return Futures.immediateFailedFuture(t);
        }
    }

    /**
     * <p>
     * Removes a channel from the archive configuration. In general, the
     * {@link #runConfigurationCommands(Iterable)} method, passing an instance
     * of {@link RemoveChannelCommand}, should be preferred over this method
     * because it provides better performance when making configuration changes
     * that affect different servers. However, this method can be used for
     * convenience if only a single channel is to be removed.
     * </p>
     * 
     * <p>
     * The future returned by this method might throw the following exceptions:
     * </p>
     * 
     * <ul>
     * <li>{@link IllegalArgumentException} if <code>channelName</code> is empty
     * or the channel is not currently registered with the server identified by
     * <code>expectedServerId</code>.</li>
     * <li>{@link NoSuchChannelException} if <code>channelName</code> does not
     * refer to an existing channel.</li>
     * <li>{@link NullPointerException} if <code>channelName</code> is
     * <code>null</code>.</li>
     * <li>{@link PendingChannelOperationException} if another operation for the
     * specified channel is pending and thus the remove operation cannot be
     * performed.</li>
     * <li>{@link RuntimeException} if an unexpected error occurs (e.g. problems
     * when accessing the database).</li>
     * </ul>
     * 
     * @param expectedServerId
     *            if not <code>null</code>, the channel is only removed if it is
     *            currently registered with the specified server.
     * @param channelName
     *            name identifying the channel to be removed.
     * @return future that completes when the operation has finished. If the
     *         operation fails, the future will throw an exception.
     */
    public ListenableFuture<Void> removeChannel(final UUID expectedServerId,
            final String channelName) {
        try {
            return runConfigurationCommandInternal(new RemoveChannelCommand(
                    channelName, expectedServerId));
        } catch (Throwable t) {
            return Futures.immediateFailedFuture(t);
        }
    }

    /**
     * <p>
     * Removes a channel from the archive configuration. In general, the
     * {@link #runConfigurationCommands(Iterable)} method, passing an instance
     * of {@link RenameChannelCommand}, should be preferred over this method
     * because it provides better performance when making configuration changes
     * that affect different servers. However, this method can be used for
     * convenience if only a single channel is to be renamed.
     * </p>
     * 
     * <p>
     * The future returned by this method might throw the following exceptions:
     * </p>
     * 
     * <ul>
     * <li>{@link ChannelAlreadyExistsException} if <code>newChannelName</code>
     * is already in use.</li>
     * <li>{@link IllegalArgumentException} if <code>oldChannelName</code> or
     * <code>newChannelName</code> is empty or the channel is not currently
     * registered with the server identified by <code>expectedServerId</code>.</li>
     * <li>{@link NoSuchChannelException} if <code>oldChannelName</code> does
     * not refer to an existing channel.</li>
     * <li>{@link NullPointerException} if <code>oldChannelName</code> or
     * <code>newChannelName</code> is <code>null</code>.</li>
     * <li>{@link PendingChannelOperationException} if another operation for the
     * specified channel is pending and thus the rename operation cannot be
     * performed.</li>
     * <li>{@link RuntimeException} if an unexpected error occurs (e.g. problems
     * when accessing the database).</li>
     * </ul>
     * 
     * @param expectedServerId
     *            if not <code>null</code>, the channel is only renamed if it is
     *            currently registered with the specified server.
     * @param oldChannelName
     *            old name of the channel that shall be renamed.
     * @param newChannelName
     *            new name of the channel that shall be renamed.
     * @return future that completes when the operation has finished. If the
     *         operation fails, the future will throw an exception.
     */
    public ListenableFuture<Void> renameChannel(final UUID expectedServerId,
            final String oldChannelName, final String newChannelName) {
        try {
            return runConfigurationCommandInternal(new RenameChannelCommand(
                    expectedServerId, newChannelName, oldChannelName));
        } catch (Throwable t) {
            return Futures.immediateFailedFuture(t);
        }
    }

    /**
     * <p>
     * Runs the specified configuration commands. Basically, this method
     * provides the same functions as the
     * {@link #addChannel(UUID, String, String, Set, Map, boolean, Map)},
     * {@link #addOrUpdateChannel(UUID, String, String, Set, Map, boolean, Map)}
     * , {@link #moveChannel(UUID, UUID, String)},
     * {@link #removeChannel(UUID, String)},
     * {@link #renameChannel(UUID, String, String)}, and
     * {@link #updateChannel(UUID, String, String, Set, Set, Set, Map, Boolean, Map, Map, Set)}
     * methods. However, it aggregates operations that require communication
     * with other servers in a way that reduces the number of network requests.
     * Therefore, it should be preferred over the aforementioned methods when
     * multiple operations shall be performed.
     * </p>
     * 
     * <p>
     * The future returned by this method only throws an exception if a problem
     * that affects all of the commands is detected. Problems with individual
     * commands results in a {@link ArchiveConfigurationCommandResult} that
     * indicates failure for the respective command. The future returned by this
     * method might throw the following exceptions:
     * </p>
     * 
     * <ul>
     * <li>{@link NullPointerException} if <code>commands</code> is
     * <code>null</code> or contains <code>null</code> elements.</li>
     * <li>{@link RuntimeException} if an unexpected error occurs that affects
     * all commands (e.g. problems when accessing the database).</li>
     * </ul>
     * 
     * <p>
     * The list returned by the future returned by this method contains the
     * results for the configuration commands in the same or in which the
     * command were passed to this method.
     * </p>
     * 
     * @param commands
     *            commands to be executed.
     * @return future that completes when all operation triggered by the
     *         commands have finished. If the one of the operation fails, the
     *         corresponding {@link ArchiveConfigurationCommandResult} will
     *         indicate failure. If a problem prevents the execution of all of
     *         the commands, the future might throw an exception.
     */
    public ListenableFuture<List<ArchiveConfigurationCommandResult>> runConfigurationCommands(
            Iterable<? extends ArchiveConfigurationCommand> commands) {
        // Our internal implementation returns a list that includes the original
        // Throwable for each result because we want to use this Throwable when
        // calling the function internally. When called via the web-service API,
        // however, we are only interested in the result objects, so we simply
        // convert the list.
        return Futures.transform(runConfigurationCommandsInternal(commands),
                CONVERT_TO_RESULT_ONLY_LIST);
    }

    /**
     * <p>
     * Updates a channel's configuration. In general, the
     * {@link #runConfigurationCommands(Iterable)} method, passing an instance
     * of {@link UpdateChannelCommand}, should be preferred over this method
     * because it provides better performance when making configuration changes
     * that affect different servers. However, this method can be used for
     * convenience if only a single channel is to be changed.
     * </p>
     * 
     * <p>
     * The future returned by this method might throw the following exceptions:
     * </p>
     * 
     * <ul>
     * <li>{@link IllegalArgumentException} if <code>channelName</code> is
     * empty, <code>decimationLevels</code> contains negative elements,
     * <code>expectedServerId</code> specifies a different server than the one
     * with which the channel is currently registered, or
     * <code>expectedControlSystemType</code>, different from the control-system
     * type of the existing channel, or if a constraint of the
     * {@link UpdateChannelCommand#UpdateChannelCommand(String, String, UUID, Set, Set, Set, Map, Boolean, Map, Map, Set)
     * UpdateChannelCommand(...)}. constructor is violated. This exception is
     * also thrown when the requested update would violate the constraint that
     * each decimation level must have a retention period that is equal to or
     * greater than the retention period of the preceding (shorter decimation
     * period) level.</li>
     * <li>{@link NoSuchChannelException} if <code>channelName</code> does not
     * refer to an existing channel.</li>
     * <li>{@link NullPointerException} if <code>channelName</code> is
     * <code>null</code> or any of the sets or maps contains <code>null</code>
     * keys or values.</li>
     * <li>{@link PendingChannelOperationException} if another operation for the
     * specified channel is pending and thus the update operation cannot be
     * performed.</li>
     * <li>{@link RuntimeException} if an unexpected error occurs (e.g. problems
     * when accessing the database).</li>
     * </ul>
     * 
     * <p>
     * This method passes all its parameters to the constructor of the
     * {@link UpdateChannelCommand} and then runs this command. Therefore, the
     * future returned by this method will throw any exception thrown by the
     * constructor of the {@link UpdateChannelCommand} or thrown when running
     * the command.
     * </p>
     * 
     * @param expectedServerId
     *            if not <code>null</code>, the update operation will fail if
     *            the channel is currently registered with a different server
     *            than the specified one.
     * @param channelName
     *            name of the channel to be updated.
     * @param expectedControlSystemType
     *            if not <code>null</code>, the update operation will fail if
     *            the channel's actual control-system type is different.
     * @param decimationLevels
     *            set of decimation levels that shall exist for the channel
     *            after the update. May only be specified if both
     *            <code>addDecimationLevels</code> and
     *            <code>removeDecimationLevels</code> are <code>null</code>.
     * @param addDecimationLevels
     *            set of decimation levels to be added. May only be specified if
     *            <code>decimationLevels</code> is <code>null</code>.
     * @param removeDecimationLevels
     *            set of decimation levels to be removed. May only be specified
     *            if <code>decimationLevels</code> is <code>null</code>.
     * @param decimationLevelToRetentionPeriod
     *            map mapping decimation levels to their respective retention
     *            period. Decimation levels specified in this map will have
     *            their retention periods updated with the specified values.
     *            However, decimation levels not explicitly specified in this
     *            map might also have their retention period reset to zero.
     *            Refer to the description of
     *            {@link UpdateChannelCommand#getDecimationLevelToRetentionPeriod()}
     *            for details.
     * @param enabled
     *            whether archiving for the channel shall be enabled or
     *            disabled.
     * @param options
     *            control-system-specific configuration options. May only be
     *            specified if both <code>addOptions</code> and
     *            <code>removeOptions</code> are <code>null</code>.
     * @param addOptions
     *            control-system-specific configuration options to be added. May
     *            only be specified if <code>options</code> is <code>null</code>
     *            .
     * @param removeOptions
     *            control-system-specific configuration options to be removed.
     *            May only be specified if <code>options</code> is
     *            <code>null</code>.
     * @return future that completes when the operation has finished. If the
     *         operation fails, the future will throw an exception.
     */
    public ListenableFuture<Void> updateChannel(UUID expectedServerId,
            String channelName, final String expectedControlSystemType,
            Set<Integer> decimationLevels, Set<Integer> addDecimationLevels,
            Set<Integer> removeDecimationLevels,
            Map<Integer, Integer> decimationLevelToRetentionPeriod,
            Boolean enabled, Map<String, String> options,
            Map<String, String> addOptions, Set<String> removeOptions) {
        try {
            return runConfigurationCommandInternal(new UpdateChannelCommand(
                    channelName, expectedControlSystemType, expectedServerId,
                    decimationLevels, addDecimationLevels,
                    removeDecimationLevels, decimationLevelToRetentionPeriod,
                    enabled, null, addOptions, removeOptions));
        } catch (Throwable t) {
            return Futures.immediateFailedFuture(t);
        }
    }

    private ListenableFuture<Boolean> addOrUpdateInternal(
            final AddOrUpdateChannelCommand command,
            boolean prefetchedChannelInformationValid,
            ChannelInformation prefetchedChannelInformation) {
        if (prefetchedChannelInformationValid) {
            // If we have no channel information, the channel does not exist yet
            // and we want to add it. If we have channel information, the
            // channel already exists and we want to update it. There is a
            // chance that the channel gets added (or removed) between doing
            // this check and actually running the operation. Therefore, we
            // catch ChannelAlreadyExistsException and NoSuchChannelException
            // and run the opposite action, if needed.
            if (prefetchedChannelInformation == null) {
                return FutureUtils.transform(AddChannelOperation.addChannel(
                        archivingService,
                        channelInformationCache,
                        channelMetaDataDAO,
                        clusterManagementService,
                        interNodeCommunicationService,
                        thisServerId,
                        new AddChannelCommand(command.getChannelName(), command
                                .getControlSystemType(), command
                                .getDecimationLevels(), command
                                .getDecimationLevelToRetentionPeriod(), command
                                .isEnabled(), command.getOptions(), command
                                .getServerId()), prefetchedChannelInformation),
                        AsyncFunctionUtils.<Boolean> identity(),
                        new AsyncFunction<Throwable, Boolean>() {
                            @Override
                            public ListenableFuture<Boolean> apply(
                                    Throwable input) throws Exception {
                                if (input instanceof ChannelAlreadyExistsException) {
                                    return addOrUpdateInternal(command, false,
                                            null);
                                } else {
                                    return Futures.immediateFailedFuture(input);
                                }
                            }
                        });
            } else {
                return FutureUtils
                        .transform(
                                UpdateChannelOperation.updateChannel(
                                        archivingService,
                                        channelInformationCache,
                                        channelMetaDataDAO,
                                        clusterManagementService,
                                        controlSystemSupportRegistry,
                                        poolExecutor,
                                        interNodeCommunicationService,
                                        thisServerId,
                                        new UpdateChannelCommand(
                                                command.getChannelName(),
                                                command.getControlSystemType(),
                                                command.getServerId(),
                                                command.getDecimationLevels(),
                                                null,
                                                null,
                                                command.getDecimationLevelToRetentionPeriod(),
                                                command.isEnabled(), command
                                                        .getOptions(), null,
                                                null),
                                        prefetchedChannelInformation),
                                AsyncFunctionUtils.<Boolean> identity(),
                                new AsyncFunction<Throwable, Boolean>() {
                                    @Override
                                    public ListenableFuture<Boolean> apply(
                                            Throwable input) throws Exception {
                                        if (input instanceof NoSuchChannelException) {
                                            return addOrUpdateInternal(command,
                                                    false, null);
                                        } else {
                                            return Futures
                                                    .immediateFailedFuture(input);
                                        }
                                    }
                                });
            }
        } else {
            // If we do not have a valid channel information, we first have to
            // get it. We need it to know whether the channel already exists
            // (and should be updated) or whether it has to be added.
            return Futures.transform(
                    channelInformationCache.getChannelAsync(
                            command.getChannelName(), false),
                    new AsyncFunction<ChannelInformation, Boolean>() {
                        @Override
                        public ListenableFuture<Boolean> apply(
                                ChannelInformation input) throws Exception {
                            return addOrUpdateInternal(command, true, input);
                        }
                    });
        }
    }

    private ListenableFuture<Void> runConfigurationCommandInternal(
            ArchiveConfigurationCommand command) {
        return Futures
                .transform(runConfigurationCommandsInternal(Collections
                        .singleton(command)), THROW_IF_NOT_SUCCESSFUL);
    }

    private ListenableFuture<List<Pair<ArchiveConfigurationCommandResult, Throwable>>> runConfigurationCommandsInternal(
            final Iterable<? extends ArchiveConfigurationCommand> commands) {
        if (commands == null) {
            return Futures.immediateFailedFuture(new NullPointerException());
        }
        if (Iterables.any(commands, Predicates.isNull())) {
            return Futures.immediateFailedFuture(new NullPointerException());
        }
        final HashMap<String, ListenableFuture<ChannelInformation>> channelInformationFutures = new HashMap<String, ListenableFuture<ChannelInformation>>();
        // For each channel specified in a command, we have to find out which
        // server is responsible for that channel. If several commands share the
        // same channel, it is sufficient to get this information once. We do
        // not have to get the channel information for refresh commands because
        // those commands are always run on the server specified in the command.
        for (ArchiveConfigurationCommand command : commands) {
            String channelName = extractChannelName(command);
            if (channelName != null
                    && !channelInformationFutures.containsKey(channelName)
                    && !command.getCommandType().equals(
                            ArchiveConfigurationCommand.Type.REFRESH_CHANNEL)) {
                channelInformationFutures.put(channelName,
                        channelInformationCache.getChannelAsync(channelName,
                                false));
            }
        }
        // We need to remember which channels have been modified and thus
        // require the channel-information cache to be updated. We have to use
        // a synchronized map because the callbacks that add channels to this
        // list might be executed in different threads. We pass this list to
        // runConfigurationCommandLocal which will fill it if needed. In the
        // end, when all operations have finishes, we process this list and
        // schedule updates of the cache. This way, the remote communication
        // required for the updates can all be aggregated in a single request.
        final List<String> channelsNeedingCacheUpdate = Collections
                .synchronizedList(new LinkedList<String>());
        // We wait for all futures to complete. We need the channel information
        // for all channels before we can continue because we want to aggregate
        // commands by their server ID. We do not have any specific error
        // handling code. If we cannot get the information for a channel, it is
        // very likely that we cannot get the information for other channels as
        // well (such a problem is most likely called by a database problem).
        // Thus, there is no good reason to separate errors for individual
        // channels and we can fail the whole operation.
        // Once we have received the channel information, we can trigger all the
        // actual operations. Even though this process should be non-blocking,
        // it is rather complex and thus we run it using the poolExecutor to be
        // safe.
        return Futures
                .transform(
                        FutureUtils.transformAnyToVoid(Futures
                                .allAsList(channelInformationFutures.values())),
                        new AsyncFunction<Void, List<Pair<ArchiveConfigurationCommandResult, Throwable>>>() {
                            @Override
                            public ListenableFuture<List<Pair<ArchiveConfigurationCommandResult, Throwable>>> apply(
                                    Void input) throws Exception {
                                // We want to aggregate commands that are
                                // supposed to be executed by the same server.
                                // This way, we can minimize the number of
                                // requests sent over the network.
                                // We use a LinkedListMultimap because this will
                                // preserve the order of commands and thus the
                                // order of the results will exactly be the
                                // order of the original commands.
                                LinkedListMultimap<UUID, ArchiveConfigurationCommand> commandsByServer = LinkedListMultimap
                                        .create();
                                for (ArchiveConfigurationCommand command : commands) {
                                    String channelName = extractChannelName(command);
                                    if (channelName == null) {
                                        // If we could not extract the channel
                                        // name, the only reasonable explanation
                                        // is that the command is of an
                                        // unsupported type. We still add it to
                                        // the map using a null key so that it
                                        // will be included in the results.
                                        commandsByServer.put(null, command);
                                    } else if (command
                                            .getCommandType()
                                            .equals(ArchiveConfigurationCommand.Type.REFRESH_CHANNEL)) {
                                        // We do not have (and do not need) the
                                        // channel information for refresh
                                        // commands. We always run them on the
                                        // specified server.
                                        commandsByServer
                                                .put(((RefreshChannelCommand) command)
                                                        .getServerId(), command);
                                    } else {
                                        // The fact that we are in this method
                                        // means that all futures completed
                                        // successfully. Therefore, we can
                                        // safely use getUnchecked(...).
                                        ChannelInformation channelInformation = FutureUtils
                                                .getUnchecked(channelInformationFutures
                                                        .get(channelName));
                                        UUID serverId;
                                        // If we do not have a channel
                                        // information (because the channel does
                                        // not exist), we can execute the
                                        // command on any server - so we can
                                        // simply execute it on this server.
                                        // However, we prefer executing it on
                                        // the target server because this makes
                                        // the whole locking scheme simpler. For
                                        // the same reason, it is better to run
                                        // a move channel operation on the new
                                        // server instead of the old server.
                                        if (channelInformation != null) {
                                            serverId = channelInformation
                                                    .getServerId();
                                        } else if (command
                                                .getCommandType()
                                                .equals(ArchiveConfigurationCommand.Type.ADD_CHANNEL)) {
                                            serverId = ((AddChannelCommand) command)
                                                    .getServerId();
                                        } else if (command
                                                .getCommandType()
                                                .equals(ArchiveConfigurationCommand.Type.ADD_OR_UPDATE_CHANNEL)) {
                                            serverId = ((AddOrUpdateChannelCommand) command)
                                                    .getServerId();
                                        } else {
                                            serverId = thisServerId;
                                        }
                                        commandsByServer.put(serverId, command);
                                    }
                                }
                                LinkedList<ListenableFuture<Pair<ArchiveConfigurationCommandResult, Throwable>>> resultFutures = new LinkedList<ListenableFuture<Pair<ArchiveConfigurationCommandResult, Throwable>>>();
                                for (UUID serverId : commandsByServer.keySet()) {
                                    List<ArchiveConfigurationCommand> commands = commandsByServer
                                            .get(serverId);
                                    // We can ignore commands for which we do
                                    // not have a server ID because we already
                                    // dealt with those commands.
                                    if (serverId == null) {
                                        for (ArchiveConfigurationCommand command : commands) {
                                            resultFutures.add(Futures.immediateFuture(Pair
                                                    .<ArchiveConfigurationCommandResult, Throwable> of(
                                                            ArchiveConfigurationCommandResult
                                                                    .failure(
                                                                            command,
                                                                            "The specified command type is not supported."),
                                                            new RuntimeException(
                                                                    "The specified command type is not supported."))));
                                        }
                                        continue;
                                    }
                                    // If the channels are local or the server
                                    // that owns the channels is offline, we
                                    // have to run the commands locally.
                                    // However, it does not make sense to run a
                                    // refresh command that was intended for a
                                    // different server locally. We can simply
                                    // report success for such a command because
                                    // a refresh on an offline server is always
                                    // considered successful (the server does
                                    // not use an outdated configuration for the
                                    // channel because it is not online at all).
                                    String targetServerBaseUrl = clusterManagementService
                                            .getInterNodeCommunicationUrl(serverId);
                                    if (thisServerId.equals(serverId)
                                            || targetServerBaseUrl == null) {
                                        for (ArchiveConfigurationCommand command : commands) {
                                            if (command
                                                    .getCommandType()
                                                    .equals(ArchiveConfigurationCommand.Type.REFRESH_CHANNEL)
                                                    && !thisServerId
                                                            .equals(serverId)) {
                                                resultFutures.add(Futures.immediateFuture(Pair
                                                        .<ArchiveConfigurationCommandResult, Throwable> of(
                                                                ArchiveConfigurationCommandResult
                                                                        .success(command),
                                                                null)));
                                            } else {
                                                // The channelInformationFuture
                                                // might be null if the command
                                                // is a refresh command. In this
                                                // case, we do not need the
                                                // ChannelInformation anyway.
                                                ListenableFuture<ChannelInformation> channelInformationFuture = channelInformationFutures
                                                        .get(extractChannelName(command));
                                                resultFutures
                                                        .add(runConfigurationCommandLocal(
                                                                command,
                                                                channelInformationFuture == null ? null
                                                                        : FutureUtils
                                                                                .getUnchecked(channelInformationFuture),
                                                                channelsNeedingCacheUpdate));
                                            }
                                        }
                                    } else {
                                        ListenableFuture<List<ArchiveConfigurationCommandResult>> remoteFuture = interNodeCommunicationService
                                                .runArchiveConfigurationCommands(
                                                        targetServerBaseUrl,
                                                        commands);
                                        int commandCounter = 0;
                                        for (final ArchiveConfigurationCommand command : commands) {
                                            final int commandIndex = commandCounter;
                                            ++commandCounter;
                                            // Even if the remote operation
                                            // fails, we want to generate a
                                            // result for each command. In case
                                            // of failure, it will simply
                                            // contain an error message.
                                            final SettableFuture<Pair<ArchiveConfigurationCommandResult, Throwable>> future = SettableFuture
                                                    .create();
                                            Futures.addCallback(
                                                    remoteFuture,
                                                    new FutureCallback<List<ArchiveConfigurationCommandResult>>() {
                                                        @Override
                                                        public void onSuccess(
                                                                List<ArchiveConfigurationCommandResult> result) {
                                                            // If the server
                                                            // responded with a
                                                            // reasonable reply
                                                            // reply, this
                                                            // should never
                                                            // happen, but it is
                                                            // better to check
                                                            // it anyway.
                                                            if (result == null
                                                                    || result
                                                                            .size() <= commandIndex) {
                                                                future.set(Pair
                                                                        .<ArchiveConfigurationCommandResult, Throwable> of(
                                                                                ArchiveConfigurationCommandResult
                                                                                        .failure(
                                                                                                command,
                                                                                                "Invalid reply from remote server."),
                                                                                new RuntimeException(
                                                                                        "Invalid reply from remote server.")));
                                                            } else {
                                                                ArchiveConfigurationCommandResult resultElement = result
                                                                        .get(commandIndex);
                                                                future.set(Pair
                                                                        .<ArchiveConfigurationCommandResult, Throwable> of(
                                                                                resultElement,
                                                                                resultElement
                                                                                        .isSuccess() ? null
                                                                                        : new RuntimeException(
                                                                                                resultElement
                                                                                                        .getErrorMessage())));
                                                            }
                                                        }

                                                        @Override
                                                        public void onFailure(
                                                                Throwable t) {
                                                            future.set(Pair
                                                                    .<ArchiveConfigurationCommandResult, Throwable> of(
                                                                            ArchiveConfigurationCommandResult
                                                                                    .failure(
                                                                                            command,
                                                                                            t.getMessage()),
                                                                            t));
                                                        }
                                                    });
                                            resultFutures.add(future);
                                        }
                                    }
                                }
                                return Futures.transform(
                                        Futures.allAsList(resultFutures),
                                        new AsyncFunction<List<Pair<ArchiveConfigurationCommandResult, Throwable>>, List<Pair<ArchiveConfigurationCommandResult, Throwable>>>() {
                                            @Override
                                            public ListenableFuture<List<Pair<ArchiveConfigurationCommandResult, Throwable>>> apply(
                                                    List<Pair<ArchiveConfigurationCommandResult, Throwable>> input) {
                                                // Before returning, we want to
                                                // update the channel cache for
                                                // those channels that have been
                                                // modified. We wait for the
                                                // update to finish so that the
                                                // user will see the updated
                                                // state. However, we do not
                                                // want the whole operation to
                                                // fail just because there is a
                                                // problem with updating the
                                                // cache.
                                                Function<Object, List<Pair<ArchiveConfigurationCommandResult, Throwable>>> returnInput = Functions
                                                        .constant(input);
                                                return FutureUtils.transform(
                                                        channelInformationCache
                                                                .updateChannels(channelsNeedingCacheUpdate),
                                                        returnInput,
                                                        returnInput);
                                            }
                                        });
                            }
                        }, poolExecutor);
    }

    private ListenableFuture<Pair<ArchiveConfigurationCommandResult, Throwable>> runConfigurationCommandLocal(
            ArchiveConfigurationCommand command,
            ChannelInformation prefetchedChannelInformation,
            List<String> channelsNeedingCacheUpdate) {
        // The parameter channelsNeedingCacheUpdate must be a synchronized (or
        // otherwise thread-safe) list, because we cannot know which thread will
        // update it.
        // When a large number of commands is run in parallel, this can overload
        // the database server resulting in many of the operations failing. In
        // addition to that, such an overload can interfere with the archive
        // operation. For this reason, we limit the number of commands that we
        // run in parallel and queue additional commands to be run when one of
        // the running operations has finished.
        // We synchronize on the pendingConfigurationCommands list both for
        // modifying the list and dealing with the
        // runningConfigurationCommandsCount. This is a lot simpler than dealing
        // with a thread-safe list and using an atomic counter.
        synchronized (pendingConfigurationCommands) {
            if (runningConfigurationCommandsCount >= maxConcurrentArchiveConfigurationCommands) {
                PendingConfigurationCommand pendingCommand = new PendingConfigurationCommand(
                        command, prefetchedChannelInformation,
                        channelsNeedingCacheUpdate);
                pendingConfigurationCommands.add(pendingCommand);
                return pendingCommand.resultFuture;
            } else {
                ++runningConfigurationCommandsCount;
            }
        }
        return runConfigurationCommandLocalNoCounterIncrement(command,
                prefetchedChannelInformation, channelsNeedingCacheUpdate);
    }

    private void runConfigurationCommandLocalDecrementCounterOrRunNextCommand() {
        // When we are done with running a command, we have to decrement the
        // counter or run the next command if one is pending.
        PendingConfigurationCommand nextConfigurationCommand;
        synchronized (pendingConfigurationCommands) {
            nextConfigurationCommand = pendingConfigurationCommands.poll();
            // If we start the next command right away,
            // we do not have to decrement the counter.
            if (nextConfigurationCommand == null) {
                --runningConfigurationCommandsCount;
            }
        }
        if (nextConfigurationCommand != null) {
            ListenableFuture<Pair<ArchiveConfigurationCommandResult, Throwable>> resultFuture;
            try {
                resultFuture = runConfigurationCommandLocalNoCounterIncrement(
                        nextConfigurationCommand.command,
                        nextConfigurationCommand.prefetchedChannelInformation,
                        nextConfigurationCommand.channelsNeedingCacheUpdate);
                FutureUtils.forward(resultFuture,
                        nextConfigurationCommand.resultFuture);
            } catch (Throwable t) {
                nextConfigurationCommand.resultFuture.setException(t);
            }
        }
    }

    private ListenableFuture<Pair<ArchiveConfigurationCommandResult, Throwable>> runConfigurationCommandLocalNoCounterIncrement(
            final ArchiveConfigurationCommand command,
            ChannelInformation prefetchedChannelInformation,
            final List<String> channelsNeedingCacheUpdate) {
        // The parameter channelsNeedingCacheUpdate must be a synchronized (or
        // otherwise thread-safe) list, because we cannot know which thread will
        // update it.
        ListenableFuture<Boolean> operationFuture = null;
        try {
            switch (command.getCommandType()) {
            case ADD_CHANNEL:
                operationFuture = AddChannelOperation.addChannel(
                        archivingService, channelInformationCache,
                        channelMetaDataDAO, clusterManagementService,
                        interNodeCommunicationService, thisServerId,
                        (AddChannelCommand) command,
                        prefetchedChannelInformation);
                break;
            case ADD_OR_UPDATE_CHANNEL:
                operationFuture = addOrUpdateInternal(
                        (AddOrUpdateChannelCommand) command, true,
                        prefetchedChannelInformation);
                break;
            case MOVE_CHANNEL:
                operationFuture = MoveChannelOperation.moveChannel(
                        archivingService, channelInformationCache,
                        channelMetaDataDAO, clusterManagementService,
                        interNodeCommunicationService, thisServerId,
                        (MoveChannelCommand) command,
                        prefetchedChannelInformation);
                break;
            case REFRESH_CHANNEL:
                // We know that extractChannelName does not return null because
                // it supports the same commands that we support here. We can
                // always use a return value of false because a refresh
                // operation never modifies the channel.
                operationFuture = Futures.transform(archivingService
                        .refreshChannel(extractChannelName(command)),
                        ALWAYS_FALSE);
                break;
            case REMOVE_CHANNEL:
                operationFuture = RemoveChannelOperation.removeChannel(
                        archivingService, channelInformationCache,
                        channelMetaDataDAO, clusterManagementService,
                        controlSystemSupportRegistry, poolExecutor,
                        interNodeCommunicationService, thisServerId,
                        (RemoveChannelCommand) command,
                        prefetchedChannelInformation);
                break;
            case RENAME_CHANNEL:
                operationFuture = RenameChannelOperation.renameChannel(
                        archivingService, channelInformationCache,
                        channelMetaDataDAO, clusterManagementService,
                        interNodeCommunicationService, thisServerId,
                        (RenameChannelCommand) command,
                        prefetchedChannelInformation);
                break;
            case UPDATE_CHANNEL:
                operationFuture = UpdateChannelOperation.updateChannel(
                        archivingService, channelInformationCache,
                        channelMetaDataDAO, clusterManagementService,
                        controlSystemSupportRegistry, poolExecutor,
                        interNodeCommunicationService, thisServerId,
                        (UpdateChannelCommand) command,
                        prefetchedChannelInformation);
                break;
            default:
                return Futures
                        .immediateFuture(Pair
                                .<ArchiveConfigurationCommandResult, Throwable> of(
                                        ArchiveConfigurationCommandResult
                                                .failure(command,
                                                        "The specified command type is not supported."),
                                        new RuntimeException(
                                                "The specified command type is not supported.")));
            }
        } catch (Throwable t) {
            operationFuture = Futures.immediateFailedFuture(t);
        } finally {
            // If we do not have an operationFuture, the transformation is not
            // going to be applied and we have to take care of decrementing the
            // counter or running the next command now.
            if (operationFuture == null) {
                runConfigurationCommandLocalDecrementCounterOrRunNextCommand();
            }
        }
        return FutureUtils
                .transform(
                        operationFuture,
                        new Function<Boolean, Pair<ArchiveConfigurationCommandResult, Throwable>>() {
                            @Override
                            public Pair<ArchiveConfigurationCommandResult, Throwable> apply(
                                    Boolean input) {
                                // We are done with running the command, so we
                                // have to decrement the counter or run the
                                // next command if one is pending.
                                runConfigurationCommandLocalDecrementCounterOrRunNextCommand();
                                if (input) {
                                    String channelName = extractChannelName(command);
                                    assert (channelName != null);
                                    channelsNeedingCacheUpdate.add(channelName);
                                    // For a rename command we also have to add
                                    // the new channel name.
                                    if (command
                                            .getCommandType()
                                            .equals(ArchiveConfigurationCommand.Type.RENAME_CHANNEL)) {
                                        channelsNeedingCacheUpdate
                                                .add(((RenameChannelCommand) command)
                                                        .getNewChannelName());
                                    }
                                }
                                return Pair
                                        .<ArchiveConfigurationCommandResult, Throwable> of(
                                                ArchiveConfigurationCommandResult
                                                        .success(command), null);
                            }
                        },
                        new Function<Throwable, Pair<ArchiveConfigurationCommandResult, Throwable>>() {
                            @Override
                            public Pair<ArchiveConfigurationCommandResult, Throwable> apply(
                                    Throwable input) {
                                // We are done with running the command, so we
                                // have to decrement the counter or run the
                                // next command if one is pending.
                                runConfigurationCommandLocalDecrementCounterOrRunNextCommand();
                                String message = input.getMessage();
                                if (message == null) {
                                    message = input.getClass().getSimpleName();
                                }
                                // If an error occurred, we cannot be sure
                                // whether the channel has been modified, so we
                                // always have to update the cache. We do not
                                // have to do this if the error is a
                                // ChannelAlreadyExistsException, a
                                // NoSuchChannelException, or a
                                // PendingChannelOperationException. In these
                                // cases, the channel has never been modified.
                                if (!(input instanceof ChannelAlreadyExistsException
                                        || input instanceof NoSuchChannelException || input instanceof PendingChannelOperationException)) {
                                    String channelName = extractChannelName(command);
                                    assert (channelName != null);
                                    channelsNeedingCacheUpdate.add(channelName);
                                    // For a rename command we also have to add
                                    // the new channel name.
                                    if (command
                                            .getCommandType()
                                            .equals(ArchiveConfigurationCommand.Type.RENAME_CHANNEL)) {
                                        channelsNeedingCacheUpdate
                                                .add(((RenameChannelCommand) command)
                                                        .getNewChannelName());
                                    }
                                }
                                return Pair.of(
                                        ArchiveConfigurationCommandResult
                                                .failure(command, message),
                                        input);
                            }
                        });
    }

}
