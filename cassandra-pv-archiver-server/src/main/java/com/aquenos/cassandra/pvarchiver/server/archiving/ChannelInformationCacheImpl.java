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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Delayed;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;

import com.aquenos.cassandra.pvarchiver.server.cluster.ClusterManagementService;
import com.aquenos.cassandra.pvarchiver.server.cluster.ServerOnlineStatusEvent;
import com.aquenos.cassandra.pvarchiver.server.cluster.ServerStatus;
import com.aquenos.cassandra.pvarchiver.server.database.ChannelMetaDataDAO;
import com.aquenos.cassandra.pvarchiver.server.database.ChannelMetaDataDAO.ChannelInformation;
import com.aquenos.cassandra.pvarchiver.server.internode.InterNodeCommunicationService;
import com.aquenos.cassandra.pvarchiver.server.util.AsyncFunctionUtils;
import com.aquenos.cassandra.pvarchiver.server.util.FutureUtils;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Standard implementation of the {@link ChannelInformationCache}.
 * 
 * @author Sebastian Marsching
 */
public class ChannelInformationCacheImpl implements ChannelInformationCache,
        DisposableBean, InitializingBean, SmartInitializingSingleton {

    /**
     * Entry in the channel cache. The entry is a simple wrapper around a
     * {@link ChannelInformation} object, adding a lower and and an upper limit
     * for the time stamp of this object. These time stamps are used when
     * determining whether another entry for the same channel is older or newer
     * than the existing entry.
     * 
     * @author Sebastian Marsching
     */
    private static class CacheEntry {

        private ChannelInformation channelInformation;
        private long newerThan;
        private long olderThan;

        public CacheEntry(ChannelInformation channelInformation,
                long newerThan, long olderThan) {
            this.channelInformation = channelInformation;
            this.newerThan = newerThan;
            this.olderThan = olderThan;
        }

        public ChannelInformation getChannelInformation() {
            return channelInformation;
        }

        public long getNewerThan() {
            return newerThan;
        }

        public long getOlderThan() {
            return olderThan;
        }

        public int fuzzyCompareTimeStamp(long otherNewerThan,
                long otherOlderThan) {
            // If one entry is definitely newer than the other on, we have an
            // unambiguous result. If the intervals overlap, we return zero
            // because we cannot definitely tell which one is newer.
            if (this.newerThan > otherOlderThan) {
                return 1;
            } else if (this.olderThan < otherNewerThan) {
                return -1;
            } else {
                return 0;
            }
        }

        public int fuzzyCompareTimeStamp(CacheEntry other) {
            return fuzzyCompareTimeStamp(other.newerThan, other.olderThan);
        }

    }

    /**
     * Internal representation of the cache state. All information associated
     * with the state is aggregated in this class so that the state can be
     * changed in a lock-free but thread-safe manner. This means that an
     * instance must not be changed after it has been published to other
     * threads. Before publishing it, the {@link InternalState#seal()} method
     * must be called in order to block future modifications and generate some
     * internal data structures that are needed to fulfill queries.
     * 
     * @author Sebastian Marsching
     */
    private static class InternalState {

        private static final Function<CacheEntry, ChannelInformation> CHANNEL_INFORMATION_FROM_CACHE_ENTRY = new Function<CacheEntry, ChannelMetaDataDAO.ChannelInformation>() {
            @Override
            public ChannelInformation apply(CacheEntry input) {
                if (input == null) {
                    return null;
                } else {
                    return input.getChannelInformation();
                }
            }
        };

        private boolean cacheValid;
        private TreeMap<String, CacheEntry> channelCache = new TreeMap<String, CacheEntry>();
        private SortedMap<String, ChannelInformation> channelInformationByName;
        private Map<UUID, SortedMap<String, ChannelInformation>> channelInformationByServer;
        // In order to save memory, we reuse objects that are typically the same
        // for many channels.
        private HashMap<String, String> controlSystemTypesForReuse;
        private HashMap<Set<Integer>, Set<Integer>> decimationLevelSetsForReuse;
        private HashMap<Integer, Integer> decimationLevelsForReuse;
        private long globalNewerThan;
        private long globalOlderThan;
        private boolean sealed;
        private HashMap<UUID, UUID> serverIdsForReuse;

        public InternalState() {
            this.cacheValid = false;
        }

        public InternalState(long globalNewerThan, long globalOlderThan) {
            this.cacheValid = true;
            this.globalNewerThan = globalNewerThan;
            this.globalOlderThan = globalOlderThan;
            this.channelCache = new TreeMap<String, CacheEntry>();
            this.controlSystemTypesForReuse = new HashMap<String, String>();
            this.decimationLevelSetsForReuse = new HashMap<Set<Integer>, Set<Integer>>();
            this.decimationLevelsForReuse = new HashMap<Integer, Integer>();
            this.serverIdsForReuse = new HashMap<UUID, UUID>();
        }

        @SuppressWarnings("unchecked")
        public InternalState(InternalState copyFrom) {
            this.cacheValid = copyFrom.cacheValid;
            this.channelCache = (TreeMap<String, CacheEntry>) copyFrom.channelCache
                    .clone();
            this.controlSystemTypesForReuse = (HashMap<String, String>) copyFrom.controlSystemTypesForReuse
                    .clone();
            this.decimationLevelSetsForReuse = (HashMap<Set<Integer>, Set<Integer>>) copyFrom.decimationLevelSetsForReuse
                    .clone();
            this.decimationLevelsForReuse = (HashMap<Integer, Integer>) copyFrom.decimationLevelsForReuse
                    .clone();
            this.globalNewerThan = copyFrom.globalNewerThan;
            this.globalOlderThan = copyFrom.globalOlderThan;
            this.serverIdsForReuse = (HashMap<UUID, UUID>) copyFrom.serverIdsForReuse
                    .clone();
            // channelInformationByName, channelInformationByServer, and sealed
            // are note copied on purpose: The copy of a state should not be
            // sealed initially, so that it can be modified.
        }

        public void invalidateCache() {
            if (sealed) {
                throw new IllegalStateException(
                        "The cache must not be invalidated after the state has been sealed.");
            }
            this.cacheValid = false;
        }

        public boolean isCacheValid() {
            return cacheValid;
        }

        public ChannelInformation getChannel(String channelName) {
            if (!sealed) {
                throw new IllegalStateException(
                        "The state must be sealed before the channel information objects can be accessed.");
            }
            return channelInformationByName.get(channelName);
        }

        public SortedMap<String, ChannelInformation> getChannels() {
            if (!sealed) {
                throw new IllegalStateException(
                        "The state must be sealed before the channel information objects can be accessed.");
            }
            return channelInformationByName;
        }

        public SortedMap<String, ChannelInformation> getChannels(UUID serverId) {
            if (!sealed) {
                throw new IllegalStateException(
                        "The state must be sealed before the channel information objects can be accessed.");
            }
            SortedMap<String, ChannelInformation> channels = channelInformationByServer
                    .get(serverId);
            if (channels == null) {
                return ImmutableSortedMap.of();
            } else {
                return channels;
            }
        }

        public Map<UUID, SortedMap<String, ChannelInformation>> getChannelsSortedByServer() {
            return channelInformationByServer;
        }

        public boolean mergeCacheEntry(String channelName,
                CacheEntry cacheEntryToBeMerged) {
            if (sealed) {
                throw new IllegalStateException(
                        "The cache must not be modified after the state has been sealed.");
            }
            boolean definitelyResolved;
            int fuzzyComparisonToGlobal = cacheEntryToBeMerged
                    .fuzzyCompareTimeStamp(globalNewerThan, globalOlderThan);
            if (fuzzyComparisonToGlobal < 0) {
                // The entry to be merged is significantly older than the last
                // major update of the cache in this state. As the cache does
                // not contain entries that are older than the last major
                // update, the entry to be merged is older than the information
                // in the cache and thus it can be discarded.
                definitelyResolved = true;
                return definitelyResolved;
            }
            CacheEntry existingCacheEntry = channelCache.get(channelName);
            if (existingCacheEntry == null) {
                // If there was no entry in the cache, it could be because the
                // channel has recently been deleted. If there is an overlap of
                // the time when the cache was initialized and the time the
                // merged entry was created, we have to check which information
                // is right by scheduling an update of that entry. We decide
                // whether to still add the entry temporarily based on the upper
                // limit on the time.
                if (fuzzyComparisonToGlobal == 0) {
                    if (cacheEntryToBeMerged.getOlderThan() > globalOlderThan) {
                        putCacheEntry(channelName,
                                cacheEntryToBeMerged.getChannelInformation(),
                                cacheEntryToBeMerged.getNewerThan(),
                                cacheEntryToBeMerged.getOlderThan());
                    }
                    definitelyResolved = false;
                } else {
                    putCacheEntry(channelName,
                            cacheEntryToBeMerged.getChannelInformation(),
                            cacheEntryToBeMerged.getNewerThan(),
                            cacheEntryToBeMerged.getOlderThan());
                    definitelyResolved = true;
                }
            } else {
                int fuzzyComparison = existingCacheEntry
                        .fuzzyCompareTimeStamp(cacheEntryToBeMerged);
                if (fuzzyComparison > 0) {
                    // The existing entry must be newer than the one to be
                    // merged, so we keep the existing one.
                    definitelyResolved = true;
                } else if (fuzzyComparison < 0) {
                    // The entry to be merged must be newer than the existing
                    // one, so we replace the existing one.
                    putCacheEntry(channelName,
                            cacheEntryToBeMerged.getChannelInformation(),
                            cacheEntryToBeMerged.getNewerThan(),
                            cacheEntryToBeMerged.getOlderThan());
                    definitelyResolved = true;
                } else {
                    // We cannot definitely tell which entry is newer. We keep
                    // the one with the greater upper limit of the time for now
                    // but schedule an update of the cache entry to be sure we
                    // keep the right one in the long run.
                    if (cacheEntryToBeMerged.getOlderThan() > existingCacheEntry
                            .getOlderThan()) {
                        putCacheEntry(channelName,
                                cacheEntryToBeMerged.getChannelInformation(),
                                cacheEntryToBeMerged.getNewerThan(),
                                cacheEntryToBeMerged.getOlderThan());
                    }
                    // We can also consider the matter resolved if both entries
                    // are actually the same (except for the time stamps).
                    ChannelInformation channelInformation1 = cacheEntryToBeMerged
                            .getChannelInformation();
                    ChannelInformation channelInformation2 = existingCacheEntry
                            .getChannelInformation();
                    definitelyResolved = (channelInformation1 == null && channelInformation2 == null)
                            || (channelInformation1 != null && channelInformation1
                                    .equals(channelInformation2));
                }
            }
            return definitelyResolved;
        }

        public Pair<InternalState, Set<String>> mergeWith(InternalState oldState) {
            InternalState mergedState = new InternalState(this);
            HashSet<String> channelsForReinspection = new HashSet<String>();
            for (Map.Entry<String, CacheEntry> oldEntry : oldState.channelCache
                    .entrySet()) {
                if (!mergedState.mergeCacheEntry(oldEntry.getKey(),
                        oldEntry.getValue())) {
                    channelsForReinspection.add(oldEntry.getKey());
                }
            }
            return Pair.<InternalState, Set<String>> of(mergedState,
                    channelsForReinspection);
        }

        public void putCacheEntry(String channelName,
                ChannelInformation channelInformation, long newerThan,
                long olderThan) {
            if (sealed) {
                throw new IllegalStateException(
                        "The cache must not be modified after the state has been sealed.");
            }
            // We rebuild the channel information from shared objects (strings,
            // etc.). This way, the memory footprint will be much smaller if we
            // have many channels that share the same attribute (control-system
            // type, decimation levels, server ID).
            if (channelInformation != null) {
                String controlSystemType = reuseControlSystemType(channelInformation
                        .getControlSystemType());
                Set<Integer> decimationLevels = reuseDecimationLevelSets(channelInformation
                        .getDecimationLevels());
                UUID serverId = reuseServerId(channelInformation.getServerId());
                channelInformation = new ChannelInformation(
                        channelInformation.getChannelDataId(), channelName,
                        controlSystemType, decimationLevels, serverId);
            }
            // We explicitly remove an existing entry with the same name from
            // the map. This looks inefficient, but we have to do this to ensure
            // that we use the same string instance in the key and in the value.
            // Otherwise, we could end up with two different instances for the
            // same string, and this could add up to a significant amount of
            // memory when we are dealing with a large number of channels.
            channelCache.remove(channelName);
            channelCache.put(channelName, new CacheEntry(channelInformation,
                    newerThan, olderThan));
        }

        public void seal() {
            if (sealed) {
                throw new IllegalStateException(
                        "A state can only be sealed once.");
            }
            sealed = true;
            // For the map containing all channels, we simply use a transformed
            // view. This means that the memory costs are virtually zero and the
            // value transformation should be very cheap.
            channelInformationByName = Collections.unmodifiableSortedMap(Maps
                    .filterValues(Maps.transformValues(channelCache,
                            CHANNEL_INFORMATION_FROM_CACHE_ENTRY), Predicates
                            .not(Predicates.isNull())));
            // We could also use views for the maps containing the channels for
            // each server. However, those views would have to filter based on
            // the values which would effectively mean iterating over all
            // elements for many queries. Therefore, we create separate maps for
            // each server.
            channelInformationByServer = new HashMap<UUID, SortedMap<String, ChannelInformation>>();
            for (ChannelInformation channelInformation : channelInformationByName
                    .values()) {
                UUID serverId = channelInformation.getServerId();
                SortedMap<String, ChannelInformation> serverMap = channelInformationByServer
                        .get(serverId);
                if (serverMap == null) {
                    serverMap = new TreeMap<String, ChannelInformation>();
                    channelInformationByServer.put(serverId, serverMap);
                }
                serverMap.put(channelInformation.getChannelName(),
                        channelInformation);
            }
            for (Map.Entry<UUID, SortedMap<String, ChannelInformation>> entry : channelInformationByServer
                    .entrySet()) {
                entry.setValue(Collections.unmodifiableSortedMap(entry
                        .getValue()));
            }
            channelInformationByServer = Collections
                    .unmodifiableMap(channelInformationByServer);
        }

        private String reuseControlSystemType(String controlSystemType) {
            String reusableControlSystemType = controlSystemTypesForReuse
                    .get(controlSystemType);
            if (reusableControlSystemType == null) {
                controlSystemTypesForReuse.put(controlSystemType,
                        controlSystemType);
                return controlSystemType;
            } else {
                return reusableControlSystemType;
            }
        }

        private Set<Integer> reuseDecimationLevelSets(
                Set<Integer> decimationLevelSet) {
            Set<Integer> reusableDecimationLevelSet = decimationLevelSetsForReuse
                    .get(decimationLevelSet);
            if (reusableDecimationLevelSet == null) {
                reusableDecimationLevelSet = ImmutableSortedSet
                        .copyOf(Iterables.transform(decimationLevelSet,
                                new Function<Integer, Integer>() {
                                    @Override
                                    public Integer apply(Integer input) {
                                        Integer reusableInteger = decimationLevelsForReuse
                                                .get(input);
                                        if (reusableInteger == null) {
                                            decimationLevelsForReuse.put(input,
                                                    input);
                                            return input;
                                        } else {
                                            return reusableInteger;
                                        }
                                    }
                                }));
                decimationLevelSetsForReuse.put(reusableDecimationLevelSet,
                        reusableDecimationLevelSet);
                return reusableDecimationLevelSet;
            } else {
                return reusableDecimationLevelSet;
            }
        }

        private UUID reuseServerId(UUID serverId) {
            UUID reusableServerId = serverIdsForReuse.get(serverId);
            if (reusableServerId == null) {
                serverIdsForReuse.put(serverId, serverId);
                return serverId;
            } else {
                return reusableServerId;
            }
        }

    }

    /**
     * Logger for this class.
     */
    protected final Log log = LogFactory.getLog(getClass());

    private ScheduledExecutorService scheduledExecutor;
    private ChannelMetaDataDAO channelMetaDataDAO;
    private ClusterManagementService clusterManagementService;
    private AtomicReference<InternalState> internalState = new AtomicReference<InternalState>(
            new InternalState());
    private InterNodeCommunicationService interNodeCommunicationService;
    private final Predicate<ServerStatus> isServerOnline = new Predicate<ServerStatus>() {
        @Override
        public boolean apply(ServerStatus input) {
            return input.isOnline();
        }
    };
    private final Function<ServerStatus, UUID> serverToId = new Function<ServerStatus, UUID>() {
        @Override
        public UUID apply(ServerStatus input) {
            return input.getServerId();
        }
    };

    /**
     * Sets the DAO for reading and modifying meta-data related to channels. The
     * channel information cache uses this DAO to read the information about
     * channels whenever an update of the cache is needed. Typically, this
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
     * Sets the cluster management service. The cluster management service is
     * used to get information about the cluster status. In particular, it is
     * needed to determine the online state of this server in order to know when
     * the cache should be updated or invalidated. Typically, this method is
     * called automatically by the Spring container.
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
     * Sets the inter-node communication service. The inter-node communication
     * service is needed to send channel-information updates to other servers in
     * the cluster.
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
     * Notifies the channel information cache that the server's online status
     * changed. Typically, this method is automatically called by the Spring
     * container when the {@link ClusterManagementService} publishes
     * {@link ServerOnlineStatusEvent}.
     * </p>
     * 
     * <p>
     * This method takes care of updating the cache when the server goes online
     * and invalidating the cache when the server goes offline. These operations
     * are performed asynchronously so that the thread sending the notification
     * does not block.
     * </p>
     * 
     * @param event
     *            event specifying the server's new online status.
     */
    @EventListener
    public void onServerOnlineStatusEvent(ServerOnlineStatusEvent event) {
        // We only consider events from the cluster management service that we
        // use.
        if (event.getSource() != clusterManagementService) {
            return;
        }

        if (event.isOnline()) {
            if (!internalState.get().isCacheValid()) {
                // The updateCache method should never throw an exception.
                updateCache(false);
            }
        } else {
            invalidateCache();
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        ThreadFactory daemonThreadFactory = new ThreadFactoryBuilder()
                .setDaemon(true).build();
        // An executor with one thread is sufficient. We only use it for tasks
        // that are not really time critical, so we can afford small delays.
        // As we also use the executor for transforming futures, we want to
        // ensure that tasks that are scheduled are run.
        scheduledExecutor = new ScheduledThreadPoolExecutor(1,
                daemonThreadFactory, new RejectedExecutionHandler() {
                    @Override
                    public void rejectedExecution(Runnable r,
                            ThreadPoolExecutor executor) {
                        // We do not want to execute tasks immediately that are
                        // scheduled to be executed in the future.
                        if (!(r instanceof Delayed)
                                || ((Delayed) r).getDelay(TimeUnit.NANOSECONDS) <= 0L) {
                            try {
                                r.run();
                            } catch (Throwable t) {
                                // We do not want exception to bubble up into
                                // the code that scheduled the task.
                            }
                        } else {
                            throw new RejectedExecutionException();
                        }
                    }
                });
    }

    @Override
    public void afterSingletonsInstantiated() {
        // We schedule background tasks here. This is better than doing it in
        // afterPropertiesSet because we can be sure that all other components
        // have been initialized. In addition to that, we can be sure that we
        // will not miss any events after running this method (while we might
        // miss events after running afterPropertiesSet).
        // We schedule the periodic update of the cache with a random delay.
        // This way, the different servers should not be in phase, avoiding a
        // periodic high load on the cluster. We use a delay between five and
        // thirty minutes. This way, the first periodic update will not happen
        // right after running the first update after going online.
        long initialDelay = 300L + new Random().nextInt(1500000);
        scheduledExecutor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                // The updateCache method should never throw an exception.
                updateCache(true);
            }
        }, initialDelay, 1800000L, TimeUnit.MILLISECONDS);
        // We still run the first update immediately when the server is online.
        // This way, we do not have to wait a long time until we are
        // operational.
        if (clusterManagementService.isOnline()
                && !internalState.get().isCacheValid()) {
            updateCache(false);
        }
    }

    @Override
    public void destroy() throws Exception {
        if (scheduledExecutor != null) {
            // We also use the executor to transform futures. Therefore, we want
            // to run all tasks that have been scheduled but not executed yet.
            // We exclude tasks that are not due yet because executing such
            // tasks early would break the contract of ScheduledExecutorService
            // and it will never be critical if such tasks are not executed at
            // all.
            for (Runnable r : scheduledExecutor.shutdownNow()) {
                if (!(r instanceof Delayed)
                        || ((Delayed) r).getDelay(TimeUnit.NANOSECONDS) <= 0L) {
                    try {
                        r.run();
                    } catch (Throwable t) {
                        // We want to continue even if one of the tasks fails.
                    }
                }
            }
            scheduledExecutor = null;
        }
    }

    @Override
    public ChannelInformation getChannel(String channelName) {
        Preconditions.checkNotNull(channelName);
        InternalState state = internalState.get();
        if (!state.isCacheValid()) {
            throw new IllegalStateException(
                    "The cache is currently not in a valid state. Either an update has failed or it has never been initialized.");
        }
        return state.getChannel(channelName);
    }

    @Override
    public ListenableFuture<ChannelInformation> getChannelAsync(
            final String channelName, boolean forceUpdate) {
        if (channelName == null) {
            return Futures.immediateFailedFuture(new NullPointerException(
                    "The channelName must not be null."));
        }
        InternalState state = internalState.get();
        if (state.isCacheValid()) {
            if (forceUpdate) {
                final long currentTimeAtStartOfUpdate = System
                        .currentTimeMillis();
                final ListenableFuture<ChannelInformation> future = channelMetaDataDAO
                        .getChannel(channelName);
                future.addListener(new Runnable() {
                    @Override
                    public void run() {
                        long currentTimeAtEndOfUpdate = System
                                .currentTimeMillis();
                        try {
                            updateCacheForChannelProcessData(channelName,
                                    FutureUtils.getUnchecked(future),
                                    currentTimeAtStartOfUpdate,
                                    currentTimeAtEndOfUpdate);
                        } catch (Throwable t) {
                            // We want to ignore errors that happen here,
                            // because they should have no effect on the rest of
                            // the operation.
                        }
                    }
                }, MoreExecutors.sameThreadExecutor());
                return future;
            } else {
                return Futures.immediateFuture(state.getChannel(channelName));
            }
        } else {
            return channelMetaDataDAO.getChannel(channelName);
        }
    }

    @Override
    public SortedMap<String, ChannelInformation> getChannels() {
        InternalState state = internalState.get();
        if (!state.isCacheValid()) {
            throw new IllegalStateException(
                    "The cache is currently not in a valid state. Either an update has failed or it has never been initialized.");
        }
        return state.getChannels();
    }

    @Override
    public SortedMap<String, ChannelInformation> getChannels(UUID serverId) {
        Preconditions.checkNotNull(serverId, "The serverId must not be null.");
        InternalState state = internalState.get();
        if (!state.isCacheValid()) {
            throw new IllegalStateException(
                    "The cache is currently not in a valid state. Either an update has failed or it has never been initialized.");
        }
        return state.getChannels(serverId);
    }

    @Override
    public Map<UUID, SortedMap<String, ChannelInformation>> getChannelsSortedByServer() {
        InternalState state = internalState.get();
        if (!state.isCacheValid()) {
            throw new IllegalStateException(
                    "The cache is currently not in a valid state. Either an update has failed or it has never been initialized.");
        }
        return state.getChannelsSortedByServer();
    }

    @Override
    public ListenableFuture<Void> processUpdate(
            List<ChannelInformation> channelInformationUpdates,
            List<String> missingChannels, Iterable<UUID> forwardToServers,
            long remoteTime) {
        // We use current local time as the upper limit of the time stamp. We
        // use the remote time minus five seconds as the lower limit of the time
        // stamp. This ensures that we do not use a lower limit that is too
        // high, even if clocks are skewed a bit. A remote time that is greater
        // than the current local time must be wrong (from the perspective of
        // our clock), so we use the local clock minus five seconds instead.
        long olderThan = System.currentTimeMillis();
        long newerThan = (remoteTime < olderThan) ? (remoteTime - 5000L)
                : (olderThan - 5000L);
        try {
            return processUpdateInternal(channelInformationUpdates,
                    missingChannels, forwardToServers, newerThan, olderThan,
                    remoteTime);
        } catch (Throwable t) {
            return Futures.immediateFailedFuture(t);
        }
    }

    @Override
    public ListenableFuture<Void> updateChannels(Iterable<String> channelNames) {
        final ImmutableList<String> copyOfChannelNames;
        try {
            copyOfChannelNames = ImmutableList.copyOf(channelNames);
        } catch (Throwable t) {
            return Futures.immediateFailedFuture(t);
        }
        final long currentTimeAtStartOfUpdate = System.currentTimeMillis();
        final LinkedList<ListenableFuture<ChannelInformation>> channelInformationFutures = new LinkedList<ListenableFuture<ChannelInformation>>();
        for (String channelName : copyOfChannelNames) {
            channelInformationFutures.add(channelMetaDataDAO
                    .getChannel(channelName));
        }
        // We use the internal executor (instead of the same-thread executor)
        // for processing the transformation as it involves significant
        // processing and the same-thread executor might run the operation in an
        // inappropriate thread.
        return Futures.transform(Futures.allAsList(channelInformationFutures),
                new AsyncFunction<List<ChannelInformation>, Void>() {
                    @Override
                    public ListenableFuture<Void> apply(
                            List<ChannelInformation> input) throws Exception {
                        long currentTimeAtEndOfUpdate = System
                                .currentTimeMillis();
                        Iterable<UUID> forwardToServers = Iterables.transform(
                                Iterables.filter(
                                        clusterManagementService.getServers(),
                                        isServerOnline), serverToId);
                        // We only want to include non-null channel information
                        // objects in the list passed to processUpdateInternal.
                        // If the channel information is null, we pass the
                        // channel name in a separate list.
                        LinkedList<ChannelInformation> nonNullChannelInformationUpdates = new LinkedList<ChannelInformation>();
                        LinkedList<String> missingChannels = new LinkedList<String>();
                        // We use the fact that the list of channel information
                        // objects must have exactly the same number of elements
                        // (and the same order) as the list of channel names.
                        Iterator<String> channelNameIterator;
                        Iterator<ChannelInformation> channelInformationIterator;
                        for (channelNameIterator = copyOfChannelNames
                                .iterator(), channelInformationIterator = input
                                .iterator(); channelNameIterator.hasNext()
                                && channelInformationIterator.hasNext();) {
                            String channelName = channelNameIterator.next();
                            ChannelInformation channelInformation = channelInformationIterator
                                    .next();
                            if (channelInformation == null) {
                                missingChannels.add(channelName);
                            } else {
                                nonNullChannelInformationUpdates
                                        .add(channelInformation);
                            }
                        }
                        return processUpdateInternal(
                                nonNullChannelInformationUpdates,
                                missingChannels, forwardToServers,
                                currentTimeAtStartOfUpdate,
                                currentTimeAtEndOfUpdate,
                                currentTimeAtStartOfUpdate);
                    }
                }, scheduledExecutor);
    }

    private void invalidateCache() {
        for (;;) {
            InternalState oldState = internalState.get();
            if (!oldState.isCacheValid()) {
                break;
            }
            InternalState newState = new InternalState(oldState);
            newState.invalidateCache();
            newState.seal();
            if (internalState.compareAndSet(oldState, newState)) {
                break;
            }
        }
    }

    private ListenableFuture<Void> processUpdateInternal(
            List<ChannelInformation> channelInformationUpdates,
            List<String> missingChannels, Iterable<UUID> forwardToServers,
            long newerThan, long olderThan, long originalNewerThan) {
        // First, we want to apply the updates locally.
        LinkedList<String> channelsNeedingReinspection = new LinkedList<String>();
        for (;;) {
            channelsNeedingReinspection.clear();
            InternalState oldState = internalState.get();
            InternalState mergedState = new InternalState(oldState);
            // We do not check whether a channel is contained in both the
            // channelInformationUpdates and the missingChannels lists. We do
            // not check for duplicate entries in one of these lists either.
            // Typically, we only process requests from other servers in the
            // cluster and those servers should only send well-formed requests.
            // Even if we receive a request which has such duplicate entries,
            // this does not have any severe consequences. It only means that we
            // will request updated data from the database because the merge
            // will report a potential conflict.
            for (ChannelInformation channelInformation : channelInformationUpdates) {
                if (!mergedState.mergeCacheEntry(channelInformation
                        .getChannelName(), new CacheEntry(channelInformation,
                        newerThan, olderThan))) {
                    channelsNeedingReinspection.add(channelInformation
                            .getChannelName());
                }
            }
            for (String channelName : missingChannels) {
                // We insert entries for null channel informations because we
                // want to keep those entries as "tombstones". This will keep
                // updates with older data (when the channel still existed) from
                // turning up inside the cache again.
                if (!mergedState.mergeCacheEntry(channelName, new CacheEntry(
                        null, newerThan, olderThan))) {
                    channelsNeedingReinspection.add(channelName);
                }
            }
            mergedState.seal();
            if (internalState.compareAndSet(oldState, mergedState)) {
                break;
            }
        }
        // We have to update those channels again for which the merge result was
        // ambiguous.
        for (String channelName : channelsNeedingReinspection) {
            updateCacheForChannel(channelName);
        }
        // Next, we have to forward the updates to the specified servers
        // (excluding this server). This server should not have been included in
        // the list anyway, but we filter it to ensure that we do not create an
        // infinite loop.
        return sendUpdate(channelInformationUpdates, missingChannels,
                Iterables.filter(forwardToServers, Predicates.not(Predicates
                        .equalTo(clusterManagementService.getThisServer()
                                .getServerId()))), originalNewerThan);
    }

    private ListenableFuture<Void> sendUpdate(
            final List<ChannelInformation> channelInformationUpdates,
            final List<String> missingChannels, Iterable<UUID> targetServers,
            final long timeStamp) {
        // We contact a maximum of five target servers directly. If there are
        // more target servers, we delegate the notification to them.
        final int maximumNumberOfDirectServers = 5;
        ArrayList<String> targetServerUrls = new ArrayList<String>(
                maximumNumberOfDirectServers);
        ArrayList<LinkedList<UUID>> targetServerForwardToIds = new ArrayList<LinkedList<UUID>>(
                maximumNumberOfDirectServers);
        int targetServerIndex = 0;
        for (UUID targetServerId : targetServers) {
            if (targetServerUrls.size() < maximumNumberOfDirectServers) {
                String targetServerUrl = clusterManagementService
                        .getInterNodeCommunicationUrl(targetServerId);
                // If we did not get a URL, the server must be offline and we
                // ignore it.
                if (targetServerUrl != null) {
                    targetServerUrls.add(targetServerUrl);
                    targetServerForwardToIds.add(new LinkedList<UUID>());
                }
            } else {
                targetServerForwardToIds.get(targetServerIndex).add(
                        targetServerId);
                targetServerIndex = (targetServerIndex == maximumNumberOfDirectServers - 1) ? 0
                        : (targetServerIndex + 1);
            }
        }
        ArrayList<ListenableFuture<Void>> futures = new ArrayList<ListenableFuture<Void>>(
                maximumNumberOfDirectServers);
        for (int i = 0; i < targetServerUrls.size(); ++i) {
            String targetServerUrl = targetServerUrls.get(i);
            final LinkedList<UUID> forwardToIds = targetServerForwardToIds
                    .get(i);
            ListenableFuture<Void> future = interNodeCommunicationService
                    .updateChannelInformation(targetServerUrl,
                            channelInformationUpdates, missingChannels,
                            forwardToIds, timeStamp);
            // If sending the updates to one of the server fails, we do not want
            // to fail the whole update. Instead, we ignore this server and send
            // the update to one of the remaining servers.
            ListenableFuture<Void> transformedFuture = FutureUtils.transform(
                    future, AsyncFunctionUtils.<Void> identity(),
                    new AsyncFunction<Throwable, Void>() {
                        @Override
                        public ListenableFuture<Void> apply(Throwable input)
                                throws Exception {
                            if (!forwardToIds.isEmpty()) {
                                return sendUpdate(channelInformationUpdates,
                                        missingChannels, forwardToIds,
                                        timeStamp);
                            }
                            return Futures.immediateFuture(null);
                        }
                    });
            futures.add(transformedFuture);
        }
        return FutureUtils.transformAnyToVoid(Futures.allAsList(futures));
    }

    private void updateCache(final boolean periodicRun) {
        final long currentTimeAtStartOfUpdate = System.currentTimeMillis();
        final ListenableFuture<? extends Iterable<? extends ChannelInformation>> future = channelMetaDataDAO
                .getChannels();
        // The processing of the channel information might take a while. In
        // particular, the iterator that returns the objects might block if the
        // results are provided in multiple pages (very likely). Therefore, we
        // cannot execute our listener using the same-thread scheduledExecutor.
        future.addListener(new Runnable() {
            @Override
            public void run() {
                try {
                    updateCacheProcessData(FutureUtils.getUnchecked(future),
                            currentTimeAtStartOfUpdate,
                            System.currentTimeMillis());
                } catch (Throwable t) {
                    // If this is a periodic run, we do not want to log the
                    // exception at all. In this case, the exception is most
                    // likely caused by a transient database problem which is
                    // not worth logging.
                    if (!periodicRun) {
                        // A NoHostAvailableException is most likely caused by a
                        // connection problem with the Apache Cassandra cluster.
                        // We only want to log such a problem at the INFO level,
                        // because this kind of error is most likely transient.
                        if (t instanceof NoHostAvailableException) {
                            log.info("Updating the channel cache failed.", t);
                        } else {
                            log.error("Updating the channel cache failed.", t);
                        }
                    }
                    // If we could not update the cache, the most likely cause
                    // are connection problems with the database. In this case,
                    // we cannot be completely sure any longer whether the cache
                    // is up-to-date. We invalidate it and schedule another
                    // update. We do not schedule an update if the server is
                    // offline. In this case, the update will be started
                    // automatically when the serves comes back online.
                    invalidateCache();
                    if (clusterManagementService.isOnline()) {
                        scheduledExecutor.schedule(new Runnable() {
                            @Override
                            public void run() {
                                updateCache(periodicRun);
                            }
                        }, 60000L, TimeUnit.MILLISECONDS);
                    }
                }
            }
        }, scheduledExecutor);
    }

    private void updateCacheForChannel(final String channelName) {
        final long currentTimeAtStartOfUpdate = System.currentTimeMillis();
        final ListenableFuture<ChannelInformation> future = channelMetaDataDAO
                .getChannel(channelName);
        future.addListener(new Runnable() {
            @Override
            public void run() {
                try {
                    updateCacheForChannelProcessData(channelName,
                            FutureUtils.getUnchecked(future),
                            currentTimeAtStartOfUpdate,
                            System.currentTimeMillis());
                } catch (Throwable t) {
                    // A NoHostAvailableException is most likely caused by a
                    // connection problem with the Apache Cassandra cluster.
                    // We only want to log such a problem at the INFO level,
                    // because this kind of error is most likely transient.
                    if (t instanceof NoHostAvailableException) {
                        log.info("Updating the channel cache failed.", t);
                    } else {
                        log.error("Updating the channel cache failed.", t);
                    }
                    // If we could not update the cache, the most likely cause
                    // are connection problems with the database. In this case,
                    // we cannot be completely sure any longer whether the cache
                    // is up-to-date. We invalidate it and schedule another
                    // update. We do not schedule an update if the server is
                    // offline. In this case, the update will be started
                    // automatically when the serves comes back online.
                    invalidateCache();
                    if (clusterManagementService.isOnline()) {
                        scheduledExecutor.schedule(new Runnable() {
                            @Override
                            public void run() {
                                updateCache(false);
                            }
                        }, 60000L, TimeUnit.MILLISECONDS);
                    }
                    return;
                }
            }
        }, MoreExecutors.sameThreadExecutor());
    }

    private void updateCacheForChannelProcessData(String channelName,
            ChannelInformation channelInformation,
            long currentTimeAtStartOfUpdate, long currentTimeAtEndOfUpdate) {
        // We do not check whether the channel information is null. This
        // is by intention because we want to store the null value as a
        // "tombstone". This way, conflicting updates that are older
        // will get ignored. The tombstone will automatically be removed
        // as part of the next major update that is done periodically.
        boolean mergeSuccessful;
        for (;;) {
            InternalState oldState = internalState.get();
            InternalState mergedState = new InternalState(oldState);
            // If there is another conflicting update, we have to run
            // this method again.
            mergeSuccessful = mergedState.mergeCacheEntry(channelName,
                    new CacheEntry(channelInformation,
                            currentTimeAtStartOfUpdate,
                            currentTimeAtEndOfUpdate));
            mergedState.seal();
            if (internalState.compareAndSet(oldState, mergedState)) {
                break;
            }
        }
        if (!mergeSuccessful) {
            updateCacheForChannel(channelInformation.getChannelName());
        }
    }

    private void updateCacheProcessData(
            Iterable<? extends ChannelInformation> channels,
            long currentTimeAtStartOfUpdate, long currentTimeAtEndOfUpdate) {
        InternalState newState = new InternalState(currentTimeAtStartOfUpdate,
                currentTimeAtEndOfUpdate);
        for (ChannelInformation channelInformation : channels) {
            newState.putCacheEntry(channelInformation.getChannelName(),
                    channelInformation, currentTimeAtStartOfUpdate,
                    currentTimeAtEndOfUpdate);
        }
        Set<String> channelsForReinspection;
        for (;;) {
            InternalState oldState = internalState.get();
            Pair<InternalState, Set<String>> mergedStateAndChannelsForReinspection = newState
                    .mergeWith(oldState);
            InternalState mergedState = mergedStateAndChannelsForReinspection
                    .getLeft();
            channelsForReinspection = mergedStateAndChannelsForReinspection
                    .getRight();
            mergedState.seal();
            if (internalState.compareAndSet(oldState, mergedState)) {
                break;
            }
        }
        for (String channelName : channelsForReinspection) {
            updateCacheForChannel(channelName);
        }
    }

}
