/*
 * Copyright 2015-2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.cluster;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.EventListener;

import com.aquenos.cassandra.pvarchiver.server.cluster.internal.ClockSkewMonitor;
import com.aquenos.cassandra.pvarchiver.server.cluster.internal.ClockSkewMonitor.ClockSkewMonitorEvent;
import com.aquenos.cassandra.pvarchiver.server.controlsystem.ControlSystemSupportRegistry;
import com.aquenos.cassandra.pvarchiver.server.database.ClusterServersDAO;
import com.aquenos.cassandra.pvarchiver.server.database.ClusterServersDAO.ClusterServer;
import com.aquenos.cassandra.pvarchiver.server.database.ClusterServersDAOInitializedEvent;
import com.aquenos.cassandra.pvarchiver.server.internode.InterNodeCommunicationService;
import com.aquenos.cassandra.pvarchiver.server.internode.InterNodeCommunicationServiceInitializedEvent;
import com.aquenos.cassandra.pvarchiver.server.spring.ServerProperties;
import com.aquenos.cassandra.pvarchiver.server.util.FutureUtils;
import com.aquenos.cassandra.pvarchiver.server.util.UUIDComparator;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * <p>
 * Manages the cluster of archive servers. This class takes care of registering
 * the server in the cluster and periodically updating its online state so that
 * other servers can detect it as being online.
 * </p>
 * 
 * <p>
 * The {@link #isOnline()} method can be used to check whether this server is
 * currently online. Components that are interested in this information can also
 * register as {@link ApplicationListener}s listening for the
 * {@link ServerOnlineStatusEvent}. As event notification is performed in an
 * asynchronous way, calls to the {@link #isOnline()} method may reflect an
 * updated online status before the corresponding event is received. Please note
 * that listeners that are registered after events have been sent will only be
 * notified when the status changes again. Therefore, when registering a new
 * listener, the listener should query the current state explicitly because it
 * might not receive an event with this state.
 * </p>
 * 
 * @author Sebastian Marsching
 */
public class ClusterManagementService implements
        ApplicationEventPublisherAware, DisposableBean, InitializingBean,
        SmartInitializingSingleton {

    private static final long CACHE_UPDATE_INTERVAL_MILLISECONDS = 5000L;
    private static final long MINIMUM_REGISTRATION_AGE_BEFORE_DELETE_MILLISECONDS = 3150000L;
    private static final long OTHER_REGISTRATION_VALID_MILLISECONDS = 15000L;
    private static final long OWN_REGISTRATION_VALID_MILLISECONDS = 10000L;
    private static final long UPDATE_LAST_ONLINE_TIME_DELAY_MILLISECONDS = 5000L;
    private static final long INITIAL_SERVER_REGISTRATION_RETRY_DELAY_SECONDS = 30L;

    /**
     * Logger for this class.
     */
    protected final Log log = LogFactory.getLog(getClass());

    private ApplicationEventPublisher applicationEventPublisher;
    private TreeMap<UUID, ClusterServer> cacheIdToServer = new TreeMap<UUID, ClusterServersDAO.ClusterServer>(
            new UUIDComparator());
    private final Object cacheLock = new Object();
    private TreeMap<String, ClusterServer> cacheNameToServer = new TreeMap<String, ClusterServersDAO.ClusterServer>();
    private ClockSkewMonitor clockSkewMonitor;
    private boolean clockSkewMonitorStarted;
    private ClusterServersDAO clusterServersDAO;
    private ControlSystemSupportRegistry controlSystemSupportRegistry;
    private boolean destroyed;
    private InterNodeCommunicationService interNodeCommunicationService;
    private String interNodeCommunicationUrl;
    private long lastOfflineTime;
    private long lastUpdateSuccessTime;
    private final Object lock = new Object();
    private boolean notificationInProgress;
    private boolean online;
    private ScheduledExecutorService scheduledExecutorService;
    private UUID serverId;
    private String serverName;
    private ServerProperties serverProperties;
    private final Runnable updateCacheRunnable = new Runnable() {
        @Override
        public void run() {
            updateCache();
        }
    };
    private long updateCacheStartTime;
    private final Runnable updateLastOnlineTimeRunnable = new Runnable() {
        @Override
        public void run() {
            updateLastOnlineTime();
        }
    };
    private final Runnable updateServerRegistrationRunnable = new Runnable() {
        @Override
        public void run() {
            updateServerRegistration();
        }
    };
    private AtomicBoolean updateTasksScheduledOnce = new AtomicBoolean();

    @Override
    public void afterPropertiesSet() throws Exception {
        serverId = serverProperties.getUuid();
        serverName = getHostName();
        StringBuilder urlBuilder = new StringBuilder();
        urlBuilder.append("http://");
        String listenAddress = serverProperties.getListenAddress()
                .getHostAddress();
        // If the listen address is an IPv6 address (contains a colon), we have
        // to wrap it in brackets in order to form a valid URL.
        if (listenAddress.contains(":")) {
            urlBuilder.append('[');
            urlBuilder.append(listenAddress);
            urlBuilder.append(']');
        } else {
            urlBuilder.append(listenAddress);
        }
        if (serverProperties.getInterNodeCommunicationPort() != 80) {
            urlBuilder.append(":");
            urlBuilder.append(serverProperties.getInterNodeCommunicationPort());
        }
        urlBuilder.append("/inter-node");
        interNodeCommunicationUrl = urlBuilder.toString();

        // We create the executor services that we need here. This ensures that
        // any method that might be called by another bean and needs an executor
        // service can use it. However, we schedule periodic or initialization
        // tasks from afterSingletonsInstanstiated. This avoids running code too
        // early and in particular avoids application events to be sent too
        // early.

        // We use our own executor service. This way, we can be sure that other
        // tasks do not block our tasks from executing. We use two threads
        // because we are executing the update method in this executor and if it
        // blocked (which is unlikely but could happen), this would stop the
        // watch-dog from running as planned if there was only one thread.
        scheduledExecutorService = new ScheduledThreadPoolExecutor(2);

        // We have to create the clock skew monitor and schedule periodical
        // processing. The clock skew monitor tries to monitor clock skew in the
        // cluster so that we can initiate an emergency shutdown if the clock
        // skew gets too big. This does not replace the regular monitoring of
        // clock synchronization in the cluster. It is just an extra safeguard
        // that reduces the risk of severe data loss in case a huge clock skew
        // is not detected by the regular measures. We only create the clock
        // skew monitor here and start it from afterSingletonsInstantiated().
        clockSkewMonitor = new ClockSkewMonitor(interNodeCommunicationService,
                new ClockSkewMonitor.ClockSkewMonitorEventListener() {
                    @Override
                    public void onClockSkewEvent(ClockSkewMonitorEvent event) {
                        // If the clock skipped back, we shutdown the JVM
                        // immediately. We only detect a skip back if it is so
                        // large that it is detected even after the full request
                        // / response cycle. Such a skip back should never
                        // happen (a reasonably configured NTP daemon will skew
                        // the clock slowly). If the clock skips back, many
                        // assumptions do not hold any longer and we cannot
                        // operate the system safely.
                        if (event.isClockSkippedBack()) {
                            log.error("System clock skipped back - shutting down now.");
                            System.exit(1);
                        }
                        long absoluteClockSkew = Math.abs(event
                                .getMinimumClockSkew());
                        // We shutdown if the clock skew exceeds 1.2 seconds.
                        // The clock skew as we calculate it is a minimal
                        // estimate (the real clock skew might be larger because
                        // of the round-trip time). There is no reasonable
                        // reason why the clock skew should be this big. Even if
                        // a leap second was not handled correctly, it should be
                        // less. We can only handle a clock skew of a few
                        // seconds before risking data consistency, so shutting
                        // down is the safe choice.
                        if (absoluteClockSkew > 1200L) {
                            log.error("The system clock of this server is skewed by at least "
                                    + absoluteClockSkew
                                    + " ms compared to server "
                                    + event.getServerId()
                                    + " - shutting down now.");
                            System.exit(1);
                            return;
                        }
                        // We already log warning above 800 ms because no
                        // reasonably configured system should have such a huge
                        // clock skew (not even if datacenters are distributed
                        // around the world).
                        if (absoluteClockSkew > 800L) {
                            log.warn("The system clock of this server is skewed by at least "
                                    + absoluteClockSkew
                                    + " ms compared to server "
                                    + event.getServerId() + ".");
                        }
                    }
                });
    }

    @Override
    public void afterSingletonsInstantiated() {
        // We have to start a few background tasks. We do not do this in
        // afterPropertiesSet because starting asynchronous tasks from
        // afterPropertiesSet is not the best idea in general and can actually
        // cause problems if these asynchronous tasks send events. These events
        // might not be received by all beans because these beans might not have
        // been registered as listeners yet. This method, on the other hand, is
        // called in the same phase in which EventListenerMethodProcessor is
        // notified, which also is a SmartInitializingSingleton. The beans are
        // notified in the order in which they are registered, and the
        // EventListenerMethodProcessor is registered very early, so before this
        // bean is registered.
        // Implementing SmartLifecycle might be a viable alternative, but that
        // interface comes with a lot of baggage (six methods that must be
        // implemented, one of which is not even used at all).

        // If the cluster servers DAO is already initialized, we can try to
        // register the server immediately. Otherwise, we start the registration
        // attempt when we receive an event indicating that the DAO is ready.
        // The same applies to the cache-update process.
        // We only want to schedule this operation once, so we have to check
        // that we did not receive the event yet (and make sure that the event
        // does not trigger another run when it is is received).
        if (clusterServersDAO.isInitialized()
                && updateTasksScheduledOnce.compareAndSet(false, true)) {
            scheduleUpdateServerRegistration(0L, TimeUnit.MILLISECONDS);
            // We want to schedule a periodic update of our servers cache. This
            // way, the cache is kept up to date and we can read from it without
            // having to block. However, we cannot use scheduleAtFixedRate,
            // because this might start multiple updates in parallel if the
            // update does not finish in time. Instead, we schedule the request
            // when we have received and processed all results.
            scheduleUpdateCache(0L, TimeUnit.MILLISECONDS);
        }

        // If the inter-node communication service has already been initialized
        // we can (and should) start the clock-skew monitor right away.
        // Otherwise, we wait until this service is ready because the clock-skew
        // monitor cannot work without this service and thus starting it would
        // not make sense.
        if (interNodeCommunicationService.isInitialized()) {
            startClockSkewMonitor();
        }
    }

    @Override
    public void destroy() throws Exception {
        // By marking this cluster manager as destroyed and resetting the online
        // flag, we can avoid a race condition with another thread that might
        // also try to modify the online state.
        boolean oldOnline;
        boolean notificationInProgress;
        synchronized (lock) {
            destroyed = true;
            oldOnline = online;
            online = false;
            notificationInProgress = this.notificationInProgress;
        }
        scheduledExecutorService.shutdownNow();
        // If we were online before and there is no other notification in
        // progress, we notify the listeners that we are now offline. If another
        // notification is in progress, that thread will take care of the
        // notification.
        if (oldOnline && !notificationInProgress) {
            publishServerOnlineStatusEvent(oldOnline);
        }
    }

    /**
     * Handles the {@link ClusterServersDAOInitializedEvent}. This event signals
     * that the cluster servers DAO needed by this service is ready for
     * operation. This service uses this information in order to start the
     * registration process that registers this server in the cluster and
     * eventually switch to the online state, if the registration attempt is
     * successful.
     * 
     * @param event
     *            event signaling that the cluster servers DAO is ready for
     *            operation.
     */
    @EventListener
    public void onClusterServersDAOInitializedEvent(
            ClusterServersDAOInitializedEvent event) {
        // We want to ignore events that are sent by a different DAO than the
        // one we are using.
        if (event.getSource() != clusterServersDAO) {
            return;
        }
        // We only want to schedule this operation once, so we have to check
        // that we did not receive the event yet (or scheduled the run during
        // initialization).
        if (!updateTasksScheduledOnce.compareAndSet(false, true)) {
            return;
        }
        // We want to update the server registration as soon as possible, but
        // not from this thread. This is why we schedule the operation with a
        // zero delay.
        scheduleUpdateServerRegistration(0L, TimeUnit.MILLISECONDS);
        // We want to schedule a periodic update of our servers cache. This way,
        // the cache is kept up to date and we can read from it without having
        // to block. However, we cannot use scheduleAtFixedRate, because this
        // might start multiple updates in parallel if the update does not
        // finish in time. Instead, we schedule the request when we have
        // received and processed all results.
        scheduleUpdateCache(0L, TimeUnit.MILLISECONDS);
    }

    /**
     * Handles the {@link InterNodeCommunicationServiceInitializedEvent}. This
     * event signals that the inter-node communication service needed by this
     * service is ready for operation. This service uses this information in
     * order start its internal clock-skew monitor (which depends on remote
     * communication) and subsequently switch to the online state, if all other
     * conditions are met as well.
     * 
     * @param event
     *            event signaling that the inter-node communication service is
     *            ready for operation.
     */
    @EventListener
    public void onInterNodeCommunicationServiceInitializedEvent(
            InterNodeCommunicationServiceInitializedEvent event) {
        // We want to ignore events that are sent by a different service than
        // the one that we are using.
        if (event.getSource() != interNodeCommunicationService) {
            return;
        }
        startClockSkewMonitor();
    }

    @Override
    public void setApplicationEventPublisher(
            ApplicationEventPublisher applicationEventPublisher) {
        this.applicationEventPublisher = applicationEventPublisher;
    }

    /**
     * Sets the cluster-servers DAO that is used for accessing the database.
     * Typically, this method is called automatically by the Spring container.
     * 
     * @param clusterServersDAO
     *            cluster-server DAO for accessing the database.
     */
    @Autowired
    public void setClusterServersDAO(ClusterServersDAO clusterServersDAO) {
        this.clusterServersDAO = clusterServersDAO;
    }

    /**
     * Sets the control-system support registry. The cluster management service
     * waits for the control-system support registry to become available before
     * switching the server online. Typically, this method is called
     * automatically by the Spring container.
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
     * service is needed to communicate with other servers in order to compare
     * the system time and trigger a warning (or shutdown) when a significant
     * clock skew is detected. Typically, this method is called automatically by
     * the Spring container.
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
     * Sets the server-specific configuration properties. These server
     * properties are used when building the web-service URL at which this
     * server is available when registering this server with the cluster.
     * 
     * @param serverProperties
     *            configuration properties used by this server.
     */
    @Autowired
    public void setServerProperties(ServerProperties serverProperties) {
        this.serverProperties = serverProperties;
    }

    /**
     * <p>
     * Returns the inter-node communication URL for the specified server. This
     * is the URL that is contacted by the {@link InterNodeCommunicationService}
     * in order to communicate with that server.
     * </p>
     * 
     * <p>
     * If the specified server ID is unknown of if the server is not online,
     * <code>null</code> is returned.
     * </p>
     * 
     * <p>
     * This service serves the query from an internal cache. For this reason,
     * the information returned might be outdated. However, this has the
     * advantage that this method will never block.
     * </p>
     * 
     * @param serverId
     *            ID of the server for which the inter-node communication URL
     *            shall be returned.
     * @return inter-node communication URL for the specified server or
     *         <code>null</code> if the specified server ID is unknown or the
     *         server is considered offline.
     * @throws NullPointerException
     *             if <code>serverId</code> is <code>null</code>.
     */
    public String getInterNodeCommunicationUrl(UUID serverId) {
        Preconditions.checkNotNull(serverId, "The serverId must not be null.");
        // We read the data from the cache. If the periodic update does not work
        // for some reason, the data in the cache will be stale. However, we
        // still use it. The alternative would be throwing an exception and this
        // would require additional handling in the calling code. Typically, it
        // should not matter if the data is outdated, because when it is
        // important, the calling code will call verifyServerOffline(...), which
        // does not use the cache.
        synchronized (cacheLock) {
            ClusterServer server = cacheIdToServer.get(serverId);
            // If there is no entry for the server, we do not have a URL.
            if (server == null) {
                return null;
            }
            // If we found an entry but the server has not been online for a
            // long time, we do not want to use the URL because some other
            // service might be listening at it now.
            if (server.getLastOnlineTime().getTime() < System
                    .currentTimeMillis()
                    - OTHER_REGISTRATION_VALID_MILLISECONDS) {
                return null;
            }
            String url = server.getInterNodeCommunicationUrl();
            // We want to make sure that the URL never ends with a slash, even
            // if it was registered differently for some reason.
            if (url.endsWith("/")) {
                return url.substring(0, url.length() - 1);
            } else {
                return url;
            }
        }
    }

    /**
     * <p>
     * Returns the status for the server with the specified ID. The status is
     * retrieved from a cache that is updated periodically. Therefore, it might
     * be outdated. Code that needs to ensure that a server is offline should
     * use the {@link #verifyServerOffline(UUID)} method which bypasses the
     * cache.
     * </p>
     * 
     * <p>
     * By using the cache, this method ensures that it will never block.
     * </p>
     * 
     * @param serverId
     *            unique identifier identifying the server.
     * @return current status of the server (possibly cached) or
     *         <code>null</code> if no status information is available for the
     *         specified ID.
     * @throws NullPointerException
     *             if <code>serverId</code> is <code>null</code>.
     */
    public ServerStatus getServer(UUID serverId) {
        Preconditions.checkNotNull(serverId, "The serverId must not be null.");
        // We read the data from the cache. If the periodic update does not work
        // for some reason, the data in the cache will be stale. However, we
        // still use it. The alternative would be throwing an exception and this
        // would require additional handling in the calling code. Typically, it
        // should not matter if the data is outdated, because when it is
        // important, the calling code will call verifyServerOffline(...), which
        // does not use the cache.
        synchronized (cacheLock) {
            ClusterServer server = cacheIdToServer.get(serverId);
            if (server == null) {
                return null;
            }
            long currentTime = System.currentTimeMillis();
            return new ServerStatus(
                    server.getServerId(),
                    server.getServerName(),
                    server.getLastOnlineTime().getTime() >= currentTime
                            - OTHER_REGISTRATION_VALID_MILLISECONDS,
                    server.getLastOnlineTime().getTime() < currentTime
                            - MINIMUM_REGISTRATION_AGE_BEFORE_DELETE_MILLISECONDS);
        }
    }

    /**
     * <p>
     * Returns the status for all servers in the cluster. The list is ordered by
     * the server IDs. The status is retrieved from a cache that is updated
     * periodically. Therefore, it might be outdated. Code that needs to ensure
     * that a server is offline should use the
     * {@link #verifyServerOffline(UUID)} method which bypasses the cache.
     * </p>
     * 
     * <p>
     * By using the cache, this method ensures that it will never block.
     * </p>
     * 
     * @return list of all servers in the cluster (never <code>null</code>).
     */
    public List<ServerStatus> getServers() {
        // We read the data from the cache. If the periodic update does not work
        // for some reason, the data in the cache will be stale. However, we
        // still use it. The alternative would be throwing an exception and this
        // would require additional handling in the calling code. Typically, it
        // should not matter if the data is outdated, because when it is
        // important, the calling code will call verifyServerOffline(...), which
        // does not use the cache.
        synchronized (cacheLock) {
            ArrayList<ServerStatus> servers = new ArrayList<ServerStatus>(
                    cacheIdToServer.size());
            long currentTime = System.currentTimeMillis();
            long onlineTimeLimit = currentTime
                    - OTHER_REGISTRATION_VALID_MILLISECONDS;
            long deletableTimeLimit = currentTime
                    - MINIMUM_REGISTRATION_AGE_BEFORE_DELETE_MILLISECONDS;
            for (ClusterServer server : cacheIdToServer.values()) {
                long lastOnlineTime = server.getLastOnlineTime().getTime();
                servers.add(new ServerStatus(server.getServerId(), server
                        .getServerName(), lastOnlineTime >= onlineTimeLimit,
                        lastOnlineTime < deletableTimeLimit));
            }
            return servers;
        }
    }

    /**
     * Returns the current status of this server. Unlike the information
     * returned by {@link #getServer(UUID)}, this reflects this server's actual
     * state and does not involve caching. In particular, the online information
     * returned with the server status reflects the current return value of
     * {@link #isOnline()}.
     * 
     * @return server status for this server (never <code>null</code>).
     */
    public ServerStatus getThisServer() {
        return new ServerStatus(serverId, serverName, isOnline(), false);
    }

    /**
     * Returns <code>true</code> if the server is currently considered online
     * (has recently registered itself in the Cassandra database),
     * <code>false</code> otherwise. In general, other components should only
     * run actions that might interfer with the operation of other servers when
     * this server has been registered in the database and thus this method
     * returns <code>true</code>.
     * 
     * @return <code>true</code> if this server is considered online,
     *         <code>false</code> otherwise.
     */
    public boolean isOnline() {
        synchronized (lock) {
            return online;
        }
    }

    /**
     * <p>
     * Removes the server with the specified ID from the cluster. A server can
     * only be removed if it has been offline for a sufficient amount of time.
     * This method calls {@link #verifyServerOffline(UUID)} to check that. This
     * method does not take care of resources associated with the specified
     * server (e.g. channels). The calling code should move such resources to a
     * different server before removing a server.
     * </p>
     * 
     * <p>
     * This method blocks while it is waiting for the verification of the
     * server's offline status and the removal of the server to finish.
     * </p>
     * 
     * @param serverId
     *            unique identifier identifying the server to be removed.
     * @throws IllegalArgumentException
     *             if the specified server cannot be deleted because it has not
     *             been offline for a sufficient amount of time.
     * @throws RuntimeException
     *             if there is an error accessing the underlying database.
     */
    public void removeServer(UUID serverId) {
        if (!FutureUtils.getUnchecked(verifyServerOffline(serverId))) {
            throw new IllegalArgumentException("Server " + serverId.toString()
                    + " is not offline.");
        }
        FutureUtils.getUnchecked(clusterServersDAO.deleteServer(serverId));
        // For an update of the cache so that the removed server is also removed
        // from the cache.
        synchronized (cacheLock) {
            updateCache();
        }
    }

    /**
     * Verifies that a server is actually offline. In contrast to the online
     * state returned by {@link #getServer(UUID)}, this method does not use a
     * cache, but always queries the database directly. The result is returned
     * through a future so that this method does not block. If there is an
     * error, the future's <code>get()</code> method throws an exception.
     * 
     * @param serverId
     *            unique identifier of the server which should be checked.
     * @return online status of the specified server exposed through a
     *         listenable future. The future returns <code>true</code> if the
     *         server is currently not online or if there is no status
     *         information for the specified ID in the database and
     *         <code>false</code> if the server with the specified ID is online.
     */
    public ListenableFuture<Boolean> verifyServerOffline(UUID serverId) {
        // We wrap the code in a try-catch block because we want all exceptions
        // to be wrapped in a future so that the calling code can do error
        // handling in one place.
        try {
            // If the server is online according to our cache, there is no need
            // to check the state.
            ServerStatus serverStatus = getServer(serverId);
            if (serverStatus != null && serverStatus.isOnline()) {
                return Futures.immediateFuture(false);
            }
            ListenableFuture<? extends ClusterServer> serverFuture = clusterServersDAO
                    .getServer(serverId);
            return Futures.transform(serverFuture,
                    new Function<ClusterServer, Boolean>() {
                        @Override
                        public Boolean apply(ClusterServer input) {
                            return (input == null || (input.getLastOnlineTime()
                                    .getTime() < System.currentTimeMillis()
                                    - OTHER_REGISTRATION_VALID_MILLISECONDS));
                        }
                    });
        } catch (Throwable t) {
            return Futures.immediateFailedFuture(t);
        }
    }

    private void checkOnlineState() {
        try {
            boolean newOnline;
            boolean oldOnline;
            synchronized (lock) {
                // If this cluster manager has been destroyed, we should not
                // modify the online state. The listeners were already notified
                // when the destroyed flag was set.
                if (destroyed) {
                    return;
                }
                long currentTime = System.currentTimeMillis();
                oldOnline = this.online;
                // We are online if we are not destroyed, we have a sufficiently
                // recent registration, enough time has passed since we were
                // offline, and the clock-skew monitor has been started.
                // We also wait for the control-system support registry to
                // become available becomes many parts of the archiving server
                // depend on it and might expose funny behavior if they start
                // operation before the control-system supports are available.
                // Just waiting for the clock-skew monitor to be started might
                // not be optimal because this means that the clock might not
                // have been checked yet. However, waiting for the first clock
                // check to finish is not a good idea either because in the
                // worst case, all other servers are offline and it might take
                // quite some time for the requests to these servers to time
                // out. This could then block the startup of this server for a
                // very long time. The delay between registering this server and
                // actually considering the server online should be sufficient
                // to successfully finish a clock check when other servers are
                // actually online, so this solution is not as bad as it might
                // look at first.
                newOnline = !destroyed
                        && (lastUpdateSuccessTime
                                + OWN_REGISTRATION_VALID_MILLISECONDS > currentTime)
                        && (lastOfflineTime
                                + CACHE_UPDATE_INTERVAL_MILLISECONDS < currentTime)
                        && clockSkewMonitorStarted
                        && controlSystemSupportRegistry.isAvailable();
                this.online = newOnline;
                if (oldOnline == newOnline) {
                    return;
                }
                this.notificationInProgress = true;
            }
            // It is possible that this cluster manager is destroyed while we
            // are sending notifications. Therefore, after sending the
            // notifications we have to check again whether it has been
            // destroyed in the meantime. In this case, we send out the
            // notifications because the destroy method will not.
            boolean destroyed;
            try {
                publishServerOnlineStatusEvent(newOnline);
            } finally {
                synchronized (lock) {
                    this.notificationInProgress = false;
                    destroyed = this.destroyed;
                }
            }
            if (destroyed) {
                publishServerOnlineStatusEvent(false);
            }
        } catch (RuntimeException e) {
            // In general, there is no reason why such an exception should
            // occur, but if we did not catch it, the executor service would
            // stop scheduling this method.
            log.error("Error in online state watch-dog thread.", e);
        }
    }

    private void startClockSkewMonitor() {
        // We only want to start the clock-skew monitor once. If we started it
        // twice, it would run at a higher frequency than intended.
        synchronized (lock) {
            if (clockSkewMonitorStarted) {
                return;
            }
            // Setting the flag before actually starting the service has the
            // advantage that we can avoid a race condition without having to
            // hold the lock while scheduling the task.
            clockSkewMonitorStarted = true;
        }
        // We want to run the monitoring task of the clock skew monitor twice a
        // second. This should detect a problem in a reasonable amount of time
        // without putting too much load on the network.
        scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            Predicate<UUID> notThisServer = Predicates.not(Predicates
                    .equalTo(serverId));

            @Override
            public void run() {
                // We make a copy of the cluster servers map so that we do not
                // have to hold the mutex when running the periodic update of
                // the clock skew monitor. We have to filter the map anyway,
                // because there is no sense in including this server.
                ImmutableMap<UUID, ClusterServer> copyOfCacheIdToServer;
                synchronized (lock) {
                    copyOfCacheIdToServer = ImmutableMap.copyOf(Maps
                            .filterKeys(cacheIdToServer, notThisServer));
                }
                // We do not want the run method to throw an exception. This
                // would have the effect that future executions of this tasks
                // would be cancelled. We do not expect an exception, because
                // all exceptions that are expected are caught, but we still
                // rather catch an unexpected exception here (and log it so that
                // the bug can be fixed) instead of inadvertently stopping the
                // period task.
                try {
                    clockSkewMonitor.periodicUpdate(copyOfCacheIdToServer);
                } catch (Throwable t) {
                    log.error(
                            "Periodic update of clock skew monitor failed"
                                    + (t.getMessage() == null ? "." : (": " + t
                                            .getMessage())), t);
                }
            }
        }, 0L, 500L, TimeUnit.MILLISECONDS);
    }

    private void updateCache() {
        updateCacheStartTime = System.currentTimeMillis();
        final ListenableFuture<? extends Iterable<? extends ClusterServer>> serversFuture = clusterServersDAO
                .getServers();
        // We execute the listener using our execution service because even
        // though the callback will most likely not block, it iterates over the
        // whole list of cluster servers and this could take a while.
        serversFuture.addListener(new Runnable() {
            @Override
            public void run() {
                try {
                    updateCacheProcessData(FutureUtils
                            .getUnchecked(serversFuture));
                } catch (Throwable t) {
                    // A NoHostAvailableException is most likely caused by a
                    // connection problem with the Apache Cassandra cluster.
                    // We only want to log such a problem at the INFO level,
                    // because this kind of error is most likely transient.
                    if (t instanceof NoHostAvailableException) {
                        log.info("Updating the server cache failed.", t);
                    } else {
                        log.error("Updating the server cache failed.", t);
                    }
                }
                long delay;
                long currentTime = System.currentTimeMillis();
                if (currentTime > updateCacheStartTime
                        + CACHE_UPDATE_INTERVAL_MILLISECONDS) {
                    delay = 0L;
                } else {
                    delay = updateCacheStartTime
                            + CACHE_UPDATE_INTERVAL_MILLISECONDS - currentTime;
                }
                scheduleUpdateCache(delay, TimeUnit.MILLISECONDS);
            }
        }, scheduledExecutorService);
    }

    private void updateCacheProcessData(
            Iterable<? extends ClusterServer> servers) {
        // The iterator might block if the response from the database is spread
        // across multiple pages. Therefore, we make a copy of the whole list
        // before taking the mutex. The alternative would be to take and release
        // the mutex for every iteration, but this seems a bit wasteful.
        ImmutableList<ClusterServer> copiedServers = ImmutableList
                .copyOf(servers);
        synchronized (cacheLock) {
            cacheIdToServer.clear();
            cacheNameToServer.clear();
            HashSet<String> seenNames = new HashSet<String>();
            for (ClusterServer server : copiedServers) {
                cacheIdToServer.put(server.getServerId(), server);
                if (seenNames.add(server.getServerName())) {
                    cacheNameToServer.put(server.getServerName(), server);
                } else {
                    // The name is not unique and we cannot safely resolve it.
                    cacheNameToServer.remove(server.getServerName());
                }
            }
        }
    }

    private void scheduleUpdateCache(long delay, TimeUnit unit) {
        scheduledExecutorService.schedule(updateCacheRunnable, delay, unit);
    }

    private void updateLastOnlineTime() {
        try {
            final long updateAttemptTime = System.currentTimeMillis();
            final ListenableFuture<?> future = clusterServersDAO
                    .createOrUpdateServer(serverId, serverName,
                            interNodeCommunicationUrl, new Date(
                                    updateAttemptTime));
            future.addListener(new Runnable() {
                @Override
                public void run() {
                    boolean successful;
                    try {
                        future.get();
                        successful = true;
                    } catch (InterruptedException e) {
                        // The future already finished, so we should never get
                        // an interrupted exception.
                        Thread.currentThread().interrupt();
                        successful = false;
                    } catch (ExecutionException e) {
                        Throwable cause = e.getCause();
                        // A NoHostAvailableException is most likely caused by a
                        // connection problem with the Apache Cassandra cluster.
                        // We only want to log such a problem at the INFO level,
                        // because this kind of error is most likely transient.
                        if (cause instanceof NoHostAvailableException) {
                            log.info(
                                    "Updating the server last-online time failed.",
                                    cause);
                        } else {
                            log.error(
                                    "Updating the server last-online time failed.",
                                    cause != null ? cause : e);
                        }
                        successful = false;
                    }
                    if (successful) {
                        synchronized (lock) {
                            // If we have been offline before, we want to stay
                            // offline until we can be sure that all other
                            // servers have updated their cache. This way, we
                            // can avoid a race condition where another server
                            // thinks that we are offline while we are just
                            // going online.
                            if (lastUpdateSuccessTime
                                    + OTHER_REGISTRATION_VALID_MILLISECONDS < updateAttemptTime) {
                                lastOfflineTime = updateAttemptTime;
                            }
                            lastUpdateSuccessTime = updateAttemptTime;
                            // If the cluster manager has been destroyed, we do
                            // not want to schedule another update.
                            if (destroyed) {
                                return;
                            }
                        }
                        // Schedule the next update of the time stamp.
                        long delay = updateAttemptTime
                                + UPDATE_LAST_ONLINE_TIME_DELAY_MILLISECONDS
                                - System.currentTimeMillis();
                        delay = Math.max(delay, 0L);
                        scheduleUpdateLastOnlineTime(delay,
                                TimeUnit.MILLISECONDS);
                    } else {
                        // When an update fails, we try again in shorter
                        // intervals until so much time has passed that we are
                        // offline anyway. In this case, we switch to longer
                        // intervals in order to avoid filling up the log with
                        // lots of error messages if the connection is
                        // interrupted for a longer period of time.
                        long lastUpdateSuccessTime;
                        synchronized (lock) {
                            // If the cluster manager has been destroyed, we do
                            // not want to schedule another update.
                            if (destroyed) {
                                return;
                            }
                            lastUpdateSuccessTime = ClusterManagementService.this.lastUpdateSuccessTime;
                        }
                        long delay;
                        if (lastUpdateSuccessTime
                                + OWN_REGISTRATION_VALID_MILLISECONDS > System
                                .currentTimeMillis()) {
                            delay = UPDATE_LAST_ONLINE_TIME_DELAY_MILLISECONDS / 5L;
                        } else {
                            delay = UPDATE_LAST_ONLINE_TIME_DELAY_MILLISECONDS * 6L;
                        }
                        scheduleUpdateLastOnlineTime(delay,
                                TimeUnit.MILLISECONDS);
                    }
                }
            }, scheduledExecutorService);
        } catch (RuntimeException e) {
            // A NoHostAvailableException is most likely caused by a
            // connection problem with the Apache Cassandra cluster.
            // We only want to log such a problem at the INFO level,
            // because this kind of error is most likely transient.
            if (e instanceof NoHostAvailableException) {
                log.info("Updating the server last-online time failed.", e);
            } else {
                log.error("Updating the server last-online time failed.", e);
            }
            scheduleUpdateLastOnlineTime(
                    UPDATE_LAST_ONLINE_TIME_DELAY_MILLISECONDS * 6L,
                    TimeUnit.MILLISECONDS);
        }
    }

    private void scheduleUpdateLastOnlineTime(long delay, TimeUnit unit) {
        // Schedule the next registration attempt.
        scheduledExecutorService.schedule(updateLastOnlineTimeRunnable, delay,
                unit);
    }

    private void updateServerRegistration() {
        try {
            final long updateAttemptTime = System.currentTimeMillis();
            final ListenableFuture<?> future = clusterServersDAO
                    .createOrUpdateServer(serverId, serverName,
                            interNodeCommunicationUrl, new Date(
                                    updateAttemptTime));
            future.addListener(new Runnable() {
                @Override
                public void run() {
                    boolean successful;
                    try {
                        future.get();
                        successful = true;
                    } catch (InterruptedException e) {
                        // The future already finished, so we should never get
                        // an interrupted exception.
                        Thread.currentThread().interrupt();
                        successful = false;
                    } catch (ExecutionException e) {
                        Throwable cause = e.getCause();
                        // A NoHostAvailableException is most likely caused by a
                        // connection problem with the Apache Cassandra cluster.
                        // We only want to log such a problem at the INFO level,
                        // because this kind of error is most likely transient.
                        if (cause instanceof NoHostAvailableException) {
                            log.info(
                                    "Updating the server registration failed.",
                                    cause);
                        } else {
                            log.error(
                                    "Updating the server registration failed.",
                                    cause != null ? cause : e);
                        }
                        successful = false;
                    }
                    if (successful) {
                        synchronized (lock) {
                            // If we have been offline before, we want to stay
                            // offline until we can be sure that all other
                            // servers have updated their cache. This way, we
                            // can avoid a race condition where another server
                            // thinks that we are offline while we are just
                            // going online.
                            if (lastUpdateSuccessTime
                                    + OTHER_REGISTRATION_VALID_MILLISECONDS < updateAttemptTime) {
                                lastOfflineTime = updateAttemptTime;
                            }
                            lastUpdateSuccessTime = updateAttemptTime;
                            // If the cluster manager has been destroyed, we do
                            // not want to schedule another update.
                            if (destroyed) {
                                return;
                            }
                        }
                        // Start the watch dog. We run the watchdog at a rate of
                        // 2 Hz. This should ensure that notifications are sent
                        // with an acceptable latency.
                        scheduledExecutorService.scheduleAtFixedRate(
                                new Runnable() {
                                    @Override
                                    public void run() {
                                        checkOnlineState();
                                    }
                                }, 0L, 500L, TimeUnit.MILLISECONDS);
                        // Schedule the next update of the time stamp.
                        long delay = updateAttemptTime
                                + UPDATE_LAST_ONLINE_TIME_DELAY_MILLISECONDS
                                - System.currentTimeMillis();
                        delay = Math.max(delay, 0L);
                        scheduleUpdateLastOnlineTime(delay,
                                TimeUnit.MILLISECONDS);
                    } else {
                        synchronized (lock) {
                            // If the cluster manager has been destroyed, we do
                            // not want to schedule another update.
                            if (destroyed) {
                                return;
                            }
                        }
                        // We retry the initial registration in longer periods,
                        // because it might be that the database server is
                        // simply not available yet.
                        scheduleUpdateServerRegistration(
                                INITIAL_SERVER_REGISTRATION_RETRY_DELAY_SECONDS,
                                TimeUnit.SECONDS);
                    }
                }
            }, scheduledExecutorService);
        } catch (RuntimeException e) {
            // A NoHostAvailableException is most likely caused by a
            // connection problem with the Apache Cassandra cluster.
            // We only want to log such a problem at the INFO level,
            // because this kind of error is most likely transient.
            if (e instanceof NoHostAvailableException) {
                log.info("Updating the server registration failed.", e);
            } else {
                log.error("Updating the server registration failed.", e);
            }
            scheduleUpdateServerRegistration(
                    INITIAL_SERVER_REGISTRATION_RETRY_DELAY_SECONDS,
                    TimeUnit.SECONDS);
        }
    }

    private void scheduleUpdateServerRegistration(long delay, TimeUnit unit) {
        // Schedule the next registration attempt.
        scheduledExecutorService.schedule(updateServerRegistrationRunnable,
                delay, unit);
    }

    private String getHostName() {
        // We try to get the host name from environment variables. They might
        // not always be defined and may contain an incorrect value, but it is
        // still the best choice except running external commands which is
        // cumbersome.
        String hostName = null;
        if (SystemUtils.IS_OS_UNIX) {
            hostName = System.getenv("HOSTNAME");
        } else if (SystemUtils.IS_OS_WINDOWS) {
            hostName = System.getenv("COMPUTERNAME");
        }
        if (hostName != null) {
            return hostName;
        }
        // Finally, we try InetAddress.getLocalHost(). This will only work if
        // DNS is setup correctly and might return a different value than
        // expected. Therefore, we try this variant last.
        try {
            hostName = InetAddress.getLocalHost().getHostName();
            int firstDot = hostName.indexOf('.');
            if (firstDot == -1) {
                return hostName;
            } else {
                hostName = hostName.substring(0, firstDot);
                // If the host name is all numeric, we probably got an IP
                // address.
                if (!StringUtils.isNumeric(hostName)) {
                    return hostName;
                }
            }
            hostName = null;
        } catch (UnknownHostException e) {
            // Ignore the exception, we simply continue with a null value.
        }
        // If everything failed, we return the special string "unknown".
        log.warn("Could not determine hostname.");
        return "unknown";
    }

    private void publishServerOnlineStatusEvent(boolean online) {
        applicationEventPublisher.publishEvent(new ServerOnlineStatusEvent(
                this, online));
    }

}
