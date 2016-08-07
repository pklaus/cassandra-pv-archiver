/*
 * Copyright 2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.cluster.internal;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.aquenos.cassandra.pvarchiver.server.cluster.ClusterManagementService;
import com.aquenos.cassandra.pvarchiver.server.database.ClusterServersDAO.ClusterServer;
import com.aquenos.cassandra.pvarchiver.server.internode.InterNodeCommunicationService;
import com.aquenos.cassandra.pvarchiver.server.util.FutureUtils;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * <p>
 * Monitors the clock skew between the sytem clock of this server and the system
 * clock of other servers. This class is only intended for use by
 * {@link ClusterManagementService}.
 * </p>
 * 
 * <p>
 * The {@link ClusterManagementService} periodically calls the
 * {@link #periodicUpdate(Map)} method. This method randomly chooses up to five
 * servers that are contacted in order to get their current system time. When
 * the current system time is reported by a remote server, the minimum clock
 * skew is calculated and the lister registered by the
 * {@link ClusterManagementService} is notified. The listener is also notified
 * if this class detects that the local system clock skipped back in time.
 * </p>
 * 
 * <p>
 * Due to the way how the algorithm works, this class can only calculate an
 * estimate representing the lower limit of the actual clock skew: There is an
 * inherent uncertainty due to the round-trip time of the network request. If
 * the clock skew is smaller than the round-trip time, it might not be detected
 * at all and a clock skew of zero might be reported.
 * </p>
 * 
 * @author Sebastian Marsching
 */
public class ClockSkewMonitor {

    /**
     * Event delivered to the {@link ClockSkewMonitorEventListener}.
     * 
     * @author Sebastian Marsching
     */
    public static class ClockSkewMonitorEvent {

        private static final ClockSkewMonitorEvent SKIPPED_BACK = new ClockSkewMonitorEvent(
                true, 0L, 0L, null);

        public static ClockSkewMonitorEvent received(long minimumClockSkew,
                long roundTripTime, UUID serverId) {
            return new ClockSkewMonitorEvent(false, minimumClockSkew,
                    roundTripTime, serverId);
        }

        public static ClockSkewMonitorEvent skippedBack() {
            return SKIPPED_BACK;
        }

        private boolean clockSkippedBack;
        private long minimumClockSkew;
        private long roundTripTime;
        private UUID serverId;

        private ClockSkewMonitorEvent(boolean clockSkippedBack,
                long minimumClockSkew, long roundTripTime, UUID serverId) {
            this.clockSkippedBack = clockSkippedBack;
            this.minimumClockSkew = minimumClockSkew;
            this.roundTripTime = roundTripTime;
            this.serverId = serverId;
        }

        /**
         * Tells whether this event is sent because the local system clock
         * skipped back in time.
         * 
         * @return <code>true</code> if the local system clock skipped back in
         *         time, <code>false</code> if this is a regular event.
         */
        public boolean isClockSkippedBack() {
            return clockSkippedBack;
        }

        /**
         * <p>
         * Returns the estimate of the lower limit of the clock skew between
         * this server and the remote server (in milliseconds).
         * </p>
         * 
         * <p>
         * A positive number indicates that the clock of this server is is
         * leaping ahead compared to the clock of the remote server. A negative
         * number indicates that it is lagging behind compared to the clock of
         * the remote server. A number of zero indicates that no estimate of the
         * lower limit of the clock skew can be calculated because of the
         * round-trip time. However, this means that the actual clock skew must
         * be smaller than the round-trip time.
         * </p>
         * 
         * @return estimate of the lower limit of the clock skew between this
         *         server and the queried remote server.
         * @throws IllegalStateException
         *             if this event indicates skipping back of the local system
         *             clock and thus {@link #isClockSkippedBack()} returns
         *             <code>true</code>.
         */
        public long getMinimumClockSkew() {
            if (clockSkippedBack) {
                throw new IllegalStateException(
                        "This method may not be called if the event signals that the system clock skipped back.");
            }
            return minimumClockSkew;
        }

        /**
         * Returns the round-trip time of the request (in milliseconds). The
         * round-trip time is the time between sending the request for the
         * remote server's current system time and receiving the response.
         * 
         * @return round-trip time of the request that was used to calculate the
         *         clock skew provided by this event.
         * @throws IllegalStateException
         *             if this event indicates skipping back of the local system
         *             clock and thus {@link #isClockSkippedBack()} returns
         *             <code>true</code>.
         */
        public long getRoundTripTime() {
            if (clockSkippedBack) {
                throw new IllegalStateException(
                        "This method may not be called if the event signals that the system clock skipped back.");
            }
            return roundTripTime;
        }

        /**
         * Returns the ID of the remote server that was queried.
         * 
         * @return ID of the remote server that was queried.
         * @throws IllegalStateException
         *             if this event indicates skipping back of the local system
         *             clock and thus {@link #isClockSkippedBack()} returns
         *             <code>true</code>.
         */
        public UUID getServerId() {
            if (clockSkippedBack) {
                throw new IllegalStateException(
                        "This method may not be called if the event signals that the system clock skipped back.");
            }
            return serverId;
        }

    }

    /**
     * Interface that must be implemented by the event listener that is
     * registered with the {@link ClockSkewMonitor}.
     * 
     * @author Sebastian Marsching
     */
    public static interface ClockSkewMonitorEventListener {

        /**
         * Notifies the listener of a new clock skew event. A clock skew event
         * is sent every time that a response is received from a remote server.
         * 
         * @param event
         *            monitor event.
         */
        void onClockSkewEvent(ClockSkewMonitorEvent event);

    }

    private static enum InternalServerState {
        OFFLINE, ONLINE, PENDING
    }

    private static class InternalServer {
        public boolean cancelled;
        public InternalServerState state;
        public String url;
    }

    /**
     * Logger for this class.
     */
    protected Log log = LogFactory.getLog(getClass());

    private InterNodeCommunicationService interNodeCommunicationService;
    private ClockSkewMonitorEventListener listener;
    private final Object lock = new Object();
    private Map<UUID, InternalServer> servers = new HashMap<UUID, InternalServer>();
    private Random random = new Random();
    private Set<UUID> serversOffline = Collections
            .newSetFromMap(new ConcurrentHashMap<UUID, Boolean>());
    private Set<UUID> serversOnline = Collections
            .newSetFromMap(new ConcurrentHashMap<UUID, Boolean>());

    /**
     * Creates a clock skew monitor.
     * 
     * @param interNodeCommunicationService
     *            inter-node communication service that is used to get the
     *            current system time of remote servers.
     * @param listener
     *            listener that shall be notified when a response is received
     *            from a remote server.
     */
    public ClockSkewMonitor(
            InterNodeCommunicationService interNodeCommunicationService,
            ClockSkewMonitorEventListener listener) {
        this.interNodeCommunicationService = interNodeCommunicationService;
        this.listener = listener;
    }

    /**
     * <p>
     * Sends requests for the current system time to remote servers. This method
     * should be called periodically. It will send a request to up to four
     * servers that replied the last time they were queried and up to one server
     * that did not reply the last time it was queried. The actual servers are
     * chosen randomly.
     * </p>
     * 
     * <p>
     * A {@link ClockSkewMonitorEvent} is sent for each response received from
     * one of the queried servers. This method only starts the requests. The
     * processing of the responses and thus the notification of the listener is
     * done asynchronously.
     * </p>
     * 
     * @param knownServers
     *            map containing all servers currently known in the cluster
     *            (using their IDs as keys). This method randomly chooses up to
     *            five of the servers using the strategy described earlier.
     */
    public void periodicUpdate(Map<UUID, ClusterServer> knownServers) {
        LinkedList<Pair<UUID, String>> targetServerIdsAndUrls = new LinkedList<Pair<UUID, String>>();
        synchronized (lock) {
            // First we remove all servers that are not known any longer. If a
            // servers is in the pending state, we do not remove it until the
            // pending request has finished. We have to do this so that we do
            // not accidentally run two requests in parallel in case the server
            // gets added again.
            for (Iterator<Map.Entry<UUID, InternalServer>> i = servers
                    .entrySet().iterator(); i.hasNext();) {
                Map.Entry<UUID, InternalServer> serverEntry = i.next();
                if (!knownServers.containsKey(serverEntry.getKey())) {
                    switch (serverEntry.getValue().state) {
                    case OFFLINE:
                        serversOffline.remove(serverEntry.getKey());
                        i.remove();
                        break;
                    case ONLINE:
                        serversOnline.remove(serverEntry.getKey());
                        i.remove();
                        break;
                    case PENDING:
                        serverEntry.getValue().cancelled = true;
                        break;
                    }
                }
            }
            // Next we add servers that are missing. For servers that are
            // already existing, we still update the URL and set cancelled to
            // false (just in case it was cancelled because it was temporarily
            // missing).
            for (Map.Entry<UUID, ClusterServer> serverEntry : knownServers
                    .entrySet()) {
                InternalServer server = servers.get(serverEntry.getKey());
                if (server == null) {
                    server = new InternalServer();
                    server.state = InternalServerState.ONLINE;
                    servers.put(serverEntry.getKey(), server);
                    serversOnline.add(serverEntry.getKey());
                }
                server.cancelled = false;
                server.url = serverEntry.getValue()
                        .getInterNodeCommunicationUrl();
            }
            // With each iteration, we want to query four servers that were
            // online when we made the last request and one that was offline
            // when we made the last request. We treat newly added servers as
            // online. When we make a request, we remove the server from the
            // corresponding set. This ensures that we do not make another
            // request to the same server while we are still waiting for a
            // response.
            Collection<UUID> randomServersOnline = randomSample(serversOnline,
                    4);
            Collection<UUID> randomServersOffline = randomSample(
                    serversOffline, 1);
            for (UUID serverId : randomServersOnline) {
                serversOnline.remove(serverId);
                InternalServer server = servers.get(serverId);
                server.state = InternalServerState.PENDING;
                targetServerIdsAndUrls.add(Pair.of(serverId, server.url));
            }
            for (UUID serverId : randomServersOffline) {
                serversOffline.remove(serverId);
                InternalServer server = servers.get(serverId);
                server.state = InternalServerState.PENDING;
                targetServerIdsAndUrls.add(Pair.of(serverId, server.url));
            }
        }
        // We can send the actual requests after releasing the mutex. This
        // reduces the risk of a dead lock.
        for (Pair<UUID, String> serverIdAndUrl : targetServerIdsAndUrls) {
            sendTimeRequest(serverIdAndUrl.getLeft(), serverIdAndUrl.getRight());
        }
    }

    private void failureEvent(UUID serverId) {
        synchronized (lock) {
            InternalServer server = servers.get(serverId);
            if (server.cancelled) {
                servers.remove(serverId);
            } else {
                server.state = InternalServerState.OFFLINE;
                serversOffline.add(serverId);
            }
        }
    }

    private <T> List<T> randomSample(Collection<? extends T> collection,
            int maximumNumberOfElements) {
        ArrayList<T> array = new ArrayList<T>(collection);
        int arraySize = array.size();
        int numberOfElements = maximumNumberOfElements < array.size() ? maximumNumberOfElements
                : array.size();
        for (int i = 0; i < numberOfElements; ++i) {
            T originalElement = array.get(i);
            int swapIndex = random.nextInt(arraySize - i) + i;
            array.set(i, array.get(swapIndex));
            array.set(swapIndex, originalElement);
        }
        return array.subList(0, numberOfElements);
    }

    private void sendTimeRequest(final UUID targetServerId,
            String targetServerUrl) {
        final long startTime = System.currentTimeMillis();
        final ListenableFuture<Long> future = interNodeCommunicationService
                .getCurrentSystemTime(targetServerUrl);
        future.addListener(new Runnable() {
            @Override
            public void run() {
                long endTime = System.currentTimeMillis();
                long remoteTime;
                try {
                    remoteTime = FutureUtils.getUnchecked(future);
                } catch (Throwable t) {
                    // This is not considered an error. The server might simply
                    // be offline.
                    failureEvent(targetServerId);
                    return;
                }
                successfulEvent(targetServerId, startTime, endTime, remoteTime);
            }
        }, MoreExecutors.sameThreadExecutor());
    }

    private void successfulEvent(UUID serverId, long startTime, long endTime,
            long remoteTime) {
        synchronized (lock) {
            InternalServer server = servers.get(serverId);
            if (server.cancelled) {
                servers.remove(serverId);
            } else {
                server.state = InternalServerState.ONLINE;
                serversOnline.add(serverId);
            }
        }
        if (startTime > endTime) {
            listener.onClockSkewEvent(ClockSkewMonitorEvent.skippedBack());
            return;
        }
        // The skew that we calculate is the lower limit of the clock skew. It
        // could (and most likely is) greater, but there is an uncertainty due
        // to the time between sending the request and receiving the response.
        long skew;
        if (remoteTime < startTime) {
            // Our clock is skewed into the future compared to the remote clock.
            skew = startTime - remoteTime;
        } else if (remoteTime > endTime) {
            // Our clock is skewed into the past compared to the remote clock.
            skew = endTime - remoteTime;
        } else {
            // Our clock could be perfectly in sync with the remote clock.
            skew = 0L;
        }
        long roundTripTime = endTime - startTime;
        listener.onClockSkewEvent(ClockSkewMonitorEvent.received(skew,
                roundTripTime, serverId));
    }

}
