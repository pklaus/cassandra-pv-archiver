/*
 * Copyright 2015-2017 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.cluster;

import java.util.UUID;

/**
 * Status information for an archive server. The status object represents the
 * state of the server when the object was created and is not updated when the
 * state changes.
 * 
 * @author Sebastian Marsching
 *
 */
public class ServerStatus {

    private final long lastOnlineTime;
    private final boolean online;
    private final boolean removable;
    private final UUID serverId;
    private final String serverName;

    /**
     * Creates a status object encapsulating the specified information.
     * 
     * @param serverId
     *            unique identifier identifying the server.
     * @param serverName
     *            human-readable identifier for the server. Typically, this is
     *            the server's host-name. For technical reasons, there is no
     *            guarantee that this identifier is unique.
     * @param online
     *            <code>true</code> if the server is online, <code>false</code>
     *            if it is offline.
     * @param removable
     *            <code>true</code> if the server has been offline for a
     *            sufficient amount of time and may be removed,
     *            <code>false</code> otherwise.
     * @param lastOnlineTime
     *            last time the server registered itself with the cluster. The
     *            time is specified as the number of milliseconds since January
     *            1st, 1970, 00:00:00 UTC. The time is according to the local
     *            clock of the server.
     */
    public ServerStatus(UUID serverId, String serverName, boolean online,
            boolean removable, long lastOnlineTime) {
        this.lastOnlineTime = lastOnlineTime;
        this.online = online;
        this.removable = removable;
        this.serverId = serverId;
        this.serverName = serverName;
    }

    /**
     * Returns the last time at which the server registered itself with the
     * cluster. The time is specified as the number of milliseconds since
     * January 1st, 1970, 00:00:00 UTC. The time is retrieved from the local
     * clock of the server at the time of registration, so it might not be
     * accurate.
     * 
     * @return last registration time for the server.
     */
    public long getLastOnlineTime() {
        return lastOnlineTime;
    }

    /**
     * Tells whether this server is currently online.
     * 
     * @return <code>true</code> if the server is online, <code>false</code> if
     *         it is offline.
     */
    public boolean isOnline() {
        return online;
    }

    /**
     * Tells whether this server has been offline for a sufficient amount of
     * time so that it may be removed. If this method returns <code>true</code>,
     * it implies that {@link #isOnline()} returns <code>false</code>.
     * 
     * @return <code>true</code> if the server has been offline long enough so
     *         that it may be removed, <code>false</code> otherwise.
     */
    public boolean isRemovable() {
        return removable;
    }

    /**
     * Returns the unique identifier that identifies the server within the
     * cluster.
     * 
     * @return unique identifier identifying the server.
     */
    public UUID getServerId() {
        return serverId;
    }

    /**
     * Returns a human-readable identifier for the server. Typically, this is
     * the server's host-name. For technical reasons, there is no guarantee that
     * this identifier is unique.
     * 
     * @return human-readable identifier for the server.
     */
    public String getServerName() {
        return serverName;
    }

}
