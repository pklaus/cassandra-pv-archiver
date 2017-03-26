/*
 * Copyright 2017 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.web.admin.controller.wsapi;

import java.util.List;
import java.util.UUID;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;

/**
 * <p>
 * Response object for the "cluster status" function of the web-service API.
 * </p>
 * 
 * <p>
 * This object is primarily intended to facilitate the JSON serialization of the
 * data provided by the API controller. For this reason, it does not perform any
 * checks on the parameters used to construct the object.
 * </p>
 * 
 * @author Sebastian Marsching
 */
public class ClusterStatusResponse {

    /**
     * <p>
     * Individual server item in the {@link ClusterStatusResponse}.
     * </p>
     * 
     * <p>
     * This object is primarily intended to facilitate the JSON serialization of
     * the data provided by the API controller. For this reason, it does not
     * perform any checks on the parameters used to construct the object.
     * </p>
     * 
     * @author Sebastian Marsching
     */
    public static class ServerItem {

        private final long lastOnlineTime;
        private final boolean online;
        private final UUID serverId;
        private final String serverName;

        /**
         * Creates a server item. This constructor does not verify the validity
         * of the arguments and simply uses them as-is.
         * 
         * @param lastOnlineTime
         *            last time the server renewed its registration with the
         *            cluster. The time is specified as the number of
         *            milliseconds since January 1st, 1970, 00:00:00 UTC. The
         *            time is according to the local clock of the server.
         * @param online
         *            flag indicating whether the server is online. The server
         *            is considered online if it recently renewed its
         *            registration with the cluster. <code>true</code> if the
         *            server is online, <code>false</code> if it is offline.
         * @param serverId
         *            unique identifier identifying the server.
         * @param serverName
         *            human-readable name of the server. Typically, this is the
         *            hostname of the server.
         */
        @JsonCreator
        public ServerItem(@JsonProperty("lastOnlineTime") long lastOnlineTime,
                @JsonProperty("online") boolean online,
                @JsonProperty("serverId") UUID serverId,
                @JsonProperty("serverName") String serverName) {
            this.lastOnlineTime = lastOnlineTime;
            this.online = online;
            this.serverId = serverId;
            this.serverName = serverName;
        }

        /**
         * Returns the last time the server renewed its registration with the
         * cluster. The time is specified as the number of milliseconds since
         * January 1st, 1970, 00:00:00 UTC. The time is according to the local
         * clock of the server.
         * 
         * @return last time the server registed with the cluster.
         */
        @JsonSerialize(using = ToStringSerializer.class)
        public long getLastOnlineTime() {
            return lastOnlineTime;
        }

        /**
         * Tells whether the server is online. The server is considered online
         * if it recently renewed its registration with the cluster.
         * 
         * @return <code>true</code> if the server is online, <code>false</code>
         *         if it is offline.
         */
        public boolean isOnline() {
            return online;
        }

        /**
         * Returns the server ID. The server ID identifies the server in the
         * cluster.
         * 
         * @return server ID.
         */
        public UUID getServerId() {
            return serverId;
        }

        /**
         * Returns the human-readable name of the server. Typically, this is the
         * hostname of the server.
         * 
         * @return server name.
         */
        public String getServerName() {
            return serverName;
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder().append(lastOnlineTime).append(online)
                    .append(serverId).append(serverName).toHashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || !obj.getClass().equals(this.getClass())) {
                return false;
            }
            ServerItem other = (ServerItem) obj;
            return new EqualsBuilder()
                    .append(this.lastOnlineTime, other.lastOnlineTime)
                    .append(this.online, other.online)
                    .append(this.serverId, other.serverId)
                    .append(this.serverName, other.serverName).isEquals();
        }

        @Override
        public String toString() {
            return ReflectionToStringBuilder.toString(this);
        }

    }

    private final List<ServerItem> servers;

    /**
     * Creates a cluster status response. This constructor does not verify the
     * validity of the arguments and simply uses them as-is.
     * 
     * @param servers
     *            list containing the status of each server in the cluster.
     */
    @JsonCreator
    public ClusterStatusResponse(
            @JsonProperty("servers") List<ServerItem> servers) {
        this.servers = servers;
    }

    /**
     * Returns the list of servers in the cluster with their status information.
     * 
     * @return list of servers in the cluster.
     */
    public List<ServerItem> getServers() {
        return servers;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(servers).toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || !obj.getClass().equals(this.getClass())) {
            return false;
        }
        ClusterStatusResponse other = (ClusterStatusResponse) obj;
        return new EqualsBuilder().append(this.servers, other.servers)
                .isEquals();
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this);
    }

}
