/*
 * Copyright 2017 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.web.admin.controller.wsapi;

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
 * Response object for the "server status" function of the web-service API.
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
public class ServerStatusResponse {

    private final String cassandraClusterName;
    private final String cassandraError;
    private final String cassandraKeyspaceName;
    private final int channelsDisconnected;
    private final int channelsError;
    private final int channelsTotal;
    private final UUID serverId;
    private final long serverLastOnlineTime;
    private final String serverName;
    private final boolean serverOnline;
    private final long totalSamplesDropped;
    private final long totalSamplesWritten;

    /**
     * Creates a server status response. This constructor does not verify the
     * validity of the arguments and simply uses them as-is.
     * 
     * @param cassandraClusterName
     *            human-readable name of the Cassandra cluster to which this
     *            server is connected. May be <code>null</code> if the server is
     *            currently not connected to a Cassandra cluster. In this case,
     *            <code>cassandraError</code> is not <code>null</code>.
     * @param cassandraError
     *            error message indicating a problem that occurred while trying
     *            to connect to the Cassandra cluster. This is <code>null</code>
     *            if there is no problem.
     * @param cassandraKeyspaceName
     *            name of the Cassandra keyspace which is used by the Cassandra
     *            PV Archiver. May be <code>null</code> if the server is
     *            currently not connected to a Cassandra cluster. In this case,
     *            <code>cassandraError</code> is not <code>null</code>.
     * @param channelsDisconnected
     *            number of channels that are in the disconnected state.
     * @param channelsError
     *            number of channels that are in the error state.
     * @param channelsTotal
     *            total number of channels on this server. If the server has not
     *            been initialized yet (e.g. because it cannot connect to the
     *            database), this number is zero, even if there are channels
     *            that belong to the server.
     * @param serverId
     *            ID of the server.
     * @param serverLastOnlineTime
     *            last time this server successfully registered itself with the
     *            cluster. The time is specified as the number of milliseconds
     *            since January 1st, 1970, 00:00:00 UTC. This number might be
     *            zero if the server has not successfully registered itself with
     *            the cluster since it was started.
     * @param serverName
     *            human-readable name of the server. Typically, this is the
     *            hostname of the server.
     * @param serverOnline
     *            flag indicating whether the server considers itself to be
     *            online. The server considers itself online when a sufficient
     *            amount of time has passed since successfully registering with
     *            the cluster and the renewal of the registration has not failed
     *            since.
     * @param totalSamplesDropped
     *            total number of samples that have been dropped (discarded) by
     *            this server because they arrived to quickly and could not be
     *            written in time. This counter is reset when the server is
     *            restarted.
     * @param totalSamplesWritten
     *            total number of samples that have been written (persisted) by
     *            this server. This counter is reset when the server is
     *            restarted.
     */
    @JsonCreator
    public ServerStatusResponse(
            @JsonProperty("cassandraClusterName") String cassandraClusterName,
            @JsonProperty("cassandraError") String cassandraError,
            @JsonProperty("cassandraKeyspaceName") String cassandraKeyspaceName,
            @JsonProperty("channelsDisconnected") int channelsDisconnected,
            @JsonProperty("channelsError") int channelsError,
            @JsonProperty("channelsTotal") int channelsTotal,
            @JsonProperty("serverId") UUID serverId,
            @JsonProperty("serverLastOnlineTime") long serverLastOnlineTime,
            @JsonProperty("serverName") String serverName,
            @JsonProperty("serverOnline") boolean serverOnline,
            @JsonProperty("totalSamplesDropped") long totalSamplesDropped,
            @JsonProperty("totalSamplesWritten") long totalSamplesWritten) {
        this.cassandraClusterName = cassandraClusterName;
        this.cassandraError = cassandraError;
        this.cassandraKeyspaceName = cassandraKeyspaceName;
        this.channelsDisconnected = channelsDisconnected;
        this.channelsError = channelsError;
        this.channelsTotal = channelsTotal;
        this.serverId = serverId;
        this.serverLastOnlineTime = serverLastOnlineTime;
        this.serverName = serverName;
        this.serverOnline = serverOnline;
        this.totalSamplesDropped = totalSamplesDropped;
        this.totalSamplesWritten = totalSamplesWritten;
    }

    /**
     * Returns the human-readable name of the Cassandra cluster to which this
     * server is connected. May be <code>null</code> if the server is currently
     * not connected to a Cassandra cluster. In this case, the value returned by
     * {@link #getCassandraError()} is not <code>null</code>.
     * 
     * @return name of the Cassandra cluster or <code>null</code> if the name is
     *         not available.
     */
    public String getCassandraClusterName() {
        return cassandraClusterName;
    }

    /**
     * Returns the error message indicating a problem that occurred while trying
     * to connect to the Cassandra cluster. This is <code>null</code> if there
     * is no problem.
     * 
     * @return error message describing why the connection to the Cassandra
     *         cluster cannot be established or <code>null</code> if there is no
     *         error.
     */
    public String getCassandraError() {
        return cassandraError;
    }

    /**
     * Returns the name of the Cassandra keyspace which is used by the Cassandra
     * PV Archiver. May be <code>null</code> if the server is currently not
     * connected to a Cassandra cluster. In this case, the value returned by
     * {@link #getCassandraError()} is not <code>null</code>.
     * 
     * @return name of the Cassandra keyspace or <code>null</code> if the name
     *         is not available.
     */
    public String getCassandraKeyspaceName() {
        return cassandraKeyspaceName;
    }

    /**
     * Returns the number of channels that are in the disconnected state.
     * Channels are disconnected if archiving is enabled, but the control-system
     * support cannot establish the connection. Ultimately, it depends on the
     * control-system support what constitutes the disconnected state. In
     * general, a channel is considered disconnected when the control-system
     * support cannot receive events for that channel, but considers the cause
     * of this problem to be outside of the scope of the archiving system (e.g.
     * the data source being temporarily unavailable).
     * 
     * @return number of channels in the disconnected state.
     */
    @JsonSerialize(using = ToStringSerializer.class)
    public int getChannelsDisconnected() {
        return channelsDisconnected;
    }

    /**
     * Returns the number of channels that are in the error state. A channel is
     * put into the error state if the archive server detects an internal
     * problem with this channel or if the channel’s control-system support
     * reports an error when trying to initialize the channel.
     * 
     * @return number of channels that are in the error state.
     */
    @JsonSerialize(using = ToStringSerializer.class)
    public int getChannelsError() {
        return channelsError;
    }

    /**
     * Returns the total number of channels on this server. If the server has
     * not been initialized yet (e.g. because it cannot connect to the
     * database), this number is zero, even if there are channels that belong to
     * the server.
     * 
     * @return total number of channels.
     */
    @JsonSerialize(using = ToStringSerializer.class)
    public int getChannelsTotal() {
        return channelsTotal;
    }

    /**
     * Returns the ID of the server. The server ID identifies a server within
     * the cluster.
     * 
     * @return ID of the server.
     */
    public UUID getServerId() {
        return serverId;
    }

    /**
     * Returns the last time this server successfully registered itself with the
     * cluster. The time is specified as the number of milliseconds since
     * January 1st, 1970, 00:00:00 UTC. This number might be zero if the server
     * has not successfully registered itself with the cluster since it was
     * started.
     * 
     * @return last time the server successfully registered with the cluster.
     */
    @JsonSerialize(using = ToStringSerializer.class)
    public long getServerLastOnlineTime() {
        return serverLastOnlineTime;
    }

    /**
     * Returns the human-readable name of the server. Typically, a server's name
     * is the hostname of the machine on which the server is running.
     * 
     * @return server name.
     */
    public String getServerName() {
        return serverName;
    }

    /**
     * Tells whether the server considers itself to be online. The server
     * considers itself online when a sufficient amount of time has passed since
     * successfully registering with the cluster and the renewal of the
     * registration has not failed since.
     * 
     * @return <code>true</code> if the server is currently online,
     *         <code>false</code> if it is offline.
     */
    public boolean isServerOnline() {
        return serverOnline;
    }

    /**
     * Returns the total number of samples that have been dropped (discarded) by
     * this server because they arrived to quickly and could not be written in
     * time. This counter is reset when the server is restarted.
     * 
     * @return total number of samples dropped by this server.
     */
    @JsonSerialize(using = ToStringSerializer.class)
    public long getTotalSamplesDropped() {
        return totalSamplesDropped;
    }

    /**
     * Returns the total number of samples that have been written (persisted) by
     * this server. This counter is reset when the server is restarted.
     * 
     * @return total number of samples written by this server.
     */
    @JsonSerialize(using = ToStringSerializer.class)
    public long getTotalSamplesWritten() {
        return totalSamplesWritten;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(cassandraClusterName)
                .append(cassandraError).append(cassandraKeyspaceName)
                .append(channelsDisconnected).append(channelsError)
                .append(channelsTotal).append(serverId)
                .append(serverLastOnlineTime).append(serverName)
                .append(serverOnline).append(totalSamplesDropped)
                .append(totalSamplesWritten).toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || !obj.getClass().equals(this.getClass())) {
            return false;
        }
        ServerStatusResponse other = (ServerStatusResponse) obj;
        return new EqualsBuilder()
                .append(this.cassandraClusterName, other.cassandraClusterName)
                .append(this.cassandraError, other.cassandraError)
                .append(this.cassandraKeyspaceName, other.cassandraKeyspaceName)
                .append(this.channelsDisconnected, other.channelsDisconnected)
                .append(this.channelsError, other.channelsError)
                .append(this.channelsTotal, other.channelsTotal)
                .append(this.serverId, other.serverId)
                .append(this.serverLastOnlineTime, other.serverLastOnlineTime)
                .append(this.serverName, other.serverName)
                .append(this.serverOnline, other.serverOnline)
                .append(this.totalSamplesDropped, other.totalSamplesDropped)
                .append(this.totalSamplesWritten, other.totalSamplesWritten)
                .isEquals();
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this);
    }

}
