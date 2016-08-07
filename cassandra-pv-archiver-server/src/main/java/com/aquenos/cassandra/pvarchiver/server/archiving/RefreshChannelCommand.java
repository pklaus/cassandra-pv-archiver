/*
 * Copyright 2015-2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.archiving;

import java.util.UUID;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

/**
 * <p>
 * Command for refreshing a channel. Refreshing a channel results in the channel
 * being shutdown temporarily and being restarted with its configuration
 * reloaded from the database. If a channel has been newly added, it results in
 * the channel being started for the first time. If a channel has been removed,
 * it results in the channel being removed from the active server configuration.
 * If a pending operation has been registered for the channel, the channel is
 * temporarily put into an error state until the pending operation is removed
 * and the channel is refreshed again.
 * </p>
 * 
 * <p>
 * In contrast to most other archive configuration commands, this command is not
 * forwarded to the server that currently owns the channel. Instead, it is
 * always executed by the server specified in the command.
 * </p>
 * 
 * <p>
 * This command object contains all data that is needed for the move operation.
 * Typically, it is passed to the {@link ArchiveConfigurationService}, but it
 * might also be sent over the network before actually being processed.
 * </p>
 * 
 * @author Sebastian Marsching
 */
public final class RefreshChannelCommand extends ArchiveConfigurationCommand {

    private String channelName;
    private UUID serverId;

    /**
     * Creates a "refresh channel" command. A refresh operation always succeeds
     * unless there is a communication problem that prevents the command from
     * being actually sent to the target server.
     * 
     * @param channelName
     *            name of the channel to be refreshed.
     * @param serverId
     *            ID of the server on which the channel shall be refreshed. This
     *            does not have to be the ID of the server that currently owns
     *            the channel. For example, after moving a channel, it should
     *            also be refreshed on the old server so that it gets removed
     *            from that server's configuration.
     * @throws IllegalArgumentException
     *             if <code>channelName</code> is empty.
     * @throws NullPointerException
     *             if <code>channelName</code> or <code>serverId</code> is
     *             <code>null</code>.
     */
    @JsonCreator
    public RefreshChannelCommand(
            @JsonProperty(value = "channelName", required = true) String channelName,
            @JsonProperty(value = "serverId", required = true) UUID serverId) {
        Preconditions.checkNotNull(channelName,
                "The channelName must not be null.");
        Preconditions.checkArgument(!channelName.isEmpty(),
                "The channelName must not be empty.");
        Preconditions.checkNotNull(serverId, "The serverId must not be null.");
        this.channelName = channelName;
        this.serverId = serverId;
    }

    @Override
    public Type getCommandType() {
        return Type.REFRESH_CHANNEL;
    }

    /**
     * Returns the name of the channel that shall be refreshed. The value
     * returned is never <code>null</code> or the empty string.
     * 
     * @return name of the channel to be refreshed.
     */
    public String getChannelName() {
        return channelName;
    }

    /**
     * Returns the ID of the server on which the channel shall be refreshed. The
     * returned is never <code>null</code>.
     * 
     * @return ID of the server on which the channel shall be refreshed.
     */
    public UUID getServerId() {
        return serverId;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(channelName).append(serverId)
                .toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || !obj.getClass().equals(this.getClass())) {
            return false;
        }
        RefreshChannelCommand other = (RefreshChannelCommand) obj;
        return new EqualsBuilder().append(this.channelName, other.channelName)
                .append(this.serverId, other.serverId).isEquals();
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this);
    }

}
