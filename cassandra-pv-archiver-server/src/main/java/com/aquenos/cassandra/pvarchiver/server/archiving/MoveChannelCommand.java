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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.google.common.base.Preconditions;

/**
 * Command for moving a channel from one server to another one. This command
 * object contains all data that is needed for the move operation. Typically, it
 * is passed to the {@link ArchiveConfigurationService}, but it might also be
 * sent over the network before actually being processed.
 * 
 * @author Sebastian Marsching
 */
public final class MoveChannelCommand extends ArchiveConfigurationCommand {

    private String channelName;
    private UUID newServerId;
    private UUID expectedOldServerId;

    /**
     * Creates a "move channel" command. If the specified
     * <code>expectedOldServerId</code> does not match the actual ID of the
     * server that currently owns the channel, the operation will fail.
     * 
     * @param channelName
     *            name of the channel to be moved.
     * @param expectedOldServerId
     *            if not <code>null</code>, the channel is only moved if it is
     *            currently registered with the specified server.
     * @param newServerId
     *            ID of the server to which the channel shall be moved.
     * @throws IllegalArgumentException
     *             if <code>channelName</code> is empty.
     * @throws NullPointerException
     *             if <code>channelName</code> or <code>newServerId</code> is
     *             <code>null</code>.
     */
    @JsonCreator
    public MoveChannelCommand(
            @JsonProperty(value = "channelName", required = true) String channelName,
            @JsonProperty("expectedOldServerId") UUID expectedOldServerId,
            @JsonProperty(value = "newServerId", required = true) UUID newServerId) {
        Preconditions.checkNotNull(channelName,
                "The channelName must not be null.");
        Preconditions.checkArgument(!channelName.isEmpty(),
                "The channelName must not be empty.");
        Preconditions.checkNotNull(newServerId,
                "The newServerId must not be null.");
        this.channelName = channelName;
        this.expectedOldServerId = expectedOldServerId;
        this.newServerId = newServerId;
    }

    @Override
    public Type getCommandType() {
        return Type.MOVE_CHANNEL;
    }

    /**
     * Returns the name of the channel that shall be moved. The value returned
     * is never <code>null</code> or the empty string.
     * 
     * @return name of the channel to be moved.
     */
    public String getChannelName() {
        return channelName;
    }

    /**
     * Returns the ID of the server to which the channel shall be moved. The
     * value returned is never <code>null</code>.
     * 
     * @return ID of the server to which the channel shall be moved.
     */
    public UUID getNewServerId() {
        return newServerId;
    }

    /**
     * Returns the ID of the server which is expected to currently own the
     * channel. If <code>null</code> the channel will be moved to the new server
     * regardless of which server currently owns the channel. If not
     * <code>null</code>, the move operation will fail if the channel is
     * currently registered with a different server than the specified one.
     * 
     * @return ID of the server that is expected to currently own the channel or
     *         <code>null</code> if the channel should be moved regardless of
     *         the server that currently owns the channel.
     */
    @JsonInclude(Include.NON_NULL)
    public UUID getExpectedOldServerId() {
        return expectedOldServerId;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(channelName)
                .append(expectedOldServerId).append(newServerId).toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || !obj.getClass().equals(this.getClass())) {
            return false;
        }
        MoveChannelCommand other = (MoveChannelCommand) obj;
        return new EqualsBuilder().append(this.channelName, other.channelName)
                .append(this.expectedOldServerId, other.expectedOldServerId)
                .append(this.newServerId, other.newServerId).isEquals();
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this);
    }

}
