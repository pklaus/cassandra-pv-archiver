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
 * Command for removing a channel. This command object contains all data that is
 * needed for the remove operation. Typically, it is passed to the
 * {@link ArchiveConfigurationService}, but it might also be sent over the
 * network before actually being processed.
 * 
 * @author Sebastian Marsching
 */
public final class RemoveChannelCommand extends ArchiveConfigurationCommand {

    private String channelName;
    private UUID expectedServerId;

    /**
     * Creates a "remove channel" command. If the specified
     * <code>expectedServerId</code> does not match the actual ID of the server
     * that currently owns the channel, the operation will fail.
     * 
     * @param channelName
     *            name of the channel to be removed.
     * @param expectedServerId
     *            if not <code>null</code>, the channel is only removed if it is
     *            currently registered with the specified server.
     * @throws IllegalArgumentException
     *             if <code>channelName</code> is empty.
     * @throws NullPointerException
     *             if <code>channelName</code> is <code>null</code>.
     */
    @JsonCreator
    public RemoveChannelCommand(
            @JsonProperty(value = "channelName", required = true) String channelName,
            @JsonProperty("expectedServerId") UUID expectedServerId) {
        Preconditions.checkNotNull(channelName,
                "The channelName must not be null.");
        Preconditions.checkArgument(!channelName.isEmpty(),
                "The channelName must not be empty.");
        this.channelName = channelName;
        this.expectedServerId = expectedServerId;
    }

    @Override
    public Type getCommandType() {
        return Type.REMOVE_CHANNEL;
    }

    /**
     * Returns the name of the channel that shall be removed. The value returned
     * is never <code>null</code> or the empty string.
     * 
     * @return name of the channel to be removed.
     */
    public String getChannelName() {
        return channelName;
    }

    /**
     * Returns the ID of the server which is expected to currently own the
     * channel. If <code>null</code> the channel will be removed regardless of
     * which server currently owns the channel. If not <code>null</code>, the
     * remove operation will fail if the channel is currently registered with a
     * different server than the specified one.
     * 
     * @return ID of the server that is expected to currently own the channel or
     *         <code>null</code> if the channel should be removed regardless of
     *         the server that currently owns the channel.
     */
    @JsonInclude(Include.NON_NULL)
    public UUID getExpectedServerId() {
        return expectedServerId;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(channelName)
                .append(expectedServerId).toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || !obj.getClass().equals(this.getClass())) {
            return false;
        }
        RemoveChannelCommand other = (RemoveChannelCommand) obj;
        return new EqualsBuilder().append(this.channelName, other.channelName)
                .append(this.expectedServerId, other.expectedServerId)
                .isEquals();
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this);
    }

}
