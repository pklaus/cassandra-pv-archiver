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
 * Command for renaming a channel. This command object contains all data that is
 * needed for the remove operation. Typically, it is passed to the
 * {@link ArchiveConfigurationService}, but it might also be sent over the
 * network before actually being processed.
 * 
 * @author Sebastian Marsching
 */
public final class RenameChannelCommand extends ArchiveConfigurationCommand {

    private UUID expectedServerId;
    private String newChannelName;
    private String oldChannelName;

    /**
     * Creates a "rename channel" command. If the specified
     * <code>expectedServerId</code> does not match the actual ID of the server
     * that currently owns the channel, the operation will fail.
     * 
     * @param expectedServerId
     *            if not <code>null</code>, the channel is only renamed if it is
     *            currently registered with the specified server.
     * @param newChannelName
     *            name under which the channel shall be known after the rename
     *            operation finishes.
     * @param oldChannelName
     *            name under which the channel is currently known.
     * @throws IllegalArgumentException
     *             if <code>newChannelName</code> or <code>oldChannelName</code>
     *             is empty.
     * @throws NullPointerException
     *             if <code>newChannelName</code> or <code>oldChannelName</code>
     *             is <code>null</code>.
     */
    @JsonCreator
    public RenameChannelCommand(
            @JsonProperty("expectedServerId") UUID expectedServerId,
            @JsonProperty(value = "newChannelName", required = true) String newChannelName,
            @JsonProperty(value = "oldChannelName", required = true) String oldChannelName) {
        Preconditions.checkNotNull(newChannelName,
                "The newChannelName must not be null.");
        Preconditions.checkArgument(!newChannelName.isEmpty(),
                "The newChannelName must not be empty.");
        Preconditions.checkNotNull(oldChannelName,
                "The oldChannelName must not be null.");
        Preconditions.checkArgument(!oldChannelName.isEmpty(),
                "The oldChannelName must not be empty.");
        this.expectedServerId = expectedServerId;
        this.newChannelName = newChannelName;
        this.oldChannelName = oldChannelName;
    }

    @Override
    public Type getCommandType() {
        return Type.RENAME_CHANNEL;
    }

    /**
     * Returns the ID of the server which is expected to currently own the
     * channel. If <code>null</code> the channel will be renamed regardless of
     * which server currently owns the channel. If not <code>null</code>, the
     * rename operation will fail if the channel is currently registered with a
     * different server than the specified one.
     * 
     * @return ID of the server that is expected to currently own the channel or
     *         <code>null</code> if the channel should be renamed regardless of
     *         the server that currently owns the channel.
     */
    @JsonInclude(Include.NON_NULL)
    public UUID getExpectedServerId() {
        return expectedServerId;
    }

    /**
     * Returns the new name for the channel to be renamed. After the rename
     * operation finishes successfully, the channel will be known under that
     * name. The value returned is never <code>null</code> or the empty string.
     * 
     * @return new name for the channel.
     */
    public String getNewChannelName() {
        return newChannelName;
    }

    /**
     * Returns the old name of the channel to be renamed. After the rename
     * operation finishes successfully, the channel will not be known under that
     * name any longer. The value returned is never <code>null</code> or the
     * empty string.
     * 
     * @return old name of the channel.
     */
    public String getOldChannelName() {
        return oldChannelName;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(expectedServerId)
                .append(newChannelName).append(oldChannelName).toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || !obj.getClass().equals(this.getClass())) {
            return false;
        }
        RenameChannelCommand other = (RenameChannelCommand) obj;
        return new EqualsBuilder()
                .append(this.expectedServerId, other.expectedServerId)
                .append(this.newChannelName, other.newChannelName)
                .append(this.oldChannelName, other.oldChannelName).isEquals();
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this);
    }

}
