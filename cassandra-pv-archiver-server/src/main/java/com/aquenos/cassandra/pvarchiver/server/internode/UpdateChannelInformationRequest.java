/*
 * Copyright 2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.internode;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

import com.aquenos.cassandra.pvarchiver.server.archiving.ChannelInformationCache;
import com.aquenos.cassandra.pvarchiver.server.database.ChannelMetaDataDAO.ChannelInformation;
import com.aquenos.cassandra.pvarchiver.server.web.internode.controller.ApiController;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * <p>
 * Request that shall be processed by the
 * {@link ChannelInformationCache#processUpdate(List, List, Iterable, long)}
 * method. Such requests are processed by the {@link ApiController} and
 * forwarded to the {@link ChannelInformationCache}.
 * </p>
 * 
 * @author Sebastian Marsching
 */
public final class UpdateChannelInformationRequest {

    private List<ChannelInformation> channelInformationUpdates;
    private List<UUID> forwardToServers;
    private List<String> missingChannels;
    private long timeStamp;

    /**
     * Creates a request.
     * 
     * @param channelInformationUpdates
     *            updated channel-information objects.
     * @param forwardToServers
     *            list of servers which should be notified with the information
     *            from the update or <code>null</code> if an empty list shall be
     *            used.
     * @param missingChannels
     *            channels that are missing now (the channel information is
     *            <code>null</code> - typically because the channel has been
     *            deleted).
     * @param timeStamp
     *            time stamp (as returned by {@link System#currentTimeMillis()}
     *            of the point in time before the channel information that is
     *            sent with this update was retrieved from the database. This
     *            information is used when resolving conflicting updates for the
     *            same channel.
     * @throws NullPointerException
     *             if <code>channelInformationUpdates</code> or
     *             <code>missingChannels</code> is <code>null</code> or
     *             <code>channelInformationUpdates</code>,
     *             <code>forwardToServers</code>, or
     *             <code>missingChannels</code> contains <code>null</code>
     *             elements.
     */
    @JsonCreator
    public UpdateChannelInformationRequest(
            @JsonProperty(value = "channelInformationUpdates", required = true) List<ChannelInformation> channelInformationUpdates,
            @JsonProperty(value = "forwardToServers") List<UUID> forwardToServers,
            @JsonProperty(value = "missingChannels", required = true) List<String> missingChannels,
            @JsonProperty(value = "timeStamp", required = true) long timeStamp) {
        Preconditions.checkNotNull(channelInformationUpdates,
                "The channelInformationUpdates list must not be null.");
        Preconditions.checkNotNull(missingChannels,
                "The missingChannels list must not be null.");
        // We do not have to test the lists for null elements, because
        // copyOf(..) will throw a NullPointerException if there are null
        // elements.
        channelInformationUpdates = ImmutableList
                .copyOf(channelInformationUpdates);
        forwardToServers = forwardToServers == null ? Collections
                .<UUID> emptyList() : ImmutableList.copyOf(forwardToServers);
        missingChannels = ImmutableList.copyOf(missingChannels);
        this.channelInformationUpdates = channelInformationUpdates;
        this.forwardToServers = forwardToServers;
        this.missingChannels = missingChannels;
        this.timeStamp = timeStamp;
    }

    /**
     * Returns the list of channel information objects that are updated with
     * this update. The list is never <code>null</code> and does never contain
     * <code>null</code> elements. However, it might be empty.
     * 
     * @return channel information objects that contain the updated information.
     */
    public List<ChannelInformation> getChannelInformationUpdates() {
        return channelInformationUpdates;
    }

    /**
     * Returns the list of servers to which the server receiving this request
     * should forward it. The list is never <code>null</code> and does never
     * contain <code>null</code> elements. However, it might be empty.
     * 
     * @return IDs of the servers to which the receiving server should forward
     *         this request.
     */
    public List<UUID> getForwardToServers() {
        return forwardToServers;
    }

    /**
     * <p>
     * Returns the list of channels which are missing. A channel is considered
     * missing if a query for its channel information resulted in a
     * <code>null</code> value. Typically, this means that the channel has been
     * deleted. A channel is included in this list when it existed earlier (or
     * was at least expected to exist) and now seems to not exist any longer.
     * </p>
     * 
     * <p>
     * The list is never <code>null</code> and does never contain
     * <code>null</code> elements. However, it might be empty.
     * </p>
     * 
     * @return names of missing channels.
     */
    public List<String> getMissingChannels() {
        return missingChannels;
    }

    @JsonSerialize(using = ToStringSerializer.class)
    public long getTimeStamp() {
        return timeStamp;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(channelInformationUpdates)
                .append(forwardToServers).append(missingChannels)
                .append(timeStamp).toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || !obj.getClass().equals(this.getClass())) {
            return false;
        }
        UpdateChannelInformationRequest other = (UpdateChannelInformationRequest) obj;
        return new EqualsBuilder()
                .append(this.channelInformationUpdates,
                        other.channelInformationUpdates)
                .append(this.forwardToServers, other.forwardToServers)
                .append(this.missingChannels, other.missingChannels)
                .append(this.timeStamp, other.timeStamp).isEquals();
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this);
    }

}
