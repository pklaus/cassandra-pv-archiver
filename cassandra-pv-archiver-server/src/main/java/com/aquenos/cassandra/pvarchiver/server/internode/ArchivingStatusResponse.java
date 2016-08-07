/*
 * Copyright 2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.internode;

import java.util.List;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

import com.aquenos.cassandra.pvarchiver.server.archiving.ChannelStatus;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * <p>
 * Response sent in reply to a request to get the server's archiving status.
 * </p>
 * 
 * <p>
 * This response object contains the archiving status of all the channels
 * managed by the server.
 * </p>
 * 
 * @author Sebastian Marsching
 *
 */
public final class ArchivingStatusResponse {

    private List<ChannelStatus> channelStatusList;

    /**
     * Creates an archiving-status response.
     * 
     * @param channelStatusList
     *            list of status information for the channels managed by the
     *            server. The channels are ordered by the natural order of their
     *            names.
     * @throws NullPointerException
     *             if <code>channelStatusList</code> is <code>null</code> or
     *             contains <code>null</code> elements.
     */
    @JsonCreator
    public ArchivingStatusResponse(
            @JsonProperty(value = "channelStatusList", required = true) List<ChannelStatus> channelStatusList) {
        Preconditions.checkNotNull(channelStatusList,
                "The channelStatusList must not be null.");
        // copyOf(...) throws a NullPointerException if there are null elements,
        // therefore we do not have to check explicitly.
        this.channelStatusList = ImmutableList.copyOf(channelStatusList);
    }

    public List<ChannelStatus> getChannelStatusList() {
        return channelStatusList;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(channelStatusList).toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || !this.getClass().equals(obj.getClass())) {
            return false;
        }
        ArchivingStatusResponse other = (ArchivingStatusResponse) obj;
        return new EqualsBuilder().append(this.channelStatusList,
                other.channelStatusList).isEquals();
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this);
    }

}
