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
import java.util.Set;
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
 * Response object for the "list all channels" function of the web-service API.
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
public class ChannelsAllResponse {

    /**
     * <p>
     * Individual channel item in the {@link ChannelsAllResponse}.
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
    public static class ChannelItem {

        private UUID channelDataId;
        private String channelName;
        private String controlSystemName;
        private String controlSystemType;
        private Set<Integer> decimationLevels;
        private UUID serverId;
        private String serverName;

        /**
         * Creates a channel item. This constructor does not verify the validity
         * of the arguments and simply uses them as-is.
         * 
         * @param channelDataId
         *            unique identifier associated with the data (samples) for
         *            each channel. This identifier must be different for each
         *            channel. It is used instead of the channel name when
         *            storing data, so that the channel can be renamed later
         *            without losing data.
         * @param channelName
         *            name of this channel. The name is a textual identifier
         *            that uniquely identifies the channel within the archive.
         * @param controlSystemName
         *            human-readable name of the control-system used for this
         *            channel.
         * @param controlSystemType
         *            control-system type of this channel. The control-system
         *            type defines the control-system that provides new data for
         *            the channel.
         * @param decimationLevels
         *            set of decimation levels defined for this channel.
         *            Typically, a channel at least has raw data (decimation
         *            level zero) associated with it. A decimation level is
         *            identified by the number of seconds between two samples.
         *            All decimation levels except the zero decimation level,
         *            which stores raw data, have fixed periods between samples.
         * @param serverId
         *            ID of the server that is responsible for this channel.
         * @param serverName
         *            human-readable name of the server that is responsible for
         *            this channel. Typically, this is the hostname of the
         *            server.
         */
        @JsonCreator
        public ChannelItem(@JsonProperty("channelDataId") UUID channelDataId,
                @JsonProperty("channelName") String channelName,
                @JsonProperty("controlSystemName") String controlSystemName,
                @JsonProperty("controlSystemType") String controlSystemType,
                @JsonProperty("decimationLevels") Set<Integer> decimationLevels,
                @JsonProperty("serverId") UUID serverId,
                @JsonProperty("serverName") String serverName) {
            this.channelDataId = channelDataId;
            this.channelName = channelName;
            this.controlSystemName = controlSystemName;
            this.controlSystemType = controlSystemType;
            this.decimationLevels = decimationLevels;
            this.serverId = serverId;
            this.serverName = serverName;
        }

        /**
         * Returns the unique identifier that is associated with the data
         * (samples) for the channel. While a channel's name might change due to
         * renaming, the data ID will be permanent (until the channel is
         * deleted), so even after renaming a channel, its associated data can
         * still be found.
         * 
         * @return unique identifier associated with the channel's data.
         */
        public UUID getChannelDataId() {
            return channelDataId;
        }

        /**
         * Returns the name of this channel. The name is a textual identifier
         * that uniquely identifies the channel within the archive.
         * 
         * @return channel name.
         */
        public String getChannelName() {
            return channelName;
        }

        /**
         * Returns the name of the control-system support for this channel. The
         * name is a human readable identifier for the control-system support.
         * 
         * @return name of the control-system support for this channel.
         */
        public String getControlSystemName() {
            return controlSystemName;
        }

        /**
         * Returns the control-system type of this channel. The control-system
         * type defines the control-system that provides new data for the
         * channel.
         * 
         * @return control-system type of the channel.
         */
        public String getControlSystemType() {
            return controlSystemType;
        }

        /**
         * Returns the set of decimation levels defined for this channel.
         * Typically, a channel at least has raw data (decimation level zero)
         * associated with it. A decimation level is identified by the number of
         * seconds between two samples. All decimation levels except the zero
         * decimation level, which stores raw data, have fixed periods between
         * samples.
         * 
         * @return set of decimation levels defined for the channel.
         */
        @JsonSerialize(contentUsing = ToStringSerializer.class)
        public Set<Integer> getDecimationLevels() {
            return decimationLevels;
        }

        /**
         * Returns the ID of the server that is responsible for this channel.
         * 
         * @return ID of the server owning this channel.
         */
        public UUID getServerId() {
            return serverId;
        }

        /**
         * Returns the name of the server that is responsible for this channel.
         * Typically, a server's name is the hostname of the machine on which
         * the server is running.
         * 
         * @return name of the server owning this channel.
         */
        public String getServerName() {
            return serverName;
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder().append(channelDataId)
                    .append(channelName).append(controlSystemName)
                    .append(controlSystemType).append(decimationLevels)
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
            ChannelItem other = (ChannelItem) obj;
            return new EqualsBuilder()
                    .append(this.channelDataId, other.channelDataId)
                    .append(this.channelName, other.channelName)
                    .append(this.controlSystemName, other.controlSystemName)
                    .append(this.controlSystemType, other.controlSystemType)
                    .append(this.decimationLevels, other.decimationLevels)
                    .append(this.serverId, other.serverId)
                    .append(this.serverName, other.serverName).isEquals();
        }

        @Override
        public String toString() {
            return ReflectionToStringBuilder.toString(this);
        }

    }

    private List<ChannelItem> channels;

    /**
     * Creates a new response. The response simply wraps the list of channels.
     * This constructor does not verify the validity of the arguments and simply
     * uses them as-is.
     * 
     * @param channels
     *            information about all channels that exist in the cluster.
     */
    @JsonCreator
    public ChannelsAllResponse(
            @JsonProperty(value = "channels") List<ChannelItem> channels) {
        this.channels = channels;
    }

    /**
     * Returns the list of all channels that exist in the archiving cluster.
     * 
     * @return list of all channels.
     */
    public List<ChannelItem> getChannels() {
        return channels;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(channels).toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || !obj.getClass().equals(this.getClass())) {
            return false;
        }
        ChannelsAllResponse other = (ChannelsAllResponse) obj;
        return new EqualsBuilder().append(this.channels, other.channels)
                .isEquals();
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this);
    }

}
