/*
 * Copyright 2017 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.web.admin.controller.wsapi;

import java.util.Map;
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
 * Response object for the "get channel" function of the web-service API.
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
public class ChannelResponse {

    private final UUID channelDataId;
    private final String channelName;
    private final String controlSystemName;
    private final String controlSystemType;
    private final Map<Integer, Integer> decimationLevelToRetentionPeriod;
    private final boolean enabled;
    private final String errorMessage;
    private final Map<String, String> options;
    private final UUID serverId;
    private final String serverName;
    private final String state;
    private final Long totalSamplesDropped;
    private final Long totalSamplesSkippedBack;
    private final Long totalSamplesWritten;

    /**
     * Creates a channel response. This constructor does not verify the validity
     * of the arguments and simply uses them as-is.
     * 
     * @param channelDataId
     *            unique identifier associated with the data (samples) for each
     *            channel. This identifier must be different for each channel.
     *            It is used instead of the channel name when storing data, so
     *            that the channel can be renamed later without losing data.
     * @param channelName
     *            name of this channel. The name is a textual identifier that
     *            uniquely identifies the channel within the archive.
     * @param controlSystemName
     *            human-readable name of the control-system used for this
     *            channel.
     * @param controlSystemType
     *            control-system type of this channel. The control-system type
     *            defines the control-system that provides new data for the
     *            channel.
     * @param decimationLevelToRetentionPeriod
     *            map that contains the retention periods of the decimation
     *            levels for the channel. The decimation levels are used as keys
     *            and the corresponding retention periods (in seconds) are
     *            stored as values. A retention period that is zero or negative
     *            means that samples for the corresponding decimation level are
     *            supposed to be retained forever.
     * @param enabled
     *            <code>true</code> if archiving is enabled and the server
     *            should archive new samples received for the channel.
     *            <code>false</code> if archiving is disabled and the server
     *            should not archive new samples for the channel but just use
     *            the samples that have been archived previously.
     * @param errorMessage
     *            error message associated with this channel. This parameter may
     *            be <code>null</code>. If the <code>state</code> is not
     *            <code>ERROR</code>, it must be <code>null</code>.
     * @param options
     *            map storing the configuration options for this channel. The
     *            configuration options are passed on to the control-system
     *            specific adapter. The meaning of the options depends on this
     *            control-system specific code. The map should not contain
     *            <code>null</code> keys or values.
     * @param serverId
     *            ID of the server that is responsible for this channel.
     * @param serverName
     *            human-readable name of the server that is responsible for this
     *            channel. Typically, this is the hostname of the server.
     * @param state
     *            state of the channel. One of the strings
     *            <code>destroyed</code>, <code>disabled</code>,
     *            <code>disconnected</code>, <code>error</code>
     *            <code>initializing</code>, or <code>ok</code>.
     * @param totalSamplesDropped
     *            total number of samples that have been dropped (discarded) for
     *            the channel because they arrived to quickly and could not be
     *            written in time. This counter is reset when a channel is
     *            (re-)initialized (e.g. because its configuration has changed).
     * @param totalSamplesSkippedBack
     *            total number of samples that have been dropped (discarded)
     *            because their time-stamp was less than or equal to a sample
     *            that was written earlier. This counter is reset when a channel
     *            is (re-)initialized (e.g. because its configuration has
     *            changed).
     * @param totalSamplesWritten
     *            total number of samples that have been written (persisted) for
     *            this channel. This counter is reset when a channel is
     *            (re-)initialized (e.g. because its configuration has changed).
     */
    @JsonCreator
    public ChannelResponse(@JsonProperty("channelDataId") UUID channelDataId,
            @JsonProperty("channelName") String channelName,
            @JsonProperty("controlSystemName") String controlSystemName,
            @JsonProperty("controlSystemType") String controlSystemType,
            @JsonProperty("decimationLevelToRetentionPeriod") Map<Integer, Integer> decimationLevelToRetentionPeriod,
            @JsonProperty("enabled") boolean enabled,
            @JsonProperty("errorMessage") String errorMessage,
            @JsonProperty("options") Map<String, String> options,
            @JsonProperty("serverId") UUID serverId,
            @JsonProperty("serverName") String serverName,
            @JsonProperty("state") String state,
            @JsonProperty("totalSamplesDropped") Long totalSamplesDropped,
            @JsonProperty("totalSamplesSkippedBack") Long totalSamplesSkippedBack,
            @JsonProperty("totalSamplesWritten") Long totalSamplesWritten) {
        this.channelDataId = channelDataId;
        this.channelName = channelName;
        this.controlSystemName = controlSystemName;
        this.controlSystemType = controlSystemType;
        this.decimationLevelToRetentionPeriod = decimationLevelToRetentionPeriod;
        this.enabled = enabled;
        this.errorMessage = errorMessage;
        this.options = options;
        this.serverId = serverId;
        this.serverName = serverName;
        this.state = state;
        this.totalSamplesDropped = totalSamplesDropped;
        this.totalSamplesSkippedBack = totalSamplesSkippedBack;
        this.totalSamplesWritten = totalSamplesWritten;
    }

    /**
     * Returns the unique identifier that is associated with the data (samples)
     * for the channel. While a channel's name might change due to renaming, the
     * data ID will be permanent (until the channel is deleted), so even after
     * renaming a channel, its associated data can still be found.
     * 
     * @return unique identifier associated with the channel's data.
     */
    public UUID getChannelDataId() {
        return channelDataId;
    }

    /**
     * Returns the name of this channel. The name is a textual identifier that
     * uniquely identifies the channel within the archive.
     * 
     * @return channel name.
     */
    public String getChannelName() {
        return channelName;
    }

    /**
     * Returns the name of the control-system support for this channel. The name
     * is a human readable identifier for the control-system support.
     * 
     * @return name of the control-system support for this channel.
     */
    public String getControlSystemName() {
        return controlSystemName;
    }

    /**
     * Returns the control-system type of this channel. The control-system type
     * defines the control-system that provides new data for the channel.
     * 
     * @return control-system type of the channel.
     */
    public String getControlSystemType() {
        return controlSystemType;
    }

    /**
     * Returns a map that contains the retention periods of the decimation
     * levels for the channel. The decimation levels are as used keys and the
     * corresponding retention periods (in seconds) are stored as values. A
     * retention period that is zero or negative means that samples for the
     * corresponding decimation level are supposed to be retained forever.
     * 
     * @return map mapping decimation levels to their retention periods.
     */
    @JsonSerialize(contentUsing = ToStringSerializer.class)
    public Map<Integer, Integer> getDecimationLevelToRetentionPeriod() {
        return decimationLevelToRetentionPeriod;
    }

    /**
     * Tells whether archiving is enabled for this channel. If
     * <code>true</code>, archiving is enabled and the server should archive new
     * samples received for the channel. If <code>false</code>, archiving is
     * disabled and the server should not archive new samples for the channel
     * but just use the samples that have been archived previously.
     * 
     * @return <code>true</code> if archiving is enabled for this channel,
     *         <code>false</code> otherwise.
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Returns the error message associated with the channel. If the channel is
     * not in the <code>ERROR</code> state, the error message is always
     * <code>null</code>. Even if it is in the <code>ERROR</code> state, the
     * error message is optional and might be <code>null</code>.
     * 
     * @return error message associated with the channel or <code>null</code> if
     *         no error message is available.
     */
    public String getErrorMessage() {
        return errorMessage;
    }

    /**
     * Returns a map storing the configuration options for this channel. The
     * configuration options are passed on to the control-system specific
     * adapter. The meaning of the options depends on this control-system
     * specific code. The map does not contain <code>null</code> keys or values.
     * 
     * @return map storing the configuration options for this channel.
     */
    public Map<String, String> getOptions() {
        return options;
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
     * Typically, a server's name is the hostname of the machine on which the
     * server is running.
     * 
     * @return name of the server owning this channel.
     */
    public String getServerName() {
        return serverName;
    }

    /**
     * Returns the state of the channel. The state is identified by one of the
     * strings <code>destroyed</code>, <code>disabled</code>,
     * <code>disconnected</code>, <code>error</code> <code>initializing</code>,
     * or <code>ok</code>. If status information is not available (e.g. because
     * the responsible server is offline), <code>null</code> is returned.
     * 
     * @return state of the channel or <code>null</code> if status information
     *         is not available.
     */
    public String getState() {
        return state;
    }

    /**
     * Returns the total number of samples dropped for this channel. Samples are
     * dropped when the queue of samples to be written grows too large.
     * Typically, this happens when samples arrive faster than they can be
     * written to the database. This counter is reset when a channel is
     * (re-)initialized (e.g. because its configuration has changed). If status
     * information is not available (e.g. because the responsible server is
     * offline), <code>null</code> is returned.
     * 
     * @return total number of samples dropped for this channel or
     *         <code>null</code> if status information is not available.
     */
    @JsonSerialize(using = ToStringSerializer.class)
    public Long getTotalSamplesDropped() {
        return totalSamplesDropped;
    }

    /**
     * Returns the total number of samples that were discarded because they
     * skipped back in time. Samples that have a time stamp that is less than or
     * equal to the time stamp of a sample that haws already been written are
     * considered to skip back in time. This might happen when a clock is not
     * stable and skips back in time. However, it might also happen when a
     * channel is disconnected and reconnects and the sample sent by the server
     * is the same one that was already sent at an earlier point in time. This
     * counter is reset when a channel is (re-)initialized (e.g. because its
     * configuration has changed). If status information is not available (e.g.
     * because the responsible server is offline), <code>null</code> is
     * returned.
     * 
     * @return total number of samples dropped for this channel or
     *         <code>null</code> if status information is not available.
     */
    @JsonSerialize(using = ToStringSerializer.class)
    public Long getTotalSamplesSkippedBack() {
        return totalSamplesSkippedBack;
    }

    /**
     * Returns the total number of samples written for this channel. Samples are
     * considered written after they have been successfully persisted to the
     * database. This counter is reset when a channel is (re-)initialized (e.g.
     * because its configuration has changed). If status information is not
     * available (e.g. because the responsible server is offline),
     * <code>null</code> is returned.
     * 
     * @return total number of samples written for this channel or
     *         <code>null</code> if status information is not available.
     */
    @JsonSerialize(using = ToStringSerializer.class)
    public Long getTotalSamplesWritten() {
        return totalSamplesWritten;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(channelDataId).append(channelName)
                .append(controlSystemName).append(controlSystemType)
                .append(decimationLevelToRetentionPeriod).append(enabled)
                .append(errorMessage).append(options).append(state)
                .append(totalSamplesDropped).append(totalSamplesSkippedBack)
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
        ChannelResponse other = (ChannelResponse) obj;
        return new EqualsBuilder()
                .append(this.channelDataId, other.channelDataId)
                .append(this.channelName, other.channelName)
                .append(this.controlSystemName, other.controlSystemName)
                .append(this.controlSystemType, other.controlSystemType)
                .append(this.decimationLevelToRetentionPeriod,
                        other.decimationLevelToRetentionPeriod)
                .append(this.enabled, other.enabled)
                .append(this.errorMessage, other.errorMessage)
                .append(this.options, other.options)
                .append(this.state, other.state)
                .append(this.totalSamplesDropped, other.totalSamplesDropped)
                .append(this.totalSamplesSkippedBack,
                        other.totalSamplesSkippedBack)
                .append(this.totalSamplesWritten, other.totalSamplesWritten)
                .isEquals();
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this);
    }

}
