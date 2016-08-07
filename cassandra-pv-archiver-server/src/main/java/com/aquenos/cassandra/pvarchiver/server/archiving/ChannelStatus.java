/*
 * Copyright 2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.archiving;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

import com.aquenos.cassandra.pvarchiver.server.database.ChannelMetaDataDAO.ChannelConfiguration;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import com.google.common.base.Preconditions;

/**
 * Status of an archived channel. The status can be retrieved from the
 * {@link ArchivingService} through the
 * {@link ArchivingService#getChannelStatus(String)} and
 * {@link ArchivingService#getChannelStatusForAllChannels()} methods.
 * 
 * @author Sebastian Marsching
 */
public final class ChannelStatus {

    /**
     * State of an archived channel. A channel always starts in the
     * {@link #INITIALIZING} state and ends in the {@link #DESTROYED} or
     * {@link #ERROR} state.
     * 
     * @author Sebastian Marsching
     */
    public static enum State {

        /**
         * The channel has just been created and is still being initialized.
         */
        INITIALIZING,

        /**
         * The channel is operating normally.
         */
        OK,

        /**
         * The channel is disabled. This can be because it has been disabled in
         * the configuration or because the control-system support chose to
         * disable archiving based on a dynamic condition. In the latter case,
         * the channel might change back to the <code>OK</code> state without
         * user intervention.
         */
        DISABLED,

        /**
         * The channel is disconnected. This means that there is no error
         * regarding the archiving, but the channel is currently not available
         * in the backing control-system and thus no new samples are being
         * received.
         */
        DISCONNECTED,

        /**
         * The channel does not operate correctly. The origin of the error might
         * be in the archiving system itself or in the control-system-specific
         * adapter.
         */
        ERROR,

        /**
         * The channel has been destroyed. Typically, this only happens when the
         * archiving service is being shut down (e.g. because the server is
         * going offline) or if the channel is being refreshed. In the latter
         * case, the channel should soon change to the {@link #INITIALIZING}
         * state.
         */
        DESTROYED

    }

    private ChannelConfiguration channelConfiguration;
    private String errorMessage;
    private State state;
    private long totalSamplesDropped;
    private long totalSamplesSkippedBack;
    private long totalSamplesWritten;

    /**
     * Creates a channel status object.
     * 
     * @param channelConfiguration
     *            configuration of the channel.
     * @param errorMessage
     *            error message associated with this channel. This parameter may
     *            be <code>null</code>. If the <code>state</code> is not
     *            {@link State#ERROR}, it must be <code>null</code>.
     * @param state
     *            state of the channel.
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
     * @throws IllegalArgumentException
     *             if the <code>errorMessage</code> is not <code>null</code> but
     *             the <code>state</code> is not {@link State#ERROR}.
     * @throws NullPointerException
     *             if <code>channelConfiguration</code> or <code>state</code> is
     *             null.
     */
    @JsonCreator
    public ChannelStatus(
            @JsonProperty(value = "channelConfiguration", required = true) ChannelConfiguration channelConfiguration,
            @JsonProperty("errorMessage") String errorMessage,
            @JsonProperty(value = "state", required = true) State state,
            @JsonProperty("totalSamplesDropped") long totalSamplesDropped,
            @JsonProperty("totalSamplesSkippedBack") long totalSamplesSkippedBack,
            @JsonProperty("totalSamplesWritten") long totalSamplesWritten) {
        Preconditions.checkNotNull(channelConfiguration,
                "The channelConfiguration must not be null.");
        Preconditions.checkNotNull(state, "The state must not be null.");
        Preconditions
                .checkArgument(state.equals(State.ERROR)
                        || errorMessage == null,
                        "The errorMessage must be null if the state is not the ERROR state.");
        this.channelConfiguration = channelConfiguration;
        this.errorMessage = errorMessage;
        this.state = state;
        this.totalSamplesDropped = totalSamplesDropped;
        this.totalSamplesSkippedBack = totalSamplesSkippedBack;
        this.totalSamplesWritten = totalSamplesWritten;
    }

    /**
     * Returns the configuration for the channel. This is the version of the
     * configuration that is currently used by the archiving service. This
     * method never returns <code>null</code>.
     * 
     * @return configuration for the channel.
     */
    public ChannelConfiguration getChannelConfiguration() {
        return channelConfiguration;
    }

    /**
     * Returns the name of the channel. This method never returns
     * <code>null</code>.
     * 
     * @return name of the channel.
     */
    @JsonIgnore
    public String getChannelName() {
        return channelConfiguration.getChannelName();
    }

    /**
     * Returns the error message associated with the channel. If the channel is
     * not in the {@link State#ERROR} state, the error message is always
     * <code>null</code>. Even if it is in the {@link State#ERROR} state, the
     * error message is optional and might be <code>null</code>.
     * 
     * @return error message associated with the channel or <code>null</code> if
     *         no error message is available.
     */
    public String getErrorMessage() {
        return errorMessage;
    }

    /**
     * Returns the state of the channel. This method never returns
     * <code>null</code>.
     * 
     * @return state of the channel.
     */
    public State getState() {
        return state;
    }

    /**
     * Returns the total number of samples dropped for this channel. Samples are
     * dropped when the queue of samples to be written grows too large.
     * Typically, this happens when samples arrive faster than they can be
     * written to the database. This counter is reset when a channel is
     * (re-)initialized (e.g. because its configuration has changed).
     * 
     * @return total number of samples dropped for this channel.
     */
    @JsonSerialize(using = ToStringSerializer.class)
    public long getTotalSamplesDropped() {
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
     * configuration has changed).
     * 
     * @return total number of samples dropped for this channel.
     */
    @JsonSerialize(using = ToStringSerializer.class)
    public long getTotalSamplesSkippedBack() {
        return totalSamplesSkippedBack;
    }

    /**
     * Returns the total number of samples written for this channel. Samples are
     * considered written after they have been successfully persisted to the
     * database. This counter is reset when a channel is (re-)initialized (e.g.
     * because its configuration has changed).
     * 
     * @return total number of samples written for this channel.
     */
    @JsonSerialize(using = ToStringSerializer.class)
    public long getTotalSamplesWritten() {
        return totalSamplesWritten;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(channelConfiguration)
                .append(errorMessage).append(state).append(totalSamplesDropped)
                .append(totalSamplesSkippedBack).append(totalSamplesWritten)
                .toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || !this.getClass().equals(obj.getClass())) {
            return false;
        }
        ChannelStatus other = (ChannelStatus) obj;
        return new EqualsBuilder()
                .append(this.channelConfiguration, other.channelConfiguration)
                .append(this.errorMessage, other.errorMessage)
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
