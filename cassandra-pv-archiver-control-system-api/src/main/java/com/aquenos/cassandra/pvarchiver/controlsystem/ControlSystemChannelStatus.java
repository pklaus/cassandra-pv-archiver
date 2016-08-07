/*
 * Copyright 2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.controlsystem;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

import com.google.common.base.Preconditions;

/**
 * Status of a {@link ControlSystemChannel}. Instances of this class can be
 * created through the {@link #connected()}, {@link #disconnected()}, and
 * {@link #error(Throwable)} methods.
 * 
 * @author Sebastian Marsching
 */
public final class ControlSystemChannelStatus {

    /**
     * State of a control-system channel.
     * 
     * @author Sebastian Marsching
     */
    public static enum State {

        /**
         * The channel is connected. This means that it is operating normally
         * and is receiving updates with value changes.
         */
        CONNECTED,

        /**
         * Archiving has been temporarily disabled. This means that while
         * archiving is enabled in the configuration, the control-system support
         * has decided that no data shall be archived at the moment (most likely
         * due to a dynamic condition not being met).
         */
        DISABLED,

        /**
         * The channel is disconnected. This means that due to a temporary
         * problem (e.g. the corresponding server being stopped), value updates
         * for the channel can currently not be received. However, this
         * situation will recover automatically when the underlying problem has
         * been solved.
         */
        DISCONNECTED,

        /**
         * The channel is a permanent error state. This means that a permanent
         * problem prevents the channel from operating. For example, an invalid
         * configuration parameter might result in such a problem.
         */
        ERROR

    }

    private static final ControlSystemChannelStatus CONNECTED = new ControlSystemChannelStatus(
            State.CONNECTED, null);
    private static final ControlSystemChannelStatus DISABLED = new ControlSystemChannelStatus(
            State.DISABLED, null);
    private static final ControlSystemChannelStatus DISCONNECTED = new ControlSystemChannelStatus(
            State.DISCONNECTED, null);

    private Throwable error;
    private State state;

    /**
     * Returns a status that indicates that the channel is connected. This means
     * that it is operating normally and is receiving updates with value
     * changes.
     * 
     * @return channel status representing the connected state.
     */
    public static ControlSystemChannelStatus connected() {
        return CONNECTED;
    }

    /**
     * <p>
     * Returns a status that indicates that archiving for the channel has been
     * temporarily disabled. This means that the control-system support is
     * operating correctly, but has decided that samples should currently not be
     * archived. Typically, such a decision will be made based on a dynamic
     * condition (e.g. a certain flag being set on the channel's server or the
     * state of another channel).
     * </p>
     * 
     * <p>
     * Unlike a channel that has been disabled in its configuration, a channel
     * that has been disabled by the control-system support can become active at
     * any time without user intervention.
     * </p>
     * 
     * @return channel status representing the disabled state.
     */
    public static ControlSystemChannelStatus disabled() {
        return DISABLED;
    }

    /**
     * Returns a status that indicates that the channel is disconnected. This
     * means that due to a temporary problem (e.g. the corresponding server
     * being stopped), value updates for the channel can currently not be
     * received. However, this situation will recover automatically when the
     * underlying problem has been solved.
     * 
     * @return channel status representing the disconnected state.
     */
    public static ControlSystemChannelStatus disconnected() {
        return DISCONNECTED;
    }

    /**
     * Returns a status that indicates that the channel is in the error state.
     * This means that a permanent problem prevents the channel from operating.
     * For example, an invalid configuration parameter might result in such a
     * problem. For temporary problems from which the channel can recover
     * automatically, when they are fixed, the disconnected state should be
     * preferred.
     * 
     * @param error
     *            optional error associated with the problem. May be
     *            <code>null</code>.
     * @return channel status representing the error state with the given cause
     *         (if specified).
     */
    public static ControlSystemChannelStatus error(Throwable error) {
        return new ControlSystemChannelStatus(State.ERROR, error);
    }

    private ControlSystemChannelStatus(State state, Throwable error) {
        Preconditions.checkNotNull(state, "The state must not be null.");
        Preconditions.checkArgument(state.equals(State.ERROR) || error == null,
                "The error must be null unless the ERROR state is specified.");
        this.state = state;
        this.error = error;
    }

    /**
     * Returns the error associated with the channel status. If this status does
     * not represent the {@link State#ERROR} state, this method always returns
     * null. Even if the channel is in the error state, the error might object
     * might be null.
     * 
     * @return error causing the channel's error state or <code>null</code> if
     *         this information is not available.
     */
    public Throwable getError() {
        return error;
    }

    /**
     * Returns the channel's state. This method never returns <code>null</code>.
     * 
     * @return channel state.
     */
    public State getState() {
        return state;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(error).append(state).toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (this == null || !this.getClass().equals(obj.getClass())) {
            return false;
        }
        ControlSystemChannelStatus other = (ControlSystemChannelStatus) obj;
        return new EqualsBuilder().append(this.error, other.error)
                .append(this.state, other.state).isEquals();
    }

    @Override
    public String toString() {
        return new ReflectionToStringBuilder(this).toString();
    }

}
