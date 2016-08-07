/*
 * Copyright 2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.controlsystem.channelaccess.internal;

import java.util.Locale;

import com.aquenos.cassandra.pvarchiver.controlsystem.channelaccess.ChannelAccessArchivingChannel;
import com.aquenos.epics.jackie.client.ChannelAccessChannel;
import com.aquenos.epics.jackie.client.ChannelAccessClient;
import com.aquenos.epics.jackie.client.ChannelAccessConnectionListener;
import com.aquenos.epics.jackie.client.ChannelAccessMonitor;
import com.aquenos.epics.jackie.client.ChannelAccessMonitorListener;
import com.aquenos.epics.jackie.common.protocol.ChannelAccessEventMask;
import com.aquenos.epics.jackie.common.protocol.ChannelAccessStatus;
import com.aquenos.epics.jackie.common.value.ChannelAccessAlarmAcknowledgementStatus;
import com.aquenos.epics.jackie.common.value.ChannelAccessChar;
import com.aquenos.epics.jackie.common.value.ChannelAccessClassName;
import com.aquenos.epics.jackie.common.value.ChannelAccessDouble;
import com.aquenos.epics.jackie.common.value.ChannelAccessEnum;
import com.aquenos.epics.jackie.common.value.ChannelAccessFloat;
import com.aquenos.epics.jackie.common.value.ChannelAccessGettableValue;
import com.aquenos.epics.jackie.common.value.ChannelAccessLong;
import com.aquenos.epics.jackie.common.value.ChannelAccessShort;
import com.aquenos.epics.jackie.common.value.ChannelAccessString;

/**
 * <p>
 * Channel that enabled and disables archiving for another channel.
 * </p>
 * 
 * <p>
 * An enabling channel can be used to dynamically enable or disable archiving
 * for a channel based on the connection state and value of another channel.
 * This class provides an abstraction for the enabling channel (the channel that
 * controls whether archiving is enabled), taking care of watching the enabling
 * channel's connection state and value.
 * </p>
 * 
 * <p>
 * If the enabling channel is not connected, archiving is disabled. If the
 * enabling channel is connected, archiving is enabled depending on the enabling
 * channel's value. When the enabling channel's value is of an integer type, the
 * target channel is enabled if the enabling channel's value is non-zero. If the
 * enabling channel's value is of a floating-point type, the target channel is
 * enabled when the enabling channel's value is neither zero nor not-a-number.
 * When the enabling channel's value is of a string type, the target channel is
 * enabled when the enabling channel's value is neither the empty string, nor
 * "0", "false", "no", or "off". Leading and trailing white-space is ignored for
 * this comparison and the comparison is not case sensitive.
 * </p>
 * 
 * <p>
 * The status of the enabling channel can be queried through
 * {@link #isEnabled()} and {@link #isEnabledOnce()} methods. The
 * {@link #updateReceived()} method can be overridden in order to be notified
 * when the enabled state changes.
 * </p>
 * 
 * <p>
 * This class is only intended for use by the
 * {@link ChannelAccessArchivingChannel} class.
 * </p>
 * 
 * @author Sebastian Marsching
 */
public class EnablingChannel {

    /**
     * Connection listener for the {@link EnablingChannel}.
     * 
     * @author Sebastian Marsching
     */
    private class ConnectionListener implements ChannelAccessConnectionListener {

        @Override
        public void connectionStateChanged(ChannelAccessChannel channel,
                boolean nowConnected) {
            // We are only interested in disconnected events. Connected events
            // should also result in a monitor update, so we can decide whether
            // we switch to the enabled state based on the value. Disconnected
            // events, however, should always result in a disabled state.
            if (!nowConnected) {
                EnablingChannel.this.enabled = false;
                updateReceived();
            }
        }

    }

    /**
     * Monitor listener for the {@link EnablingChannel}.
     * 
     * @author Sebastian Marsching
     */
    private class MonitorListener implements
            ChannelAccessMonitorListener<ChannelAccessGettableValue<?>> {

        @Override
        public void monitorError(
                ChannelAccessMonitor<? extends ChannelAccessGettableValue<?>> monitor,
                ChannelAccessStatus status, String message) {
            // A monitor error can have different reasons. Such an event is most
            // likely when the channel was disconnected before, so we will
            // simply stay in the disabled state. If we are in the enabled
            // state, we could think about switching to the disabled state, but
            // this might not always be the best strategy, so we simply always
            // stay in the state that we have been in before.
        }

        @Override
        public void monitorEvent(
                ChannelAccessMonitor<? extends ChannelAccessGettableValue<?>> monitor,
                ChannelAccessGettableValue<?> value) {
            boolean enabled;
            switch (value.getType()) {
            case DBR_CHAR:
            case DBR_STS_CHAR:
            case DBR_TIME_CHAR:
            case DBR_GR_CHAR:
            case DBR_CTRL_CHAR:
                enabled = isEnabled((int) ((ChannelAccessChar) value)
                        .getValue().get());
                break;
            case DBR_DOUBLE:
            case DBR_STS_DOUBLE:
            case DBR_TIME_DOUBLE:
            case DBR_GR_DOUBLE:
            case DBR_CTRL_DOUBLE:
                enabled = isEnabled(((ChannelAccessDouble) value).getValue()
                        .get());
                break;
            case DBR_ENUM:
            case DBR_STS_ENUM:
            case DBR_TIME_ENUM:
            case DBR_GR_ENUM:
            case DBR_CTRL_ENUM:
                // If we wanted to use the enum label, we would have to make
                // sure that we receive a value of type DBR_GR_ENUM or
                // DBR_CTRL_ENUM. This would mean a lot of extra work for a
                // small benefit, so we simply treat an enum as an integer.
                enabled = isEnabled((int) ((ChannelAccessEnum) value)
                        .getValue().get());
                break;
            case DBR_FLOAT:
            case DBR_STS_FLOAT:
            case DBR_TIME_FLOAT:
            case DBR_GR_FLOAT:
            case DBR_CTRL_FLOAT:
                enabled = isEnabled((double) ((ChannelAccessFloat) value)
                        .getValue().get());
                break;
            case DBR_LONG:
            case DBR_STS_LONG:
            case DBR_TIME_LONG:
            case DBR_GR_LONG:
            case DBR_CTRL_LONG:
                enabled = isEnabled(((ChannelAccessLong) value).getValue()
                        .get());
                break;
            case DBR_SHORT:
            case DBR_STS_SHORT:
            case DBR_TIME_SHORT:
            case DBR_GR_SHORT:
            case DBR_CTRL_SHORT:
                enabled = isEnabled((int) ((ChannelAccessShort) value)
                        .getValue().get());
                break;
            case DBR_STRING:
            case DBR_STS_STRING:
            case DBR_TIME_STRING:
            case DBR_GR_STRING:
            case DBR_CTRL_STRING:
                enabled = isEnabled(((ChannelAccessString) value).getValue()
                        .get(0));
                break;
            case DBR_STSACK_STRING:
                enabled = isEnabled(((ChannelAccessAlarmAcknowledgementStatus) value)
                        .getValue().get(0));
                break;
            case DBR_CLASS_NAME:
                enabled = isEnabled(((ChannelAccessClassName) value).getValue()
                        .get(0));
                break;
            default:
                // We do not expect to receive a different type (we listed all
                // types that are valid for a monitor), but if we do due to a
                // bug in a server, we treat this as a disabled value.
                enabled = false;
                break;

            }
            EnablingChannel.this.enabled = enabled;
            EnablingChannel.this.enabledOnce = enabled;
            updateReceived();
        }

    }

    private static boolean isEnabled(double d) {
        // We treat not-a-number and zero as false, all other numbers (including
        // positive and negative infinity) as true.
        if (Double.isNaN(d) || d == 0.0) {
            return false;
        } else {
            return true;
        }
    }

    private static boolean isEnabled(int i) {
        // We treat zero as false and all non-zero values as true.
        return i != 0;
    }

    private static boolean isEnabled(String s) {
        // We treat the empty string, "0", "false", "no", and "off" as false,
        // all other strings as true. We trim the string before comparing it so
        // that whitespace is ignored. We also ignore whether the string is in
        // upper case or lower case.
        switch (s.trim().toLowerCase(Locale.ENGLISH)) {
        case "":
        case "0":
        case "false":
        case "no":
        case "off":
            return false;
        default:
            return true;
        }
    }

    private ChannelAccessChannel channel;
    private volatile boolean enabled;
    private volatile boolean enabledOnce;
    private ChannelAccessMonitor<? extends ChannelAccessGettableValue<?>> monitor;

    /**
     * Creates an enabling channel using the specified channel name.
     * 
     * @param channelName
     *            channel name of the enabling channel. The specified channel is
     *            watched for connection state and value changes.
     * @param client
     *            Channel Access client that is used for connecting to the
     *            channel.
     */
    public EnablingChannel(String channelName, ChannelAccessClient client) {
        this.channel = client.getChannel(channelName);
        this.monitor = channel.monitorNative(1,
                ChannelAccessEventMask.DBE_VALUE);
        this.channel.addConnectionListener(new ConnectionListener());
        this.monitor.addMonitorListener(new MonitorListener());

    }

    /**
     * Destroys this enabling channel. This frees the resources allocated for
     * the channel (in particular the underlying Channel Access channel and the
     * monitor).
     */
    public void destroy() {
        // Destroying a channel also destroys its monitor, so we do not have to
        // destroy the monitor explicitly.
        channel.destroy();
    }

    /**
     * Tells whether archiving should currently be enabled. Returns
     * <code>true</code> if this enabling channel is connected and the enabling
     * channel's value represents the enabled state. If the the enabling channel
     * is disconnected or has a value that represents the disabled state,
     * <code>false</code> is returned.
     * 
     * @return <code>true</code> if archiving should currently be enabled,
     *         <code>false</code> otherwise.
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Tells whether archiving is currently enabled or has been enabled at some
     * point in time. This method returns <code>true</code>, if
     * {@link #isEnabled()} currently returns <code>true</code> or has (or would
     * have) returned <code>true</code> at some earlier point in time since
     * creating this enabling channel.
     * 
     * @return <code>true</code> if archiving is enabled or has been enabled
     *         earlier, <code>false</code> if archiving has never been enabled
     *         since this enabling channel was created.
     */
    public boolean isEnabledOnce() {
        // enabledOnce is set after setting enabled, so it is possible that
        // enabledOnce is not set yet when enabled has already been set. This is
        // why we also check enabled when enabledOnce is false.
        return enabledOnce || enabled;
    }

    /**
     * Called every time that the enabling channel's connection state or value
     * changes. This does not necessarily mean that the enabled state also
     * changes, so child classes implementing this method should call
     * {@link #isEnabled()} in order to find out the current state. The base
     * class's implementation of this method is empty, so child classes do not
     * have to call the super method.
     */
    protected void updateReceived() {
        // We keep this method empty. It can be implemented by child classes
        // that want to be notified.
    }

}
