/*
 * Copyright 2015-2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.controlsystem.channelaccess;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang3.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aquenos.cassandra.pvarchiver.controlsystem.ControlSystemChannel;
import com.aquenos.cassandra.pvarchiver.controlsystem.ControlSystemChannelStatus;
import com.aquenos.cassandra.pvarchiver.controlsystem.SampleListener;
import com.aquenos.cassandra.pvarchiver.controlsystem.channelaccess.internal.ChannelAccessDisabledSample;
import com.aquenos.cassandra.pvarchiver.controlsystem.channelaccess.internal.ChannelAccessDisconnectedSample;
import com.aquenos.cassandra.pvarchiver.controlsystem.channelaccess.internal.ChannelAccessRawSample;
import com.aquenos.cassandra.pvarchiver.controlsystem.channelaccess.internal.ChannelAccessSampleValueAccess;
import com.aquenos.cassandra.pvarchiver.controlsystem.channelaccess.internal.ConfigurationOptions;
import com.aquenos.cassandra.pvarchiver.controlsystem.channelaccess.internal.EnablingChannel;
import com.aquenos.cassandra.pvarchiver.controlsystem.channelaccess.internal.LimitingSampleWriterDelegate;
import com.aquenos.cassandra.pvarchiver.controlsystem.channelaccess.internal.SampleSizeEstimator;
import com.aquenos.cassandra.pvarchiver.controlsystem.channelaccess.internal.SampleWriterDelegate;
import com.aquenos.cassandra.pvarchiver.controlsystem.channelaccess.internal.SimpleSampleWriterDelegate;
import com.aquenos.epics.jackie.client.ChannelAccessChannel;
import com.aquenos.epics.jackie.client.ChannelAccessClient;
import com.aquenos.epics.jackie.client.ChannelAccessConnectionListener;
import com.aquenos.epics.jackie.client.ChannelAccessMonitor;
import com.aquenos.epics.jackie.client.ChannelAccessMonitorListener;
import com.aquenos.epics.jackie.common.protocol.ChannelAccessStatus;
import com.aquenos.epics.jackie.common.value.ChannelAccessControlsValue;
import com.aquenos.epics.jackie.common.value.ChannelAccessGettableValue;
import com.aquenos.epics.jackie.common.value.ChannelAccessTimeValue;
import com.aquenos.epics.jackie.common.value.ChannelAccessValueType;

/**
 * Channel implementation for the {@link ChannelAccessControlSystemSupport}.
 */
public class ChannelAccessArchivingChannel implements ControlSystemChannel {

    /**
     * Monitor listener that listens for meta-data changes. When a meta-data
     * change is detected, the corresponding value is saved so that the value's
     * meta-data can be used the next time a value with a time stamp is received
     * by the other monitor. When this monitor listener receives its first
     * value, it registers a {@link TimeMonitorListener} with the monitor that
     * monitors value and alarm-state changes and receives samples with time
     * stamps. This ordering ensures that the lister for that monitor can always
     * rely on the channel's meta data being available.
     * 
     * @author Sebastian Marsching
     */
    private class ControlsMonitorListener implements
            ChannelAccessMonitorListener<ChannelAccessGettableValue<?>> {

        @Override
        public void monitorError(
                ChannelAccessMonitor<? extends ChannelAccessGettableValue<?>> monitor,
                ChannelAccessStatus status, String message) {
            boolean writeDisconnectedSample;
            for (;;) {
                InternalState oldState = internalState.get();
                if (oldState == null
                        || oldState.controlsMonitorListener != this) {
                    // This listener is not responsible any longer, so it should
                    // ignore all events that it receives.
                    return;
                }
                // When disconnected samples should be written and the last
                // sample was not a disconnected sample, we should write a
                // disconnected sample now. This can happen if we did not have a
                // monitor earlier because the channel was disabled and it now
                // got enabled but the monitor registration failed.
                writeDisconnectedSample = configurationOptions
                        .isWriteSampleWhenDisconnected()
                        && !oldState.lastSampleType
                                .equals(SampleType.DISCONNECTED);
                if (oldState.controlsMonitorOk || writeDisconnectedSample) {
                    // We have to update the state when the monitor OK flag
                    // changed and when we have to change the last sample type.
                    InternalState newState = new InternalState(oldState);
                    newState.controlsMonitorOk = false;
                    if (writeDisconnectedSample) {
                        newState.lastSampleType = SampleType.DISCONNECTED;
                    }
                    // We replace the state, but only if no other thread has
                    // changed it in the meantime.
                    if (!internalState.compareAndSet(oldState, newState)) {
                        continue;
                    }
                }
                break;
            }
            // We write the sample after the loop so that it is not written
            // multiple times when the first attempt to change the state fails.
            if (writeDisconnectedSample) {
                writeDisconnectedSample();
            }
        }

        @Override
        public void monitorEvent(
                ChannelAccessMonitor<? extends ChannelAccessGettableValue<?>> monitor,
                ChannelAccessGettableValue<?> value) {
            InternalState newState;
            for (;;) {
                InternalState oldState = internalState.get();
                if (oldState == null
                        || oldState.controlsMonitorListener != this) {
                    // This listener is not responsible any longer, so it should
                    // ignore all events that it receives.
                    return;
                }
                newState = new InternalState(oldState);
                newState.controlsMonitorOk = true;
                // This cast is safe because we requested a controls type.
                newState.lastControlsValue = (ChannelAccessControlsValue<?>) value;
                // Now that we have our first controls value, we can also
                // register the listener for the time monitor. Here, we only
                // create it. We register it after we have updated the state so
                // that it can access the last controls value when it gets run
                // for the first time.
                if (newState.timeMonitorListener == null) {
                    newState.timeMonitorListener = new TimeMonitorListener();
                }
                // We replace the state, but only if no other thread has changed
                // it in the meantime.
                if (internalState.compareAndSet(oldState, newState)) {
                    // If we created a monitor listener, we have to register it
                    // now.
                    if (oldState.timeMonitorListener == null) {
                        newState.timeMonitor
                                .addMonitorListener(newState.timeMonitorListener);
                    }
                    break;
                }
            }
        }
    }

    /**
     * Local clock with nanosecond precision. This class provides access to the
     * system clock, returning time stamps with nanosecond precision. As Java's
     * {@link System#currentTimeMillis()} only has millisecond precision,
     * nanosecond precision is attained by incrementing the time stamp returned
     * by one nanosecond each time <code>currentTimeNanos</code> is called when
     * the time returned by <code>System.currentTimeMillis()</code> has not
     * changed yet. This way, the same time is never returned twice.
     * 
     * @author Sebastian Marsching
     */
    private static class LocalClock {

        private AtomicLong lastTimeStamp = new AtomicLong();

        public long currentTimeNanos() {
            long baseTime = System.currentTimeMillis() * 1000000L;
            long lastTimeStamp = this.lastTimeStamp.get();
            // If a new millisecond has started, we can use that time.
            // Otherwise, we increment the last time-stamp by one nanosecond.
            // In theory, this could lead to a situation where our time-stamp
            // "runs away" from the system clock, but this would only happen if
            // we requested time-stamps at a rate above 1 GHz, so we should be
            // safe.
            // When we cannot swap the last time-stamp (because another thread
            // was quicker), we simply call this method again. This should be a
            // very rare event (typically, the clock source will mostly be used
            // by the same thread), and it is much easier than handling this
            // kind of event without any recursion.
            if (baseTime > lastTimeStamp) {
                if (this.lastTimeStamp.compareAndSet(lastTimeStamp, baseTime)) {
                    return baseTime;
                } else {
                    return currentTimeNanos();
                }
            } else {
                if (this.lastTimeStamp.compareAndSet(lastTimeStamp,
                        lastTimeStamp + 1L)) {
                    return lastTimeStamp + 1L;
                } else {
                    return currentTimeNanos();
                }
            }
        }

    }

    /**
     * Monitor listener that listens for value and alarm-state changes. This
     * listener takes care of notifying the {@link SampleListener} with the new
     * sample.
     * 
     * @author Sebastian Marsching
     */
    private class TimeMonitorListener implements
            ChannelAccessMonitorListener<ChannelAccessGettableValue<?>> {

        @Override
        public void monitorError(
                ChannelAccessMonitor<? extends ChannelAccessGettableValue<?>> monitor,
                ChannelAccessStatus status, String message) {
            boolean writeDisconnectedSample;
            for (;;) {
                InternalState oldState = internalState.get();
                if (oldState == null || oldState.timeMonitorListener != this) {
                    // This listener is not responsible any longer, so it should
                    // ignore all events that it receives.
                    return;
                }
                // When disconnected samples should be written and the last
                // sample was not a disconnected sample, we should write a
                // disconnected sample now. This can happen if we did not have a
                // monitor earlier because the channel was disabled and it now
                // got enabled but the monitor registration failed.
                writeDisconnectedSample = configurationOptions
                        .isWriteSampleWhenDisconnected()
                        && !oldState.lastSampleType
                                .equals(SampleType.DISCONNECTED);
                if (oldState.timeMonitorOk || writeDisconnectedSample) {
                    // We have to update the state when the monitor OK flag
                    // changed and when we have to change the last sample type.
                    InternalState newState = new InternalState(oldState);
                    newState.timeMonitorOk = false;
                    if (writeDisconnectedSample) {
                        newState.lastSampleType = SampleType.DISCONNECTED;
                    }
                    // We replace the state, but only if no other thread has
                    // changed it in the meantime.
                    if (!internalState.compareAndSet(oldState, newState)) {
                        continue;
                    }
                }
                break;
            }
            // We write the sample after the loop so that it is not written
            // multiple times when the first attempt to change the state fails.
            if (writeDisconnectedSample) {
                writeDisconnectedSample();
            }
        }

        @Override
        public void monitorEvent(
                ChannelAccessMonitor<? extends ChannelAccessGettableValue<?>> monitor,
                ChannelAccessGettableValue<?> value) {
            InternalState oldState;
            for (;;) {
                oldState = internalState.get();
                if (oldState == null || oldState.timeMonitorListener != this) {
                    // This listener is not responsible any longer, so it should
                    // ignore all events that it receives.
                    return;
                }
                if (!oldState.timeMonitorOk
                        || !oldState.lastSampleType.equals(SampleType.REGULAR)) {
                    // If the monitor state changed or the last written sample
                    // was of a different type, we should update the state.
                    InternalState newState = new InternalState(oldState);
                    newState.lastSampleType = SampleType.REGULAR;
                    newState.timeMonitorOk = true;
                    newState.timeMonitorOkOnce = true;
                    // We replace the state, but only if no other thread has
                    // changed it in the meantime.
                    if (!internalState.compareAndSet(oldState, newState)) {
                        continue;
                    }
                }
                break;
            }
            // We requested a time type, so this cast is safe.
            ChannelAccessTimeValue<?> timeValue = (ChannelAccessTimeValue<?>) value;
            // The time stamp that we use when writing the sample depends on the
            // selected clock source.
            long localTime = localClock.currentTimeNanos();
            long originalTime = (timeValue.getTimeSeconds() + ChannelAccessSampleValueAccess.OFFSET_EPICS_EPOCH_TO_UNIX_EPOCH_SECONDS)
                    * 1000000000L + timeValue.getTimeNanoseconds();
            long maxClockSkew = configurationOptions.getMaxClockSkew();
            long timeStamp;
            switch (configurationOptions.getClockSource()) {
            case LOCAL:
                timeStamp = localTime;
                break;
            case ORIGIN:
                if (maxClockSkew != 0L
                        && absoluteDifference(localTime, originalTime) > maxClockSkew) {
                    log.warn("Discarding a value received for channel \""
                            + StringEscapeUtils.escapeJava(channelName)
                            + "\" because its time-stamp "
                            + timeValue.getTimeStamp()
                            + " differs too much from the local clock.");
                    return;
                } else {
                    timeStamp = originalTime;
                }
                break;
            case PREFER_ORIGIN:
                if (maxClockSkew != 0L
                        && absoluteDifference(localTime, originalTime) > maxClockSkew) {
                    timeStamp = localTime;
                } else {
                    timeStamp = originalTime;
                }
                break;
            default:
                // Throwing inside the listener does not make sense, so we
                // rather write a log message. This case should never happen
                // during regular operation anyway, but indicates a bug in this
                // code.
                log.error("Unhandled clock-source case in monitor listener.");
                return;
            }
            // Finally, we notify the sample listener. There is a race
            // condition, so it could happen that we notify the listener after
            // the channel has been already disconnected. However, the sample
            // listener will discard samples that have the same or an older
            // time-stamp, so this is okay.
            // There is another race condition regarding the sample's meta-data.
            // When the channel's meta-data changes, the time monitor might be
            // notified before the controls monitor. In this case, we might
            // write a sample with the wrong (old) meta-data. Unfortunately,
            // there is no way around this because Channel Access does not
            // provide any way to receive a control sample with a time stamp so
            // there is always an ambiguity regarding the order of samples.
            ChannelAccessRawSample sample = sampleValueAccess.createRawSample(
                    timeValue, oldState.lastControlsValue, timeStamp);
            sampleWriterDelegate.writeSample(sample,
                    SampleSizeEstimator.estimateRawSampleSize(sample));
        }
    }

    /**
     * Holds the archiving channel's internal state. The state is aggregated in
     * one class so that it can be changed in a consistent way without have to
     * use a mutex. This way, we can be sure to avoid dead locks. Instances of
     * this class are never modified after making them visible to other threads.
     * 
     * @author Sebastian Marsching
     */
    private static class InternalState {

        public InternalState() {
        }

        public InternalState(InternalState oldState) {
            this.connected = oldState.connected;
            this.controlsMonitor = oldState.controlsMonitor;
            this.controlsMonitorListener = oldState.controlsMonitorListener;
            this.controlsMonitorOk = oldState.controlsMonitorOk;
            this.enabled = oldState.enabled;
            this.lastControlsValue = oldState.lastControlsValue;
            this.lastSampleType = oldState.lastSampleType;
            this.timeMonitor = oldState.timeMonitor;
            this.timeMonitorListener = oldState.timeMonitorListener;
            this.timeMonitorOk = oldState.timeMonitorOk;
            this.timeMonitorOkOnce = oldState.timeMonitorOkOnce;
        }

        public boolean connected;
        public boolean enabled;
        public ChannelAccessMonitor<?> controlsMonitor;
        public ChannelAccessMonitorListener<ChannelAccessGettableValue<?>> controlsMonitorListener;
        public boolean controlsMonitorOk;
        public ChannelAccessControlsValue<?> lastControlsValue;
        public SampleType lastSampleType = SampleType.NONE;
        public ChannelAccessMonitor<?> timeMonitor;
        public ChannelAccessMonitorListener<ChannelAccessGettableValue<?>> timeMonitorListener;
        public boolean timeMonitorOk;
        public boolean timeMonitorOkOnce;

    }

    /**
     * Type of a sample. This is not the exact type (as specified by
     * {@link ChannelAccessSampleType}), but just a category that we use
     * internally in order to decided when a disabled or disconnected sample
     * should be written.
     * 
     * @author Sebastian Marsching
     */
    private static enum SampleType {
        NONE, REGULAR, DISABLED, DISCONNECTED;
    }

    private static long absoluteDifference(long a, long b) {
        return (a > b) ? (a - b) : (b - a);
    }

    private ChannelAccessChannel channel;
    private String channelName;
    private ConfigurationOptions configurationOptions;
    private long connectionDeadline;
    private EnablingChannel enablingChannel;
    private AtomicReference<InternalState> internalState;
    private LocalClock localClock = new LocalClock();
    private final Logger log = LoggerFactory.getLogger(getClass());
    private ChannelAccessSampleValueAccess sampleValueAccess;
    private SampleWriterDelegate sampleWriterDelegate;

    /**
     * Creates a Channel Access channel. This constructor is package-private
     * because channels should only be created through the
     * {@link ChannelAccessControlSystemSupport}.
     * 
     * @param channelName
     *            name of the channel. This is the name identifying the channel
     *            in the archive. The same name is used to connect to the
     *            control-system channel via Channel Access.
     * @param configurationOptions
     *            configuration options that shall be used by the newly created
     *            channel.
     * @param sampleListener
     *            sample listener that gets notified when a new sample is
     *            received from the Channel Access server.
     * @param sampleValueAccess
     *            sample-value utility used for creating samples.
     * @param client
     *            Channel Access client that is used to create the channel.
     * @param scheduledExecutor
     *            scheduled executor service that can be used for scheduling
     *            tasks that need to be run periodically or after a certain
     *            delay.
     */
    ChannelAccessArchivingChannel(String channelName,
            ConfigurationOptions configurationOptions,
            SampleListener<ChannelAccessSample> sampleListener,
            ChannelAccessSampleValueAccess sampleValueAccess,
            ChannelAccessClient client,
            ScheduledExecutorService scheduledExecutor) {
        this.channelName = channelName;
        this.configurationOptions = configurationOptions;
        this.sampleValueAccess = sampleValueAccess;
        // We want to write a "disconnected" sample if the channel is not
        // connected within five seconds after creating it.
        this.connectionDeadline = System.currentTimeMillis() + 5000L;
        this.channel = client.getChannel(channelName);
        // When there is neither a lower nor an upper limit on the update
        // period, we can use a simplified sample writer that should have a
        // better performance.
        if (configurationOptions.getMaxUpdatePeriod() == 0L
                && configurationOptions.getMinUpdatePeriod() == 0L) {
            this.sampleWriterDelegate = new SimpleSampleWriterDelegate(this,
                    sampleListener);
        } else {
            this.sampleWriterDelegate = new LimitingSampleWriterDelegate(this,
                    configurationOptions, sampleListener, scheduledExecutor);
        }
        this.internalState = new AtomicReference<InternalState>();
        InternalState newState = new InternalState();
        // We have to create the enabling channel before setting the state
        // because it might trigger the checkAndUpdateState method and if this
        // method sees a non-null state, it would assume that the channel is
        // always enabled when the enablingChannel is null.
        if (configurationOptions.getEnablingChannel() != null) {
            this.enablingChannel = new EnablingChannel(
                    configurationOptions.getEnablingChannel(), client) {
                @Override
                protected void updateReceived() {
                    checkAndUpdateState();
                }
            };
        }
        newState.enabled = (enablingChannel == null);
        this.internalState = new AtomicReference<InternalState>(newState);
        // It is possible that the enabling channel's updateReceived method got
        // triggered before we set the state. In this case, it will not have
        // done anything because the state was still null. We simply call the
        // checkAndUpdateState method explicitly, so that it can process the
        // state change if the channel has already been enabled. If this method
        // got called after the state had been set, calling a second time will
        // not hurt us.
        checkAndUpdateState();
        // The connection listener must be registered after updating the state
        // because it relies on seeing the right state.
        channel.addConnectionListener(new ChannelAccessConnectionListener() {
            @Override
            public void connectionStateChanged(ChannelAccessChannel channel,
                    boolean nowConnected) {
                checkAndUpdateState();
            }
        });
        // If a sample should be written when the channel is disabled or
        // disconnected, we start a timer that checks the channel's state after
        // a few seconds. This way, such a sample can be written, even if the
        // channel is never enabled or connected and thus listeners are never
        // triggered.
        if (configurationOptions.isWriteSampleWhenDisabled()
                || configurationOptions.isWriteSampleWhenDisconnected()) {
            scheduledExecutor.schedule(new Runnable() {
                @Override
                public void run() {
                    checkStateAfterConnectionDeadline();
                }
            }, 5000L, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void destroy() {
        InternalState oldState = this.internalState.getAndSet(null);
        if (oldState == null) {
            return;
        }
        // Destroying the channel also destroys the monitors.
        channel.destroy();
        if (enablingChannel != null) {
            enablingChannel.destroy();
        }
        // We also have to destroy the sample writer so that it does not
        // schedule any further checks.
        sampleWriterDelegate.destroy();
    }

    @Override
    public String getChannelName() {
        return channelName;
    }

    @Override
    public ControlSystemChannelStatus getStatus() {
        InternalState currentState = this.internalState.get();
        if (currentState == null) {
            return ControlSystemChannelStatus.error(new IllegalStateException(
                    "The channel has been destroyed."));
        }
        if (!currentState.enabled) {
            return ControlSystemChannelStatus.disabled();
        } else if (currentState.controlsMonitorOk && currentState.timeMonitorOk) {
            return ControlSystemChannelStatus.connected();
        } else {
            return ControlSystemChannelStatus.disconnected();
        }
    }

    private void checkAndUpdateState() {
        // We loop until we can replace the state or determine that we already
        // in the correct state.
        for (;;) {
            InternalState oldState = internalState.get();
            if (oldState == null) {
                // The channel has been destroyed.
                return;
            }
            boolean nowConnected = channel.isConnected();
            boolean nowEnabled = (enablingChannel == null)
                    || enablingChannel.isEnabled();
            // If the connected and enabled state have not changed, we do not
            // have to do anything.
            if (nowConnected == oldState.connected
                    && nowEnabled == oldState.enabled) {
                return;
            }
            InternalState newState = new InternalState(oldState);
            newState.connected = nowConnected;
            newState.enabled = nowEnabled;
            // We can always destroy the monitors from the old state. We only
            // get here when the connected or the enabled state have changed.
            // When there are monitors, the channel must have been both
            // connected and enabled in the old state. This means that channel
            // is not connect or not enabled any longer and thus we do not need
            // the monitors. It is possible that another thread destroys the
            // monitors in parallel, but this is safe because the destroy method
            // can safely be called more than once.
            if (oldState.controlsMonitor != null) {
                oldState.controlsMonitor.destroy();
            }
            if (oldState.timeMonitor != null) {
                oldState.timeMonitor.destroy();
            }
            if (nowConnected && nowEnabled) {
                ChannelAccessValueType nativeDataType;
                try {
                    nativeDataType = channel.getNativeDataType();
                } catch (IllegalStateException e) {
                    // If the channel has been disconnected again before this
                    // listener got called, we might catch an
                    // IllegalStateException. We simply ignore such an exception
                    // - we will make the necessary steps when the channel is
                    // connected the next time.
                    return;
                }
                // It is very unlikely that the native data-type is a type that
                // does not have a corresponding controls type. However, if it
                // is such a rare type, we request a string instead.
                ChannelAccessValueType controlsType;
                try {
                    controlsType = nativeDataType.toControlsType();
                } catch (IllegalArgumentException e) {
                    controlsType = ChannelAccessValueType.DBR_CTRL_STRING;
                }
                // There always is a time type corresponding to a controls type.
                ChannelAccessValueType timeType = controlsType.toTimeType();
                ChannelAccessMonitor<? extends ChannelAccessGettableValue<?>> controlsMonitor = null;
                ChannelAccessMonitor<? extends ChannelAccessGettableValue<?>> timeMonitor = null;
                try {
                    controlsMonitor = channel.monitor(controlsType,
                            configurationOptions.getMetaDataMonitorMask());
                    timeMonitor = channel.monitor(timeType,
                            configurationOptions.getMonitorMask());
                } catch (IllegalStateException e) {
                    // This can only happen if the channel has been
                    // destroyed, so we simply return if we get such an
                    // exception.
                    if (controlsMonitor != null) {
                        controlsMonitor.destroy();
                    }
                    if (timeMonitor != null) {
                        timeMonitor.destroy();
                    }
                    return;
                }
                newState.controlsMonitor = controlsMonitor;
                newState.controlsMonitorListener = new ControlsMonitorListener();
                newState.controlsMonitorOk = false;
                newState.lastControlsValue = null;
                newState.timeMonitor = timeMonitor;
                // We do not register a listener for the timeMonitor yet. We
                // register this listener when we have the first controls
                // sample. This way, we always have the necessary meta-data
                // when we receive a time sample.
                newState.timeMonitorListener = null;
                newState.timeMonitorOk = false;
            } else {
                newState.controlsMonitor = null;
                newState.controlsMonitorListener = null;
                newState.controlsMonitorOk = false;
                newState.lastControlsValue = null;
                newState.timeMonitor = null;
                newState.timeMonitorListener = null;
                newState.timeMonitorOk = false;
            }
            // When the channel is disconnected or disabled, we might want
            // to write a special sample. We do this after successfully
            // changing the state in order to avoid writing two samples when
            // this method runs concurrently. This means that there is a
            // risk that the sample that we write here will actually be
            // older than a sample that is written by another thread and
            // will thus be discarded. This is okay, because we do not
            // really care about this special sample if another sample is
            // written within a very short period.
            // If the channel is enabled but not connected, we write a
            // disconnected sample. We only do this if we already received
            // at least one sample or when the connection deadline has
            // passed. Otherwise, the channel might still get connected
            // before the deadline. Of course, we only write a sample if the
            // corresponding option has been enabled.
            boolean writeDisconnectedSample = configurationOptions
                    .isWriteSampleWhenDisconnected()
                    && !nowConnected
                    && (oldState.connected || !oldState.enabled)
                    && nowEnabled
                    && (oldState.timeMonitorOkOnce || System
                            .currentTimeMillis() >= connectionDeadline)
                    && !oldState.lastSampleType.equals(SampleType.DISCONNECTED);
            // If the channel is disabled, we write a disabled sample. We
            // only do this if the channel has been enabled at least once or
            // when the connection deadline has passed. Otherwise, the
            // channel might still get enabled before the deadline. Of
            // course, we only write a sample if the corresponding option
            // has been enabled.
            boolean writeDisabledSample = configurationOptions
                    .isWriteSampleWhenDisabled()
                    && !nowEnabled
                    && oldState.enabled
                    && enablingChannel != null
                    && (enablingChannel.isEnabledOnce() || System
                            .currentTimeMillis() >= connectionDeadline)
                    && !oldState.lastSampleType.equals(SampleType.DISABLED);
            // We only write a disabled or disconnected sample if the last
            // sample that was written was not of the same type. We record which
            // type of sample we are going to write so that we will not write
            // the same type again before a sample of a different type has been
            // written.
            if (writeDisabledSample) {
                newState.lastSampleType = SampleType.DISABLED;
            }
            if (writeDisconnectedSample) {
                newState.lastSampleType = SampleType.DISCONNECTED;
            }
            // We try to replace the state object. This might fail if another is
            // running this method concurrently.
            if (internalState.compareAndSet(oldState, newState)) {
                // If we created a controls monitor and listener, we now want to
                // register this listener with the monitor. We do not do this
                // earlier because the listener will check whether it is the
                // active listener and this check might fail if the listener is
                // notified before we have replaced the state.
                if (newState.controlsMonitor != null
                        && newState.controlsMonitorListener != null) {
                    newState.controlsMonitor
                            .addMonitorListener(newState.controlsMonitorListener);
                }
                // We only write the disabled or disconnected sample after
                // changing the state. This way, we can avoid writing multiple
                // samples when replacing the state fails due to a concurrent
                // modification.
                if (writeDisabledSample) {
                    // When using the origin time for regular samples, it is
                    // hard to choose the right local time to use for the
                    // "disabled" sample. We only have access to a clock with
                    // millisecond resolution and if we choose the time stamp
                    // too small, a sample that we received before the channel
                    // was disabled might already have had a greater time-stamp.
                    // On the other hand, if we choose the time stamp too large,
                    // a sample that we receive after the channel is re-enabled
                    // might not be written because its time-stamp is too small.
                    // We choose the time-stamp large (at the upper end of the
                    // current millisecond) because the time needed to register
                    // monitors for a channel should typically be larger than a
                    // millisecond (it requires at least one full network
                    // round-trip).
                    // When using the local time for regular samples, we can
                    // simply use the local clock because our internal
                    // implementation guarantees that the time stamp is
                    // incremented, even if we are still in the same
                    // millisecond.
                    long timeStamp;
                    switch (configurationOptions.getClockSource()) {
                    case LOCAL:
                        timeStamp = localClock.currentTimeNanos();
                        break;
                    default:
                        timeStamp = System.currentTimeMillis() * 1000000L + 999999L;
                        break;
                    }
                    sampleWriterDelegate.writeSample(
                            new ChannelAccessDisabledSample(timeStamp, true),
                            SampleSizeEstimator.estimateDisabledSampleSize());
                }
                if (writeDisconnectedSample) {
                    writeDisconnectedSample();
                }
                // After replacing the state, we also have to check that neither
                // the connected nor the enabled flag have changed in the
                // meantime. If they have changed, we have to call this method
                // again because a call that might have been made to this method
                // in the meantime might have returned because it still saw the
                // old state. For this reason, we only break the loop if neither
                // the connected nor the enabled flag have changed.
                nowConnected = channel.isConnected();
                nowEnabled = (enablingChannel == null)
                        || enablingChannel.isEnabled();
                if (nowConnected == newState.connected
                        && nowEnabled == newState.enabled) {
                    return;
                }
            } else {
                // If we created new monitors and could not replace the state,
                // we have to destroy the monitors that we just created.
                if (newState.controlsMonitor != null) {
                    newState.controlsMonitor.destroy();
                }
                if (newState.timeMonitor != null) {
                    newState.timeMonitor.destroy();
                }
            }
        }
    }

    private void checkStateAfterConnectionDeadline() {
        InternalState oldState;
        boolean writeDisabledSample;
        boolean writeDisconnectedSample;
        for (;;) {
            oldState = internalState.get();
            if (oldState == null) {
                // If this channel has already been destroyed, we do not do
                // anything.
            }
            InternalState newState = new InternalState(oldState);
            // When we are supposed to write a disabled sample, we check whether
            // the channel has been enabled at least once. If it has never been
            // enabled, we write the special sample, otherwise we do not have to
            // because it is either enabled or the sample got written when the
            // channel changed from enabled to disabled.
            writeDisabledSample = configurationOptions
                    .isWriteSampleWhenDisabled()
                    && enablingChannel != null
                    && !enablingChannel.isEnabledOnce()
                    && !oldState.lastSampleType.equals(SampleType.DISABLED);
            // When we are supposed to write a disconnected sample, we check
            // whether we have received at least one sample. If we never
            // received a sample, but the channel is enabled, we write a
            // disconnected sample.
            writeDisconnectedSample = configurationOptions
                    .isWriteSampleWhenDisconnected()
                    && oldState.enabled
                    && !oldState.connected
                    && !oldState.timeMonitorOkOnce
                    && !oldState.lastSampleType.equals(SampleType.DISCONNECTED);
            if (writeDisabledSample) {
                newState.lastSampleType = SampleType.DISABLED;
            }
            if (writeDisconnectedSample) {
                newState.lastSampleType = SampleType.DISCONNECTED;
            }
            // When we can replace the state, we are done with the loop,
            // otherwise we have to try again with the state that has been
            // modified concurrently.
            if (internalState.compareAndSet(oldState, newState)) {
                break;
            }
        }
        if (writeDisconnectedSample) {
            // We use the original connection deadline as the sample's
            // time-stamp. This way, if the channel got connected and
            // disconnected after the deadline passed, but before the timer was
            // triggered (a timer might be executed later than the specified
            // point in time), we will not write a second "disconnected" sample
            // because the one that we write now has the same or an older
            // time-stamp than the one already written. Obviously, there is a
            // race condition here so that we cannot strictly guarantee the
            // desired behavior and two samples might be written if the timing
            // is just right. However, this approach should avoid duplicate
            // samples in most cases.
            sampleWriterDelegate.writeSample(
                    new ChannelAccessDisconnectedSample(
                            connectionDeadline * 1000000L, true),
                    SampleSizeEstimator.estimateDisconnectedSampleSize());
        }
        if (writeDisabledSample) {
            // We use the original connection deadline as the sample's
            // time-stamp. This way, if the channel gets enabled and disabled
            // after the deadline has passed, but before the timer is
            // triggered (a timer might be executed later than the specified
            // point in time), we will not write a second "disabled" sample
            // because the one that we write now has the same or an older
            // time-stamp than the one already written. Obviously, there is a
            // race condition here so that we cannot strictly guarantee the
            // desired behavior and two samples might be written if the timing
            // is just right. However, this approach should avoid duplicate
            // samples in most cases.
            sampleWriterDelegate.writeSample(new ChannelAccessDisabledSample(
                    connectionDeadline * 1000000L, true), SampleSizeEstimator
                    .estimateDisabledSampleSize());
        }
    }

    private void writeDisconnectedSample() {
        // When using the origin time for regular samples, it is hard to choose
        // the right local time to use for the "disconnected" sample. We only
        // have access to a clock with millisecond resolution and if we choose
        // the time stamp too small, a sample that we received before the
        // channel disconnected might already have had a greater time-stamp. On
        // the other hand, if we choose the time stamp too large, a sample that
        // we receive after the channel reconnects might not be written because
        // its time-stamp is too small. We choose the time-stamp large (at the
        // upper end of the current millisecond) because the time needed to
        // connect a channel should typically be larger than a millisecond (a
        // connection requires several network round-trips and even a monitor
        // registration needs at least one full network round-trip).
        // When using the local time for regular samples, we can simply use the
        // local clock because our internal implementation guarantees that the
        // time stamp is incremented, even if we are still in the same
        // millisecond.
        long timeStamp;
        switch (configurationOptions.getClockSource()) {
        case LOCAL:
            timeStamp = localClock.currentTimeNanos();
            break;
        default:
            timeStamp = System.currentTimeMillis() * 1000000L + 999999L;
            break;
        }
        sampleWriterDelegate.writeSample(new ChannelAccessDisconnectedSample(
                timeStamp, true), SampleSizeEstimator
                .estimateDisconnectedSampleSize());
    }

}
