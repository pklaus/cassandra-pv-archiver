/*
 * Copyright 2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.controlsystem.channelaccess.internal;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

import com.aquenos.cassandra.pvarchiver.controlsystem.channelaccess.ChannelAccessControlSystemSupport;
import com.aquenos.cassandra.pvarchiver.controlsystem.channelaccess.ChannelAccessSample;
import com.aquenos.cassandra.pvarchiver.controlsystem.channelaccess.ChannelAccessSampleType;

/**
 * <p>
 * Channel Access sample that indicates that the channel is disabled. Such a
 * sample is written when archiving is disabled in a channel's configuration or
 * when the channel is temporarily disabled because of a dynamic condition not
 * being met. Such a sample is only written if the
 * <code>writeSampleWhenDisabled</code> flag is <code>true</code>.
 * </p>
 * 
 * <p>
 * This class is only intended for use by
 * {@link ChannelAccessControlSystemSupport} and its associated classes.
 * </p>
 * 
 * @author Sebastian Marsching
 */
public class ChannelAccessDisabledSample implements ChannelAccessSample {

    private boolean originalSample;
    private long timeStamp;

    /**
     * Creates a "disabled" sample with the specified time-stamp.
     * 
     * @param timeStamp
     *            time stamp associated with this sample. The time stamp is
     *            returned as the number of nanoseconds since epoch (January
     *            1st, 1970, 00:00:00 UTC).
     * @param originalSample
     *            <code>true</code> if this sample is an original sample that
     *            has been received over the network, <code>false</code> if it
     *            the result of a decimation process.
     */
    public ChannelAccessDisabledSample(long timeStamp, boolean originalSample) {
        this.timeStamp = timeStamp;
        this.originalSample = originalSample;
    }

    @Override
    public boolean isOriginalSample() {
        return originalSample;
    }

    @Override
    public long getTimeStamp() {
        return timeStamp;
    }

    @Override
    public ChannelAccessSampleType getType() {
        return ChannelAccessSampleType.DISABLED;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(83, 29).append(originalSample)
                .append(timeStamp).toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || !this.getClass().equals(obj.getClass())) {
            return false;
        }
        ChannelAccessDisabledSample other = (ChannelAccessDisabledSample) obj;
        return this.originalSample == other.originalSample
                && this.timeStamp == other.timeStamp;
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.reflectionToString(this);
    }

}
