/*
 * Copyright 2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.controlsystem.channelaccess.internal;

import com.aquenos.cassandra.pvarchiver.controlsystem.channelaccess.ChannelAccessSample;

/**
 * <p>
 * Simple sample aggregator that simply uses the first sample for the specified
 * period. The {@link #getAggregatedSample()} method simply returns the sample
 * that was specified when the
 * {@link #processSample(ChannelAccessSample, long, long)} method was called for
 * the first time. However, the returned sample's time-stamp is not the time
 * stamp of the original sample but the time stamp specified as the interval's
 * start time.
 * </p>
 * 
 * <p>
 * This class is intended for use by {@link ChannelAccessSampleDecimator} and
 * its associated classes only.
 * </p>
 * 
 * @author Sebastian Marsching
 */
public class PickFirstSampleAggregator extends SampleAggregator {

    private ChannelAccessSample firstSample;
    private long intervalStartTime;

    /**
     * Creates a new sample aggregator for the interval that starts at the
     * specified time.
     * 
     * @param intervalStartTime
     *            start time of the interval for which this aggregator processes
     *            samples. The time is specified as the number of nanoseconds
     *            since epoch (January 1st, 1970, 00:00:00 UTC).
     */
    public PickFirstSampleAggregator(long intervalStartTime) {
        this.intervalStartTime = intervalStartTime;
    }

    @Override
    public ChannelAccessSample getAggregatedSample() {
        if (firstSample instanceof ChannelAccessRawSample) {
            ChannelAccessRawSample sample = (ChannelAccessRawSample) firstSample;
            return new ChannelAccessRawSample(intervalStartTime,
                    sample.getType(), sample.getValue(), false);
        } else if (firstSample instanceof ChannelAccessDisabledSample) {
            return new ChannelAccessDisabledSample(intervalStartTime, false);
        } else if (firstSample instanceof ChannelAccessDisconnectedSample) {
            return new ChannelAccessDisconnectedSample(intervalStartTime, false);
        } else {
            throw new RuntimeException("First sample is of unexpected type "
                    + firstSample.getClass().getName() + ".");
        }
    }

    @Override
    public void processSample(ChannelAccessSample sample, long startTimeStamp,
            long endTimeStamp) {
        // This class is only used internally, so we use an assertion instead of
        // a precondition.
        assert (sample instanceof ChannelAccessRawSample
                || sample instanceof ChannelAccessDisabledSample || sample instanceof ChannelAccessDisconnectedSample);
        if (firstSample == null) {
            firstSample = sample;
        }
    }

}
