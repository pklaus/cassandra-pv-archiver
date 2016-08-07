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
 * Base class of all sample aggregators. Each sample aggregator handles the
 * aggregation of samples of a specific type for a specific period of time.
 * </p>
 * 
 * <p>
 * This class is intended for use by {@link ChannelAccessSampleDecimator} and
 * its associated classes only.
 * </p>
 * 
 * @author Sebastian Marsching
 */
public abstract class SampleAggregator {

    /**
     * Default constructor. This constructor should be called by child classes.
     * It is package-private so that only classes that are in the same package
     * can inherit from this class.
     */
    SampleAggregator() {
    }

    /**
     * Returns the aggregated sample. This method must only be called after
     * calling {@link #processSample(ChannelAccessSample, long, long)} at least
     * once.
     * 
     * @return aggregated sample created from the samples passed to this
     *         aggregator.
     */
    public abstract ChannelAccessSample getAggregatedSample();

    /**
     * Processes a sample, updating the state of this aggregator. Depending on
     * the aggregator's implementation, each sample can contribute to the final
     * aggregated sample returned by this aggregator. However, some aggregators
     * might choose to only use a single (in particular the first) sample.
     * 
     * @param sample
     *            sample to be processed. Each call of this method must pass a
     *            sample of the same type. The sample's time-stamp should not be
     *            used. Instead, the specified <code>startTimeStamp</code> and
     *            <code>endTimeStamp</code> should be used.
     * @param startTimeStamp
     *            time stamp marking the start of the interval for which this
     *            sample should be used (inclusive). The time is specified as
     *            the number of nanoseconds since epoch (January 1st, 1970,
     *            00:00:00 UTC).
     * @param endTimeStamp
     *            time stamp marking the end of the interval for which this
     *            sample should be used (exclusive). The time is specified as
     *            the number of nanoseconds since epoch (January 1st, 1970,
     *            00:00:00 UTC).
     */
    public abstract void processSample(ChannelAccessSample sample,
            long startTimeStamp, long endTimeStamp);

}
