/*
 * Copyright 2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.controlsystem;

/**
 * Abstract base class for sample decimator implementations. This base class
 * takes care of storing the channel name and the interval start time and
 * length. When creating an implementation of the {@link SampleDecimator}
 * interface, the {@link AbstractStatefulSampleDecimator} should be considered
 * as a base class instead of using this class directly. However, this class can
 * be used when the behavior exhibited by the
 * <code>AbstractStatefulSampleDecimator</code> is not desired.
 * 
 * @author Sebastian Marsching
 *
 * @param <SampleType>
 *            type that is implemented by all samples that are passed to this
 *            sample decimator and that is also implemented by the sample
 *            generated by this sample decimator. Typically, this is the sample
 *            type used by the control-system support that provides the concrete
 *            implementation that inherits from this base class.
 */
public abstract class AbstractSampleDecimator<SampleType extends Sample>
        implements SampleDecimator<SampleType> {

    private final String channelName;
    private final long intervalLength;
    private final long intervalStartTime;

    /**
     * Creates the sample decimator and initializes the channel name and the
     * interval start-time and length with the specified values.
     * 
     * @param channelName
     *            name identifying the channel covered by this sample decimator.
     * @param intervalStartTime
     *            start time of the interval covered by this sample decimator.
     *            The time is specified as the number of nanoseconds since epoch
     *            (January 1st, 1970, 00:00:00 UTC).
     * @param intervalLength
     *            length of the interval covered by this sample decimator. The
     *            length is specified in nanoseconds.
     */
    public AbstractSampleDecimator(String channelName, long intervalStartTime,
            long intervalLength) {
        this.channelName = channelName;
        this.intervalStartTime = intervalStartTime;
        this.intervalLength = intervalLength;
    }

    @Override
    public String getChannelName() {
        return channelName;
    }

    @Override
    public long getIntervalLength() {
        return intervalLength;
    }

    @Override
    public long getIntervalStartTime() {
        return intervalStartTime;
    }

}