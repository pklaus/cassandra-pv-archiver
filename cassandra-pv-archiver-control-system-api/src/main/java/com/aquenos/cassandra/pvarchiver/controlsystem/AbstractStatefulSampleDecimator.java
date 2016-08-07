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
 * <p>
 * Abstract base class for sample decimator implementations, handling some of
 * the state management. In addition to storing the channel name and the
 * interval start time and length, this class takes care of handling some of the
 * state when processing samples.
 * </p>
 * 
 * <p>
 * Each sample passed to this class's {@link #processSample(Sample)} method is
 * forwarded to the {@link #processSampleInternal(Sample, long, long)} method,
 * adding the start and end time of the interval for which this sample should be
 * used. Usually, the start time is the time also specified by the sample and
 * the end time is the time specified by the next sample. However, when
 * processing the first sample, the start time is the start time of the interval
 * that is covered by this decimator. Consequently, when processing the last
 * sample, the end time is the end time of the interval covered by this
 * decimator.
 * </p>
 * 
 * <p>
 * The {@link #buildDecimatedSample()} method forwards to the
 * {@link #buildDecimatedSampleInternal()} method because it has to make a last
 * call to {@link #processSampleInternal(Sample, long, long)} for the last
 * sample that has been received but not processed yet.
 * </p>
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
public abstract class AbstractStatefulSampleDecimator<SampleType extends Sample>
        extends AbstractSampleDecimator<SampleType> {

    private boolean firstSample;
    private SampleType lastSample;

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
    public AbstractStatefulSampleDecimator(String channelName,
            long intervalStartTime, long intervalLength) {
        super(channelName, intervalStartTime, intervalLength);
        firstSample = true;
    }

    @Override
    public final void buildDecimatedSample() {
        long startTime = firstSample ? getIntervalStartTime() : lastSample
                .getTimeStamp();
        processSampleInternal(lastSample, startTime, getIntervalStartTime()
                + getIntervalLength());
        buildDecimatedSampleInternal();
    }

    @Override
    public final void processSample(SampleType sample) {
        if (lastSample != null) {
            long startTime = firstSample ? getIntervalStartTime() : lastSample
                    .getTimeStamp();
            processSampleInternal(lastSample, startTime, sample.getTimeStamp());
            firstSample = false;
        }
        lastSample = sample;
    }

    /**
     * Builds the decimated sample. This method has to be implemented by child
     * classes in order to calculate the sample that is subsequently returned by
     * {@link #getDecimatedSample()}. This method is only called once and only
     * after all calls to {@link #processSampleInternal(Sample, long, long)}
     * have been made.
     */
    protected abstract void buildDecimatedSampleInternal();

    /**
     * <p>
     * Processes a sample, updating the decimators internal state. This method
     * has to be implemented by child classes in order to process source
     * samples. The specified start time is the start of the interval for which
     * the specified sample should be used (inclusive). The specified end time
     * is the end of the interval for which the specified sample should be used
     * (exclusive). Both times are specified as the number of nanoseconds since
     * epoch (January 1st, 1970, 00:00:00 UTC).
     * </p>
     * 
     * <p>
     * Usually, the start time is the time also specified by the sample and the
     * end time is the time specified by the next sample. However, when
     * processing the first sample, the start time is the start time of the
     * interval that is covered by this decimator. Consequently, when processing
     * the last sample, the end time is the end time of the interval covered by
     * this decimator.
     * </p>
     * 
     * @param sample
     *            sample that shall be processed.
     * @param startTime
     *            start time of the interval for which the specified sample
     *            shall be used.
     * @param endTime
     *            end time of the interval for which the specified sample shall
     *            be used.
     */
    protected abstract void processSampleInternal(SampleType sample,
            long startTime, long endTime);

}
