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
import com.aquenos.epics.jackie.common.value.ChannelAccessAlarmSeverity;
import com.aquenos.epics.jackie.common.value.ChannelAccessAlarmStatus;

/**
 * <p>
 * Base class for aggregators that do some kind of averaging. This base class
 * takes care of maximizing the alarm severity and recording the period that is
 * covered by the samples. It also keeps a reference to the first sample. This
 * class can only handle samples of type {@link ChannelAccessUdtSample}, so the
 * calling code has to ensure that only samples of this type are passed to the
 * {@link #processSample(ChannelAccessSample, long, long)} method.
 * </p>
 * 
 * <p>
 * This class is intended for use by {@link ChannelAccessSampleDecimator} and
 * its associated classes only.
 * </p>
 * 
 * @author Sebastian Marsching
 */
public abstract class AveragingSampleAggregator extends SampleAggregator {

    /**
     * Period that is covered by samples (in nanoseconds). Each time a sample is
     * processed by the {@link #processSample(ChannelAccessSample, long, long)}
     * method, the period for which this sample is supposed to be used is added
     * to this sum.
     */
    protected long coveredPeriod;

    /**
     * First sample that was passed to
     * {@link #processSample(ChannelAccessSample, long, long)}.
     */
    protected ChannelAccessUdtSample firstSample;

    /**
     * Length of the interval for which this sample aggregator shall produce an
     * aggregated sample. The length is specified in nanoseconds.
     */
    protected long intervalLength;

    /**
     * Start time of the interval for which this sample aggregator shall produce
     * an aggregated sample. The time is specified as the number of nanoseconds
     * since epoch (January 1st, 1970, 00:00:00 UTC).
     */
    protected long intervalStartTime;

    /**
     * Highest alarm severity found in a sample so far. Each time a sample is
     * processed by the {@link #processSample(ChannelAccessSample, long, long)}
     * method, it is checked whether that sample has a higher alarm severity
     * then the existing severity. If so, this field is updated with the
     * severity from the sample and the {@link #maxAlarmStatus} fields is
     * updated with the sample's alarm-status.
     */
    protected ChannelAccessAlarmSeverity maxAlarmSeverity;

    /**
     * Alarm status associated with the highest alarm severity found in a sample
     * so far. This field is updated whenever {@link #maxAlarmSeverity} is
     * updated. This means that this field has the alarm status of the sample
     * that had the highest alarm severity. If there are several samples with
     * the same alarm severity, this is the status of the first of these
     * samples.
     */
    protected ChannelAccessAlarmStatus maxAlarmStatus;

    /**
     * Channel Access sample-value access that can be used for accessing the
     * internal data-structures of samples.
     */
    protected ChannelAccessSampleValueAccess sampleValueAccess;

    /**
     * Creates an averaging sample aggregator, initializing it with the
     * specified interval and Channel Access sample-value access. This
     * constructor is package-private so that only classes in the same package
     * can inherit from this class.
     * 
     * @param intervalStartTime
     *            start time of the interval for which this aggregator processes
     *            samples. The time is specified as the number of nanoseconds
     *            since epoch (January 1st, 1970, 00:00:00 UTC).
     * @param intervalLength
     *            length of the interval for which this aggregator processes
     *            samples. The length is specified in nanoseconds.
     * @param sampleValueAccess
     *            sample-value access used for accessing the internal
     *            data-structures of the samples. This object is also used for
     *            finally creating the sample instance returned by this
     *            aggregator.
     */
    AveragingSampleAggregator(long intervalStartTime, long intervalLength,
            ChannelAccessSampleValueAccess sampleValueAccess) {
        this.intervalStartTime = intervalStartTime;
        this.intervalLength = intervalLength;
        this.sampleValueAccess = sampleValueAccess;
    }

    @Override
    public final void processSample(ChannelAccessSample sample,
            long startTimeStamp, long endTimeStamp) {
        // This method is only used internally, so we use assertions instead of
        // preconditions.
        assert (sample instanceof ChannelAccessUdtSample);
        assert (firstSample == null || sample.getType().equals(
                firstSample.getType()));
        ChannelAccessUdtSample udtSample = (ChannelAccessUdtSample) sample;
        if (firstSample == null) {
            firstSample = udtSample;
            maxAlarmSeverity = sampleValueAccess
                    .deserializeAlarmSeverityColumn(udtSample);
            maxAlarmStatus = sampleValueAccess
                    .deserializeAlarmStatusColumn(udtSample);
        }
        long sampleValidPeriod = calculateSampleValidPeriod(startTimeStamp,
                endTimeStamp, udtSample);
        coveredPeriod += sampleValidPeriod;
        ChannelAccessAlarmSeverity alarmSeverity = sampleValueAccess
                .deserializeAlarmSeverityColumn(udtSample);
        if (alarmSeverity.toSeverityCode() > maxAlarmSeverity.toSeverityCode()) {
            maxAlarmSeverity = alarmSeverity;
            maxAlarmStatus = sampleValueAccess
                    .deserializeAlarmStatusColumn(udtSample);
        }
        processSampleValue(udtSample, sampleValidPeriod);
    }

    /**
     * Returns the length of the period that is covered by samples that have
     * been processed by this aggregator. This number is calculating by summing
     * up all the periods which have been specified when calling
     * {@link #processSample(ChannelAccessSample, long, long)}. The period is
     * specified in nanoseconds.
     * 
     * @return period covered by the samples that have been processed by this
     *         aggregator (in nanoseconds).
     */
    public long getCoveredPeriod() {
        return coveredPeriod;
    }

    /**
     * Returns the length of the time period for which the specified sample
     * contains valid data (in nanoseconds). The base implementation simply
     * subtracts the <code>startTimeStamp</code> from the
     * <code>endTimeStamp</code>, but child classes may override this behavior,
     * for example in order to consider the fact that a source sample that is
     * already an aggregated sample might not contain valid data for the whole
     * period that it covers.
     * 
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
     * @param sample
     *            sample for which the length of the validity period is supposed
     *            to be calculated.
     * @return length of the period for which this sample is considered to be
     *         valid. This number is typically used as a weight for the sample
     *         and it is also considered when calculating the total time that is
     *         covered by the aggregated sample as returned by
     *         {@link #getCoveredPeriod()}.
     */
    protected long calculateSampleValidPeriod(long startTimeStamp,
            long endTimeStamp, ChannelAccessUdtSample sample) {
        return endTimeStamp - startTimeStamp;
    }

    /**
     * Internally processes a sample. This method must be implemented by child
     * classes in order to update the aggregate state that is type specific
     * (e.g. minimum and maximum, mean, standard deviation, etc.).
     * 
     * @param sample
     *            sample that shall be processed.
     * @param sampleValidPeriod
     *            period for which this sample is valid (in nanoseconds).
     *            Typically, this number should be used as a weight when
     *            calculating the average of many samples.
     */
    protected abstract void processSampleValue(ChannelAccessUdtSample sample,
            long sampleValidPeriod);

}
