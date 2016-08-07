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
import com.aquenos.cassandra.pvarchiver.controlsystem.channelaccess.ChannelAccessSampleType;

/**
 * <p>
 * Aggregator for aggregated scalar short samples. This aggregator consumes
 * samples of type <code>AGGREGATED_SCALAR_SHORT</code> and produces samples of
 * the same type.
 * </p>
 * 
 * <p>
 * This class is intended for use by {@link ChannelAccessSampleDecimator} and
 * its associated classes only.
 * </p>
 * 
 * @author Sebastian Marsching
 */
public class AggregatedScalarShortAggregator extends AveragingSampleAggregator {

    private short maximum;
    private short minimum;
    private double sum;
    private double sumOfSquares;

    /**
     * Creates a new aggregator. This aggregator aggregates samples for the
     * specified interval and uses the specified Channel Access sample-value
     * access for accessing the internal data-structures of the samples.
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
    public AggregatedScalarShortAggregator(long intervalStartTime,
            long intervalLength,
            ChannelAccessSampleValueAccess sampleValueAccess) {
        super(intervalStartTime, intervalLength, sampleValueAccess);
        minimum = Short.MAX_VALUE;
        maximum = Short.MIN_VALUE;
    }

    @Override
    public ChannelAccessSample getAggregatedSample() {
        double mean = sum / coveredPeriod;
        double variance = sumOfSquares / coveredPeriod - sum * sum
                / coveredPeriod / coveredPeriod;
        double std = (variance > 0.0) ? Math.sqrt(variance) : 0.0;
        return sampleValueAccess.createAggregatedScalarShortSample(
                intervalStartTime, mean, std, minimum, maximum,
                maxAlarmSeverity, maxAlarmStatus, ((double) coveredPeriod)
                        / ((double) intervalLength), firstSample);
    }

    @Override
    protected long calculateSampleValidPeriod(long startTimeStamp,
            long endTimeStamp, ChannelAccessUdtSample sample) {
        // This class is only used internally, so we use an assertion instead of
        // a precondition.
        assert (sample instanceof ChannelAccessAggregatedSample);
        assert (sample.getType()
                .equals(ChannelAccessSampleType.AGGREGATED_SCALAR_SHORT));
        double sampleCoveredFraction = sampleValueAccess
                .deserializeCoveredPeriodFractionColumn((ChannelAccessAggregatedSample) sample);
        return (long) ((endTimeStamp - startTimeStamp) * sampleCoveredFraction);
    }

    @Override
    protected void processSampleValue(ChannelAccessUdtSample sample,
            long sampleValidPeriod) {
        // This class is only used internally, so we use an assertion instead of
        // a precondition.
        assert (sample instanceof ChannelAccessAggregatedSample);
        assert (sample.getType()
                .equals(ChannelAccessSampleType.AGGREGATED_SCALAR_SHORT));
        ChannelAccessAggregatedSample aggregatedSample = (ChannelAccessAggregatedSample) sample;
        double sampleMean = sampleValueAccess
                .deserializeMeanColumn(aggregatedSample);
        double sampleStd = sampleValueAccess
                .deserializeStdColumn(aggregatedSample);
        minimum = (short) Math.min(
                sampleValueAccess.deserializeShortMinColumn(aggregatedSample),
                minimum);
        maximum = (short) Math.max(
                sampleValueAccess.deserializeShortMaxColumn(aggregatedSample),
                maximum);
        sum += sampleValidPeriod * sampleMean;
        sumOfSquares += sampleValidPeriod
                * (sampleStd * sampleStd + sampleMean * sampleMean);
    }

}
