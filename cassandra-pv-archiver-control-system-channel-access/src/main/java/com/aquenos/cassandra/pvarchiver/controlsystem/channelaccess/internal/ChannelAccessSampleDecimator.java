/*
 * Copyright 2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.controlsystem.channelaccess.internal;

import java.util.EnumMap;

import com.aquenos.cassandra.pvarchiver.controlsystem.AbstractStatefulSampleDecimator;
import com.aquenos.cassandra.pvarchiver.controlsystem.ControlSystemSupport;
import com.aquenos.cassandra.pvarchiver.controlsystem.channelaccess.ChannelAccessSample;
import com.aquenos.cassandra.pvarchiver.controlsystem.channelaccess.ChannelAccessSampleType;

/**
 * <p>
 * Decimator for Channel Access samples. This decimator either picks the first
 * sample in the specified period or builds an aggregated sample from all the
 * samples being present in the period, depending on the type of the samples.
 * </p>
 * 
 * <p>
 * When the source samples are of a type that can be aggregated (these are the
 * <code>SCALAR_CHAR</code>, <code>SCALAR_DOUBLE</code>,
 * <code>SCALAR_FLOAT</code>, <code>SCALAR_LONG</code>,
 * <code>SCALAR_SHORT</code>, <code>AGGREGATED_SCALAR_CHAR</code>,
 * <code>AGGREGATED_SCALAR_DOUBLE</code>, <code>AGGREGATED_SCALAR_FLOAT</code>,
 * <code>AGGREGATED_SCALAR_LONG</code>, and <code>AGGREGATED_SCALAR_SHORT</code>
 * types), this aggregate is calculated. If the source samples are of a type
 * that cannot be aggregated (all other types), the sample that is valid for the
 * start of the interval is used.
 * </p>
 * 
 * <p>
 * When the type of the source samples is not homogeneous, the behavior depends:
 * When there are only samples that can be aggregated (as defined in the
 * previous paragraph) or <code>DISABLED</code> or <code>DISCONNECTED</code>
 * samples, the type that can be aggregated and covers the greatest fraction of
 * the interval is used. When there is at least one type that cannot be
 * aggregated (not including the <code>DISABLED</code> or
 * <code>DISCONNECTED</code> type), the type of the sample that is valid for the
 * start of the interval is used. If this happens to be a type that is
 * aggregatable, an an aggregated sample is generated. Otherwise, this sample is
 * used.
 * </p>
 * 
 * <p>
 * When an aggregated sample is generated, it is of one of the types
 * <code>AGGREGATED_SCALAR_CHAR</code>, <code>AGGREGATED_SCALAR_DOUBLE</code>,
 * <code>AGGREGATED_SCALAR_FLOAT</code>, <code>AGGREGATED_SCALAR_LONG</code>, or
 * <code>AGGREGATED_SCALAR_SHORT</code>, depending on the type of its source
 * samples.
 * </p>
 * 
 * <p>
 * This class should not be instantiated directly. Instead, instances should be
 * retrieved through the
 * {@link ControlSystemSupport#createSampleDecimator(String, java.util.Map, long, long)}
 * method.
 * </p>
 * 
 * @author Sebastian Marsching
 */
public class ChannelAccessSampleDecimator extends
        AbstractStatefulSampleDecimator<ChannelAccessSample> {

    private ChannelAccessSample decimatedSample;
    private int decimatedSampleSize;
    private ChannelAccessSampleType firstType;
    private ChannelAccessSampleValueAccess sampleValueAccess;
    private EnumMap<ChannelAccessSampleType, SampleAggregator> typeToAggregator = new EnumMap<ChannelAccessSampleType, SampleAggregator>(
            ChannelAccessSampleType.class);

    /**
     * Creates a sample decimator for the specified channel and interval. The
     * specified Channel Access sample-value access is used for accessing the
     * internal data-structures of the samples.
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
     * @param sampleValueAccess
     *            sample-value access used for accessing the internal
     *            data-structures of the samples. This object is also used for
     *            finally creating the sample instance returned by this
     *            aggregator.
     */
    public ChannelAccessSampleDecimator(String channelName,
            long intervalStartTime, long intervalLength,
            ChannelAccessSampleValueAccess sampleValueAccess) {
        super(channelName, intervalStartTime, intervalLength);
        this.sampleValueAccess = sampleValueAccess;
    }

    @Override
    public ChannelAccessSample getDecimatedSample() {
        // We use an assertion for checking that we have a decimated sample.
        // When the regular call order is kept, this should always
        // be true, so we do not use a precondition.
        assert (decimatedSample != null);
        return decimatedSample;
    }

    @Override
    public int getDecimatedSampleEstimatedSize() {
        // We use an assertion for checking that we have a decimated sample.
        // When the regular call order is kept, this should always
        // be true, so we do not use a precondition.
        assert (decimatedSample != null);
        return decimatedSampleSize;
    }

    @Override
    protected void buildDecimatedSampleInternal() {
        // We use an assertion for checking that we have at least one
        // aggregator. When the regular call order is kept, this should always
        // be true, so we do not use a precondition.
        assert (!typeToAggregator.isEmpty());
        // If we only have one aggregator, things are very simple: We can simply
        // use this aggregator. Luckily, this is going to be the most common
        // case because in general the original samples are going to be
        // homogeneous.
        // If there is more than one aggregator, things get more difficult.
        // Typically, we simply use the aggregator that is responsible for the
        // type that we found first. However, if all types are aggregatable
        // (they produce AGGREGATED_ samples) and the only non-aggregatable type
        // that is present is the DISABLED or DISCONNECTED type, we prefer to
        // use the aggregatable type that covers the greatest fraction of the
        // interval.
        if (typeToAggregator.size() == 1) {
            decimatedSample = typeToAggregator.values().iterator().next()
                    .getAggregatedSample();
        } else {
            long longestCoveredPeriod = 0;
            ChannelAccessSampleType longestCoveredPeriodType = null;
            for (ChannelAccessSampleType type : typeToAggregator.keySet()) {
                boolean aggregatableType;
                switch (type) {
                case AGGREGATED_SCALAR_CHAR:
                case AGGREGATED_SCALAR_DOUBLE:
                case AGGREGATED_SCALAR_FLOAT:
                case AGGREGATED_SCALAR_LONG:
                case AGGREGATED_SCALAR_SHORT:
                case SCALAR_CHAR:
                case SCALAR_DOUBLE:
                case SCALAR_FLOAT:
                case SCALAR_LONG:
                case SCALAR_SHORT:
                    aggregatableType = true;
                    long coveredPeriod = ((AveragingSampleAggregator) typeToAggregator
                            .get(type)).getCoveredPeriod();
                    if (coveredPeriod > longestCoveredPeriod) {
                        longestCoveredPeriod = coveredPeriod;
                        longestCoveredPeriodType = type;
                    }
                    break;
                case DISABLED:
                case DISCONNECTED:
                    // We do not want the presence of a disabled or disconnected
                    // sample to keep us from generating an aggregated sample.
                    continue;
                default:
                    // If there is at least one sample that is not of an
                    // aggregatable type and that is not a disconnected sample,
                    // we switch to the default behavior of picking the first
                    // sample.
                    aggregatableType = false;
                    break;
                }
                if (!aggregatableType) {
                    longestCoveredPeriodType = null;
                    break;
                }
            }
            decimatedSample = typeToAggregator
                    .get((longestCoveredPeriodType != null) ? longestCoveredPeriodType
                            : firstType).getAggregatedSample();
        }
        // The sample returned by the aggregator should have the start time of
        // the interval as its time stamp.
        assert (decimatedSample.getTimeStamp() == getIntervalStartTime());
        switch (decimatedSample.getType()) {
        case AGGREGATED_SCALAR_CHAR:
        case AGGREGATED_SCALAR_DOUBLE:
        case AGGREGATED_SCALAR_FLOAT:
        case AGGREGATED_SCALAR_LONG:
        case AGGREGATED_SCALAR_SHORT:
            decimatedSampleSize = SampleSizeEstimator
                    .estimateAggregatedSampleSize((ChannelAccessAggregatedSample) decimatedSample);
            break;
        case ARRAY_CHAR:
        case ARRAY_DOUBLE:
        case ARRAY_ENUM:
        case ARRAY_FLOAT:
        case ARRAY_LONG:
        case ARRAY_SHORT:
        case ARRAY_STRING:
        case SCALAR_CHAR:
        case SCALAR_DOUBLE:
        case SCALAR_ENUM:
        case SCALAR_FLOAT:
        case SCALAR_LONG:
        case SCALAR_SHORT:
        case SCALAR_STRING:
            decimatedSampleSize = SampleSizeEstimator
                    .estimateRawSampleSize((ChannelAccessRawSample) decimatedSample);
            break;
        case DISABLED:
            decimatedSampleSize = SampleSizeEstimator
                    .estimateDisabledSampleSize();
            break;
        case DISCONNECTED:
            decimatedSampleSize = SampleSizeEstimator
                    .estimateDisconnectedSampleSize();
            break;
        default:
            break;
        }
    }

    @Override
    protected void processSampleInternal(ChannelAccessSample sample,
            long startTime, long endTime) {
        ChannelAccessSampleType sampleType = sample.getType();
        if (firstType == null) {
            firstType = sampleType;
        }
        SampleAggregator aggregator;
        aggregator = typeToAggregator.get(sampleType);
        if (aggregator == null) {
            switch (sampleType) {
            case AGGREGATED_SCALAR_CHAR:
                aggregator = new AggregatedScalarCharAggregator(
                        getIntervalStartTime(), getIntervalLength(),
                        sampleValueAccess);
                break;
            case AGGREGATED_SCALAR_DOUBLE:
                aggregator = new AggregatedScalarDoubleAggregator(
                        getIntervalStartTime(), getIntervalLength(),
                        sampleValueAccess);
                break;
            case AGGREGATED_SCALAR_FLOAT:
                aggregator = new AggregatedScalarFloatAggregator(
                        getIntervalStartTime(), getIntervalLength(),
                        sampleValueAccess);
                break;
            case AGGREGATED_SCALAR_LONG:
                aggregator = new AggregatedScalarLongAggregator(
                        getIntervalStartTime(), getIntervalLength(),
                        sampleValueAccess);
                break;
            case AGGREGATED_SCALAR_SHORT:
                aggregator = new AggregatedScalarShortAggregator(
                        getIntervalStartTime(), getIntervalLength(),
                        sampleValueAccess);
                break;
            case SCALAR_CHAR:
                aggregator = new ScalarCharAggregator(getIntervalStartTime(),
                        getIntervalLength(), sampleValueAccess);
                break;
            case SCALAR_DOUBLE:
                aggregator = new ScalarDoubleAggregator(getIntervalStartTime(),
                        getIntervalLength(), sampleValueAccess);
                break;
            case SCALAR_FLOAT:
                aggregator = new ScalarFloatAggregator(getIntervalStartTime(),
                        getIntervalLength(), sampleValueAccess);
                break;
            case SCALAR_LONG:
                aggregator = new ScalarLongAggregator(getIntervalStartTime(),
                        getIntervalLength(), sampleValueAccess);
                break;
            case SCALAR_SHORT:
                aggregator = new ScalarShortAggregator(getIntervalStartTime(),
                        getIntervalLength(), sampleValueAccess);
                break;
            default:
                aggregator = new PickFirstSampleAggregator(
                        getIntervalStartTime());
                break;

            }
            typeToAggregator.put(sampleType, aggregator);
        }
        aggregator.processSample(sample, startTime, endTime);
    }

}
