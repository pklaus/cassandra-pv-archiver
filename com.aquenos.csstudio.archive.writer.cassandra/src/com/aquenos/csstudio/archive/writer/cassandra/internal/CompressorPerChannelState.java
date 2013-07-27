/*
 * Copyright 2013 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.csstudio.archive.writer.cassandra.internal;

import java.util.Map;

import org.csstudio.data.values.ISeverity;
import org.csstudio.data.values.ITimestamp;
import org.csstudio.data.values.IValue;

import com.aquenos.csstudio.archive.cassandra.Sample;

/**
 * Stores state information for a channel that needs to be retained between
 * consecutive runs of the sample compressor.
 * 
 * @author Sebastian Marsching
 * 
 */
public class CompressorPerChannelState {

    /**
     * Stores state that is common to all types of samples.
     */
    public static class GeneralState {
        ITimestamp start;
        ITimestamp end;
        ITimestamp nextSampleTime;
        boolean skippedLastSample = false;
        int insertCounter = 0;
        ISeverity maxSeverity;
        IValue centerValue;
        boolean haveNewData = false;
        public Sample sourceSample;
        public Sample lastSourceSample;
        public Sample lastCompressedSample;
        boolean firstRun = true;

        public void reset() {
            start = null;
            end = null;
            nextSampleTime = null;
            skippedLastSample = false;
            insertCounter = 0;
            maxSeverity = null;
            centerValue = null;
            haveNewData = false;
            sourceSample = null;
            lastSourceSample = null;
            lastCompressedSample = null;
            firstRun = true;
        }
    }

    /**
     * Stores state that is specific to numeric samples.
     */
    public static class NumericState {
        Double doubleMin;
        Double doubleMax;
        double[] doubleSum;

        public void reset() {
            doubleMin = null;
            doubleMax = null;
            doubleSum = null;
        }
    }

    /**
     * Stores all state for a compression level.
     */
    public static class CompressorPerLevelState {
        /**
         * Compression period that is used as the source for this compression
         * period. This is not really state information, but it needs to be
         * calculated once at startup ant the most convenient place to store it
         * is here.
         */
        public long sourceCompressionPeriod;
        /**
         * Retention period for this compression level. We cache this here, so
         * that we do not have to retrieve the configuration on each run.
         */
        public long retentionPeriod;
        public GeneralState generalState;
        public NumericState numericState;
    }

    /**
     * Map storing the state for all compression levels of the channel.
     */
    public Map<Long, CompressorPerLevelState> perLevelStates;

}
