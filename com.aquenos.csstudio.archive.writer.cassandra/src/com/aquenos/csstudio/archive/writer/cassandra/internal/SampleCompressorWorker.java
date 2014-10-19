/*
 * Copyright 2012-2013 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.csstudio.archive.writer.cassandra.internal;

import java.util.Arrays;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.csstudio.data.values.IDoubleValue;
import org.csstudio.data.values.IEnumeratedMetaData;
import org.csstudio.data.values.IEnumeratedValue;
import org.csstudio.data.values.ILongValue;
import org.csstudio.data.values.IMetaData;
import org.csstudio.data.values.IMinMaxDoubleValue;
import org.csstudio.data.values.INumericMetaData;
import org.csstudio.data.values.ISeverity;
import org.csstudio.data.values.IStringValue;
import org.csstudio.data.values.ITimestamp;
import org.csstudio.data.values.IValue;
import org.csstudio.data.values.IValue.Quality;
import org.csstudio.data.values.TimestampFactory;
import org.csstudio.data.values.ValueFactory;

import com.aquenos.csstudio.archive.cassandra.Sample;
import com.aquenos.csstudio.archive.cassandra.SampleStore;
import com.aquenos.csstudio.archive.cassandra.util.TimestampArithmetics;
import com.aquenos.csstudio.archive.cassandra.util.astyanax.NotifyingMutationBatch;
import com.aquenos.csstudio.archive.cassandra.util.astyanax.WrappedNotifyingMutationBatch;
import com.aquenos.csstudio.archive.writer.cassandra.internal.CompressorPerChannelState.CompressorPerLevelState;
import com.aquenos.csstudio.archive.writer.cassandra.internal.CompressorPerChannelState.GeneralState;
import com.aquenos.csstudio.archive.writer.cassandra.internal.CompressorPerChannelState.NumericState;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ConsistencyLevel;

/**
 * Calculates compressed samples and deletes old samples. This class is only
 * intended for internal use by the classes in the same bundle.
 * 
 * @author Sebastian Marsching
 */
public class SampleCompressorWorker implements Runnable {
    private final ITimestamp ONE_NANOSECOND = TimestampFactory.createTimestamp(
            0L, 1L);

    private final Logger logger = Logger.getLogger(WriterBundle.NAME);
    private Keyspace keyspace;
    private ConsistencyLevel writeDataConsistencyLevel;
    private SampleStore sampleStore;
    private SampleCompressor sampleCompressor;
    private BlockingQueue<CompressionRequest> receiveCompressionRequestQueue;
    private BlockingQueue<CompressionResponse> sendCompressionResponseQueue;

    private enum ValueType {
        DOUBLE, ENUM, LONG, STRING
    }

    private class SampleIterator implements Iterator<Sample> {

        private Sample next;
        private Deque<Sample> sampleQueue;
        private Iterator<Sample> sampleIterator;

        public SampleIterator(Deque<Sample> sampleQueue,
                Iterator<Sample> sampleIterator) {
            this.sampleQueue = sampleQueue;
            this.sampleIterator = sampleIterator;
        }

        @Override
        public boolean hasNext() {
            if (next != null) {
                return true;
            }
            if (sampleIterator != null && sampleIterator.hasNext()) {
                next = sampleIterator.next();
                if (sampleQueue.isEmpty()
                        || next.getValue()
                                .getTime()
                                .isLessThan(
                                        sampleQueue.peek().getValue().getTime())) {
                    return true;
                } else {
                    next = null;
                    sampleIterator = null;
                }
            }
            if (!sampleQueue.isEmpty()) {
                next = sampleQueue.poll();
                return true;
            }
            return false;
        }

        @Override
        public Sample next() {
            if (next != null) {
                Sample sample = next;
                next = null;
                return sample;
            } else {
                throw new NoSuchElementException();
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

    }

    public SampleCompressorWorker(Keyspace keyspace,
            ConsistencyLevel writeDataConsistencyLevel,
            SampleStore sampleStore, SampleCompressor sampleCompressor,
            BlockingQueue<CompressionRequest> receiveCompressionRequestQueue,
            BlockingQueue<CompressionResponse> sendCompressionResponseQueue) {
        this.keyspace = keyspace;
        this.writeDataConsistencyLevel = writeDataConsistencyLevel;
        this.sampleStore = sampleStore;
        this.sampleCompressor = sampleCompressor;
        this.receiveCompressionRequestQueue = receiveCompressionRequestQueue;
        this.sendCompressionResponseQueue = sendCompressionResponseQueue;
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            CompressionRequest request;
            try {
                request = receiveCompressionRequestQueue.take();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                // We can return, because we did not take a channel name from
                // the queue, so there is nothing left to do.
                return;
            }
            boolean exceptionOccurred = true;
            try {
                processChannel(request);
                exceptionOccurred = false;
            } catch (Throwable e) {
                // Log exception and continue with next channel
                logger.log(Level.WARNING,
                        "Error while compressing samples for channel "
                                + request.channelName + ": " + e.getMessage(),
                        e);
            } finally {
                // Notify the management thread that the processing of the
                // channel has finished and the channel might be processed
                // again.
                CompressionResponse response = new CompressionResponse();
                response.request = request;
                response.success = !exceptionOccurred;
                sendCompressionResponseQueue.add(response);
            }
        }
    }

    private void processChannel(CompressionRequest request)
            throws ConnectionException {
        String channelName = request.channelName;
        CompressorPerChannelState channelState = request.compressorState;
        for (Map.Entry<Long, CompressorPerLevelState> entry : channelState.perLevelStates
                .entrySet()) {
            long compressionPeriod = entry.getKey();
            CompressorPerLevelState compressionState = entry.getValue();
            // Only compression levels with a compression period greater than
            // zero store compressed samples.
            if (compressionPeriod > 0L) {
                // Generate compressed samples for this compression level and
                // channel.
                boolean exceptionOccurred = true;
                try {
                    long sourceCompressionPeriod = compressionState.sourceCompressionPeriod;
                    Boolean sourceSamplesLost = request.sourceSamplesLost
                            .get(sourceCompressionPeriod);
                    if (sourceSamplesLost == null) {
                        sourceSamplesLost = false;
                    }
                    compressChannel(
                            channelName,
                            compressionPeriod,
                            sourceCompressionPeriod,
                            new LinkedList<Sample>(request.sourceSamples
                                    .get(sourceCompressionPeriod)),
                            sourceSamplesLost, compressionState.generalState,
                            compressionState.numericState);
                    exceptionOccurred = false;
                } finally {
                    if (exceptionOccurred) {
                        compressionState.generalState.reset();
                        compressionState.numericState.reset();
                    }
                }
            }
            // Delete old samples if retention is enabled.
            if (request.deleteOldSamples
                    && compressionState.retentionPeriod > 0) {
                deleteOldSamples(channelName, compressionPeriod,
                        compressionState.retentionPeriod);
            }
        }
    }

    private void deleteOldSamples(String channelName, long compressionPeriod,
            long retentionPeriod) throws ConnectionException {
        ITimestamp end = TimestampFactory.createTimestamp(sampleStore
                .getLastSampleTimestamp(compressionPeriod, channelName)
                .seconds()
                - retentionPeriod, 0L);
        if (end.seconds() < 0 || (end.seconds() == 0 && end.nanoseconds() == 0)) {
            return;
        }
        sampleStore.deleteSamples(compressionPeriod, channelName, end);
    }

    private void compressChannel(String channelName, long compressionPeriod,
            long sourceCompressionPeriod, Deque<Sample> sourceSamples,
            boolean sourceSamplesLost, GeneralState generalState,
            NumericState numericState) throws ConnectionException {
        // The two state objects are reused between invocations, however we have
        // to reset the insert counter, because the mutation batch is always
        // executed at the end of an invocation.
        generalState.insertCounter = 0;
        // If this is the first time that this method is run, we have to
        // initialize some state information.
        if (generalState.firstRun) {
            // Ensure that the state is reset in case it was initialized
            // incompletely.
            generalState.reset();
            numericState.reset();
            ITimestamp compressionPeriodAsTimestamp = TimestampFactory
                    .createTimestamp(compressionPeriod, 0L);
            ITimestamp halfCompressionPeriodAsTimestamp = TimestampArithmetics
                    .divide(compressionPeriodAsTimestamp, 2);
            ITimestamp lastSampleTime = sampleStore.getLastSampleTimestamp(
                    compressionPeriod, channelName);
            // We have to determine the timestamp of the next compressed sample
            // to be calculated.
            if (lastSampleTime != null) {
                generalState.nextSampleTime = TimestampArithmetics.add(
                        lastSampleTime, compressionPeriodAsTimestamp);
                // We want to make sure that the next sample time is aligned,
                // even if for some reason we have an unaligned sample in the
                // database.
                generalState.nextSampleTime = alignNextSampleTime(
                        generalState.nextSampleTime, compressionPeriod);
            } else {
                // calculateNextSampleTime already takes care of aligning the
                // time, so we do not have to do this here.
                generalState.nextSampleTime = calculateNextSampleTime(
                        channelName, compressionPeriod, sourceCompressionPeriod);
            }
            if (generalState.nextSampleTime == null) {
                // If there are no source samples and no compressed samples have
                // been stored yet, the nextSampleTime is null. In this case we
                // cannot create a compressed sample and just return.
                return;
            }
            // For calculating the compressed sample we need source samples
            // starting half a compression period before the compressed sample.
            // In addition to that, we need one earlier sample, so that we know
            // the source sample value for the complete compression period
            // (otherwise a period in the start would be missing, if the first
            // sample was not exactly at the start).
            do {
                generalState.start = TimestampArithmetics.substract(
                        generalState.nextSampleTime,
                        halfCompressionPeriodAsTimestamp);
                generalState.lastSourceSample = getFirst(sampleStore
                        .findSamples(sourceCompressionPeriod, channelName,
                                generalState.start, null, 1, true));
                if (generalState.lastSourceSample == null) {
                    // There is no sample that is old enough. This can happen if
                    // we already have compressed samples and relatively new
                    // source samples have been deleted. In this case, we
                    // calculate the time for the next compressed sample based
                    // on the timestamp of the oldest available source sample.
                    generalState.nextSampleTime = calculateNextSampleTime(
                            channelName, compressionPeriod,
                            sourceCompressionPeriod);
                    if (generalState.nextSampleTime == null) {
                        // There are no source samples, thus we cannot calculate
                        // any compressed samples.
                        return;
                    }
                }
            } while (generalState.lastSourceSample == null);
            // The end of the compression period is defined by the start plus
            // the compression period. We update the start and end in order to
            // reflect the compressed sample currently being built while we
            // iterate over the source samples.
            generalState.end = TimestampArithmetics.add(generalState.start,
                    compressionPeriodAsTimestamp);
            // We also need the last compressed sample in order to decide
            // whether the value has changed and thus the new compressed sample
            // should be saved. It is okay, if we do not find any such sample.
            // We just save the new compressed sample without comparing it.
            generalState.lastCompressedSample = getFirst(sampleStore
                    .findSamples(compressionPeriod, channelName,
                            TimestampArithmetics
                                    .substract(generalState.nextSampleTime,
                                            ONE_NANOSECOND), null, 1, true));
        }
        // Now that we have a sample before the compression period, we can start
        // collecting all the samples in the compression period and create the
        // compressed sample. We do this until we run out of source samples.
        NotifyingMutationBatch mutationBatch = new WrappedNotifyingMutationBatch(
                keyspace.prepareMutationBatch().withConsistencyLevel(
                        writeDataConsistencyLevel));
        // We use a virtual iterator that combines the data from the database
        // with the data we got from the queue.
        ITimestamp queryStart;
        if (generalState.sourceSample == null) {
            queryStart = TimestampArithmetics.add(generalState.lastSourceSample
                    .getValue().getTime(), ONE_NANOSECOND);
        } else {
            queryStart = TimestampArithmetics.add(generalState.sourceSample
                    .getValue().getTime(), ONE_NANOSECOND);
        }
        // We have to remove source samples from the queue that are too old.
        // This can happen because samples might have been added to the queue,
        // while we were already reading them from the database.
        while (!sourceSamples.isEmpty()
                && sourceSamples.peek().getValue().getTime()
                        .isLessThan(queryStart)) {
            sourceSamples.poll();
        }
        Iterator<Sample> sourceSampleIterator;
        if (generalState.firstRun || sourceSamplesLost) {
            ITimestamp queryEnd;
            if (sourceSamples.isEmpty()) {
                queryEnd = null;
            } else {
                queryEnd = sourceSamples.peek().getValue().getTime();
            }
            Iterator<Sample> sourceSampleFromStoreIterator = sampleStore
                    .findSamples(sourceCompressionPeriod, channelName,
                            queryStart, queryEnd, -1, false).iterator();
            sourceSampleIterator = new SampleIterator(sourceSamples,
                    sourceSampleFromStoreIterator);
        } else {
            sourceSampleIterator = new SampleIterator(sourceSamples, null);
        }
        // Now we have initialized everything and do not have to do this again
        // the next time.
        generalState.firstRun = false;

        // Now we can actually actually process the samples.
        processSourceSamples(channelName, compressionPeriod, generalState,
                numericState, mutationBatch, sourceSampleIterator);

        // Finally we want to execute the remaining inserts in the mutation
        // batch.
        mutationBatch.execute();
    }

    private void processSourceSamples(String channelName,
            long compressionPeriod, GeneralState generalState,
            NumericState numericState, NotifyingMutationBatch mutationBatch,
            Iterator<Sample> sourceSampleIterator) throws ConnectionException {
        ITimestamp compressionPeriodAsTimestamp = TimestampFactory
                .createTimestamp(compressionPeriod, 0L);
        while (sourceSampleIterator.hasNext()) {
            // We only want to increment the iterator if the next sample is
            // the first sample, or if the current sample is within the current
            // compression period. If the current sample is ahead of the current
            // compression period, the start and end of the compression period
            // will be incremented below, when we insert the compressed sample.
            // Thus, the timestamp of the current sample will be less than the
            // end of the compression period in one of the future iterations.
            if (generalState.sourceSample == null) {
                generalState.sourceSample = sourceSampleIterator.next();
            } else if (generalState.sourceSample.getValue().getTime()
                    .isLessThan(generalState.end)) {
                // If sourceSample was already set, we want to save it in
                // lastSourceSample.
                generalState.lastSourceSample = generalState.sourceSample;
                generalState.sourceSample = sourceSampleIterator.next();
            }
            // If the new source sample is still before the compression period
            // we want to calculate, we skip it. This way the newest sample
            // right before the compression period will be in lastSourceSample.
            IValue sourceValue = generalState.sourceSample.getValue();
            if (sourceValue.getTime().isLessOrEqual(generalState.start)) {
                generalState.lastSourceSample = generalState.sourceSample;
                continue;
            }

            IValue lastSourceValue = generalState.lastSourceSample.getValue();
            ValueType lastValueType = getValueType(lastSourceValue);

            // First we handle the generic properties of the last sample.
            // We update our intermediate value when the last value was in the
            // current or the last compression period or the next sample is in
            // the current compression period. We have to do the latter because
            // the older sample might still contribute to the compressed value
            // of the current compression period.
            if (lastSourceValue.getTime().isGreaterThan(
                    TimestampArithmetics.substract(generalState.start,
                            compressionPeriodAsTimestamp))
                    || sourceValue.getTime().isLessThan(generalState.end)) {
                updateGeneralState(generalState, lastSourceValue);
                // The numeric state is only updated if the value-type of
                // the last sample is numeric.
                if (isNumericType(lastValueType)) {
                    updateNumericState(numericState, lastSourceValue,
                            sourceValue.getTime(), generalState.start,
                            generalState.end);
                }
            }

            // If we have a sample that is greater than or equal to the current
            // period end, we can calculate the compressed sample for the
            // current period.
            if (sourceValue.getTime().isGreaterOrEqual(generalState.end)) {
                // Insert compressed sample based on the aggregated data.
                insertCompressedSample(mutationBatch, generalState,
                        numericState, lastValueType, channelName,
                        compressionPeriod);
                // Reset state data.
                resetGeneralState(generalState);
                resetNumericState(numericState);
                // If the last sample's validity extends into the next
                // compression period, we have to take it into account. This
                // basically is the same situation as the sample before the
                // first compression period.
                // However, we do not add the last sample to the intermediate
                // sum, when the time of the current sample is later then the
                // end of the current compression period. In this case the
                // sample iterator will not be incremented at the start of the
                // next iteration and thus the last sample will be added to the
                // intermediate sum. If we also added it here, we would add it
                // twice and thus generate a bogus sum.
                if (sourceValue.getTime().isGreaterThan(generalState.start)
                        && sourceValue.getTime().isLessThan(generalState.end)) {
                    // We update our intermediate value when the last value was
                    // in the current or the last compression period or the next
                    // sample is in the current compression period. We have to
                    // do the latter because the older sample might still
                    // contribute to the compressed value of the current
                    // compression period.
                    if (lastSourceValue.getTime().isGreaterThan(
                            TimestampArithmetics.substract(generalState.start,
                                    compressionPeriodAsTimestamp))
                            || sourceValue.getTime().isLessThan(
                                    generalState.end)) {
                        updateGeneralState(generalState, lastSourceValue);
                        // The numeric state is only updated if the value-type
                        // of the last sample is numeric.
                        if (isNumericType(lastValueType)) {
                            updateNumericState(numericState, lastSourceValue,
                                    sourceValue.getTime(), generalState.start,
                                    generalState.end);
                        }
                    }
                }
            }

            // Check whether the value type has changed. We are only interested
            // in this change, if it happens within the compression period that
            // we are currently calculating. We cannot do this in an else branch
            // of the if-statement right above, because the end time might have
            // changed within the if-block.
            if (sourceValue.getTime().isLessThan(generalState.end)
                    && !getValueType(sourceValue).equals(lastValueType)) {
                // The value type changed. This can happen if the channel is
                // connected to a different device. If the type changes,
                // averaging over the samples of different types does not make
                // sense. If we already passed the timestamp of the compressed
                // sample to be calculated, we calculate the have not passed it
                // yet, we calculate the compressed sample based on the data to
                // come.
                if (generalState.nextSampleTime.isLessThan(sourceValue
                        .getTime())) {
                    // Insert compressed sample based on the data we have
                    // aggregated so far.
                    generalState.end = sourceValue.getTime();
                    insertCompressedSample(mutationBatch, generalState,
                            numericState, lastValueType, channelName,
                            compressionPeriod);
                    // Reset state data. We do not update the state because this
                    // will happen in the next iteration (or a subsequent
                    // iteration if there are several samples which belong to
                    // the old compression period.
                    resetGeneralState(generalState);
                    resetNumericState(numericState);
                    //
                } else {
                    // Drop the data we have already collected and continue with
                    // the current sample.
                    resetGeneralState(generalState);
                    resetNumericState(numericState);
                    // We do not update the state because this will in the next
                    // iteration.
                    generalState.start = sourceValue.getTime();
                }
            }

            // We want to execute the mutation batch every 5000 samples, so that
            // the batch does not grow too big.
            if (generalState.insertCounter != 0
                    && generalState.insertCounter % 5000 == 0) {
                mutationBatch.execute();
            }
        }
    }

    private ITimestamp calculateNextSampleTime(String channelName,
            long compressionPeriod, long sourceCompressionPeriod)
            throws ConnectionException {
        ITimestamp compressionPeriodAsTimestamp = TimestampFactory
                .createTimestamp(compressionPeriod, 0L);
        ITimestamp halfCompressionPeriodAsTimestamp = TimestampArithmetics
                .divide(compressionPeriodAsTimestamp, 2);
        ITimestamp nextSampleTime;
        Sample firstSourceSample = getFirst(sampleStore.findSamples(
                sourceCompressionPeriod, channelName, null, null, 1, false));
        if (firstSourceSample == null) {
            // There are no source samples, thus there is no valid next
            // timestamp.
            return null;
        }
        ITimestamp sourceSampleTime = firstSourceSample.getValue().getTime();
        // A compressed sample needs source samples for at least half the
        // compression period before the compressed samples time. This means
        // we can add half the compression period to the time of the first
        // source sample and the result is the minimum time for the
        // compressed sample.
        nextSampleTime = TimestampArithmetics.add(sourceSampleTime,
                halfCompressionPeriodAsTimestamp);
        // We want to make sure that samples for different channels are
        // nicely aligned in time.
        nextSampleTime = alignNextSampleTime(nextSampleTime, compressionPeriod);
        return nextSampleTime;
    }

    private ITimestamp alignNextSampleTime(ITimestamp nextSampleTime,
            long compressionPeriod) {
        // The sample times for compressed samples should be aligned, so that
        // their time relative to the start of epoch (January 1st, 1970,
        // 00:00:00 UTC) is always an integer multiple of the compression
        // period.
        long seconds = nextSampleTime.seconds();
        if (nextSampleTime.nanoseconds() > 0L) {
            seconds += 1;
        }
        long remainingSeconds = seconds % compressionPeriod;
        if (remainingSeconds != 0) {
            seconds += compressionPeriod - remainingSeconds;
        }
        return TimestampFactory.createTimestamp(seconds, 0L);
    }

    private void insertCompressedSample(NotifyingMutationBatch mutationBatch,
            GeneralState generalState, NumericState numericState,
            ValueType lastValueType, String channelName, long compressionPeriod)
            throws ConnectionException {
        ITimestamp compressionPeriodAsTimestamp = TimestampFactory
                .createTimestamp(compressionPeriod, 0L);
        ITimestamp halfCompressionPeriodAsTimestamp = TimestampArithmetics
                .divide(compressionPeriodAsTimestamp, 2);
        // Calculate the compressed value.
        IValue compressedValue;
        ITimestamp nextSampleTime = generalState.nextSampleTime;
        if (!generalState.haveNewData
                && generalState.lastCompressedSample != null) {
            // If we do not have any new data, we can save some time by just
            // using the values from the last compressed sample and only
            // updating the timestamp. This can save considerable time when we
            // are skipping a long period without source samples.
            IValue lastValue = generalState.lastCompressedSample.getValue();
            if (lastValue instanceof IMinMaxDoubleValue) {
                IMinMaxDoubleValue value = (IMinMaxDoubleValue) lastValue;
                compressedValue = ValueFactory.createMinMaxDoubleValue(
                        nextSampleTime, value.getSeverity(), value.getStatus(),
                        (INumericMetaData) value.getMetaData(),
                        value.getQuality(), value.getValues(),
                        value.getMinimum(), value.getMaximum());
            } else if (lastValue instanceof IDoubleValue) {
                IDoubleValue value = (IDoubleValue) lastValue;
                compressedValue = ValueFactory.createDoubleValue(
                        nextSampleTime, value.getSeverity(), value.getStatus(),
                        (INumericMetaData) value.getMetaData(),
                        value.getQuality(), value.getValues());
            } else if (lastValue instanceof IEnumeratedValue) {
                IEnumeratedValue value = (IEnumeratedValue) lastValue;
                compressedValue = ValueFactory.createEnumeratedValue(
                        nextSampleTime, value.getSeverity(), value.getStatus(),
                        value.getMetaData(), value.getQuality(),
                        value.getValues());
            } else if (lastValue instanceof ILongValue) {
                ILongValue value = (ILongValue) lastValue;
                compressedValue = ValueFactory.createLongValue(nextSampleTime,
                        value.getSeverity(), value.getStatus(),
                        (INumericMetaData) value.getMetaData(),
                        value.getQuality(), value.getValues());
            } else if (lastValue instanceof IStringValue) {
                IStringValue value = (IStringValue) lastValue;
                compressedValue = ValueFactory.createStringValue(
                        nextSampleTime, value.getSeverity(), value.getStatus(),
                        value.getQuality(), value.getValues());
            } else {
                // We should never get a different type, because we created the
                // original values ourselves.
                throw new RuntimeException("IValue of unhandled type "
                        + lastValue.getClass().getName());
            }
        } else {
            // We processed data, thus we have to calculate the next compressed
            // sample.
            ISeverity maxSeverity = generalState.maxSeverity;
            if (isNumericType(lastValueType) && numericState.doubleSum != null
                    && numericState.doubleSum.length != 0) {
                // We calculate the average by dividing the sum by the
                // period. We cannot use the compression period here,
                // because the start might have been readjusted due to value
                // type changes.
                ITimestamp averageTime = TimestampArithmetics.substract(
                        generalState.end, generalState.start);
                double[] doubleAverage = divide(numericState.doubleSum,
                        averageTime.toDouble());
                IMetaData metaData = generalState.centerValue.getMetaData();
                INumericMetaData numericMetaData = null;
                if (metaData != null && metaData instanceof INumericMetaData) {
                    numericMetaData = (INumericMetaData) metaData;
                }
                compressedValue = ValueFactory.createMinMaxDoubleValue(
                        generalState.nextSampleTime, generalState.maxSeverity,
                        "<averaged>", numericMetaData, Quality.Interpolated,
                        doubleAverage, numericState.doubleMin,
                        numericState.doubleMax);
            } else {
                // Either we got a non-numeric type, or all values were
                // empty arrays. In both cases, averaging does not make
                // sense and we just use the sample that is valid for our
                // compressed sample time.
                IValue value = generalState.centerValue;
                IMetaData metaData = value.getMetaData();
                INumericMetaData numericMetaData = null;
                IEnumeratedMetaData enumMetaData = null;
                if (metaData != null && metaData instanceof INumericMetaData) {
                    numericMetaData = (INumericMetaData) metaData;
                } else if (metaData != null
                        && metaData instanceof IEnumeratedMetaData) {
                    enumMetaData = (IEnumeratedMetaData) metaData;
                }
                if (value instanceof IDoubleValue) {
                    compressedValue = ValueFactory.createDoubleValue(
                            nextSampleTime, maxSeverity, "<compressed>",
                            numericMetaData, Quality.Interpolated,
                            ((IDoubleValue) value).getValues());
                } else if (value instanceof IEnumeratedValue) {
                    compressedValue = ValueFactory.createEnumeratedValue(
                            nextSampleTime, maxSeverity, "<compressed>",
                            enumMetaData, Quality.Interpolated,
                            ((IEnumeratedValue) value).getValues());
                } else if (value instanceof ILongValue) {
                    compressedValue = ValueFactory.createLongValue(
                            nextSampleTime, maxSeverity, "<compressed>",
                            numericMetaData, Quality.Interpolated,
                            ((ILongValue) value).getValues());
                } else if (value instanceof IStringValue) {
                    compressedValue = ValueFactory.createStringValue(
                            nextSampleTime, maxSeverity, "<compressed>",
                            Quality.Interpolated,
                            ((IStringValue) value).getValues());
                } else {
                    compressedValue = null;
                }
            }
        }
        if (compressedValue != null) {
            if (generalState.haveNewData
                    && (generalState.lastCompressedSample == null || !approximatelyEquals(
                            generalState.lastCompressedSample.getValue(),
                            compressedValue))) {
                // If we skipped the last compressed sample and now the next
                // compressed sample has changed, we also insert the last
                // skipped sample. We have to do this, because the trend tool in
                // CSS interpolates the minimum / maximum limits, which leads to
                // a funny display (value is not interpolated).
                if (generalState.skippedLastSample) {
                    sampleStore.insertSample(mutationBatch, compressionPeriod,
                            channelName,
                            generalState.lastCompressedSample.getValue());
                    generalState.insertCounter++;
                    sampleCompressor
                            .queueSample(generalState.lastCompressedSample);
                }
                sampleStore.insertSample(mutationBatch, compressionPeriod,
                        channelName, compressedValue);
                generalState.insertCounter++;
                sampleCompressor.queueSample(new Sample(compressionPeriod,
                        channelName, compressedValue));
                generalState.skippedLastSample = false;
            } else {
                generalState.skippedLastSample = true;
            }
            // Even if we did not insert the sample, we want to use the new
            // timestamp, so that we have the right timestamp if we insert it
            // later.
            generalState.lastCompressedSample = new Sample(compressionPeriod,
                    channelName, compressedValue);
        }
        // We update the timestamps so that they point to the next compression
        // period.
        generalState.nextSampleTime = TimestampFactory.createTimestamp(
                nextSampleTime.seconds() + compressionPeriod, 0);
        generalState.start = TimestampArithmetics.substract(
                generalState.nextSampleTime, halfCompressionPeriodAsTimestamp);
        generalState.end = TimestampArithmetics.add(
                generalState.nextSampleTime, halfCompressionPeriodAsTimestamp);
    }

    private void updateGeneralState(GeneralState generalState,
            IValue sourceValue) {
        if (sourceValue.getTime().isLessOrEqual(generalState.nextSampleTime)) {
            generalState.centerValue = sourceValue;
        }
        generalState.maxSeverity = maximizeSeverity(generalState.maxSeverity,
                sourceValue.getSeverity());
        generalState.haveNewData = true;
    }

    private void updateNumericState(NumericState numericState,
            IValue sourceValue, ITimestamp nextTimestamp, ITimestamp start,
            ITimestamp end) {
        // Long values do not support minimum maximum, thus we even save the
        // sample as double if the original samples were longs.
        ITimestamp lastTime = sourceValue.getTime();
        if (lastTime.isLessThan(start)) {
            lastTime = start;
        }
        ITimestamp endTime = nextTimestamp;
        if (endTime.isGreaterThan(end)) {
            endTime = end;
        }
        ITimestamp diffTime = TimestampArithmetics.substract(endTime, lastTime);
        double[] lastDoubleValues = getDoubleValue(sourceValue);
        if (numericState.doubleSum != null) {
            numericState.doubleSum = add(numericState.doubleSum,
                    multiply(lastDoubleValues, diffTime.toDouble()));
        } else {
            numericState.doubleSum = multiply(lastDoubleValues,
                    diffTime.toDouble());
        }
        if (sourceValue instanceof IMinMaxDoubleValue) {
            // The source samples are already averaged samples. In this case, we
            // have to consider the minimum and maximum of the original values.
            IMinMaxDoubleValue minMaxValue = (IMinMaxDoubleValue) sourceValue;
            double sourceMin = minMaxValue.getMinimum();
            double sourceMax = minMaxValue.getMaximum();
            if (numericState.doubleMin == null
                    || numericState.doubleMax == null) {
                numericState.doubleMin = sourceMin;
                numericState.doubleMax = sourceMax;
            } else {
                numericState.doubleMin = Math.min(numericState.doubleMin,
                        sourceMin);
                numericState.doubleMax = Math.max(numericState.doubleMax,
                        sourceMax);
            }
        } else {
            numericState.doubleMin = getMin(lastDoubleValues,
                    numericState.doubleMin);
            numericState.doubleMax = getMax(lastDoubleValues,
                    numericState.doubleMax);
        }
    }

    private void resetGeneralState(GeneralState generalState) {
        // We do not change the start, end, nextSampleTimestamp and
        // skippedLastSample, because these fields are usually not reset but
        // just updated.
        generalState.centerValue = null;
        generalState.maxSeverity = null;
        generalState.haveNewData = false;
    }

    private void resetNumericState(NumericState numericState) {
        numericState.doubleSum = null;
        numericState.doubleMin = null;
        numericState.doubleMax = null;
    }

    private boolean approximatelyEquals(IValue value1, IValue value2) {
        ValueType valueType = getValueType(value1);
        if (!valueType.equals(getValueType(value2))) {
            return false;
        }
        switch (valueType) {
        case DOUBLE:
            IDoubleValue doubleValue1 = (IDoubleValue) value1;
            IDoubleValue doubleValue2 = (IDoubleValue) value2;
            if (!Arrays.equals(doubleValue1.getValues(),
                    doubleValue2.getValues())) {
                return false;
            }
            if (doubleValue1 instanceof IMinMaxDoubleValue) {
                if (!(doubleValue2 instanceof IMinMaxDoubleValue)) {
                    return false;
                }
                IMinMaxDoubleValue minMaxDoubleValue1 = (IMinMaxDoubleValue) value1;
                IMinMaxDoubleValue minMaxDoubleValue2 = (IMinMaxDoubleValue) value2;
                if (minMaxDoubleValue1.getMinimum() != minMaxDoubleValue2
                        .getMinimum()) {
                    return false;
                }
                if (minMaxDoubleValue1.getMaximum() != minMaxDoubleValue2
                        .getMaximum()) {
                    return false;
                }
            } else {
                if (doubleValue2 instanceof IMinMaxDoubleValue) {
                    return false;
                }
            }
            break;
        case ENUM:
            IEnumeratedValue enumValue1 = (IEnumeratedValue) value1;
            IEnumeratedValue enumValue2 = (IEnumeratedValue) value2;
            if (!Arrays.equals(enumValue1.getValues(), enumValue2.getValues())) {
                return false;
            }
            break;
        case LONG:
            ILongValue longValue1 = (ILongValue) value1;
            ILongValue longValue2 = (ILongValue) value2;
            if (!Arrays.equals(longValue1.getValues(), longValue2.getValues())) {
                return false;
            }
            break;
        case STRING:
            IStringValue stringValue1 = (IStringValue) value1;
            IStringValue stringValue2 = (IStringValue) value2;
            if (!Arrays.deepEquals(stringValue1.getValues(),
                    stringValue2.getValues())) {
                return false;
            }
        }
        IMetaData metaData1 = value1.getMetaData();
        IMetaData metaData2 = value2.getMetaData();
        if (metaData1 != null) {
            if (metaData2 == null) {
                return false;
            }
            if (!metaData1.equals(metaData2)) {
                return false;
            }
        } else {
            if (metaData2 != null) {
                return false;
            }
        }
        if (!severityEquals(value1.getSeverity(), value2.getSeverity())) {
            return false;
        }
        return true;
    }

    private boolean severityEquals(ISeverity severity1, ISeverity severity2) {
        if (severity1 == null && severity2 == null) {
            return true;
        }
        if ((severity1 == null && severity2 != null)
                || (severity1 != null && severity2 == null)) {
            return false;
        }
        if ((severity1.hasValue() != severity2.hasValue())
                || (severity1.isOK() != severity2.isOK())
                || (severity1.isMinor() != severity2.isMinor())
                || (severity1.isMajor() != severity2.isMajor())
                || (severity1.isInvalid() != severity2.isInvalid())) {
            return false;
        }
        return true;
    }

    private Double getMin(double[] values, Double value) {
        Double arrayMin = getMin(values);
        if (value == null) {
            return arrayMin;
        } else if (arrayMin == null) {
            return value;
        } else {
            return Math.min(arrayMin, value);
        }
    }

    private Double getMax(double[] values, Double value) {
        Double arrayMax = getMax(values);
        if (value == null) {
            return arrayMax;
        } else if (arrayMax == null) {
            return value;
        } else {
            return Math.max(arrayMax, value);
        }
    }

    private Double getMin(double[] values) {
        if (values.length == 0) {
            return null;
        }
        double min = values[0];
        for (double value : values) {
            min = Math.min(value, min);
        }
        return min;
    }

    private Double getMax(double[] values) {
        if (values.length == 0) {
            return null;
        }
        double max = values[0];
        for (double value : values) {
            max = Math.max(value, max);
        }
        return max;
    }

    private ISeverity maximizeSeverity(ISeverity severity1, ISeverity severity2) {
        if (severity1 == null) {
            severity1 = ValueFactory.createOKSeverity();
        }
        if (severity2 == null) {
            severity2 = ValueFactory.createOKSeverity();
        }
        if (severity1.isOK()) {
            if (severity2.isMinor()) {
                return ValueFactory.createMinorSeverity();
            } else if (severity2.isMajor()) {
                return ValueFactory.createMajorSeverity();
            } else if (severity2.isInvalid()) {
                return ValueFactory.createInvalidSeverity();
            } else {
                return ValueFactory.createOKSeverity();
            }
        } else if (severity1.isMinor()) {
            if (severity2.isMajor()) {
                return ValueFactory.createMajorSeverity();
            } else if (severity2.isInvalid()) {
                return ValueFactory.createInvalidSeverity();
            } else {
                return ValueFactory.createMinorSeverity();
            }
        } else if (severity1.isMajor()) {
            if (severity2.isInvalid()) {
                return ValueFactory.createInvalidSeverity();
            } else {
                return ValueFactory.createMajorSeverity();
            }
        } else if (severity1.isInvalid()) {
            return ValueFactory.createInvalidSeverity();
        } else {
            if (severity1.isOK()) {
                return ValueFactory.createOKSeverity();
            } else if (severity1.isMinor()) {
                return ValueFactory.createMinorSeverity();
            } else if (severity1.isMajor()) {
                return ValueFactory.createMajorSeverity();
            } else {
                return ValueFactory.createInvalidSeverity();
            }
        }
    }

    private boolean isNumericType(ValueType valueType) {
        return valueType == ValueType.DOUBLE || valueType == ValueType.LONG;
    }

    private double[] multiply(double[] values, double multiplicator) {
        double[] newValues = new double[values.length];
        for (int i = 0; i < values.length; i++) {
            newValues[i] = values[i] * multiplicator;
        }
        return newValues;
    }

    private double[] divide(double[] values, double divisor) {
        double[] newValues = new double[values.length];
        for (int i = 0; i < values.length; i++) {
            newValues[i] = values[i] / divisor;
        }
        return newValues;
    }

    private double[] add(double[] values1, double[] values2) {
        double[] sum = new double[Math.max(values1.length, values2.length)];
        for (int i = 0; i < sum.length; i++) {
            if (i < values1.length && i < values2.length) {
                sum[i] = values1[i] + values2[i];
            } else if (i < values1.length) {
                sum[i] = values1[i];
            } else {
                sum[i] = values2[i];
            }
        }
        return sum;
    }

    private double[] getDoubleValue(IValue value) {
        if (value instanceof IDoubleValue) {
            IDoubleValue doubleValue = (IDoubleValue) value;
            return doubleValue.getValues();
        } else if (value instanceof ILongValue) {
            ILongValue longValue = (ILongValue) value;
            long[] longValues = longValue.getValues();
            double[] doubleValues = new double[longValues.length];
            for (int i = 0; i < longValues.length; i++) {
                doubleValues[i] = (double) longValues[i];
            }
            return doubleValues;
        } else {
            return null;
        }
    }

    private ValueType getValueType(IValue value) {
        if (value instanceof IDoubleValue) {
            return ValueType.DOUBLE;
        } else if (value instanceof IEnumeratedValue) {
            return ValueType.ENUM;
        } else if (value instanceof ILongValue) {
            return ValueType.LONG;
        } else if (value instanceof IStringValue) {
            return ValueType.STRING;
        } else {
            return null;
        }
    }

    private <V> V getFirst(Iterable<? extends V> iterable) {
        Iterator<? extends V> i = iterable.iterator();
        if (i.hasNext()) {
            return i.next();
        } else {
            return null;
        }
    }
}
