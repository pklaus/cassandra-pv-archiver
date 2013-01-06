/*
 * Copyright 2012-2013 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.csstudio.archive.reader.cassandra.internal;

import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.TreeSet;

import org.csstudio.archive.reader.ValueIterator;
import org.csstudio.data.values.ITimestamp;
import org.csstudio.data.values.IValue;
import org.csstudio.data.values.TimestampFactory;

import com.aquenos.csstudio.archive.cassandra.SampleStore;
import com.aquenos.csstudio.archive.cassandra.util.Pair;
import com.aquenos.csstudio.archive.cassandra.util.TimestampArithmetics;
import com.aquenos.csstudio.archive.config.cassandra.CassandraArchiveConfig;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

/**
 * Value iterator that automatically chooses the best compression-level for
 * reading samples and can even switch between different compression-levels for
 * different periods of time. This class is only intended for internal use by
 * classes in the same bundle.
 * 
 * @author Sebastian Marsching
 * @see CassandraValueIterator
 */
public class CassandraMultiCompressionLevelValueIterator implements
        ValueIterator, Cancelable {
    private final static ITimestamp ONE_NANOSECOND = TimestampFactory
            .createTimestamp(0L, 1L);

    private CassandraArchiveConfig config;
    private SampleStore sampleStore;
    private String channelName;
    private ITimestamp start;
    private ITimestamp end;
    private LinkedList<Long> compressionLevelPriorities;
    LinkedList<Pair<CassandraValueIterator, IValue>> storedIterators;
    private long bestAvailableCompressionPeriod;
    private CancelationProvider cancelationProvider;
    private volatile boolean cancelationRequested = false;

    public CassandraMultiCompressionLevelValueIterator(SampleStore sampleStore,
            CassandraArchiveConfig config, String channelName,
            ITimestamp start, ITimestamp end, TreeSet<Long> compressionPeriods,
            long requestedResolution, CancelationProvider cancelationProvider)
            throws ConnectionException {
        this.config = config;
        this.sampleStore = sampleStore;
        this.start = start;
        this.end = end;
        this.channelName = channelName;
        this.cancelationProvider = cancelationProvider;
        // Build lists of different compression-levels, ordered by priority.
        buildCompressionLevelPriorityList(compressionPeriods,
                requestedResolution);
        LinkedList<Pair<CassandraValueIterator, IValue>> usableIterators = new LinkedList<Pair<CassandraValueIterator, IValue>>();
        CassandraValueIterator iterator;
        ITimestamp nextTimestamp = null;
        int compressionLevelIndex = 0;
        do {
            // Search for earlier samples with a different compression level,
            // until we found a sample at or before the start time or we tried
            // all available compression levels.
            long compressionPeriod = compressionLevelPriorities
                    .get(compressionLevelIndex);
            // Unless we have not found any samples yet, we only look for
            // samples, which are older than the sample we already found. This
            // way we might be able to fill the period between the requested
            // start and the first sample we found in the best compression
            // level.
            iterator = new CassandraValueIterator(sampleStore, config,
                    channelName, start, nextTimestamp == null ? end
                            : nextTimestamp, true, compressionPeriod,
                    cancelationProvider);
            boolean hasSamples = iterator.hasNext();
            if (hasSamples) {
                IValue firstValue = iterator.next();
                usableIterators.push(new Pair<CassandraValueIterator, IValue>(
                        iterator, firstValue));
                nextTimestamp = TimestampArithmetics.substract(
                        firstValue.getTime(), ONE_NANOSECOND);
                if (firstValue.getTime().isLessOrEqual(start)) {
                    // We found a value at or before start.
                    break;
                }
            }
            if (!hasSamples && nextTimestamp == null) {
                // We did not find a single sample so far. Therefore we know,
                // that the current compression level is unusable and can safely
                // remove it. We cannot make this assumption if the
                // nextLesserTimestamp is set, because we might just not have
                // found a sample because we artificially limited the time
                // range.
                compressionLevelPriorities.pop();
            } else {
                compressionLevelIndex++;
            }
        } while (compressionLevelIndex < compressionLevelPriorities.size());
        this.bestAvailableCompressionPeriod = compressionLevelPriorities.peek();
        this.storedIterators = usableIterators;
    }

    private void buildCompressionLevelPriorityList(
            TreeSet<Long> compressionPeriods, long requestedResolution) {
        // As the number of compression levels usually is very low, the
        // advantage of using a LinkedList for the head-remove operation
        // most-likely outweighs the advantage of ArrayList for indexed item
        // operations.
        this.compressionLevelPriorities = new LinkedList<Long>();
        long optimalCompressionPeriod;
        if (compressionPeriods.size() > 1 && requestedResolution > 0) {
            // Look for compression level which is equal to or denser than
            // the requested resolution.
            Long level = compressionPeriods.floor(requestedResolution);
            if (level != null) {
                optimalCompressionPeriod = level;
            } else {
                // If we could not find a matching compression level, we use the
                // next less dense one.
                level = compressionPeriods.ceiling(requestedResolution);
                optimalCompressionPeriod = level;
            }
        } else {
            optimalCompressionPeriod = 0L;
        }
        // First, we add the best matching compression level followed by the
        // compression levels with a shorter compression period.
        Long currentCompressionPeriod = optimalCompressionPeriod;
        while (currentCompressionPeriod != null) {
            compressionLevelPriorities.add(currentCompressionPeriod);
            currentCompressionPeriod = compressionPeriods
                    .lower(currentCompressionPeriod);
        }
        // Second, we add the compression levels with a longer compression
        // period.
        currentCompressionPeriod = optimalCompressionPeriod;
        while (currentCompressionPeriod != null) {
            // In the second run we want to omit the first level.
            currentCompressionPeriod = compressionPeriods
                    .higher(currentCompressionPeriod);
            if (currentCompressionPeriod != null) {
                compressionLevelPriorities.add(currentCompressionPeriod);
            }
        }
    }

    @Override
    public boolean hasNext() {
        boolean hasNext = storedIterators.size() > 0;
        if (!hasNext) {
            // Free resources as early as possible
            close();
        }
        return hasNext;
    }

    @Override
    public IValue next() throws Exception {
        if (!hasNext()) {
            throw new NoSuchElementException("No element available.");
        }
        if (cancelationRequested) {
            throw new RuntimeException("Request has been canceled.");
        }
        IValue nextValue = storedIterators.peek().getSecond();
        if (nextValue == null) {
            // Get value from iterator
            nextValue = storedIterators.peek().getFirst().next();
        } else {
            // Remove stored value from list
            CassandraValueIterator iterator = storedIterators.pop().getFirst();
            storedIterators.push(new Pair<CassandraValueIterator, IValue>(
                    iterator, null));
        }
        if (!storedIterators.peek().getFirst().hasNext()) {
            storedIterators.removeFirst();
        }
        ITimestamp valueTimestamp = nextValue.getTime();
        if (storedIterators.size() == 0) {
            ITimestamp nextTimestamp = TimestampArithmetics.add(valueTimestamp,
                    TimestampFactory.createTimestamp(
                            bestAvailableCompressionPeriod, 0L));
            if (nextTimestamp.isLessOrEqual(end)) {
                // Look for newer samples with a different compression level.
                while (compressionLevelPriorities.size() > 1) {
                    // We can always remove the first compression level, because
                    // either we checked it in an earlier iteration of this
                    // loop, or
                    // it is the first one at all, which was already queried for
                    // the
                    // whole time range in the beginning.
                    compressionLevelPriorities.pop();
                    long compressionPeriod = compressionLevelPriorities.peek();
                    CassandraValueIterator iterator = new CassandraValueIterator(
                            sampleStore, config, channelName, nextTimestamp,
                            end, false, compressionPeriod, cancelationProvider);
                    if (iterator.hasNext()) {
                        storedIterators
                                .add(new Pair<CassandraValueIterator, IValue>(
                                        iterator, null));
                        break;
                    }
                }
            }
        }
        return nextValue;
    }

    @Override
    public void close() {
        // Close underlying iterators and remove them
        for (Pair<CassandraValueIterator, IValue> pair : storedIterators) {
            CassandraValueIterator iterator = pair.getFirst();
            iterator.close();
        }
        storedIterators.clear();
        // Unregister from cancelation provider
        cancelationProvider.unregister(this);
    }

    @Override
    public void cancel() {
        cancelationRequested = true;
        // Unregister from cancelation provider.
        cancelationProvider.unregister(this);
    }

    @Override
    protected void finalize() throws Throwable {
        try {
            close();
        } finally {
            super.finalize();
        }
    }

    @Override
    public String toString() {
        return "Cassandra Sample Iterator [ channel = " + channelName
                + ", start = " + start + ", end = " + end + "]";
    }

}
