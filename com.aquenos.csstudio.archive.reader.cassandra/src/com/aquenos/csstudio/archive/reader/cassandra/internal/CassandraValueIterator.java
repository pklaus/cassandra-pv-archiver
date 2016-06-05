/*
 * Copyright 2012-2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.csstudio.archive.reader.cassandra.internal;

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.csstudio.archive.reader.ValueIterator;
import org.csstudio.data.values.ITimestamp;
import org.csstudio.data.values.IValue;

import com.aquenos.csstudio.archive.cassandra.Sample;
import com.aquenos.csstudio.archive.cassandra.SampleStore;
import com.aquenos.csstudio.archive.config.cassandra.CassandraArchiveConfig;

/**
 * Value iterator that returns samples from a single compression-level of a
 * given channel. This class is only intended for internal use by classes in the
 * same bundle.
 * 
 * @author Sebastian Marsching
 * @see CassandraMultiCompressionLevelValueIterator
 */
public class CassandraValueIterator implements ValueIterator, Cancelable {

    private String channelName;
    private ITimestamp start;
    private ITimestamp end;
    private long compressionPeriod;
    private SampleStore sampleStore;
    private Iterator<Sample> sampleIterator;
    private Sample lastSample;
    private Exception storedException;
    private CancelationProvider cancelationProvider;
    private volatile boolean cancelationRequested = false;

    public CassandraValueIterator(SampleStore sampleStore,
            CassandraArchiveConfig config, String channelName,
            ITimestamp start, ITimestamp end, boolean includeEarlierSample,
            long compressionPeriod, CancelationProvider cancelationProvider)
            throws Exception {
        if (compressionPeriod < 0) {
            throw new IllegalArgumentException(
                    "Compression period must not be negative.");
        }
        if (start == null) {
            throw new IllegalArgumentException(
                    "Start timestamp must not be null.");
        }
        if (end == null) {
            throw new IllegalArgumentException(
                    "End timestamp must not be null.");
        }
        // If there is no sample for the start timestamp, we use the sample
        // right before the start.
        if (includeEarlierSample) {
            for (Sample sample : sampleStore.findSamples(compressionPeriod,
                    channelName, start, null, 1, true)) {
                start = sample.getValue().getTime();
            }
        }

        // Now that we have the final start timestamp, we can create the
        // iterator, which will return the samples.
        this.sampleIterator = sampleStore.findSamples(compressionPeriod,
                channelName, start, end, -1, false).iterator();
        this.sampleStore = sampleStore;
        this.channelName = channelName;
        this.start = start;
        this.end = end;
        this.compressionPeriod = compressionPeriod;
        this.cancelationProvider = cancelationProvider;
        this.cancelationProvider.register(this);
    }

    @Override
    public boolean hasNext() {
        if (storedException != null) {
            return true;
        }
        if (sampleIterator == null && lastSample == null) {
            return false;
        }
        if ((sampleIterator != null && sampleIterator.hasNext())
                || lastSample != null) {
            return true;
        } else {
            // We request one last sample, so that a graph can draw a
            // line-segment pointing out of the visible area.
            sampleIterator = null;
            try {
                for (Sample sample : sampleStore.findSamples(compressionPeriod,
                        channelName, end, null, 1, false)) {
                    // We only return the sample, if the timestamp is greater
                    // than the requested end time. If it is equal, we already
                    // returned it.
                    if (sample.getValue().getTime().isGreaterThan(end)) {
                        lastSample = sample;
                    }
                }
            } catch (Exception e) {
                // According to the ValueIterator API, hasNext() should not
                // throw an exception, but next() may.
                storedException = e;
                return true;
            }
            return (lastSample != null);
        }
    }

    @Override
    public IValue next() throws Exception {
        if (!hasNext()) {
            throw new NoSuchElementException("No element available.");
        }
        if (storedException != null) {
            throw storedException;
        }
        if (cancelationRequested) {
            throw new IllegalStateException("Request has been canceled.");
        }
        IValue value;
        if (lastSample != null) {
            value = lastSample.getValue();
            lastSample = null;
        } else {
            value = sampleIterator.next().getValue();
        }
        return value;
    }

    @Override
    public void close() {
        // Unregister from cancelation provider.
        cancelationProvider.unregister(this);
        // Clear data
        sampleIterator = null;
        lastSample = null;
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
