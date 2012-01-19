/*
 * Copyright 2012 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.csstudio.archive.reader.cassandra.internal;

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
	private static int SAMPLES_PER_REQUEST = 5000;
	private static Sample[] EMPTY_SAMPLE_ARRAY = new Sample[0];

	private String channelName;
	private ITimestamp start;
	private ITimestamp end;
	private String compressionLevelName;
	private SampleStore sampleStore;
	private Sample[] samples;
	private int sampleIndex;
	private CancelationProvider cancelationProvider;
	private volatile boolean cancelationRequested = false;

	public CassandraValueIterator(SampleStore sampleStore,
			CassandraArchiveConfig config, String channelName,
			ITimestamp start, ITimestamp end, String compressionLevelName,
			CancelationProvider cancelationProvider) {
		// Find first sample that has a timestamp greater than or equal to the
		// start time.
		Sample[] firstSample = sampleStore.findSamples(compressionLevelName,
				channelName, start, null, 1);
		if (firstSample.length > 0
				&& !firstSample[0].getValue().getTime().equals(start)
				&& firstSample[0].getPrecedingSampleTimestamp() != null) {
			// Look for the preceding sample, so that we start with the sample
			// right before the start of the requested range.
			start = firstSample[0].getPrecedingSampleTimestamp();
		}
		if (firstSample.length > 0) {
			this.samples = sampleStore.findSamples(compressionLevelName,
					channelName, start, end, SAMPLES_PER_REQUEST);
		} else {
			// There might still be an even older sample. As we could not find
			// any sample at or after the start time, the newest sample must be
			// before the start time.
			ITimestamp lastSampleTimestamp = config.getLastSampleTime(
					compressionLevelName, channelName);
			if (lastSampleTimestamp != null) {
				this.samples = sampleStore.findSamples(compressionLevelName,
						channelName, lastSampleTimestamp, null, 1);
			} else {
				// If we do not have information about the timestamp of the
				// latest sample, we cannot return any results.
				this.samples = EMPTY_SAMPLE_ARRAY;
			}
		}
		this.sampleIndex = 0;
		this.sampleStore = sampleStore;
		this.channelName = channelName;
		this.start = start;
		this.end = end;
		this.compressionLevelName = compressionLevelName;
		this.cancelationProvider = cancelationProvider;
		this.cancelationProvider.register(this);
	}

	@Override
	public boolean hasNext() {
		boolean hasNext = sampleIndex < samples.length;
		if (!hasNext) {
			// We can free up resources as early as possible.
			close();
		}
		return hasNext;
	}

	@Override
	public IValue next() {
		if (!hasNext()) {
			throw new NoSuchElementException("No element available.");
		}
		if (cancelationRequested) {
			throw new RuntimeException("Request has been canceled.");
		}
		IValue value = samples[sampleIndex].getValue();
		sampleIndex++;
		// We only return one sample that is behind the end of the requested
		// range.
		if (sampleIndex == samples.length) {
			// We reached the end of the samples. Either we returned all
			// available samples, or we need to perform another query.
			if (sampleIndex == SAMPLES_PER_REQUEST) {
				// There might be more samples.
				Sample[] nextSamples = sampleStore.findSamples(
						compressionLevelName, this.channelName,
						value.getTime(), this.end, SAMPLES_PER_REQUEST);
				if (nextSamples.length > 1) {
					this.samples = nextSamples;
					// Skip the first sample, because it is the same we just
					// returned.
					this.sampleIndex = 1;
				} else {
					this.samples = EMPTY_SAMPLE_ARRAY;
					this.sampleIndex = 0;
				}
			}
		}
		return value;
	}

	@Override
	public void close() {
		// Unregister from cancelation provider.
		cancelationProvider.unregister(this);
		// Clear data
		sampleIndex = 0;
		samples = EMPTY_SAMPLE_ARRAY;
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
