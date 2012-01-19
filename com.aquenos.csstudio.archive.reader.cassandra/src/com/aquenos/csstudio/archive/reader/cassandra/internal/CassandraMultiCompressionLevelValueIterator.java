/*
 * Copyright 2012 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.csstudio.archive.reader.cassandra.internal;

import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.TreeMap;

import org.csstudio.archive.reader.ValueIterator;
import org.csstudio.data.values.ITimestamp;
import org.csstudio.data.values.IValue;
import org.csstudio.data.values.TimestampFactory;

import com.aquenos.csstudio.archive.cassandra.SampleStore;
import com.aquenos.csstudio.archive.cassandra.TimestampArithmetics;
import com.aquenos.csstudio.archive.cassandra.util.Pair;
import com.aquenos.csstudio.archive.config.cassandra.CassandraArchiveConfig;

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
	private CassandraArchiveConfig config;
	private SampleStore sampleStore;
	private String channelName;
	private ITimestamp start;
	private ITimestamp end;
	private LinkedList<Pair<String, ITimestamp>> compressionLevelPriorities;
	LinkedList<Pair<CassandraValueIterator, IValue>> storedIterators;
	private ITimestamp bestAvailableCompressionPeriod;
	private CancelationProvider cancelationProvider;
	private volatile boolean cancelationRequested = false;

	// private int compressionLevelIndex;

	public CassandraMultiCompressionLevelValueIterator(SampleStore sampleStore,
			CassandraArchiveConfig config, String channelName,
			ITimestamp start, ITimestamp end,
			TreeMap<Long, String> compressionPeriodsToLevels,
			long requestedResolution, CancelationProvider cancelationProvider) {
		this.config = config;
		this.sampleStore = sampleStore;
		this.start = start;
		this.end = end;
		this.channelName = channelName;
		this.cancelationProvider = cancelationProvider;
		// Build lists of different compression-levels, ordered by priority.
		buildCompressionLevelPriorityList(compressionPeriodsToLevels,
				requestedResolution);
		LinkedList<Pair<CassandraValueIterator, IValue>> usableIterators = new LinkedList<Pair<CassandraValueIterator, IValue>>();
		CassandraValueIterator iterator;
		ITimestamp nextLesserTimestamp = null;
		int compressionLevelIndex = 0;
		do {
			// Search for earlier samples with a different compression level,
			// until we found a sample at or before the start time or we tried
			// all available compression levels.
			String compressionLevelName = compressionLevelPriorities.get(
					compressionLevelIndex).getFirst();
			// Unless we have not found any samples at yet, we only look for
			// samples, which are older than the preceding sample in the same
			// compression level would be. This way we avoid getting samples,
			// just because of a higher time resolution.
			iterator = new CassandraValueIterator(sampleStore, config,
					channelName, start, nextLesserTimestamp == null ? end
							: nextLesserTimestamp, compressionLevelName,
					cancelationProvider);
			boolean hasSamples = iterator.hasNext();
			if (hasSamples) {
				IValue firstValue = iterator.next();
				usableIterators.push(new Pair<CassandraValueIterator, IValue>(
						iterator, firstValue));
				ITimestamp compressionPeriodTimestamp = compressionLevelPriorities
						.get(compressionLevelIndex).getSecond();
				nextLesserTimestamp = TimestampArithmetics.substract(
						firstValue.getTime(), compressionPeriodTimestamp);
				if (firstValue.getTime().isLessOrEqual(start)) {
					// We found a value at or before start.
					break;
				}
			}
			if (!hasSamples && nextLesserTimestamp == null) {
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
		this.bestAvailableCompressionPeriod = compressionLevelPriorities.peek()
				.getSecond();
		this.storedIterators = usableIterators;
	}

	private void buildCompressionLevelPriorityList(
			TreeMap<Long, String> compressionPeriodsToLevels,
			long requestedResolution) {
		// As the number of compression levels usually is very low, the
		// advantage of using a LinkedList for the head-remove operation
		// most-likely outweighs the advantage of ArrayList for indexed item
		// operations.
		this.compressionLevelPriorities = new LinkedList<Pair<String, ITimestamp>>();
		long optimalCompressionPeriod;
		if (compressionPeriodsToLevels.size() > 1 && requestedResolution > 0) {
			// Look for compression level which is equal to less dense than
			// resolution.
			Entry<Long, String> level = compressionPeriodsToLevels
					.floorEntry(requestedResolution);
			if (level != null) {
				optimalCompressionPeriod = level.getKey();
			} else {
				// If we could not find a matching compression level, we use the
				// next denser one.
				level = compressionPeriodsToLevels
						.floorEntry(requestedResolution);
				optimalCompressionPeriod = level.getKey();
			}
		} else {
			optimalCompressionPeriod = 0L;
		}
		// First, we add the best matching compression level followed by the
		// compression levels with a shorter compression period.
		Long currentCompressionPeriod = optimalCompressionPeriod;
		while (currentCompressionPeriod != null) {
			compressionLevelPriorities.add(new Pair<String, ITimestamp>(
					compressionPeriodsToLevels.get(currentCompressionPeriod),
					TimestampFactory.createTimestamp(currentCompressionPeriod,
							0L)));
			currentCompressionPeriod = compressionPeriodsToLevels
					.lowerKey(currentCompressionPeriod);
		}
		// Second, we add the compression levels with a longer compression
		// period.
		currentCompressionPeriod = optimalCompressionPeriod;
		while (currentCompressionPeriod != null) {
			// In the second run we want to omit the first level.
			currentCompressionPeriod = compressionPeriodsToLevels
					.higherKey(currentCompressionPeriod);
			if (currentCompressionPeriod != null) {
				compressionLevelPriorities.add(new Pair<String, ITimestamp>(
						compressionPeriodsToLevels
								.get(currentCompressionPeriod),
						TimestampFactory.createTimestamp(
								currentCompressionPeriod, 0L)));
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
					bestAvailableCompressionPeriod);
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
					String compressionLevelName = compressionLevelPriorities
							.peek().getFirst();
					CassandraValueIterator iterator = new CassandraValueIterator(
							sampleStore, config, compressionLevelName,
							nextTimestamp, end, compressionLevelName,
							cancelationProvider);
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
