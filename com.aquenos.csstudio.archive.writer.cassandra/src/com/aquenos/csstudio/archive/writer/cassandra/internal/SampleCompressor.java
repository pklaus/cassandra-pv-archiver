/*
 * Copyright 2012 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.csstudio.archive.writer.cassandra.internal;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.TreeMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;

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
import com.aquenos.csstudio.archive.cassandra.TimestampArithmetics;
import com.aquenos.csstudio.archive.cassandra.util.Pair;
import com.aquenos.csstudio.archive.config.cassandra.CassandraArchiveConfig;
import com.aquenos.csstudio.archive.config.cassandra.CompressionLevelConfig;
import com.aquenos.csstudio.archive.config.cassandra.CompressionLevelState;

/**
 * Calculates compressed samples and deletes old samples. This class is only
 * intended for internal use by the classes in the same bundle.
 * 
 * @author Sebastian Marsching
 */
public class SampleCompressor {
	private double[] EMPTY_DOUBLE_ARRAY = new double[0];

	private final Logger logger = Logger.getLogger(WriterBundle.NAME);
	private Keyspace keyspace;
	private SampleStore sampleStore;
	private CassandraArchiveConfig archiveConfig;

	private ScheduledThreadPoolExecutor executor;
	private State state = State.STOPPED;
	private final Object stateLock = new Object();

	private Queue<String> channelQueue = new LinkedList<String>();
	private Queue<String> inactiveChannelQueue = new LinkedList<String>();
	private final Object queueLock = new Object();

	private final Runnable compressionRunner = new Runnable() {
		@Override
		public void run() {
			runCompressionAndDeletion();
		}
	};

	public enum State {
		RUNNING, WAITING, STOPPED
	}

	private enum ValueType {
		DOUBLE, ENUM, LONG, STRING
	}

	private class SampleCache {
		private int cacheSize = 10000;
		private String compressionLevelName;
		private String channelName;
		private TreeMap<ITimestamp, Sample> cache = new TreeMap<ITimestamp, Sample>();
		private ITimestamp cacheLow;
		private ITimestamp cacheHigh;

		public SampleCache(String compressionLevelName, String channelName) {
			this.compressionLevelName = compressionLevelName;
			this.channelName = channelName;
		}

		public Sample findPrecedingSample(Mutator<byte[]> mutator, Sample sample) {
			ITimestamp precedingSampleTimestamp = sample
					.getPrecedingSampleTimestamp();
			Sample precedingSample = null;
			if (precedingSampleTimestamp != null) {
				precedingSample = findSample(precedingSampleTimestamp);
			}
			if (precedingSample == null) {
				// Use SampleStore#getPrecedingSample because this method can
				// actually search for sample, if the timestamp is unknown.
				return sampleStore.findPrecedingSample(mutator, sample);
			} else {
				return precedingSample;
			}
		}

		public Sample findSample(ITimestamp timestamp) {
			Sample sample = cache.get(timestamp);
			if (sample != null) {
				return sample;
			}
			if (cacheLow != null && cacheHigh != null
					&& cacheLow.isLessOrEqual(timestamp)
					&& cacheLow.isGreaterOrEqual(timestamp)) {
				return null;
			}
			Sample[] newSamples = findSamples(timestamp, null, 1);
			if (newSamples.length == 0) {
				return null;
			}
			sample = newSamples[0];
			if (sample.getValue().getTime().equals(timestamp)) {
				return sample;
			} else {
				return null;
			}
		}

		public Sample[] findSamples(ITimestamp start, ITimestamp end,
				int numberOfSamples) {
			// Ensure that the cache is big enough
			cacheSize = Math.max(cacheSize, numberOfSamples + 1);
			if (numberOfSamples == 0) {
				return new Sample[0];
			}
			if (start == null) {
				start = TimestampArithmetics.MIN_TIME;
			}
			if (end == null) {
				end = TimestampArithmetics.MAX_TIME;
			}
			if (cacheLow == null || start.isLessThan(cacheLow)) {
				// Start is outside of cache range, so we have to ask the
				// database server.
				int queryNum;
				if (end.equals(TimestampArithmetics.MAX_TIME)) {
					// Open limit query, thus we do not request too many
					// samples.
					queryNum = 50;
				} else {
					// End is limited by timestamp, thus we can request a rather
					// large number, as most likely the end timestamp will limit
					// it.
					queryNum = 1000;
				}
				// If more samples have been requested by the calling code, we
				// request the same number of samples.
				queryNum = Math.max(numberOfSamples, 50);
				ITimestamp queryEnd = end;
				if (cacheLow != null && end.isGreaterOrEqual(cacheLow)
						&& cacheHigh != null && start.isLessOrEqual(cacheHigh)) {
					// We can limit the query to the samples that are not in the
					// cache yet.
					queryEnd = cache.firstKey();
				}
				Sample[] newSamples = queryAndCache(start, queryEnd, queryNum);
				// Return samples
				if (end.equals(queryEnd)) {
					// The samples returned exactly match the request.
					return newSamples;
				} else if (end.isLessThan(queryEnd)) {
					// The samples returned include all requested samples,
					// however there might be more samples, because we
					// readjusted the end.
					int numberOfSamplesToUse = 0;
					for (Sample sample : newSamples) {
						if (sample.getValue().getTime().isLessOrEqual(end)) {
							numberOfSamplesToUse++;
						} else {
							break;
						}
					}
					return Arrays.copyOf(newSamples, numberOfSamplesToUse);
				} else {
					// We shortened the query, because we have parts of the
					// period in the cache. Now we first use the samples
					// returned and then add the samples from the cache.
					// We cannot take all samples from the cache, because the
					// samples from the new request have not been added, if
					// there was only a single sample.
					ArrayList<Sample> samples = new ArrayList<Sample>(
							numberOfSamples);
					for (Sample sample : newSamples) {
						// There is no need to check the timestamp, because
						// the end timestamp used is actually smaller than
						// the end timestamp requested by the calling code.
						samples.add(sample);
					}
					// Now we add samples from the cache, until we either hit
					// the end of the cache or we find a sample with a timestamp
					// greater than the end timestamp.
					Entry<ITimestamp, Sample> entry = cache.ceilingEntry(start);
					if (samples.size() != 0) {
						// Find first sample that comes after the last sample
						// we already added to the list.
						entry = cache.higherEntry(samples
								.get(samples.size() - 1).getValue().getTime());
					} else {
						// Start with the first sample that is at or after
						// start.
						entry = cache.ceilingEntry(start);
					}
					while (entry != null) {
						if (entry.getKey().isGreaterThan(end)) {
							return samples.toArray(new Sample[samples.size()]);
						}
						samples.add(entry.getValue());
						entry = cache.higherEntry(entry.getKey());
					}
					return samples.toArray(new Sample[samples.size()]);
				}
			}
			// If we got until here, we know that we have samples starting at
			// or after start in the cache. In this case we only have to check,
			// that we also have all samples up to end.
			ArrayList<Sample> samples = new ArrayList<Sample>(numberOfSamples);
			Entry<ITimestamp, Sample> entry = cache.ceilingEntry(start);
			while (entry != null) {
				if (entry.getKey().isGreaterThan(end)
						|| samples.size() >= numberOfSamples) {
					return samples.toArray(new Sample[samples.size()]);
				}
				samples.add(entry.getValue());
				entry = cache.higherEntry(entry.getKey());
			}
			if (samples.size() >= numberOfSamples
					|| (cacheHigh != null && end.isLessOrEqual(cacheHigh))) {
				// The samples from the cache were sufficient - either because
				// we got enough of them or because the cache covers the whole
				// requested range.
				return samples.toArray(new Sample[samples.size()]);
			} else {
				// End is outside cache range and the cache did not have the
				// right number of entries, so we have to ask the server.
				// Request the number of samples requested by the calling
				// code or 50 samples, whichever is more.
				int queryNum = numberOfSamples - samples.size();
				queryNum = Math.max(queryNum, 50);
				ITimestamp queryStart;
				if (samples.size() > 0) {
					queryStart = samples.get(samples.size() - 1).getValue()
							.getTime();
				} else {
					queryStart = start;
				}
				Sample[] newSamples = queryAndCache(queryStart,
						TimestampArithmetics.MAX_TIME, queryNum);
				// Add new samples to result. First check whether first sample
				// is already included in the list.
				if (newSamples.length > 0
						&& (samples.size() == 0 || !newSamples[0]
								.getValue()
								.getTime()
								.equals(samples.get(samples.size() - 1)
										.getValue().getTime()))) {
					// The last value in the cache and the first value
					// returned do not equal, so we add the first value.
					if (newSamples[0].getValue().getTime().isLessOrEqual(end)) {
						samples.add(newSamples[0]);
					}
				}
				// Add all remaining samples
				for (int i = 1; i < newSamples.length; i++) {
					Sample sample = newSamples[i];
					if (sample.getValue().getTime().isLessOrEqual(end)) {
						samples.add(sample);
					} else {
						break;
					}
				}
				return samples.toArray(new Sample[samples.size()]);
			}
		}

		private Sample[] queryAndCache(ITimestamp start, ITimestamp end,
				int numberOfSamples) {
			Sample[] samples = sampleStore.findSamples(compressionLevelName,
					channelName, start, end, numberOfSamples);
			if (samples.length <= 1 && cache.size() > 0) {
				// If no samples or only one sample is returned, and we already
				// have data in the cache, we do not cache but just return the
				// requested samples.
				return samples;
			}
			ITimestamp newCacheHigh;
			if (samples.length < numberOfSamples) {
				// If we got less samples than requested, we know for sure, that
				// we got all samples available between the start and the end
				// timestamp.
				newCacheHigh = end;
			} else {
				// If we got the number of samples we requested, there might be
				// more samples with a timestamp less than end.
				newCacheHigh = samples[samples.length - 1].getValue().getTime();
			}
			if (cache.size() != 0) {
				// How do cached samples and new samples overlap?
				// -1: Overlap at start of cache
				// 0: No overlap
				// 1: Overlap at end of cache
				int overlap;
				if (cacheLow.isLessOrEqual(newCacheHigh)
						&& cacheLow.isGreaterOrEqual(start)) {
					overlap = -1;
				} else if (cacheHigh.isLessOrEqual(newCacheHigh)
						&& cacheHigh.isGreaterOrEqual(start)) {
					overlap = 1;
				} else {
					overlap = 0;
				}
				if (overlap == 0 || cache.size() + samples.length > cacheSize) {
					// We have to free up some space in order to add the new
					// samples.
					int numberToDelete = (cache.size() + samples.length)
							- cacheSize;
					if (overlap == 0 || numberToDelete >= cache.size()) {
						// If we have to delete all samples (either because we
						// will refill the complete cache or because there is no
						// overlap, we just clear the whole map, because this
						// should be much quicker.
						cache.clear();
						cacheLow = null;
						cacheHigh = null;
					} else {
						while (numberToDelete > 0) {
							if (overlap > 0) {
								cache.remove(cache.firstKey());
							} else {
								cache.remove(cache.lastKey());
							}
						}
						if (overlap > 0) {
							cacheLow = cache.firstKey();
						} else {
							cacheHigh = cache.lastKey();
						}
					}
				}
			}
			// Add all new samples
			for (Sample sample : samples) {
				cache.put(sample.getValue().getTime(), sample);
			}
			if (cacheLow == null || start.isLessThan(cacheLow)) {
				cacheLow = start;
			}
			if (cacheHigh == null || newCacheHigh.isGreaterThan(cacheHigh)) {
				cacheHigh = newCacheHigh;
			}
			return samples;
		}
	}

	public SampleCompressor(Keyspace keyspace) {
		this.keyspace = keyspace;
		this.sampleStore = new SampleStore(this.keyspace);
		this.archiveConfig = new CassandraArchiveConfig(this.keyspace);
	}

	public void start() {
		start(0, TimeUnit.SECONDS);
	}

	private void start(long delay, TimeUnit delayUnits) {
		synchronized (stateLock) {
			if (!state.equals(State.STOPPED)) {
				throw new IllegalStateException(
						"SampleCompressor must be in stopped state to be started.");
			}
			executor = new ScheduledThreadPoolExecutor(1);
			executor.schedule(compressionRunner, delay, delayUnits);
			state = State.WAITING;
		}
	}

	public void stop() {
		synchronized (stateLock) {
			if (state.equals(State.STOPPED)) {
				return;
			}
			executor.shutdown();
			while (!executor.isShutdown()) {
				try {
					stateLock.wait(100);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			}
			executor = null;
			state = State.STOPPED;
		}
	}

	public State getState() {
		synchronized (stateLock) {
			return state;
		}
	}

	public void queueChannelProcessRequest(String channelName) {
		synchronized (queueLock) {
			channelQueue.add(channelName);
		}
	}

	public void queueChannelProcessRequests(Collection<String> channelNames) {
		synchronized (queueLock) {
			channelQueue.addAll(channelNames);
		}
	}

	private void runCompressionAndDeletion() {
		synchronized (stateLock) {
			state = State.RUNNING;
		}
		State finalState = State.STOPPED;
		try {
			long startTime = System.currentTimeMillis();
			synchronized (queueLock) {
				// Exchange active and inactive queue object.
				Queue<String> temp = inactiveChannelQueue;
				inactiveChannelQueue = channelQueue;
				channelQueue = temp;
			}
			// Ensure every channel is only processed once.
			LinkedHashSet<String> channelNames = new LinkedHashSet<String>();
			channelNames.addAll(inactiveChannelQueue);
			inactiveChannelQueue.clear();
			// Process channels
			for (String channelName : channelNames) {
				try {
					processChannel(channelName);
				} catch (Exception e) {
					// Log exception and continue with next channel
					logger.log(Level.WARNING,
							"Error while compressing samples for channel "
									+ channelName + ": " + e.getMessage(), e);
				}
			}
			// Schedule next run
			// TODO Make run interval configurable
			long runInterval = 30000;
			long delay = runInterval - (System.currentTimeMillis() - startTime);
			delay = Math.max(delay, 0);
			executor.schedule(compressionRunner, delay, TimeUnit.MILLISECONDS);
			finalState = State.WAITING;
		} finally {
			synchronized (stateLock) {
				state = finalState;
			}
		}
	}

	private void processChannel(String channelName) {
		Pair<CompressionLevelConfig, CompressionLevelState>[] pairs = archiveConfig
				.findCompressionLevelConfigsAndStates(channelName);
		for (Pair<CompressionLevelConfig, CompressionLevelState> pair : pairs) {
			CompressionLevelConfig compressionLevelConfig = pair.getFirst();
			CompressionLevelState compressionLevelState = pair.getSecond();
			boolean enableCompression = compressionLevelConfig
					.getCompressionPeriod() > 0;
			boolean enableRetention = compressionLevelConfig
					.getRetentionPeriod() > 0;
			boolean rawCompressionLevel = compressionLevelConfig
					.getCompressionLevelName().equals(
							CompressionLevelConfig.RAW_COMPRESSION_LEVEL_NAME);
			if (enableCompression == false && enableRetention == false) {
				// Invalid configuration, continue with next compression level
				continue;
			}
			if (enableCompression && !rawCompressionLevel) {
				// The compression action might change the timestamps, thus we
				// have to use the configuration returned by the compress
				// method.
				compressionLevelState = compressChannel(compressionLevelConfig,
						compressionLevelState);
			} else if (enableCompression) {
				// We never compress the raw samples themselves.
				logger.log(
						Level.INFO,
						"Skipping compression for channel \""
								+ channelName
								+ "\" compression level \"raw\", although positive compression period is specified in configuration.");
			}
			if (enableRetention) {
				deleteOldSamples(compressionLevelConfig, compressionLevelState);
			}
		}
	}

	private void deleteOldSamples(
			CompressionLevelConfig compressionLevelConfig,
			CompressionLevelState compressionLevelState) {
		String compressionLevelName = compressionLevelConfig
				.getCompressionLevelName();
		String channelName = compressionLevelConfig.getChannelName();
		ITimestamp end = TimestampFactory.createTimestamp(
				compressionLevelState.getLastSavedSampleTime().seconds()
						- compressionLevelConfig.getRetentionPeriod(), 0L);
		if (end.seconds() < 0 || (end.seconds() == 0 && end.nanoseconds() == 0)) {
			return;
		}
		int maxSamples = 5000;
		int samplesFound;
		do {
			ITimestamp[] timestamps = sampleStore.findSampleTimestamps(
					compressionLevelName, channelName, null, end, maxSamples);
			samplesFound = timestamps.length;
			if (samplesFound == 0) {
				break;
			}
			Mutator<byte[]> mutator = HFactory.createMutator(this.keyspace,
					BytesArraySerializer.get());
			for (ITimestamp timestamp : timestamps) {
				sampleStore.deleteSample(mutator, compressionLevelName,
						channelName, timestamp);
			}
			mutator.execute();
		} while (samplesFound == maxSamples);
	}

	private CompressionLevelState compressChannel(
			CompressionLevelConfig compressionLevelConfig,
			CompressionLevelState compressionLevelState) {
		String channelName = compressionLevelConfig.getChannelName();
		SampleCache cache = new SampleCache(
				CompressionLevelConfig.RAW_COMPRESSION_LEVEL_NAME, channelName);
		ITimestamp nextSampleTime = compressionLevelState.getNextSampleTime();
		Sample lastSavedSample = null;
		ITimestamp lastSavedSampleTime = compressionLevelState
				.getLastSavedSampleTime();
		long compressionInterval = compressionLevelConfig
				.getCompressionPeriod();
		ITimestamp compressionIntervalTimestamp = TimestampFactory
				.createTimestamp(compressionInterval, 0L);
		ITimestamp halfCompressionIntervalTimestamp = TimestampArithmetics
				.divide(compressionIntervalTimestamp, 2);
		// We share the mutator over several iterations in order to reduce the
		// number of requests sent.
		Mutator<byte[]> mutator = HFactory.createMutator(keyspace,
				BytesArraySerializer.get());
		int insertCounts = 0;
		// Process samples for channel until we got no more raw samples.
		while (true) {
			if (isNull(nextSampleTime)) {
				// There is no sample yet, thus we use the first time available.
				Sample[] rawSamples = cache.findSamples(null, null, 1);
				if (rawSamples.length == 0) {
					// There are no raw samples.
					break;
				}
				ITimestamp rawSampleTime = rawSamples[0].getValue().getTime();
				// After the first raw sample, we have to wait at least two half
				// the compression period.
				nextSampleTime = TimestampArithmetics.add(rawSampleTime,
						halfCompressionIntervalTimestamp);
				// We want to make sure that samples for different channels are
				// nicely aligned in time.
				long seconds = nextSampleTime.seconds();
				if (nextSampleTime.nanoseconds() > 0L) {
					seconds += 1;
				}
				long remainingSeconds = seconds % compressionInterval;
				if (remainingSeconds != 0) {
					seconds += compressionInterval - remainingSeconds;
				}
				nextSampleTime = TimestampFactory.createTimestamp(seconds, 0L);
			}
			// Calculate start and end of current sample interval.
			ITimestamp start = TimestampArithmetics.substract(nextSampleTime,
					halfCompressionIntervalTimestamp);
			ITimestamp end = TimestampArithmetics.add(nextSampleTime,
					halfCompressionIntervalTimestamp);
			// Doing the query with start first increases the chances that the
			// next query might be in the cache (while it does not work the
			// other way round).
			Sample[] firstSamples = cache.findSamples(start, null, 1);
			// Find first sample at end of or after the period. If we do not
			// find such a sample, we cannot be sure that the period is already
			// closed.
			if (cache.findSamples(end, null, 1).length == 0) {
				break;
			}
			Sample firstSample = firstSamples[0];
			IValue lastValue;
			if (firstSample.getValue().getTime().equals(start)) {
				lastValue = firstSample.getValue();
			} else {
				Sample precedingSample = cache.findPrecedingSample(mutator,
						firstSample);
				if (precedingSample == null) {
					// This problem should actually never happen, because we
					// either have used an older sample previously or we ensured
					// we have a sample which is early enough above. However, if
					// samples have been delete since we calculated the last
					// average, this could happen. In this case we delete the
					// information about the last processed sample, so that at
					// the next iteration we use the first raw sample.
					lastSavedSampleTime = TimestampFactory.createTimestamp(0L,
							0L);
					lastSavedSample = null;
					nextSampleTime = lastSavedSampleTime;
					compressionLevelState = archiveConfig
							.updateCompressionLevelState(mutator,
									compressionLevelState, lastSavedSampleTime,
									nextSampleTime);
					break;
				} else {
					lastValue = precedingSample.getValue();
				}
			}
			ITimestamp lastTime = lastValue.getTime();
			ValueType lastValueType = getValueType(lastValue);
			ITimestamp averageStartTime = start;
			double[] doubleSum = EMPTY_DOUBLE_ARRAY;
			double[] lastDoubleValues;
			Double doubleMin = null;
			Double doubleMax = null;
			if (isNumericType(lastValueType)) {
				lastDoubleValues = getDoubleValue(lastValue);
				doubleMin = getMin(lastDoubleValues);
				doubleMax = getMax(lastDoubleValues);
			} else {
				lastDoubleValues = EMPTY_DOUBLE_ARRAY;
			}
			ISeverity maxSeverity = null;
			IValue centerValue = lastValue;
			boolean gotAllSamples = false;
			while (!gotAllSamples) {
				int samplesPerRequest = 5000;
				Sample[] samples = cache.findSamples(start, end,
						samplesPerRequest);
				if (samples.length < samplesPerRequest) {
					gotAllSamples = true;
				}
				for (int i = 0; i < samples.length; i++) {
					if (!gotAllSamples && i == samples.length - 1) {
						// Skip last sample because it will reappear in next
						// sample list.
						continue;
					}
					Sample sample = samples[i];
					IValue value = sample.getValue();
					if (value.getTime().isLessOrEqual(nextSampleTime)) {
						centerValue = value;
					}
					ValueType valueType = getValueType(value);
					if (!valueType.equals(lastValueType)) {
						// The value type changed. This can happen if the
						// channelis connected to a different device. If the
						// typechangesaveraging over the samples of different
						// types doesnot make sense. Therefore we insert two
						// samples (the lastof the old and the first of the new
						// type. Then, we continue with the averaging.
						sampleStore.insertSample(mutator,
								compressionLevelConfig
										.getCompressionLevelName(),
								channelName, lastValue, lastTime);
						lastValue = sample.getValue();
						lastValueType = getValueType(lastValue);
						lastTime = lastValue.getTime();
						sampleStore.insertSample(mutator,
								compressionLevelConfig
										.getCompressionLevelName(),
								channelName, lastValue, lastTime);
						lastSavedSample = new Sample(
								compressionLevelConfig
										.getCompressionLevelName(),
								channelName, lastValue, lastSavedSampleTime);
						lastSavedSampleTime = lastTime;
						archiveConfig.updateCompressionLevelState(mutator,
								compressionLevelState, lastSavedSampleTime,
								nextSampleTime);
						if (lastTime.isGreaterOrEqual(nextSampleTime)) {
							// There is no sense in continuing with the
							// averaging.
							// Instead we will use the values we got so far
							// or use the center value.
							gotAllSamples = true;
							// As we will not continue, we have to set the end
							// timestamp so that the averaging will work
							// correctly.
							end = lastTime;
							break;
						} else {
							averageStartTime = lastTime;
							doubleSum = EMPTY_DOUBLE_ARRAY;
							lastDoubleValues = EMPTY_DOUBLE_ARRAY;
							doubleMin = null;
							doubleMax = null;
							maxSeverity = null;
						}
					} else {
						ITimestamp valueTime = value.getTime();
						maxSeverity = maximizeSeverity(maxSeverity,
								value.getSeverity());
						if ((valueType.equals(ValueType.DOUBLE) || valueType
								.equals(ValueType.LONG))
								&& valueTime.isGreaterThan(lastTime)) {
							ITimestamp diffTime = TimestampArithmetics
									.substract(
											valueTime,
											lastTime.isGreaterThan(averageStartTime) ? lastTime
													: averageStartTime);
							// Each samples weight depends on the period of time
							// it was valid for.
							doubleSum = add(
									doubleSum,
									multiply(lastDoubleValues,
											diffTime.toDouble()));
							// Update last value variables
							lastValue = value;
							lastTime = valueTime;
							lastDoubleValues = getDoubleValue(value);
							doubleMin = getMin(lastDoubleValues, doubleMin);
							doubleMax = getMax(lastDoubleValues, doubleMax);
						}
					}
				}
			}
			IValue compressedValue;
			if (isNumericType(lastValueType) && doubleSum.length > 0) {
				// Long values do not support minimum maximum, thus we even save
				// the sample as double if the original samples were longs.
				ITimestamp diffTime = TimestampArithmetics.substract(end,
						lastTime.isGreaterThan(averageStartTime) ? lastTime
								: averageStartTime);
				doubleSum = add(doubleSum,
						multiply(lastDoubleValues, diffTime.toDouble()));
				ITimestamp averageTime = TimestampArithmetics.substract(end,
						averageStartTime);
				double[] doubleAverage = divide(doubleSum,
						averageTime.toDouble());
				IMetaData metaData = lastValue.getMetaData();
				INumericMetaData numericMetaData = null;
				if (metaData != null && metaData instanceof INumericMetaData) {
					numericMetaData = (INumericMetaData) metaData;
				}
				compressedValue = ValueFactory.createMinMaxDoubleValue(
						nextSampleTime, maxSeverity, "<averaged>",
						numericMetaData, Quality.Interpolated, doubleAverage,
						doubleMin, doubleMax);
			} else {
				// Either we got a non-numeric type, or all values were empty
				// arrays. In both cases, averaging does not make sense and we
				// just use the sample that is valid for our compressed sample
				// time.
				IMetaData metaData = centerValue.getMetaData();
				INumericMetaData numericMetaData = null;
				IEnumeratedMetaData enumMetaData = null;
				if (metaData != null && metaData instanceof INumericMetaData) {
					numericMetaData = (INumericMetaData) metaData;
				} else if (metaData != null
						&& metaData instanceof IEnumeratedMetaData) {
					enumMetaData = (IEnumeratedMetaData) metaData;
				}
				if (maxSeverity == null) {
					maxSeverity = centerValue.getSeverity();
				}
				if (centerValue instanceof IDoubleValue) {
					compressedValue = ValueFactory.createDoubleValue(
							nextSampleTime, maxSeverity, "<compressed>",
							numericMetaData, Quality.Interpolated,
							((IDoubleValue) centerValue).getValues());
				} else if (centerValue instanceof IEnumeratedValue) {
					compressedValue = ValueFactory.createEnumeratedValue(
							nextSampleTime, maxSeverity, "<compressed>",
							enumMetaData, Quality.Interpolated,
							((IEnumeratedValue) centerValue).getValues());
				} else if (centerValue instanceof ILongValue) {
					compressedValue = ValueFactory.createLongValue(
							nextSampleTime, maxSeverity, "<compressed>",
							numericMetaData, Quality.Interpolated,
							((ILongValue) centerValue).getValues());
				} else if (centerValue instanceof IStringValue) {
					compressedValue = ValueFactory.createStringValue(
							nextSampleTime, maxSeverity, "<compressed>",
							Quality.Interpolated,
							((IStringValue) centerValue).getValues());
				} else {
					compressedValue = null;
				}
			}
			if (compressedValue != null) {
				if (lastSavedSample == null && lastSavedSampleTime != null) {
					lastSavedSample = sampleStore.findSample(
							compressionLevelConfig.getCompressionLevelName(),
							channelName, lastSavedSampleTime);
				}
				if (lastSavedSample == null
						|| !approximatelyEquals(lastSavedSample.getValue(),
								compressedValue)) {
					sampleStore.insertSample(mutator,
							compressionLevelConfig.getCompressionLevelName(),
							channelName, compressedValue, lastSavedSampleTime);
					lastSavedSample = new Sample(
							compressionLevelConfig.getCompressionLevelName(),
							channelName, compressedValue, lastSavedSampleTime);
					lastSavedSampleTime = nextSampleTime;
				}
				nextSampleTime = TimestampFactory.createTimestamp(
						nextSampleTime.seconds() + compressionInterval, 0);
				compressionLevelState = archiveConfig
						.updateCompressionLevelState(mutator,
								compressionLevelState, lastSavedSampleTime,
								nextSampleTime);
			}
			insertCounts++;
			if (insertCounts % 1000 == 0) {
				mutator.execute();
				mutator = HFactory.createMutator(keyspace,
						BytesArraySerializer.get());
			}
		}
		// Perform outstanding insert operations.
		mutator.execute();
		return compressionLevelState;
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
		if (!value1.getSeverity().equals(value2.getSeverity())) {
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

	private boolean isNull(ITimestamp timestamp) {
		return (timestamp == null)
				|| (timestamp.seconds() == 0 && timestamp.nanoseconds() == 0);
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
}
