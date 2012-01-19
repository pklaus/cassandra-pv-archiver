/*
 * Copyright 2012 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.csstudio.archive.cassandra;

import java.util.ArrayList;
import java.util.HashSet;

import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.RangeSlicesQuery;
import me.prettyprint.hector.api.query.SliceQuery;

import org.csstudio.data.values.ITimestamp;
import org.csstudio.data.values.IValue;
import org.csstudio.data.values.IValue.Quality;
import org.csstudio.data.values.TimestampFactory;

import com.aquenos.csstudio.archive.cassandra.internal.ColumnFamilySamples;
import com.aquenos.csstudio.archive.cassandra.internal.SampleKey;
import com.aquenos.csstudio.archive.cassandra.util.Pair;

/**
 * Stores samples in the Cassandra database.
 * 
 * @author Sebastian Marsching
 * @see Sample
 */
public class SampleStore {

	private Keyspace keyspace;

	/**
	 * Constructor. Creates a sample store whichs reads data from and writes
	 * data to the passed keyspace.
	 * 
	 * @param keyspace
	 *            keyspace used for accessing the database.
	 */
	public SampleStore(Keyspace keyspace) {
		this.keyspace = keyspace;
	}

	/**
	 * Finds a raw sample in the database. This is the same as calling
	 * <code>findSample(null, channelName, timestamp)</code>.
	 * 
	 * @param channelName
	 *            name of the channel to find the sample for.
	 * @param timestamp
	 *            timestamp of the sample.
	 * @return sample read from the database if no raw sample with the given
	 *         timestamp exists for the given channel.
	 */
	public Sample findSample(String channelName, ITimestamp timestamp) {
		return findSample(null, channelName, timestamp);
	}

	/**
	 * Finds a sample in the database.
	 * 
	 * @param compressionLevelName
	 *            name of the compression level to search the sample in.
	 * @param channelName
	 *            name of the channel to find the sample for.
	 * @param timestamp
	 *            timestamp of the sample.
	 * @return sample read from the database or <code>null</code> if no sample
	 *         for the given channel, compression level and timestamp exists.
	 */
	public Sample findSample(String compressionLevelName, String channelName,
			ITimestamp timestamp) {
		byte[] key = ColumnFamilySamples.getKey(compressionLevelName,
				channelName, timestamp);
		SliceQuery<byte[], String, byte[]> query = HFactory.createSliceQuery(
				this.keyspace, BytesArraySerializer.get(),
				StringSerializer.get(), BytesArraySerializer.get());
		query.setColumnFamily(ColumnFamilySamples.NAME);
		query.setColumnNames(ColumnFamilySamples.ALL_COLUMNS);
		query.setKey(key);
		ColumnSlice<String, byte[]> slice = query.execute().get();
		return ColumnFamilySamples.readSample(compressionLevelName, key, slice,
				Quality.Original);
	}

	/**
	 * Finds the sample preceding another sample. If the
	 * <code>precedingTimestamp</code> property of the passed sample is not set
	 * correctly and the <code>mutator</code> is not <code>null</code>, the
	 * preceding timestamp stored in the database is corrected.
	 * 
	 * @param mutator
	 *            mutator to use for correcting the preceding timestamp stored
	 *            in the database.
	 * @param sample
	 *            sample to find the preceding sample for.
	 * @return sample preceding the given sample or <code>null</code> if the
	 *         sample is the first one for its compression level and channel.
	 */
	public Sample findPrecedingSample(Mutator<byte[]> mutator, Sample sample) {
		String compressionLevelName = sample.getCompressionLevelName();
		String channelName = sample.getChannelName();
		ITimestamp timestamp = sample.getPrecedingSampleTimestamp();
		Sample precedingSample = null;
		if (timestamp != null) {
			precedingSample = findSample(compressionLevelName, channelName,
					timestamp);
		}
		if (precedingSample != null) {
			return precedingSample;
		}
		// Either timestamp is not set or sample does not exist (any longer).
		// Thus, we have to search for the closest sample.
		// First, we look whether any sample exists before the given sample.
		ITimestamp referenceTime = sample.getValue().getTime();
		ITimestamp[] samples = findSampleTimestamps(compressionLevelName,
				channelName, null, referenceTime, 500);
		if (samples.length <= 1) {
			// The given sample is already the first sample. If the preceding
			// timestamp was not null, we set it to null.
			if (sample.getPrecedingSampleTimestamp() != null) {
				sample.setPrecedingSampleTimestamp(null);
				if (mutator != null) {
					insertSample(mutator, compressionLevelName, channelName,
							sample.getValue(), null);
				}
			}
			return null;
		}
		ITimestamp startTimestamp = null;
		ITimestamp lowerLimit = null;
		ITimestamp upperLimit = null;
		long secondsBack = 5;
		while (precedingSample == null) {
			if (samples.length > 1
					&& samples[samples.length - 1]
							.isGreaterOrEqual(referenceTime)) {
				// Preceding sample must be in array.
				for (int i = samples.length - 1; i >= 0; i--) {
					if (samples[i].isLessThan(referenceTime)) {
						precedingSample = findSample(compressionLevelName,
								channelName, samples[i]);
						break;
					}
				}
				if (precedingSample == null) {
					// Actually, this code should never be executed, however not
					// having it triggers a compiler warning and in fact there
					// is a very low risk that the sample has been deleted
					// between finding the timestamp and trying to read the
					// sample.
					return null;
				}
				ITimestamp precedingSampleTimestamp = precedingSample
						.getValue().getTime();
				// Update preceding sample timestamp with correct value.
				sample.setPrecedingSampleTimestamp(precedingSampleTimestamp);
				if (mutator != null) {
					insertSample(mutator, compressionLevelName, channelName,
							sample.getValue(), precedingSampleTimestamp);
				}
				return precedingSample;
			}
			if (samples.length <= 1 || startTimestamp == null) {
				// The last timestamp we used for the start was too high. Or
				// this is the first iteration.
				// The last start time can be used as an upper limit.
				upperLimit = startTimestamp;
				if (lowerLimit == null) {
					// If we have not found the lower limit yet, we double the
					// time we go back.
					secondsBack *= 2;
					startTimestamp = TimestampArithmetics.substract(
							referenceTime,
							TimestampFactory.createTimestamp(secondsBack, 0));
				} else {
					// If we already have a lower limit, we use the time in the
					// middle between the upper and lower limit as the new
					// start. Upper limit cannot be null, because it will always
					// be set if lower limit is set.
					ITimestamp timeDifference = TimestampArithmetics.substract(
							upperLimit, lowerLimit);
					startTimestamp = TimestampArithmetics.add(lowerLimit,
							TimestampArithmetics.divide(timeDifference, 2));
				}
				samples = findSampleTimestamps(compressionLevelName,
						channelName, startTimestamp, referenceTime, 500);
			} else {
				// The startTimestamp was too low (we got too many samples).
				// Now we know the lower limit.
				lowerLimit = startTimestamp;
				// For the next iteration we use the center between the lower
				// and the upper limit as a start.
				ITimestamp timeDifference = TimestampArithmetics.substract(
						upperLimit, lowerLimit);
				startTimestamp = TimestampArithmetics.add(lowerLimit,
						TimestampArithmetics.divide(timeDifference, 2));
			}
		}
		return precedingSample;
	}

	/**
	 * Finds raw samples in the database. This is the same as calling
	 * <code>findSamples(null, channelName, start, end, numberOfSamples)</code>.
	 * 
	 * @param channelName
	 *            name of the channel to find samples for.
	 * @param start
	 *            start timestamp. Only samples whose timestamps are greater
	 *            than or equal the start timestamp are returned. If the start
	 *            timestamp is <code>null</code>, the first sample returned is
	 *            the first sample in the database.
	 * @param end
	 *            end timestamp. Only samples whose timestamps are less than or
	 *            equal the end timestamp are returned. If the end timestamp is
	 *            <code>null</code>, no upper limit is placed on the samples'
	 *            timestamps.
	 * @param numberOfSamples
	 *            maximum number of samples to return. If less samples match the
	 *            predicates specified by <code>start</code> and
	 *            <code>end</code>, only these samples will be returned.
	 *            However, this method will never return more samples than
	 *            specified by this parameter.
	 * @return array of samples matching the predicates, never <code>null</code>
	 *         .
	 */
	public Sample[] findSamples(String channelName, ITimestamp start,
			ITimestamp end, int numberOfSamples) {
		return findSamples(null, channelName, start, end, numberOfSamples);
	}

	/**
	 * Finds samples in the database.
	 * 
	 * @param compressionLevelName
	 *            name of the compression level to find samples for.
	 * @param channelName
	 *            name of the channel to find samples for.
	 * @param start
	 *            start timestamp. Only samples whose timestamps are greater
	 *            than or equal the start timestamp are returned. If the start
	 *            timestamp is <code>null</code>, the first sample returned is
	 *            the first sample in the database.
	 * @param end
	 *            end timestamp. Only samples whose timestamps are less than or
	 *            equal the end timestamp are returned. If the end timestamp is
	 *            <code>null</code>, no upper limit is placed on the samples'
	 *            timestamps.
	 * @param numberOfSamples
	 *            maximum number of samples to return. If less samples match the
	 *            predicates specified by <code>start</code> and
	 *            <code>end</code>, only these samples will be returned.
	 *            However, this method will never return more samples than
	 *            specified by this parameter.
	 * @return array of samples matching the predicates, never <code>null</code>
	 *         .
	 */
	public Sample[] findSamples(String compressionLevelName,
			String channelName, ITimestamp start, ITimestamp end,
			int numberOfSamples) {
		if (start == null) {
			start = TimestampArithmetics.MIN_TIME;
		}
		if (end == null) {
			end = TimestampArithmetics.MAX_TIME;
		}
		RangeSlicesQuery<byte[], String, byte[]> query = HFactory
				.createRangeSlicesQuery(this.keyspace,
						BytesArraySerializer.get(), StringSerializer.get(),
						BytesArraySerializer.get());
		String columnFamilyName = ColumnFamilySamples.NAME;
		query.setColumnFamily(columnFamilyName);
		query.setColumnNames(ColumnFamilySamples.ALL_COLUMNS);
		int remainingSamples = numberOfSamples;
		ArrayList<Sample> samples = new ArrayList<Sample>(numberOfSamples);
		byte[] startKey = ColumnFamilySamples.getKey(compressionLevelName,
				channelName, start);
		byte[] endKey = ColumnFamilySamples.getKey(compressionLevelName,
				channelName, end);
		boolean firstQuery = true;
		while (remainingSamples > 0) {
			query.setKeys(startKey, endKey);
			// Always request at least 50 rows, because there might be
			// null-rows.
			int maxRows = Math.max(remainingSamples, 50);
			query.setRowCount(maxRows);
			OrderedRows<byte[], String, byte[]> rows = query.execute().get();
			boolean fullResult = rows.getCount() == maxRows;
			boolean firstRow = true;
			for (Row<byte[], String, byte[]> row : rows) {
				if (!firstQuery && firstRow) {
					// We see the last row of the last query again, so we skip
					// it.
					firstRow = false;
					continue;
				}
				firstRow = false;
				startKey = row.getKey();
				Quality quality;
				if (compressionLevelName == null
						|| compressionLevelName.length() == 0
						|| compressionLevelName.equals("raw")) {
					quality = Quality.Original;
				} else {
					quality = Quality.Interpolated;
				}
				Sample sample = ColumnFamilySamples.readSample(
						compressionLevelName, row.getKey(),
						row.getColumnSlice(), quality);
				if (sample != null) {
					samples.add(sample);
					remainingSamples--;
				}
			}
			firstQuery = false;
			if (!fullResult) {
				break;
			}
		}
		return samples.toArray(new Sample[samples.size()]);
	}

	/**
	 * Finds timestamps of samples in the database.
	 * 
	 * @param compressionLevelName
	 *            name of the compression level to find the samples' timestamps
	 *            for.
	 * @param channelName
	 *            name of the channel to find the samples' timestamps for.
	 * @param start
	 *            start timestamp. Only timestamps that are greater than or
	 *            equal the start timestamp are returned. If the start timestamp
	 *            is <code>null</code>, the first timestamp returned is the
	 *            timestamp of the first sample in the database.
	 * @param end
	 *            end timestamp. Only timestamps that are less than or equal the
	 *            end timestamp are returned. If the end timestamp is
	 *            <code>null</code>, no upper limit is placed on the timestamps.
	 * @param numberOfTimestamps
	 *            maximum number of timestamps to return. If less timestamps
	 *            match the predicates specified by <code>start</code> and
	 *            <code>end</code>, only these timestamps will be returned.
	 *            However, this method will never return more timestamps than
	 *            specified by this parameter.
	 * @return array of samples' timestamps matching the predicates, never
	 *         <code>null</code>.
	 */
	public ITimestamp[] findSampleTimestamps(String compressionLevelName,
			String channelName, ITimestamp start, ITimestamp end,
			int numberOfTimestamps) {
		if (start == null) {
			start = TimestampArithmetics.MIN_TIME;
		}
		if (end == null) {
			end = TimestampArithmetics.MAX_TIME;
		}
		RangeSlicesQuery<byte[], String, byte[]> query = HFactory
				.createRangeSlicesQuery(this.keyspace,
						BytesArraySerializer.get(), StringSerializer.get(),
						BytesArraySerializer.get());
		String columnFamilyName = ColumnFamilySamples.NAME;
		query.setColumnFamily(columnFamilyName);
		// We are not really interested in the severity column. However,
		// querying for a column that exists for all valid samples avoids seeing
		// samples that have been deleted.
		query.setColumnNames(ColumnFamilySamples.COLUMN_SEVERITY);
		int remainingSamples = numberOfTimestamps;
		ArrayList<ITimestamp> timestamps = new ArrayList<ITimestamp>(
				numberOfTimestamps);
		byte[] startKey = ColumnFamilySamples.getKey(compressionLevelName,
				channelName, start);
		byte[] endKey = ColumnFamilySamples.getKey(compressionLevelName,
				channelName, end);
		boolean firstQuery = true;
		while (remainingSamples > 0) {
			query.setKeys(startKey, endKey);
			query.setRowCount(remainingSamples);
			OrderedRows<byte[], String, byte[]> rows = query.execute().get();
			boolean fullResult = rows.getCount() == remainingSamples
					&& remainingSamples > 1;
			boolean firstRow = true;
			for (Row<byte[], String, byte[]> row : rows) {
				if (!firstQuery && firstRow) {
					// We see the last row of the last query again, so we skip
					// it.
					firstRow = false;
					continue;
				}
				firstRow = false;
				startKey = row.getKey();
				ITimestamp timestamp = SampleKey.extractTimestamp(startKey);
				HColumn<String, byte[]> column = row.getColumnSlice()
						.getColumnByName(ColumnFamilySamples.COLUMN_SEVERITY);
				if (column != null && column.getValue().length > 0) {
					timestamps.add(timestamp);
				}
			}
			firstQuery = false;
			if (!fullResult) {
				break;
			}
		}
		return timestamps.toArray(new ITimestamp[timestamps.size()]);
	}

	/**
	 * Inserts a raw sample into the database creating and immediately executing
	 * a {@link Mutator}.
	 * 
	 * @param channelName
	 *            name of the channel to insert the sample for.
	 * @param value
	 *            sample's value.
	 * @param precedingSampleTime
	 *            timestamp of the sample inserted previously.
	 */
	public void insertSample(String channelName, IValue value,
			ITimestamp precedingSampleTime) {
		Mutator<byte[]> mutator = HFactory.createMutator(this.keyspace,
				BytesArraySerializer.get());
		insertSample(mutator, channelName, value, precedingSampleTime);
		mutator.execute();
	}

	/**
	 * Inserts a raw sample into the database using the passed mutator. The
	 * sample will not really be inserted until the mutators
	 * <code>execute</code> method is called.
	 * 
	 * @param mutator
	 *            mutator to use for the insertion operation.
	 * @param channelName
	 *            name of the channel to insert the sample for.
	 * @param value
	 *            sample's value.
	 * @param precedingSampleTime
	 *            timestamp of the sample inserted previously.
	 */
	public void insertSample(Mutator<byte[]> mutator, String channelName,
			IValue value, ITimestamp precedingSampleTime) {
		insertSample(mutator, null, channelName, value, precedingSampleTime);
	}

	/**
	 * Inserts a sample into the database creating and immediately executing a
	 * {@link Mutator}.
	 * 
	 * @param compressionLevelName
	 *            name of the compression level to insert the sample for.
	 * @param channelName
	 *            name of the channel to insert the sample for.
	 * @param value
	 *            sample's value.
	 * @param precedingSampleTime
	 *            timestamp of the sample inserted previously.
	 */
	public void insertSample(String compressionLevelName, String channelName,
			IValue value, ITimestamp precedingSampleTime) {
		Mutator<byte[]> mutator = HFactory.createMutator(this.keyspace,
				BytesArraySerializer.get());
		insertSample(mutator, compressionLevelName, channelName, value,
				precedingSampleTime);
		mutator.execute();
	}

	/**
	 * Inserts a sample into the database using the passed mutator. The sample
	 * will not really be inserted until the mutators <code>execute</code>
	 * method is called.
	 * 
	 * @param compressionLevelName
	 *            name of the compression level to insert the sample for.
	 * @param mutator
	 *            mutator to use for the insertion operation.
	 * @param channelName
	 *            name of the channel to insert the sample for.
	 * @param value
	 *            sample's value.
	 * @param precedingSampleTime
	 *            timestamp of the sample inserted previously.
	 */
	public void insertSample(Mutator<byte[]> mutator,
			String compressionLevelName, String channelName, IValue value,
			ITimestamp precedingSampleTime) {
		ColumnFamilySamples.insertSample(mutator, compressionLevelName,
				channelName, value, precedingSampleTime);
	}

	/**
	 * Deletes a sample from the database.
	 * 
	 * @param mutator
	 *            mutator to use for the delete operation.
	 * @param compressionLevelName
	 *            name of the compression level to delete the sample from.
	 * @param channelName
	 *            name of the channel to delete the sample from.
	 * @param timestamp
	 *            timestamp of the sample.
	 */
	public void deleteSample(Mutator<byte[]> mutator,
			String compressionLevelName, String channelName,
			ITimestamp timestamp) {
		mutator.addDeletion(ColumnFamilySamples.getKey(compressionLevelName,
				channelName, timestamp), ColumnFamilySamples.NAME);
	}

	/**
	 * Performs a clean-up of the column-families used to store samples. During
	 * the clean-up operation, all samples which have a
	 * channel/compression-level combination not listed in
	 * <code>compressionLevelNames</code> are deleted. This includes raw
	 * samples. In order to not delete raw samples, a pair of channel name,
	 * "raw" has to be present for each channel.
	 * 
	 * @param compressionLevelNames
	 *            set of pairs, where the first entry of each pair is the name
	 *            of a channel and the second entry of each pair is a
	 *            compression level name.
	 * @param printStatus
	 *            if set to <code>true</code>, for each sample being deleted a
	 *            message is printed to the standard output.
	 */
	public void performCleanUp(
			HashSet<Pair<String, String>> compressionLevelNames,
			boolean printStatus) {
		RangeSlicesQuery<byte[], String, byte[]> query = HFactory
				.createRangeSlicesQuery(keyspace, BytesArraySerializer.get(),
						StringSerializer.get(), BytesArraySerializer.get());
		query.setColumnFamily(ColumnFamilySamples.NAME);
		// By using a keys-only query, we might see rows which are already
		// deleted. However, deleting them twice will not hurt and not
		// requesting any columns will save us I/O and data-transfer.
		query.setReturnKeysOnly();
		byte[] rowKey = new byte[0];
		int rowsRequested = 5000;
		int rowsReturned = 0;
		do {
			query.setRowCount(rowsRequested);
			query.setKeys(rowKey, null);
			OrderedRows<byte[], String, byte[]> rows = query.execute().get();
			rowsReturned = rows.getCount();
			Mutator<byte[]> mutator = HFactory.createMutator(keyspace,
					BytesArraySerializer.get());
			for (Row<byte[], String, byte[]> row : rows) {
				rowKey = row.getKey();
				String compressionLevelName = SampleKey
						.extractCompressionLevelName(rowKey);
				String channelName = SampleKey.extractChannelName(rowKey);
				ITimestamp timestamp = SampleKey.extractTimestamp(rowKey);
				if (compressionLevelName == null || channelName == null
						|| timestamp == null) {
					if (printStatus) {
						System.out.println("Removing invalid sample "
								+ timestamp + " for channel \"" + channelName
								+ "\" (compression level \""
								+ compressionLevelName + "\")");
					}
					mutator.addDeletion(rowKey, ColumnFamilySamples.NAME);
					continue;
				}
				if (!compressionLevelNames.contains(new Pair<String, String>(
						channelName, compressionLevelName))) {
					if (printStatus) {
						System.out.println("Removing dangling sample "
								+ timestamp + " for channel \"" + channelName
								+ "\" (compression level \""
								+ compressionLevelName + "\")");
					}
					mutator.addDeletion(rowKey, ColumnFamilySamples.NAME);
					continue;
				}

			}
			mutator.execute();
		} while (rowsReturned == rowsRequested);

	}
}
