/*
 * Copyright 2012 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.csstudio.archive.writer.cassandra;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.Queue;

import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.FailoverPolicy;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.ConsistencyLevelPolicy;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;

import org.csstudio.archive.config.ChannelConfig;
import org.csstudio.archive.writer.ArchiveWriter;
import org.csstudio.archive.writer.WriteChannel;
import org.csstudio.data.values.IDoubleValue;
import org.csstudio.data.values.IEnumeratedValue;
import org.csstudio.data.values.ILongValue;
import org.csstudio.data.values.IStringValue;
import org.csstudio.data.values.ITimestamp;
import org.csstudio.data.values.IValue;

import com.aquenos.csstudio.archive.cassandra.Sample;
import com.aquenos.csstudio.archive.cassandra.SampleStore;
import com.aquenos.csstudio.archive.config.cassandra.CassandraArchiveConfig;
import com.aquenos.csstudio.archive.config.cassandra.CompressionLevelConfig;
import com.aquenos.csstudio.archive.writer.cassandra.internal.CassandraWriteChannel;
import com.aquenos.csstudio.archive.writer.cassandra.internal.SampleCompressor;

/**
 * Writer for the Cassandra archive.
 * 
 * @author Sebastian Marsching
 */
public class CassandraArchiveWriter implements ArchiveWriter {

	// We do not share the cluster reference with the configuration bundle,
	// because
	// the shutdown must be handled separately. This is okay, because the number
	// of
	// connections is very small anyway.
	private final static String HECTOR_CLUSTER_NAME = "archiveCluster";

	private Queue<Sample> sampleQueue = new LinkedList<Sample>();
	private Map<String, ITimestamp> channelNameToLastTimestamp = new HashMap<String, ITimestamp>();

	private Cluster cluster;
	private Keyspace keyspace;

	private CassandraArchiveConfig config;
	private SampleStore sampleStore;
	private SampleCompressor sampleCompressor;

	/**
	 * Creates a Cassandra archive writer using the specified parameters.
	 * 
	 * @param cassandraHosts
	 *            host or comma-seperated list of hosts that provide the
	 *            Cassandra database.
	 * @param cassandraPort
	 *            TCP port to contact the Cassandra server(s) on.
	 * @param keyspaceName
	 *            name of the keyspace in the Cassandra database.
	 * @param consistencyLevelPolicy
	 *            consistency-level policy to use.
	 * @param failoverPolicy
	 *            fail-over policy to user.
	 * @param username
	 *            username to use for authentication or <code>null</code> to use
	 *            no authentication.
	 * @param password
	 *            password to use for authentication or <code>null</code> to use
	 *            no authentication.
	 */
	public CassandraArchiveWriter(String cassandraHosts, int cassandraPort,
			String cassandraKeyspace,
			ConsistencyLevelPolicy consistencyLevelPolicy,
			FailoverPolicy failoverPolicy, String username, String password) {
		this(cassandraHosts, cassandraPort, cassandraKeyspace,
				consistencyLevelPolicy, failoverPolicy, username, password, 0);
	}

	/**
	 * Creates a Cassandra archive reader using the specified parameters. Offers
	 * the option to run the sample compressor for the engine. Only one instance
	 * of the sample compressor should exist for a single engine at any point in
	 * time.
	 * 
	 * @param cassandraHosts
	 *            host or comma-seperated list of hosts that provide the
	 *            Cassandra database.
	 * @param cassandraPort
	 *            TCP port to contact the Cassandra server(s) on.
	 * @param keyspaceName
	 *            name of the keyspace in the Cassandra database.
	 * @param consistencyLevelPolicy
	 *            consistency-level policy to use.
	 * @param failoverPolicy
	 *            fail-over policy to user.
	 * @param username
	 *            username to use for authentication or <code>null</code> to use
	 *            no authentication.
	 * @param password
	 *            password to use for authentication or <code>null</code> to use
	 *            no authentication.
	 * @param numCompressorWorkers
	 *            the number of worker threads created for periodically
	 *            performing the compression for all channels managed by the
	 *            specified engine. If this is less than one, the compression is
	 *            deactivated.
	 */
	protected CassandraArchiveWriter(String cassandraHosts, int cassandraPort,
			String cassandraKeyspace,
			ConsistencyLevelPolicy consistencyLevelPolicy,
			FailoverPolicy failoverPolicy, String username, String password,
			int numCompressorWorkers) {
		CassandraHostConfigurator hostConfigurator = new CassandraHostConfigurator();
		hostConfigurator.setHosts(cassandraHosts);
		hostConfigurator.setPort(cassandraPort);
		String keyspaceName = cassandraKeyspace;
		this.cluster = HFactory.getOrCreateCluster(HECTOR_CLUSTER_NAME,
				hostConfigurator);
		TreeMap<String, String> credentials = new TreeMap<String, String>();
		if (username != null && password != null) {
			credentials.put("username", username);
			credentials.put("password", password);
		}
		Keyspace keyspace = HFactory.createKeyspace(keyspaceName, this.cluster,
				consistencyLevelPolicy, failoverPolicy, credentials);
		initialize(keyspace, numCompressorWorkers);
	}

	/**
	 * Creates a Cassandra archive reader using the passed keyspace to access
	 * the database. The connection to the database is not closed when the
	 * {@link #close()} method is called.
	 * 
	 * @param keyspace
	 *            keyspace that stores the configuration and samples.
	 */
	public CassandraArchiveWriter(Keyspace keyspace) {
		initialize(keyspace, 0);
	}

	/**
	 * Creates a Cassandra archive reader using the passed keyspace to access
	 * the database. The connection to the database is not closed when the
	 * {@link #close()} method is called. Offers the option to run the sample
	 * compressor for the engine. Only one instance of the sample compressor
	 * should exist for a single engine at any point in time.
	 * 
	 * @param keyspace
	 *            keyspace that stores the configuration and samples.
	 * @param numCompressorWorkers
	 *            the number of worker threads created for periodically
	 *            performing the compression for all channels managed by the
	 *            specified engine. If this is less than one, the compression is
	 *            deactivated.
	 */
	protected CassandraArchiveWriter(Keyspace keyspace, int numCompressorWorkers) {
		initialize(keyspace, numCompressorWorkers);
	}

	private void initialize(Keyspace keyspace, int numCompressorWorkers) {
		this.keyspace = keyspace;
		this.config = new CassandraArchiveConfig(this.keyspace);
		this.sampleStore = new SampleStore(this.keyspace);
		if (numCompressorWorkers > 0) {
			this.sampleCompressor = new SampleCompressor(this.keyspace,
					numCompressorWorkers);
			this.sampleCompressor.start();
		}
	}

	@Override
	public WriteChannel getChannel(String name) throws Exception {
		return new CassandraWriteChannel(name);
	}

	@Override
	public void addSample(WriteChannel channel, IValue sample) throws Exception {
		// Check that we got the right (our) channel implementation, because
		// we will cast it later.
		if (!(channel instanceof CassandraWriteChannel)) {
			throw new Exception("Got instance of "
					+ channel.getClass().getName()
					+ ", but expected instance of "
					+ CassandraWriteChannel.class.getName());
		}
		// Check that seconds part of time is greater than or equal to zero.
		if (sample.getTime().seconds() < 0L) {
			throw new Exception(
					"Seconds port of timestamp must be greater than or equal to zero, but is "
							+ sample.getTime().seconds());
		}
		// Check that nanoseconds part of time is within expected range, because
		// we will cast it to a 32-bit integer later.
		long nanoSeconds = sample.getTime().nanoseconds();
		if (nanoSeconds < 0L || nanoSeconds >= 1000000000L) {
			throw new Exception(
					"Nanoseconds part of timestamp must be between zero and one billion but is "
							+ nanoSeconds + " for channel " + channel.getName());
		}
		// Check that value is of a supported type, because we will cast it
		// later.
		if (!(sample instanceof IDoubleValue
				|| sample instanceof IEnumeratedValue
				|| sample instanceof ILongValue || sample instanceof IStringValue)) {
			throw new Exception("Sample value for channel " + channel.getName()
					+ " is of unsupported type " + sample.getClass().getName());
		}
		sampleQueue.add(new Sample(
				CompressionLevelConfig.RAW_COMPRESSION_LEVEL_NAME, channel
						.getName(), sample, null));
	}

	@Override
	public void flush() throws Exception {
		// Find all channels for which wo do not have the last timestamp.
		HashSet<String> channelNames = new HashSet<String>();
		for (Sample sample : sampleQueue) {
			String channelName = sample.getChannelName();
			if (!channelNameToLastTimestamp.containsKey(channelName)) {
				channelNames.add(channelName);
			}
		}
		readLastTimeStampsForChannels(channelNames);

		// Insert samples into database
		Mutator<byte[]> mutator = HFactory.createMutator(this.keyspace,
				BytesArraySerializer.get());
		HashMap<String, ITimestamp> newLastSampleTimes = new HashMap<String, ITimestamp>();
		LinkedList<String> affectedChannels = null;
		if (this.sampleCompressor != null) {
			affectedChannels = new LinkedList<String>();
		}
		while (!sampleQueue.isEmpty()) {
			Sample sample = sampleQueue.remove();
			String channelName = sample.getChannelName();
			IValue value = sample.getValue();
			ITimestamp precedingSampleTime = newLastSampleTimes
					.get(channelName);
			if (precedingSampleTime == null) {
				precedingSampleTime = channelNameToLastTimestamp
						.get(channelName);
			}
			sampleStore.insertSample(mutator, channelName, value,
					precedingSampleTime);
			if (affectedChannels != null) {
				affectedChannels.add(channelName);
			}
			newLastSampleTimes.put(channelName, value.getTime());
		}
		for (Entry<String, ITimestamp> channelToTimestamp : newLastSampleTimes
				.entrySet()) {
			// Update timestamp in channel configuration
			String channelName = channelToTimestamp.getKey();
			ITimestamp timestamp = channelToTimestamp.getValue();
			config.updateLastSampleTime(mutator, channelName, timestamp);
		}
		mutator.execute();

		// Update last sample time only, after the new samples have been
		// successfully inserted.
		this.channelNameToLastTimestamp.putAll(newLastSampleTimes);

		// If we have a sample compressor, it should process the channels we
		// wrote samples for at its next run.
		if (affectedChannels != null && this.sampleCompressor != null) {
			this.sampleCompressor.queueChannelProcessRequests(affectedChannels);
		}
	}

	private void readLastTimeStampsForChannels(
			Collection<? extends String> channelNames) {
		ChannelConfig[] channelConfigs = config.findChannels(channelNames
				.toArray(new String[channelNames.size()]));
		for (ChannelConfig channelConfig : channelConfigs) {
			String channelName = channelConfig.getName();
			ITimestamp lastSampleTime = channelConfig.getLastSampleTime();
			channelNameToLastTimestamp.put(channelName, lastSampleTime);
		}
	}

	@Override
	public void close() {
		if (sampleCompressor != null) {
			sampleCompressor.stop();
			sampleCompressor = null;
		}
		if (cluster != null) {
			HFactory.shutdownCluster(cluster);
			this.cluster = null;
		}
	}

}
