/*
 * Copyright 2012 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.csstudio.archive.config.cassandra;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.FailoverPolicy;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.ConsistencyLevelPolicy;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.beans.Rows;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.MultigetSliceQuery;
import me.prettyprint.hector.api.query.RangeSlicesQuery;
import me.prettyprint.hector.api.query.SliceQuery;

import org.csstudio.archive.config.ArchiveConfig;
import org.csstudio.archive.config.ChannelConfig;
import org.csstudio.archive.config.EngineConfig;
import org.csstudio.archive.config.GroupConfig;
import org.csstudio.archive.config.SampleMode;
import org.csstudio.data.values.ITimestamp;
import org.csstudio.data.values.TimestampFactory;

import com.aquenos.csstudio.archive.cassandra.ColumnReader;
import com.aquenos.csstudio.archive.cassandra.ColumnWriter;
import com.aquenos.csstudio.archive.cassandra.util.Pair;
import com.aquenos.csstudio.archive.config.cassandra.internal.ColumnFamilyChannelConfiguration;
import com.aquenos.csstudio.archive.config.cassandra.internal.ColumnFamilyChannelConfigurationToCompressionLevels;
import com.aquenos.csstudio.archive.config.cassandra.internal.ColumnFamilyCompressionLevelConfiguration;
import com.aquenos.csstudio.archive.config.cassandra.internal.ColumnFamilyEngineConfiguration;
import com.aquenos.csstudio.archive.config.cassandra.internal.ColumnFamilyEngineConfigurationToGroups;
import com.aquenos.csstudio.archive.config.cassandra.internal.ColumnFamilyGroupConfiguration;
import com.aquenos.csstudio.archive.config.cassandra.internal.ColumnFamilyGroupConfigurationToChannels;
import com.aquenos.csstudio.archive.config.cassandra.internal.CompressionLevelOrGroupConfigurationKey;
import com.aquenos.csstudio.archive.config.cassandra.internal.EngineOrChannelConfigurationKey;

/**
 * Configuration for the Cassandra Archive.
 * 
 * @author Sebastian Marsching
 */
public class CassandraArchiveConfig implements ArchiveConfig {

	private final static byte[] EMPTY_BYTE_ARRAY = new byte[0];

	private Cluster cluster;
	private Keyspace keyspace;

	/**
	 * Creates an archive configuration object interfacing to the database
	 * specified by the parameters. The connection to the database is closed
	 * when the {@link #close()} method is called.
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
	public CassandraArchiveConfig(String cassandraHosts, int cassandraPort,
			String keyspaceName, ConsistencyLevelPolicy consistencyLevelPolicy,
			FailoverPolicy failoverPolicy, String username, String password) {
		CassandraHostConfigurator hostConfigurator = new CassandraHostConfigurator();
		hostConfigurator.setHosts(cassandraHosts);
		hostConfigurator.setPort(cassandraPort);
		// We use a random identifier for the cluster, because there might be
		// several instances of the archive configuration. If we used the same
		// cluster for all of them, a cluster still being used might be shutdown
		// when one of them is closed.
		this.cluster = HFactory.getOrCreateCluster(
				UUID.randomUUID().toString(), hostConfigurator);
		TreeMap<String, String> credentials = new TreeMap<String, String>();
		if (username != null && password != null) {
			credentials.put("username", username);
			credentials.put("password", password);
		}
		this.keyspace = HFactory.createKeyspace(keyspaceName, this.cluster,
				consistencyLevelPolicy, failoverPolicy, credentials);
	}

	/**
	 * Creates an archive configuration object backed by the specified keyspace.
	 * The connection to the database is not closed when the {@link #close()}
	 * method is called.
	 * 
	 * @param keyspace
	 *            keyspace that stores configuration.
	 */
	public CassandraArchiveConfig(Keyspace keyspace) {
		this.cluster = null;
		this.keyspace = keyspace;
	}

	private CassandraGroupConfig narrowGroupConfig(GroupConfig groupConfig) {
		if (!(groupConfig instanceof CassandraGroupConfig)) {
			throw new IllegalArgumentException(
					"Group config must be an instance of "
							+ CassandraGroupConfig.class.getName()
							+ " but is an instance of "
							+ groupConfig.getClass().getName());
		}
		return (CassandraGroupConfig) groupConfig;
	}

	private CassandraChannelConfig narrowChannelConfig(
			ChannelConfig channelConfig) {
		if (!(channelConfig instanceof CassandraChannelConfig)) {
			throw new IllegalArgumentException(
					"Channel config must be an instance of "
							+ CassandraGroupConfig.class.getName()
							+ " but is an instance of "
							+ channelConfig.getClass().getName());
		}
		return (CassandraChannelConfig) channelConfig;
	}

	@Override
	public EngineConfig[] getEngines() throws Exception {
		// Number of rows requested per query.
		final byte[] emptyBytes = new byte[0];
		byte[] startKey = emptyBytes;
		int maxRows = 1000;
		int rowsReturned = 0;
		ArrayList<EngineConfig> configs = new ArrayList<EngineConfig>();
		do {
			RangeSlicesQuery<byte[], String, byte[]> query = HFactory
					.createRangeSlicesQuery(this.keyspace,
							BytesArraySerializer.get(), StringSerializer.get(),
							BytesArraySerializer.get());
			query.setColumnFamily(ColumnFamilyEngineConfiguration.NAME);
			query.setColumnNames(ColumnFamilyEngineConfiguration.ALL_COLUMNS);
			query.setRowCount(maxRows);
			query.setKeys(startKey, emptyBytes);
			OrderedRows<byte[], String, byte[]> rows = query.execute().get();
			rowsReturned = rows.getCount();
			configs.ensureCapacity(configs.size() + rowsReturned);
			boolean firstIteration = false;
			for (Row<byte[], String, byte[]> row : rows) {
				byte[] rowKey = row.getKey();
				if (firstIteration && Arrays.equals(rowKey, startKey)) {
					// Skip duplicate row
					continue;
				}
				firstIteration = false;
				// Save key of last row being read to use it at the start for
				// next query.
				startKey = rowKey;
				ColumnSlice<String, byte[]> slice = row.getColumnSlice();
				EngineConfig config = ColumnFamilyEngineConfiguration
						.readEngineConfig(rowKey, slice);
				if (config != null) {
					configs.add(config);
				}
			}
		} while (rowsReturned == maxRows);
		return configs.toArray(new EngineConfig[configs.size()]);
	}

	@Override
	public EngineConfig findEngine(String name) throws Exception {
		byte[] engineKey = ColumnFamilyEngineConfiguration.getKey(name);
		SliceQuery<byte[], String, byte[]> query = HFactory.createSliceQuery(
				this.keyspace, BytesArraySerializer.get(),
				StringSerializer.get(), BytesArraySerializer.get());
		query.setColumnFamily(ColumnFamilyEngineConfiguration.NAME);
		query.setColumnNames(ColumnFamilyEngineConfiguration.ALL_COLUMNS);
		query.setKey(engineKey);
		ColumnSlice<String, byte[]> slice = query.execute().get();
		return ColumnFamilyEngineConfiguration.readEngineConfig(engineKey,
				slice);
	}

	@Override
	public GroupConfig[] getGroups(EngineConfig engine) throws Exception {
		String engineName = engine.getName();
		String startColumn = "";
		// Number of columns requested per query.
		int maxColumns = 2000;
		int columnsReturned = 0;
		LinkedList<byte[]> groupKeys = new LinkedList<byte[]>();
		do {
			SliceQuery<byte[], String, byte[]> query = HFactory
					.createSliceQuery(this.keyspace,
							BytesArraySerializer.get(), StringSerializer.get(),
							BytesArraySerializer.get());
			query.setColumnFamily(ColumnFamilyEngineConfigurationToGroups.NAME);
			query.setKey(ColumnFamilyEngineConfigurationToGroups
					.getKey(engineName));
			query.setRange(startColumn, "", false, maxColumns);
			List<HColumn<String, byte[]>> columns = query.execute().get()
					.getColumns();
			columnsReturned = columns.size();
			boolean firstIteration = true;
			for (HColumn<String, byte[]> column : columns) {
				String groupName = column.getName();
				if (firstIteration && groupName.equals(startColumn)) {
					// Skip duplicate column
					continue;
				}
				firstIteration = false;
				// Save last retrieved column as start for next query.
				startColumn = groupName;
				byte[] groupKey = ColumnFamilyGroupConfiguration.getKey(
						engineName, groupName);
				groupKeys.add(groupKey);
			}
		} while (columnsReturned == maxColumns);
		return getGroups(engineName, groupKeys);
	}

	private GroupConfig[] getGroups(String engineName,
			Iterable<byte[]> groupKeys) {
		MultigetSliceQuery<byte[], String, byte[]> query = HFactory
				.createMultigetSliceQuery(this.keyspace,
						BytesArraySerializer.get(), StringSerializer.get(),
						BytesArraySerializer.get());
		query.setColumnFamily(ColumnFamilyGroupConfiguration.NAME);
		query.setColumnNames(ColumnFamilyGroupConfiguration.ALL_COLUMNS);
		query.setKeys(groupKeys);
		Rows<byte[], String, byte[]> rows = query.execute().get();
		ArrayList<GroupConfig> configs = new ArrayList<GroupConfig>(
				rows.getCount());
		for (Row<byte[], String, byte[]> row : rows) {
			byte[] groupKey = row.getKey();
			ColumnSlice<String, byte[]> slice = row.getColumnSlice();
			CassandraGroupConfig config = ColumnFamilyGroupConfiguration
					.readGroupConfig(engineName, groupKey, slice);
			if (config != null) {
				configs.add(config);
			}
		}
		return configs.toArray(new GroupConfig[configs.size()]);
	}

	/**
	 * Returns the name of the engine a group belongs to.
	 * 
	 * @param groupConfig
	 *            group configuration to find out engine for.
	 * @return the name of the engine for the passed group configuration.
	 */
	public String getEngineName(GroupConfig groupConfig) {
		CassandraGroupConfig cassandraGroupConfig = narrowGroupConfig(groupConfig);
		return cassandraGroupConfig.getEngineName();
	}

	@Override
	public ChannelConfig[] getChannels(GroupConfig groupConfig)
			throws Exception {
		CassandraGroupConfig cassandraGroupConfig = narrowGroupConfig(groupConfig);
		String engineName = cassandraGroupConfig.getEngineName();
		String groupName = cassandraGroupConfig.getName();
		byte[] groupKey = ColumnFamilyGroupConfiguration.getKey(engineName,
				groupName);

		String startColumn = "";
		// Number of columns requested per query.
		int maxColumns = 2000;
		int columnsReturned = 0;
		LinkedList<byte[]> channelKeys = new LinkedList<byte[]>();
		do {
			SliceQuery<byte[], String, byte[]> query = HFactory
					.createSliceQuery(this.keyspace,
							BytesArraySerializer.get(), StringSerializer.get(),
							BytesArraySerializer.get());
			query.setColumnFamily(ColumnFamilyGroupConfigurationToChannels.NAME);
			query.setKey(groupKey);
			query.setRange(startColumn, "", false, maxColumns);
			List<HColumn<String, byte[]>> columns = query.execute().get()
					.getColumns();
			columnsReturned = columns.size();
			boolean firstIteration = true;
			for (HColumn<String, byte[]> column : columns) {
				String channelName = column.getName();
				if (firstIteration && channelName.equals(startColumn)) {
					// Skip duplicate column
					continue;
				}
				firstIteration = false;
				// Save last retrieved column as start for next query.
				startColumn = channelName;
				byte[] channelKey = ColumnFamilyChannelConfiguration
						.getKey(channelName);
				channelKeys.add(channelKey);
			}
		} while (columnsReturned == maxColumns);
		return getChannels(engineName, groupName, channelKeys);
	}

	private ChannelConfig[] getChannels(String engineName, String groupName,
			LinkedList<byte[]> channelKeys) {
		MultigetSliceQuery<byte[], String, byte[]> query = HFactory
				.createMultigetSliceQuery(this.keyspace,
						BytesArraySerializer.get(), StringSerializer.get(),
						BytesArraySerializer.get());
		query.setColumnFamily(ColumnFamilyChannelConfiguration.NAME);
		query.setColumnNames(ColumnFamilyChannelConfiguration.ALL_COLUMNS);
		query.setKeys(channelKeys);
		Rows<byte[], String, byte[]> rows = query.execute().get();
		ArrayList<ChannelConfig> configs = new ArrayList<ChannelConfig>(
				rows.getCount());
		for (Row<byte[], String, byte[]> row : rows) {
			ColumnSlice<String, byte[]> slice = row.getColumnSlice();
			ChannelConfig config = ColumnFamilyChannelConfiguration
					.readChannelConfig(row.getKey(), slice);
			if (config != null) {
				configs.add(config);
			}
		}
		return configs.toArray(new ChannelConfig[configs.size()]);
	}

	/**
	 * Returns the channel configuration for the channel with the specified
	 * name.
	 * 
	 * @param channelName
	 *            name of the channel to find the configuration for.
	 * @return the channel configuration for <code>channelName</code> or
	 *         <code>null</code> if no channel with this name exists.
	 */
	public ChannelConfig findChannel(String channelName) {
		ChannelConfig[] configs = findChannels(new String[] { channelName });
		if (configs.length > 0) {
			return configs[0];
		} else {
			return null;
		}
	}

	/**
	 * Returns the channel configurations for the specified channel names. No
	 * guarantees are made regarding the order of the returned channel
	 * configurations. If no configuration exists for one of the specified
	 * channels, the configurations for the other channels are still returned.
	 * 
	 * @param channelNames
	 *            array of channel names.
	 * @return array of channel configurations for the channel names.
	 */
	public ChannelConfig[] findChannels(String[] channelNames) {
		byte[][] channelKeys = new byte[channelNames.length][];
		for (int i = 0; i < channelNames.length; i++) {
			channelKeys[i] = ColumnFamilyChannelConfiguration
					.getKey(channelNames[i]);
		}
		MultigetSliceQuery<byte[], String, byte[]> query = HFactory
				.createMultigetSliceQuery(this.keyspace,
						BytesArraySerializer.get(), StringSerializer.get(),
						BytesArraySerializer.get());
		query.setColumnFamily(ColumnFamilyChannelConfiguration.NAME);
		query.setColumnNames(ColumnFamilyChannelConfiguration.ALL_COLUMNS);
		query.setKeys(channelKeys);
		Rows<byte[], String, byte[]> rows = query.execute().get();
		ArrayList<ChannelConfig> configs = new ArrayList<ChannelConfig>(
				rows.getCount());
		for (Row<byte[], String, byte[]> row : rows) {
			CassandraChannelConfig config = ColumnFamilyChannelConfiguration
					.readChannelConfig(row.getKey(), row.getColumnSlice());
			if (config != null) {
				configs.add(config);
			}
		}
		return configs.toArray(new ChannelConfig[configs.size()]);
	}

	/**
	 * Returns an array with all valid channel names. No guarantees are made
	 * regarding the order of the channel names.
	 * 
	 * @return array of all channel names.
	 */
	public String[] getChannelNames() {
		int maxRows = 10000;
		RangeSlicesQuery<byte[], String, byte[]> query = HFactory
				.createRangeSlicesQuery(this.keyspace,
						BytesArraySerializer.get(), StringSerializer.get(),
						BytesArraySerializer.get());
		query.setColumnFamily(ColumnFamilyChannelConfiguration.NAME);
		// Although we are not interested in the channel configuration, we have
		// to query for at least one column that is always included in order to
		// avoid seeing keys for deleted rows.
		query.setColumnNames(ColumnFamilyChannelConfiguration.COLUMN_SAMPLE_MODE);
		byte[] startKey = EMPTY_BYTE_ARRAY;
		int rowsFound = 0;
		ArrayList<String> channelNames = new ArrayList<String>(0);
		do {
			query.setKeys(startKey, EMPTY_BYTE_ARRAY);
			query.setRowCount(maxRows);
			OrderedRows<byte[], String, byte[]> rows = query.execute().get();
			channelNames.ensureCapacity(channelNames.size() + rows.getCount());
			boolean firstRow = true;
			for (Row<byte[], String, byte[]> row : rows) {
				if (firstRow && rowsFound != 0) {
					// Skip first row, because we got it with the last query.
					firstRow = false;
					continue;
				}
				firstRow = false;
				startKey = row.getKey();
				HColumn<String, byte[]> column = row
						.getColumnSlice()
						.getColumnByName(
								ColumnFamilyChannelConfiguration.COLUMN_SAMPLE_MODE);
				if (column != null && column.getValue().length != 0) {
					channelNames.add(ColumnFamilyChannelConfiguration
							.getChannelName(row.getKey()));
				}
			}
			rowsFound = rows.getCount();

		} while (rowsFound == maxRows);
		return channelNames.toArray(new String[channelNames.size()]);
	}

	/**
	 * Returns the engine name for a channel configuration.
	 * 
	 * @param channelConfig
	 *            channel configuration to find out the engine name for.
	 * @return engine name
	 */
	public String getEngineName(ChannelConfig channelConfig) {
		CassandraChannelConfig cassandraChannelConfig = narrowChannelConfig(channelConfig);
		return cassandraChannelConfig.getEngineName();
	}

	/**
	 * Returns the group name for a channel configuration.
	 * 
	 * @param channelConfig
	 *            channel configuration to find out the group name for.
	 * @return group name
	 */
	public String getGroupName(ChannelConfig channelConfig) {
		CassandraChannelConfig cassandraChannelConfig = narrowChannelConfig(channelConfig);
		return cassandraChannelConfig.getGroupName();
	}

	/**
	 * Returns the timestamp of the latest sample stored.
	 * 
	 * @param compressionLevelName
	 *            name of the compression-level to find the latest sample
	 *            timestamp for.
	 * @param channelName
	 *            name of the channel to find the latest sample timestamp for.
	 * @return timestamp of the latest sample for the specified compression
	 *         level and channel or <code>null</code> if no such sample exists
	 *         or the timestamp is unknown.
	 */
	public ITimestamp getLastSampleTime(String compressionLevelName,
			String channelName) {
		if (compressionLevelName == null
				|| compressionLevelName.length() == 0
				|| compressionLevelName
						.equals(CompressionLevelConfig.RAW_COMPRESSION_LEVEL_NAME)) {
			return getLastSampleTime(channelName);
		} else {
			CompressionLevelState state = findCompressionLevelState(
					compressionLevelName, channelName);
			if (state == null) {
				return null;
			}
			return state.getLastSavedSampleTime();
		}
	}

	/**
	 * Returns the timestamp for the latest raw sample. This is effectively the
	 * same as calling
	 * <code>getLastSampleTime(CompressionLevelConfig.RAW_COMPRESSION_LEVEL_NAME, channelName)</code>
	 * .
	 * 
	 * @param channelName
	 *            name of the channel to find the latest raw sample's timestamp
	 *            for.
	 * @return timestamp of the latest raw sample for the specified channel or
	 *         <code>null</code> if no such sample exists or the timestamp is
	 *         unknown.
	 */
	public ITimestamp getLastSampleTime(String channelName) {
		// We need this method, because there might be no configuration for
		// a channel, but still the last sample time might be available.
		SliceQuery<byte[], String, byte[]> query = HFactory.createSliceQuery(
				this.keyspace, BytesArraySerializer.get(),
				StringSerializer.get(), BytesArraySerializer.get());
		query.setColumnFamily(ColumnFamilyChannelConfiguration.NAME);
		query.setColumnNames(ColumnFamilyChannelConfiguration.COLUMN_LAST_SAMPLE_TIME);
		query.setKey(ColumnFamilyChannelConfiguration.getKey(channelName));
		ColumnSlice<String, byte[]> slice = query.execute().get();
		ITimestamp lastSampleTimeStamp = ColumnReader.readTimestamp(slice,
				ColumnFamilyChannelConfiguration.COLUMN_LAST_SAMPLE_TIME);
		return lastSampleTimeStamp;
	}

	@Override
	public void close() {
		if (cluster != null) {
			HFactory.shutdownCluster(cluster);
			this.cluster = null;
		}
	}

	/**
	 * Creates an engine configuration in the database.
	 * 
	 * @param engineName
	 *            name of the engine
	 * @param description
	 *            description of the engine
	 * @param engineUrl
	 *            URL of the engine's web-interface.
	 * @return newly created engine configuration
	 * @throws URISyntaxException
	 *             if <code>engineUrl</code> is not a valid URL.
	 */
	public EngineConfig createEngine(String engineName, String description,
			String engineUrl) throws URISyntaxException {
		Mutator<byte[]> mutator = HFactory.createMutator(this.keyspace,
				BytesArraySerializer.get());
		ColumnFamilyEngineConfiguration.insertEngineConfig(mutator, engineName,
				description, engineUrl);
		mutator.execute();
		return new EngineConfig(engineName, description, engineUrl);
	}

	/**
	 * Deletes an engine configuration from the database. Deleting an engine
	 * configuration also deletes all group configurations associated with the
	 * engine.
	 * 
	 * @param engineConfig
	 *            engine configuration that should be deleted.
	 * @throws Exception
	 *             if {@link #getGroups(EngineConfig)} or
	 *             {@link #deleteGroup(GroupConfig)} throw an exception for the
	 *             passed engine configuration.
	 */
	public void deleteEngine(EngineConfig engineConfig) throws Exception {
		Mutator<byte[]> mutator = HFactory.createMutator(this.keyspace,
				BytesArraySerializer.get());
		for (GroupConfig groupConfig : getGroups(engineConfig)) {
			deleteGroup(mutator, groupConfig);
		}
		byte[] engineKey = ColumnFamilyEngineConfiguration.getKey(engineConfig
				.getName());
		mutator.addDeletion(engineKey, ColumnFamilyEngineConfiguration.NAME);
		mutator.addDeletion(engineKey,
				ColumnFamilyEngineConfigurationToGroups.NAME);
		mutator.execute();
	}

	/**
	 * Creates a group configuration in the database.
	 * 
	 * @param engineConfig
	 *            configuration of the engine to create the group for.
	 * @param groupName
	 *            name of the group.
	 * @return newly created group configuration.
	 */
	public GroupConfig createGroup(EngineConfig engineConfig, String groupName) {
		String engineName = engineConfig.getName();
		Mutator<byte[]> mutator = HFactory.createMutator(this.keyspace,
				BytesArraySerializer.get());
		String enablingChannel = "";
		ColumnFamilyGroupConfiguration.insertGroupConfig(mutator, engineName,
				groupName, enablingChannel);
		// Register group with engine configuration
		byte[] engineKey = ColumnFamilyEngineConfiguration.getKey(engineName);
		ColumnWriter.insertString(mutator, engineKey,
				ColumnFamilyEngineConfigurationToGroups.NAME, groupName, "");
		mutator.execute();
		return new CassandraGroupConfig(engineName, groupName, enablingChannel);
	}

	/**
	 * Deletes a group creating and immediately executing a {@link Mutator}.
	 * Deleting a group configuration also deletes all channel configurations
	 * associated with the group.
	 * 
	 * @param groupConfig
	 *            configuration of the group to delete.
	 * @throws Exception
	 *             if {@link #getChannels(GroupConfig)} throws an exception for
	 *             the passed group configuration.
	 */
	public void deleteGroup(GroupConfig groupConfig) throws Exception {
		Mutator<byte[]> mutator = HFactory.createMutator(this.keyspace,
				BytesArraySerializer.get());
		deleteGroup(mutator, groupConfig);
		mutator.execute();
	}

	/**
	 * Deletes a group using the passed mutator. The delete will not be
	 * performed in the database until the mutator's <code>delete</code> method
	 * is called. Deleting a group configuration also deletes all channel
	 * configurations associated with the group.
	 * 
	 * @param mutator
	 *            mutator to use for the delete operation.
	 * @param groupConfig
	 *            configuration of the group to delete.
	 * @throws Exception
	 *             if {@link #getChannels(GroupConfig)} throws an exception for
	 *             the passed group configuration.
	 */
	public void deleteGroup(Mutator<byte[]> mutator, GroupConfig groupConfig)
			throws Exception {
		CassandraGroupConfig cassandraGroupConfig = narrowGroupConfig(groupConfig);
		for (ChannelConfig channelConfig : getChannels(groupConfig)) {
			deleteChannel(mutator, channelConfig);
		}
		byte[] groupKey = ColumnFamilyGroupConfiguration.getKey(
				cassandraGroupConfig.getEngineName(),
				cassandraGroupConfig.getName());
		mutator.addDeletion(groupKey, ColumnFamilyGroupConfiguration.NAME);
		mutator.addDeletion(groupKey,
				ColumnFamilyGroupConfigurationToChannels.NAME);
		// Also delete reference to group in engine configuration
		byte[] engineKey = ColumnFamilyEngineConfigurationToGroups
				.getKey(cassandraGroupConfig.getEngineName());
		mutator.addDeletion(engineKey,
				ColumnFamilyEngineConfigurationToGroups.NAME,
				groupConfig.getName(), StringSerializer.get());
	}

	/**
	 * Updates the enabling channel of a group with the specified name.
	 * 
	 * @param groupConfig
	 *            group configuration to change the enabling channel for.
	 * @param enablingChannelName
	 *            name of the channel that acts as a enabling channel for the
	 *            group.
	 * @return updated group configuration
	 * @see GroupConfig#getEnablingChannel()
	 */
	public GroupConfig updateGroupEnablingChannel(GroupConfig groupConfig,
			String enablingChannelName) {
		CassandraGroupConfig cassandraGroupConfig = narrowGroupConfig(groupConfig);
		Mutator<byte[]> mutator = HFactory.createMutator(this.keyspace,
				BytesArraySerializer.get());
		mutator.addInsertion(ColumnFamilyGroupConfiguration.getKey(
				cassandraGroupConfig.getEngineName(),
				cassandraGroupConfig.getName()),
				ColumnFamilyGroupConfiguration.NAME,
				HFactory.createStringColumn(
						ColumnFamilyGroupConfiguration.COLUMN_ENABLING_CHANNEL,
						enablingChannelName));
		mutator.execute();
		return new CassandraGroupConfig(cassandraGroupConfig.getEngineName(),
				cassandraGroupConfig.getName(), enablingChannelName);
	}

	/**
	 * Creates the configuration for a channel in the database.
	 * 
	 * @param groupConfig
	 *            configuration of the group the channel is part of.
	 * @param channelName
	 *            name of the channel.
	 * @param sampleMode
	 *            mode used for sampling (scan or monitor).
	 * @return newly created channel configuration.
	 */
	public ChannelConfig createChannel(GroupConfig groupConfig,
			String channelName, SampleMode sampleMode) {
		CassandraGroupConfig cassandraGroupConfig = narrowGroupConfig(groupConfig);
		String engineName = cassandraGroupConfig.getEngineName();
		String groupName = cassandraGroupConfig.getName();
		// We try to read the last sample timestamp from the database, because
		// it might still exist from an old configuration.
		SliceQuery<byte[], String, byte[]> query = HFactory.createSliceQuery(
				this.keyspace, BytesArraySerializer.get(),
				StringSerializer.get(), BytesArraySerializer.get());
		query.setColumnFamily(ColumnFamilyChannelConfiguration.NAME);
		query.setColumnNames(ColumnFamilyChannelConfiguration.COLUMN_LAST_SAMPLE_TIME);
		query.setKey(ColumnFamilyChannelConfiguration.getKey(channelName));
		ColumnSlice<String, byte[]> slice = query.execute().get();
		ITimestamp lastSampleTime = ColumnReader.readTimestamp(slice,
				ColumnFamilyChannelConfiguration.COLUMN_LAST_SAMPLE_TIME);
		if (lastSampleTime == null) {
			lastSampleTime = TimestampFactory.createTimestamp(0, 0);
		}
		// Insert new configuration into the database
		Mutator<byte[]> mutator = HFactory.createMutator(this.keyspace,
				BytesArraySerializer.get());
		ColumnFamilyChannelConfiguration.insertChannelConfig(mutator,
				engineName, groupName, channelName, sampleMode, lastSampleTime);
		// Register channel with group configuration.
		byte[] groupKey = ColumnFamilyGroupConfigurationToChannels.getKey(
				engineName, groupName);
		ColumnWriter.insertString(mutator, groupKey,
				ColumnFamilyGroupConfigurationToChannels.NAME, channelName, "");
		mutator.execute();
		// We do not create the channel configuration object directly, because
		// we want to add the last sample time, if it already existed in the
		// database.
		return new CassandraChannelConfig(engineName, groupName, channelName,
				sampleMode, lastSampleTime);
	}

	/**
	 * Deletes a channel configuration from the database creating and
	 * immediately executing a {@link Mutator}. Deleting a channel configuration
	 * will not automatically delete the samples for the channel, however they
	 * are deleted the next time
	 * {@link com.aquenos.csstudio.archive.cassandra.SampleStore#performCleanUp(java.util.HashSet, boolean)}
	 * is executed.
	 * 
	 * @param channelConfig
	 *            channel configuration to delete.
	 */
	public void deleteChannel(ChannelConfig channelConfig) {
		Mutator<byte[]> mutator = HFactory.createMutator(this.keyspace,
				BytesArraySerializer.get());
		deleteChannel(mutator, channelConfig);
		mutator.execute();
	}

	/**
	 * Deletes a channel configuration from the database using the passed
	 * mutator. Deleting a channel configuration will not automatically delete
	 * the samples for the channel, however they are deleted the next time
	 * {@link com.aquenos.csstudio.archive.cassandra.SampleStore#performCleanUp(java.util.HashSet, boolean)}
	 * is executed.
	 * 
	 * @param mutator
	 *            mutator to use for the delete operation.
	 * @param channelConfig
	 *            channel configuration to delete.
	 */
	private void deleteChannel(Mutator<byte[]> mutator,
			ChannelConfig channelConfig) {
		CassandraChannelConfig cassandraChannelConfig = narrowChannelConfig(channelConfig);
		String channelName = channelConfig.getName();
		byte[] channelKey = ColumnFamilyChannelConfiguration
				.getKey(channelName);
		// We do not want to delete the last sample time, because we might need
		// it, if the channel is recreated with a different configuration.
		for (String columnName : ColumnFamilyChannelConfiguration.ALL_COLUMNS_BUT_LAST_SAMPLE_TIME) {
			mutator.addDeletion(channelKey,
					ColumnFamilyChannelConfiguration.NAME, columnName,
					StringSerializer.get());
		}
		// Also remove reference to channel from group configuration.
		byte[] groupKey = ColumnFamilyGroupConfigurationToChannels.getKey(
				cassandraChannelConfig.getEngineName(),
				cassandraChannelConfig.getGroupName());
		mutator.addDeletion(groupKey,
				ColumnFamilyGroupConfigurationToChannels.NAME, channelName,
				StringSerializer.get());
		// Remove compression level configurations (but not state).
		for (byte[] compressionLevelKey : getCompressionLevelKeys(channelName)) {
			mutator.addDeletion(compressionLevelKey,
					ColumnFamilyCompressionLevelConfiguration.NAME);
			// Also delete references.
			String compressionLevelName = CompressionLevelOrGroupConfigurationKey
					.extractSecondName(compressionLevelKey);
			mutator.addDeletion(channelKey,
					ColumnFamilyChannelConfigurationToCompressionLevels.NAME,
					compressionLevelName, StringSerializer.get());
		}
	}

	/**
	 * Updates the timestamp that references the latest sample stored for a
	 * channel. Creates and immediately executes a {@link Mutator}.
	 * 
	 * @param channelName
	 *            name of the channel to update the timestamp for.
	 * @param lastSampleTime
	 *            timestamp of the latest raw sample stored for the channel.
	 */
	public void updateLastSampleTime(String channelName,
			ITimestamp lastSampleTime) {
		Mutator<byte[]> mutator = HFactory.createMutator(this.keyspace,
				BytesArraySerializer.get());
		updateLastSampleTime(mutator, channelName, lastSampleTime);
		mutator.execute();
	}

	/**
	 * Updates the timestamp that references the latest sample stored for a
	 * channel. Uses the passed mutator.
	 * 
	 * @param mutator
	 *            mutator to use for the update operation.
	 * @param channelName
	 *            name of the channel to update the timestamp for.
	 * @param lastSampleTime
	 *            timestamp of the latest raw sample stored for the channel.
	 */
	public void updateLastSampleTime(Mutator<byte[]> mutator,
			String channelName, ITimestamp lastSampleTime) {
		byte[] channelKey = ColumnFamilyChannelConfiguration
				.getKey(channelName);
		if (lastSampleTime == null) {
			mutator.addDeletion(channelKey,
					ColumnFamilyChannelConfiguration.NAME,
					ColumnFamilyChannelConfiguration.COLUMN_LAST_SAMPLE_TIME,
					StringSerializer.get());
			mutator.addDeletion(
					ColumnFamilyCompressionLevelConfiguration.getKey(
							channelName,
							CompressionLevelConfig.RAW_COMPRESSION_LEVEL_NAME),
					ColumnFamilyCompressionLevelConfiguration.NAME,
					ColumnFamilyCompressionLevelConfiguration.COLUMN_LAST_SAVED_SAMPLE_TIME,
					StringSerializer.get());
		} else {
			ColumnWriter.insertTimestamp(mutator, channelKey,
					ColumnFamilyChannelConfiguration.NAME,
					ColumnFamilyChannelConfiguration.COLUMN_LAST_SAMPLE_TIME,
					lastSampleTime);
			// Also update timestamp in compression-level column-family.
			ColumnWriter
					.insertTimestamp(
							mutator,
							ColumnFamilyCompressionLevelConfiguration
									.getKey(channelName,
											CompressionLevelConfig.RAW_COMPRESSION_LEVEL_NAME),
							ColumnFamilyCompressionLevelConfiguration.NAME,
							ColumnFamilyCompressionLevelConfiguration.COLUMN_LAST_SAVED_SAMPLE_TIME,
							lastSampleTime);
		}
	}

	/**
	 * Finds all compression-level configurations for a given channel. A
	 * configuration for the special "raw" level might but does not have to be
	 * part of this list.
	 * 
	 * @param channelName
	 *            name of the channel to find the compression-level
	 *            configurations for.
	 * @return array with all compression-level configurations for the specified
	 *         channel, never <code>null</code>.
	 */
	public CompressionLevelConfig[] findCompressionLevelConfigs(
			String channelName) {
		Pair<CompressionLevelConfig, CompressionLevelState>[] pairs = findCompressionLevelConfigsAndStates(channelName);
		ArrayList<CompressionLevelConfig> configs = new ArrayList<CompressionLevelConfig>(
				pairs.length);
		for (Pair<CompressionLevelConfig, CompressionLevelState> pair : pairs) {
			configs.add(pair.getFirst());
		}
		return configs.toArray(new CompressionLevelConfig[configs.size()]);
	}

	/**
	 * Finds the state for a compression level and channel.
	 * 
	 * @param compressionLevelName
	 *            name of the compression level.
	 * @param channelName
	 *            name of the channel
	 * @return state for compression level and channel or <code>null</code> if
	 *         no state is stored for the specified combination of compression
	 *         level and channel name.
	 */
	public CompressionLevelState findCompressionLevelState(
			String compressionLevelName, String channelName) {
		byte[] rowKey = ColumnFamilyCompressionLevelConfiguration.getKey(
				channelName, compressionLevelName);
		SliceQuery<byte[], String, byte[]> query = HFactory.createSliceQuery(
				this.keyspace, BytesArraySerializer.get(),
				StringSerializer.get(), BytesArraySerializer.get());
		query.setColumnFamily(ColumnFamilyCompressionLevelConfiguration.NAME);
		query.setColumnNames(ColumnFamilyCompressionLevelConfiguration.STATE_COLUMNS);
		query.setKey(rowKey);
		ColumnSlice<String, byte[]> slice = query.execute().get();
		return ColumnFamilyCompressionLevelConfiguration
				.readCompressionLevelState(channelName, rowKey, slice);

	}

	/**
	 * Find the compression level configurations and respective states for a
	 * channel. The configuration and state for the special "raw" level can but
	 * does not have to be part of this list.
	 * 
	 * @param channelName
	 *            name of the channel to find the compression level
	 *            configuration and states for.
	 * @return array of pairs built of the compression level configuration and
	 *         the respective states, never <code>null</code>.
	 */
	public Pair<CompressionLevelConfig, CompressionLevelState>[] findCompressionLevelConfigsAndStates(
			String channelName) {
		return getCompressionLevelConfigsAndStates(channelName,
				getCompressionLevelKeys(channelName));
	}

	private LinkedList<byte[]> getCompressionLevelKeys(String channelName) {
		byte[] channelKey = ColumnFamilyChannelConfigurationToCompressionLevels
				.getKey(channelName);
		String startColumn = "";
		// Number of columns requested per query. It is very unlikely that there
		// are more than twenty compression levels for a single channel.
		int maxColumns = 20;
		int columnsReturned = 0;
		LinkedList<byte[]> compressionLevelKeys = new LinkedList<byte[]>();
		do {
			SliceQuery<byte[], String, byte[]> query = HFactory
					.createSliceQuery(this.keyspace,
							BytesArraySerializer.get(), StringSerializer.get(),
							BytesArraySerializer.get());
			query.setColumnFamily(ColumnFamilyChannelConfigurationToCompressionLevels.NAME);
			query.setKey(channelKey);
			query.setRange(startColumn, "", false, maxColumns);
			List<HColumn<String, byte[]>> columns = query.execute().get()
					.getColumns();
			columnsReturned = columns.size();
			boolean firstIteration = true;
			for (HColumn<String, byte[]> column : columns) {
				String compressionLevelName = column.getName();
				if (firstIteration && compressionLevelName.equals(startColumn)) {
					// Skip duplicate column
					continue;
				}
				firstIteration = false;
				// Save last retrieved column as start for next query.
				startColumn = compressionLevelName;
				byte[] compressionLevelKey = ColumnFamilyCompressionLevelConfiguration
						.getKey(channelName, compressionLevelName);
				compressionLevelKeys.add(compressionLevelKey);
			}
		} while (columnsReturned == maxColumns);
		return compressionLevelKeys;
	}

	private Pair<CompressionLevelConfig, CompressionLevelState>[] getCompressionLevelConfigsAndStates(
			String channelName, LinkedList<byte[]> compressionLevelKeys) {
		MultigetSliceQuery<byte[], String, byte[]> query = HFactory
				.createMultigetSliceQuery(this.keyspace,
						BytesArraySerializer.get(), StringSerializer.get(),
						BytesArraySerializer.get());
		query.setColumnFamily(ColumnFamilyCompressionLevelConfiguration.NAME);
		query.setColumnNames(ColumnFamilyCompressionLevelConfiguration.ALL_COLUMNS);
		query.setKeys(compressionLevelKeys);
		Rows<byte[], String, byte[]> rows = query.execute().get();
		ArrayList<Pair<CompressionLevelConfig, CompressionLevelState>> configs = new ArrayList<Pair<CompressionLevelConfig, CompressionLevelState>>(
				rows.getCount());
		for (Row<byte[], String, byte[]> row : rows) {
			ColumnSlice<String, byte[]> slice = row.getColumnSlice();
			CompressionLevelConfig config = ColumnFamilyCompressionLevelConfiguration
					.readCompressionLevelConfig(channelName, row.getKey(),
							slice);
			CompressionLevelState state = ColumnFamilyCompressionLevelConfiguration
					.readCompressionLevelState(channelName, row.getKey(), slice);
			if (config != null) {
				if (state == null) {
					state = new CompressionLevelState(
							config.getCompressionLevelName(), channelName,
							null, null);
				}
				configs.add(new Pair<CompressionLevelConfig, CompressionLevelState>(
						config, state));
			}
		}
		@SuppressWarnings("unchecked")
		Pair<CompressionLevelConfig, CompressionLevelState>[] result = (Pair<CompressionLevelConfig, CompressionLevelState>[]) configs
				.toArray(new Pair[configs.size()]);
		return result;
	}

	/**
	 * Creates a compression level configuration in the database.
	 * 
	 * @param channelName
	 *            name of the channel to create a compression-level for.
	 * @param compressionLevelName
	 *            name of the compression level to create a confiugration for.
	 * @param compressionPeriod
	 *            period (in seconds) between two samples stored for the
	 *            compression-level . Does not have a meaning for the special
	 *            "raw" level. If zero, no samples are generated for the
	 *            compression level (unless it is the special "raw" level, for
	 *            this level the compression period is always zero, as it does
	 *            not have a meaning).
	 * @param retentionPeriod
	 *            period (in seconds) after which a sample is deleted for the
	 *            compression-level. This time is measured going backwards in
	 *            time from the newest sample stored. This period should be
	 *            greater than <code>compressionPeriod</code>. For the special
	 *            "raw" level, this period should be at least twice the period
	 *            used as the longest compression period for the same channel.
	 *            If the retention period is zero, no samples are deleted from
	 *            the compression level.
	 * @return newly created compression level configuration.
	 */
	public CompressionLevelConfig createCompressionLevel(String channelName,
			String compressionLevelName, long compressionPeriod,
			long retentionPeriod) {
		Mutator<byte[]> mutator = HFactory.createMutator(keyspace,
				BytesArraySerializer.get());
		ColumnFamilyCompressionLevelConfiguration.insertCompressionLevelConfig(
				mutator, compressionLevelName, channelName, compressionPeriod,
				retentionPeriod);
		// Also create reference
		mutator.addInsertion(
				ColumnFamilyChannelConfigurationToCompressionLevels
						.getKey(channelName),
				ColumnFamilyChannelConfigurationToCompressionLevels.NAME,
				HFactory.createColumn(compressionLevelName, EMPTY_BYTE_ARRAY,
						StringSerializer.get(), BytesArraySerializer.get()));
		mutator.execute();
		return new CompressionLevelConfig(compressionLevelName, channelName,
				compressionPeriod, retentionPeriod);
	}

	/**
	 * Updates the sate for a compression level. Uses a newly created
	 * {@link Mutator} that is executed immediately.
	 * 
	 * @param compressionLevelState
	 *            compression level state to update.
	 * @param lastSavedSampleTime
	 *            timestamp of the latest sample stored for the compression
	 *            level.
	 * @param nextSampleTime
	 *            timestamp of the next sample that should be processed for the
	 *            compression level. Does not have a meaning for the special
	 *            "raw" compression level.
	 * @return updated compression-level state.
	 */
	public CompressionLevelState updateCompressionLevelState(
			CompressionLevelState compressionLevelState,
			ITimestamp lastSavedSampleTime, ITimestamp nextSampleTime) {
		Mutator<byte[]> mutator = HFactory.createMutator(keyspace,
				BytesArraySerializer.get());
		CompressionLevelState updatedState = updateCompressionLevelState(
				mutator, compressionLevelState, lastSavedSampleTime,
				nextSampleTime);
		mutator.execute();
		return updatedState;
	}

	/**
	 * Updates the sate for a compression level. Uses the passed mutator to
	 * perform the update operation.
	 * 
	 * @param mutator
	 *            mutator used for the update operation.
	 * @param compressionLevelState
	 *            compression level state to update.
	 * @param lastSavedSampleTime
	 *            timestamp of the latest sample stored for the compression
	 *            level.
	 * @param nextSampleTime
	 *            timestamp of the next sample that should be processed for the
	 *            compression level. Does not have a meaning for the special
	 *            "raw" compression level.
	 * @return updated compression-level state.
	 */
	public CompressionLevelState updateCompressionLevelState(
			Mutator<byte[]> mutator,
			CompressionLevelState compressionLevelState,
			ITimestamp lastSavedSampleTime, ITimestamp nextSampleTime) {
		String compressionLevelName = compressionLevelState
				.getCompressionLevelName();
		String channelName = compressionLevelState.getChannelName();
		if (compressionLevelState.getCompressionLevelName().equals(
				CompressionLevelConfig.RAW_COMPRESSION_LEVEL_NAME)) {
			// For raw samples, timestamp stored with channel configuration must
			// also be updated.
			updateLastSampleTime(compressionLevelState.getChannelName(),
					lastSavedSampleTime);
			// For raw samples next sample time is always zero.
			nextSampleTime = TimestampFactory.createTimestamp(0L, 0L);
		} else {
			byte[] rowKey = ColumnFamilyCompressionLevelConfiguration.getKey(
					channelName, compressionLevelName);
			if (lastSavedSampleTime == null) {
				mutator.addDeletion(
						rowKey,
						ColumnFamilyCompressionLevelConfiguration.NAME,
						ColumnFamilyCompressionLevelConfiguration.COLUMN_LAST_SAVED_SAMPLE_TIME,
						StringSerializer.get());
			} else {
				ColumnWriter
						.insertTimestamp(
								mutator,
								rowKey,
								ColumnFamilyCompressionLevelConfiguration.NAME,
								ColumnFamilyCompressionLevelConfiguration.COLUMN_LAST_SAVED_SAMPLE_TIME,
								lastSavedSampleTime);
			}
			if (nextSampleTime == null) {
				mutator.addDeletion(
						rowKey,
						ColumnFamilyCompressionLevelConfiguration.NAME,
						ColumnFamilyCompressionLevelConfiguration.COLUMN_NEXT_SAMPLE_TIME,
						StringSerializer.get());
			} else {
				ColumnWriter
						.insertTimestamp(
								mutator,
								rowKey,
								ColumnFamilyCompressionLevelConfiguration.NAME,
								ColumnFamilyCompressionLevelConfiguration.COLUMN_NEXT_SAMPLE_TIME,
								nextSampleTime);
			}
		}
		return new CompressionLevelState(compressionLevelName, channelName,
				lastSavedSampleTime, nextSampleTime);
	}

	/**
	 * Performs a clean-up on the column-families used to store the
	 * configuration. This removes all entries which are invalid or not directly
	 * or indirectly references by an engine. This does not delete any samples.
	 * 
	 * @param printStatus
	 *            if set to <code>true</code>, for each row being deleted a
	 *            message is printed to the standard output.
	 * @throws Exception
	 *             if {@link #getEngines()}, {@link #getGroups(EngineConfig)} or
	 *             {@link #getChannels(GroupConfig)} throws an exception.
	 */
	public void performCleanUp(boolean printStatus) throws Exception {
		HashMap<String, EngineConfig> engineConfigs = new HashMap<String, EngineConfig>();
		HashMap<Pair<String, String>, GroupConfig> groupConfigs = new HashMap<Pair<String, String>, GroupConfig>();
		HashMap<String, ChannelConfig> channelConfigs = new HashMap<String, ChannelConfig>();
		HashMap<Pair<String, String>, CompressionLevelConfig> compressionLevelConfigs = new HashMap<Pair<String, String>, CompressionLevelConfig>();
		for (EngineConfig engineConfig : getEngines()) {
			engineConfigs.put(engineConfig.getName(), engineConfig);
			for (GroupConfig groupConfig : getGroups(engineConfig)) {
				groupConfigs.put(
						new Pair<String, String>(engineConfig.getName(),
								groupConfig.getName()), groupConfig);
				for (ChannelConfig channelConfig : getChannels(groupConfig)) {
					channelConfigs.put(channelConfig.getName(), channelConfig);
					// Make sure raw level is always included
					compressionLevelConfigs.put(new Pair<String, String>(
							channelConfig.getName(),
							CompressionLevelConfig.RAW_COMPRESSION_LEVEL_NAME),
							null);
					for (CompressionLevelConfig compressionLevelConfig : findCompressionLevelConfigs(channelConfig
							.getName())) {
						compressionLevelConfigs.put(
								new Pair<String, String>(channelConfig
										.getName(), compressionLevelConfig
										.getCompressionLevelName()),
								compressionLevelConfig);
					}
				}
			}
		}

		cleanUpEngines(engineConfigs, printStatus);
		cleanUpGroups(groupConfigs, printStatus);
		cleanUpChannels(channelConfigs, printStatus);
		cleanUpCompressionLevels(compressionLevelConfigs, printStatus);
	}

	private void cleanUpEngines(HashMap<String, EngineConfig> engineConfigs,
			boolean printStatus) {
		cleanUpEngineOrChannelColumnFamily(engineConfigs,
				ColumnFamilyEngineConfiguration.NAME, "engine configuration",
				printStatus);
		cleanUpEngineOrChannelColumnFamily(engineConfigs,
				ColumnFamilyEngineConfigurationToGroups.NAME,
				"engine-to-group mappings for engine", printStatus);
	}

	private void cleanUpGroups(
			HashMap<Pair<String, String>, GroupConfig> groupConfigs,
			boolean printStatus) {
		cleanUpGroupOrCompressionLevelColumnFamily(groupConfigs,
				ColumnFamilyGroupConfiguration.NAME, "engine",
				"group configuration", printStatus);
		cleanUpGroupOrCompressionLevelColumnFamily(groupConfigs,
				ColumnFamilyGroupConfiguration.NAME, "engine",
				"group-to-channel mappings for group", printStatus);
	}

	private void cleanUpChannels(HashMap<String, ChannelConfig> channelConfigs,
			boolean printStatus) {
		cleanUpEngineOrChannelColumnFamily(channelConfigs,
				ColumnFamilyChannelConfiguration.NAME, "channel configuration",
				printStatus);
		cleanUpEngineOrChannelColumnFamily(channelConfigs,
				ColumnFamilyChannelConfigurationToCompressionLevels.NAME,
				"channel-to-compression-level mappings for channel",
				printStatus);
	}

	private void cleanUpCompressionLevels(
			HashMap<Pair<String, String>, CompressionLevelConfig> compressionLevelConfigs,
			boolean printStatus) {
		cleanUpGroupOrCompressionLevelColumnFamily(compressionLevelConfigs,
				ColumnFamilyCompressionLevelConfiguration.NAME, "channel",
				"compression-level configuration", printStatus);
	}

	private void cleanUpEngineOrChannelColumnFamily(Map<String, ?> configs,
			String columnFamilyName, String configurationName,
			boolean printStatus) {
		RangeSlicesQuery<byte[], String, byte[]> query = HFactory
				.createRangeSlicesQuery(keyspace, BytesArraySerializer.get(),
						StringSerializer.get(), BytesArraySerializer.get());
		query.setColumnFamily(columnFamilyName);
		// By using a keys-only query, we might see rows which are already
		// deleted. However, deleting them twice will not hurt and not
		// requesting any columns will save us I/O and data-transfer.
		query.setReturnKeysOnly();
		byte[] rowKey = EngineOrChannelConfigurationKey.generateDatabaseKey("");
		int rowsRequested = 5000;
		int rowsReturned = 0;
		do {
			query.setKeys(rowKey, null);
			query.setRowCount(rowsRequested);
			OrderedRows<byte[], String, byte[]> rows = query.execute().get();
			rowsReturned = rows.getCount();
			Mutator<byte[]> mutator = HFactory.createMutator(keyspace,
					BytesArraySerializer.get());
			for (Row<byte[], String, byte[]> row : rows) {
				rowKey = row.getKey();
				String name = EngineOrChannelConfigurationKey
						.extractRealKey(rowKey);
				if (name == null || !configs.containsKey(name)) {
					if (printStatus) {
						System.out.println("Removing invalid or dangling "
								+ configurationName + " \"" + name + "\")");
					}
					mutator.addDeletion(rowKey, columnFamilyName);
				}

			}
			mutator.execute();
		} while (rowsReturned == rowsRequested);
	}

	private void cleanUpGroupOrCompressionLevelColumnFamily(
			Map<Pair<String, String>, ?> configs, String columnFamilyName,
			String firstConfigurationName, String secondConfigurationName,
			boolean printStatus) {
		RangeSlicesQuery<byte[], String, byte[]> query = HFactory
				.createRangeSlicesQuery(keyspace, BytesArraySerializer.get(),
						StringSerializer.get(), BytesArraySerializer.get());
		query.setColumnFamily(columnFamilyName);
		// By using a keys-only query, we might see rows which are already
		// deleted. However, deleting them twice will not hurt and not
		// requesting any columns will save us I/O and data-transfer.
		query.setReturnKeysOnly();
		byte[] rowKey = CompressionLevelOrGroupConfigurationKey
				.generateDatabaseKey("", "");
		int rowsRequested = 5000;
		int rowsReturned = 0;
		do {
			query.setKeys(rowKey, null);
			query.setRowCount(rowsRequested);
			OrderedRows<byte[], String, byte[]> rows = query.execute().get();
			rowsReturned = rows.getCount();
			Mutator<byte[]> mutator = HFactory.createMutator(keyspace,
					BytesArraySerializer.get());
			for (Row<byte[], String, byte[]> row : rows) {
				rowKey = row.getKey();
				String firstName = CompressionLevelOrGroupConfigurationKey
						.extractFirstName(rowKey);
				String secondName = CompressionLevelOrGroupConfigurationKey
						.extractSecondName(rowKey);
				if (firstName == null
						|| secondName == null
						|| !configs.containsKey(new Pair<String, String>(
								firstName, secondName))) {
					if (printStatus) {
						System.out.println("Removing dangling or invalid "
								+ secondConfigurationName + " \"" + secondName
								+ "\" (" + firstConfigurationName + " \""
								+ firstName + "\")");
					}
					mutator.addDeletion(rowKey, columnFamilyName);
				}

			}
			mutator.execute();
		} while (rowsReturned == rowsRequested);
	}

}
