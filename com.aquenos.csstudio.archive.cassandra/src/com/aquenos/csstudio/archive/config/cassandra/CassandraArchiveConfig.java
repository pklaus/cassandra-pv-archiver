/*
 * Copyright 2012-2013 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.csstudio.archive.config.cassandra;

import java.math.BigInteger;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.UUID;

import org.csstudio.archive.config.ArchiveConfig;
import org.csstudio.archive.config.ChannelConfig;
import org.csstudio.archive.config.EngineConfig;
import org.csstudio.archive.config.GroupConfig;
import org.csstudio.archive.config.SampleMode;

import com.aquenos.csstudio.archive.cassandra.EngineNameHolder;
import com.aquenos.csstudio.archive.cassandra.SampleStore;
import com.aquenos.csstudio.archive.cassandra.util.Pair;
import com.aquenos.csstudio.archive.config.cassandra.internal.ColumnFamilyChannelConfiguration;
import com.aquenos.csstudio.archive.config.cassandra.internal.ColumnFamilyChannelConfigurationToCompressionLevels;
import com.aquenos.csstudio.archive.config.cassandra.internal.ColumnFamilyEngineConfiguration;
import com.aquenos.csstudio.archive.config.cassandra.internal.ColumnFamilyEngineConfigurationToGroups;
import com.aquenos.csstudio.archive.config.cassandra.internal.ColumnFamilyGroupConfiguration;
import com.aquenos.csstudio.archive.config.cassandra.internal.ColumnFamilyGroupConfigurationToChannels;
import com.netflix.astyanax.AstyanaxConfiguration;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.SimpleAuthenticationCredentials;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.retry.RetryPolicy;
import com.netflix.astyanax.serializers.BigIntegerSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.netflix.astyanax.util.RangeBuilder;

/**
 * Configuration for the Cassandra Archive.
 * 
 * @author Sebastian Marsching
 */
public class CassandraArchiveConfig implements ArchiveConfig {

    private final static String SPECIAL_DISABLED_CHANNELS_GROUP_NAME = "__disabled_channels";

    private AstyanaxContext<Cluster> context;
    private Cluster cluster;
    private Keyspace keyspace;
    private ConsistencyLevel readDataConsistencyLevel;
    private ConsistencyLevel writeDataConsistencyLevel;
    private ConsistencyLevel readMetaDataConsistencyLevel;
    private ConsistencyLevel writeMetaDataConsistencyLevel;
    private SampleStore sampleStore;

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
     * @param readDataConsistencyLevel
     *            consistency level used when reading sample data.
     * @param writeDataConsistencyLevel
     *            consistency level used when writing sample data.
     * @param readMetaDataConsistencyLevel
     *            consistency level used when reading meta-data (that means
     *            configuration data and sample bucket sizes).
     * @param writeMetaDataConsistencyLevel
     *            consistency level used when writing meta-data (that means
     *            configuration data and sample bucket sizes).
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
            String keyspaceName, ConsistencyLevel readDataConsistencyLevel,
            ConsistencyLevel writeDataConsistencyLevel,
            ConsistencyLevel readMetaDataConsistencyLevel,
            ConsistencyLevel writeMetaDataConsistencyLevel,
            RetryPolicy retryPolicy, String username, String password) {
        this.readDataConsistencyLevel = readDataConsistencyLevel;
        this.writeDataConsistencyLevel = writeDataConsistencyLevel;
        this.readMetaDataConsistencyLevel = readMetaDataConsistencyLevel;
        this.writeMetaDataConsistencyLevel = writeMetaDataConsistencyLevel;
        // We use a random identifier for the cluster, because there might be
        // several instances of the archive configuration. If we used the same
        // cluster for all of them, a cluster still being used might be shutdown
        // when one of them is closed.
        String uuid = UUID.randomUUID().toString();
        ConnectionPoolConfigurationImpl cpConfig = new ConnectionPoolConfigurationImpl(
                uuid).setSeeds(cassandraHosts).setPort(cassandraPort);
        if (username != null && password != null) {
            cpConfig.setAuthenticationCredentials(new SimpleAuthenticationCredentials(
                    username, password));
        }
        AstyanaxConfiguration asConfig = new AstyanaxConfigurationImpl()
                .setDefaultReadConsistencyLevel(this.readDataConsistencyLevel)
                .setDefaultWriteConsistencyLevel(this.writeDataConsistencyLevel)
                .setRetryPolicy(retryPolicy)
                .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE);
        AstyanaxContext<Cluster> context = new AstyanaxContext.Builder()
                .forCluster(uuid).forKeyspace(keyspaceName)
                .withConnectionPoolConfiguration(cpConfig)
                .withAstyanaxConfiguration(asConfig)
                .buildCluster(ThriftFamilyFactory.getInstance());
        this.context = context;
        this.context.start();
        this.cluster = this.context.getEntity();
        this.keyspace = this.cluster.getKeyspace(keyspaceName);
        this.sampleStore = new SampleStore(this.cluster, this.keyspace,
                this.readDataConsistencyLevel, this.writeDataConsistencyLevel,
                this.readMetaDataConsistencyLevel,
                this.writeMetaDataConsistencyLevel, false, false);
        checkOrCreateColumnFamilies();
    }

    /**
     * Creates an archive configuration object backed by the specified keyspace.
     * The connection to the database is not closed when the {@link #close()}
     * method is called.
     * 
     * @param cluster
     *            Cassandra cluster that stores the database.
     * @param keyspace
     *            keyspace that stores configuration.
     * @param readDataConsistencyLevel
     *            consistency level used when reading sample data.
     * @param writeDataConsistencyLevel
     *            consistency level used when writing sample data.
     * @param readMetaDataConsistencyLevel
     *            consistency level used when reading meta-data (that means
     *            configuration data and sample bucket sizes).
     * @param writeMetaDataConsistencyLevel
     *            consistency level used when writing meta-data (that means
     *            configuration data and sample bucket sizes).
     */
    public CassandraArchiveConfig(Cluster cluster, Keyspace keyspace,
            ConsistencyLevel readDataConsistencyLevel,
            ConsistencyLevel writeDataConsistencyLevel,
            ConsistencyLevel readMetaDataConsistencyLevel,
            ConsistencyLevel writeMetaDataConsistencyLevel) {
        this(cluster, keyspace, readDataConsistencyLevel,
                writeDataConsistencyLevel, readMetaDataConsistencyLevel,
                writeMetaDataConsistencyLevel, null);
    }

    /**
     * Creates an archive configuration object backed by the specified keyspace.
     * The connection to the database is not closed when the {@link #close()}
     * method is called. The supplied {@link SampleStore} is used for get the
     * last sample time of a channel, when requested.
     * 
     * @param cluster
     *            Cassandra cluster that stores the database.
     * @param keyspace
     *            keyspace that stores configuration.
     * @param readDataConsistencyLevel
     *            consistency level used when reading sample data.
     * @param writeDataConsistencyLevel
     *            consistency level used when writing sample data.
     * @param readMetaDataConsistencyLevel
     *            consistency level used when reading meta-data (that means
     *            configuration data and sample bucket sizes).
     * @param writeMetaDataConsistencyLevel
     *            consistency level used when writing meta-data (that means
     *            configuration data and sample bucket sizes).
     * @param sampleStore
     *            sample store that is used for getting the last sample time of
     *            a channel. If <code>null</code>, an internal sample store is
     *            created for this purpose. The sample store must use the same
     *            consistency levels that are supplied to this configuration
     *            object.
     */
    public CassandraArchiveConfig(Cluster cluster, Keyspace keyspace,
            ConsistencyLevel readDataConsistencyLevel,
            ConsistencyLevel writeDataConsistencyLevel,
            ConsistencyLevel readMetaDataConsistencyLevel,
            ConsistencyLevel writeMetaDataConsistencyLevel,
            SampleStore sampleStore) {
        this.cluster = cluster;
        this.keyspace = keyspace;
        this.readDataConsistencyLevel = readDataConsistencyLevel;
        this.writeDataConsistencyLevel = writeDataConsistencyLevel;
        this.readMetaDataConsistencyLevel = readMetaDataConsistencyLevel;
        this.writeMetaDataConsistencyLevel = writeMetaDataConsistencyLevel;
        if (sampleStore == null) {
            this.sampleStore = new SampleStore(this.cluster, this.keyspace,
                    this.readDataConsistencyLevel,
                    this.writeDataConsistencyLevel,
                    this.readMetaDataConsistencyLevel,
                    this.writeMetaDataConsistencyLevel, false, false);
        } else {
            this.sampleStore = sampleStore;
        }
        checkOrCreateColumnFamilies();
    }

    private void checkOrCreateColumnFamilies() {
        // Create missing column families and check that existing column
        // families have right configuration.
        ColumnFamilyEngineConfiguration.createOrCheckColumnFamily(cluster,
                keyspace);
        ColumnFamilyEngineConfigurationToGroups.createOrCheckColumnFamily(
                cluster, keyspace);
        ColumnFamilyGroupConfiguration.createOrCheckColumnFamily(cluster,
                keyspace);
        ColumnFamilyGroupConfigurationToChannels.createOrCheckColumnFamily(
                cluster, keyspace);
        ColumnFamilyChannelConfiguration.createOrCheckColumnFamily(cluster,
                keyspace);
        ColumnFamilyChannelConfigurationToCompressionLevels
                .createOrCheckColumnFamily(cluster, keyspace);
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
        ArrayList<EngineConfig> configs = new ArrayList<EngineConfig>();
        Rows<String, String> rows = keyspace
                .prepareQuery(ColumnFamilyEngineConfiguration.CF)
                .setConsistencyLevel(readMetaDataConsistencyLevel).getAllRows()
                .setIncludeEmptyRows(false).setRowLimit(1000)
                .withColumnSlice(ColumnFamilyEngineConfiguration.ALL_COLUMNS)
                .execute().getResult();
        for (Row<String, String> row : rows) {
            EngineConfig config = ColumnFamilyEngineConfiguration
                    .readEngineConfig(row.getKey(), row.getColumns());
            if (config != null) {
                configs.add(config);
            }
        }
        return configs.toArray(new EngineConfig[configs.size()]);
    }

    @Override
    public EngineConfig findEngine(String name) throws Exception {
        // Store the engine name so that other components have the chance to
        // find out, for which engine this instance is running.
        EngineNameHolder.setEngineName(name);
        ColumnList<String> columns = keyspace
                .prepareQuery(ColumnFamilyEngineConfiguration.CF)
                .setConsistencyLevel(readMetaDataConsistencyLevel).getKey(name)
                .withColumnSlice(ColumnFamilyEngineConfiguration.ALL_COLUMNS)
                .execute().getResult();
        return ColumnFamilyEngineConfiguration.readEngineConfig(name, columns);
    }

    @Override
    public GroupConfig[] getGroups(EngineConfig engine) throws Exception {
        String engineName = engine.getName();
        ColumnList<String> columns = keyspace
                .prepareQuery(ColumnFamilyEngineConfigurationToGroups.CF)
                .setConsistencyLevel(readMetaDataConsistencyLevel)
                .getKey(engine.getName()).execute().getResult();
        LinkedList<Pair<String, String>> groupKeys = new LinkedList<Pair<String, String>>();
        for (String groupName : columns.getColumnNames()) {
            groupKeys.add(new Pair<String, String>(engineName, groupName));
        }
        return getGroups(engineName, groupKeys);
    }

    private GroupConfig[] getGroups(String engineName,
            Iterable<Pair<String, String>> groupKeys)
            throws ConnectionException {
        Rows<Pair<String, String>, String> rows = keyspace
                .prepareQuery(ColumnFamilyGroupConfiguration.CF)
                .setConsistencyLevel(readMetaDataConsistencyLevel)
                .getKeySlice(groupKeys)
                .withColumnSlice(ColumnFamilyGroupConfiguration.ALL_COLUMNS)
                .execute().getResult();
        ArrayList<GroupConfig> configs = new ArrayList<GroupConfig>(rows.size());
        for (Row<Pair<String, String>, String> row : rows) {
            CassandraGroupConfig config = ColumnFamilyGroupConfiguration
                    .readGroupConfig(engineName, (String) row.getKey()
                            .getSecond(), row.getColumns());
            if (config != null) {
                configs.add(config);
            }
        }
        return configs.toArray(new GroupConfig[configs.size()]);
    }

    /**
     * Returns the special group that contains disabled channels. This group is
     * not returned by {@link #getGroups(EngineConfig)}, so that it is not seen
     * by the archive engine. However, for reading data the channels in this
     * group can be used like all other channels.
     * 
     * @param engineConfig
     *            configuration of the engine the group shall be returned for.
     * @return configuration for a special group that can be used for disabled
     *         channels.
     */
    public GroupConfig getDisabledChannelsGroup(EngineConfig engineConfig) {
        return new CassandraGroupConfig(engineConfig.getName(),
                SPECIAL_DISABLED_CHANNELS_GROUP_NAME, "");
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

        ColumnList<String> columns = keyspace
                .prepareQuery(ColumnFamilyGroupConfigurationToChannels.CF)
                .setConsistencyLevel(readMetaDataConsistencyLevel)
                .getKey(ColumnFamilyGroupConfigurationToChannels.getKey(
                        engineName, groupName)).execute().getResult();
        return getChannels(engineName, groupName, columns.getColumnNames());
    }

    private ChannelConfig[] getChannels(String engineName, String groupName,
            Iterable<String> channelNames) throws ConnectionException {
        Rows<String, String> rows = keyspace
                .prepareQuery(ColumnFamilyChannelConfiguration.CF)
                .setConsistencyLevel(readMetaDataConsistencyLevel)
                .getKeySlice(channelNames)
                .withColumnSlice(ColumnFamilyChannelConfiguration.ALL_COLUMNS)
                .execute().getResult();
        ArrayList<ChannelConfig> configs = new ArrayList<ChannelConfig>(
                rows.size());
        for (Row<String, String> row : rows) {
            ColumnList<String> columns = row.getColumns();
            ChannelConfig config = ColumnFamilyChannelConfiguration
                    .readChannelConfig(row.getKey(), columns, sampleStore);
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
     * @throws ConnectionException
     *             if query in Cassandra database fails.
     */
    public ChannelConfig findChannel(String channelName)
            throws ConnectionException {
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
     * @throws ConnectionException
     *             if query in Cassandra database fails.
     */
    public ChannelConfig[] findChannels(String[] channelNames)
            throws ConnectionException {
        Rows<String, String> rows = keyspace
                .prepareQuery(ColumnFamilyChannelConfiguration.CF)
                .setConsistencyLevel(readMetaDataConsistencyLevel)
                .getKeySlice(channelNames)
                .withColumnSlice(ColumnFamilyChannelConfiguration.ALL_COLUMNS)
                .execute().getResult();
        ArrayList<ChannelConfig> configs = new ArrayList<ChannelConfig>(
                rows.size());
        for (Row<String, String> row : rows) {
            CassandraChannelConfig config = ColumnFamilyChannelConfiguration
                    .readChannelConfig(row.getKey(), row.getColumns(),
                            sampleStore);
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
        OperationResult<Rows<String, String>> result;
        try {
            result = keyspace
                    .prepareQuery(ColumnFamilyChannelConfiguration.CF)
                    .setConsistencyLevel(readMetaDataConsistencyLevel)
                    .getAllRows()
                    .withColumnSlice(
                            ColumnFamilyChannelConfiguration.COLUMN_SAMPLE_MODE)
                    .setIncludeEmptyRows(false).execute();
        } catch (ConnectionException e) {
            throw new RuntimeException(e);
        }

        ArrayList<String> channelNames = new ArrayList<String>(0);
        for (Row<String, String> row : result.getResult()) {
            channelNames.add(row.getKey());
        }
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

    @Override
    public void close() {
        if (context != null) {
            context.shutdown();
            this.context = null;
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
     * @throws ConnectionException
     *             if query in Cassandra database fails.
     */
    public EngineConfig createEngine(String engineName, String description,
            String engineUrl) throws URISyntaxException, ConnectionException {
        MutationBatch mutationBatch = keyspace.prepareMutationBatch()
                .withConsistencyLevel(writeMetaDataConsistencyLevel);
        ColumnFamilyEngineConfiguration.insertEngineConfig(mutationBatch,
                engineName, description, engineUrl);
        mutationBatch.execute();
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
        MutationBatch mutationBatch = keyspace.prepareMutationBatch()
                .withConsistencyLevel(writeMetaDataConsistencyLevel);
        // Delete all groups for this engine.
        for (GroupConfig groupConfig : getGroups(engineConfig)) {
            deleteGroup(mutationBatch, groupConfig);
        }
        // Also remove the special deactivated channels group.
        deleteGroup(mutationBatch, getDisabledChannelsGroup(engineConfig));
        // Delete the engine configuration.
        mutationBatch.withRow(ColumnFamilyEngineConfiguration.CF,
                engineConfig.getName()).delete();
        mutationBatch.withRow(ColumnFamilyEngineConfigurationToGroups.CF,
                engineConfig.getName()).delete();
        mutationBatch.execute();
    }

    /**
     * Creates a group configuration in the database.
     * 
     * @param engineConfig
     *            configuration of the engine to create the group for.
     * @param groupName
     *            name of the group.
     * @return newly created group configuration.
     * @throws ConnectionException
     *             if query in Cassandra database fails.
     */
    public GroupConfig createGroup(EngineConfig engineConfig, String groupName)
            throws ConnectionException {
        if (groupName.equals(SPECIAL_DISABLED_CHANNELS_GROUP_NAME)) {
            // The special deactivated channels group does not have a real
            // configuration and is not referenced by the engine configuration.
            // It is only used to aggregate all the channels which should still
            // exist but are not active any longer.
            return new CassandraGroupConfig(engineConfig.getName(), groupName,
                    "");
        }
        String engineName = engineConfig.getName();
        MutationBatch mutationBatch = keyspace.prepareMutationBatch()
                .withConsistencyLevel(writeMetaDataConsistencyLevel);
        String enablingChannel = "";
        ColumnFamilyGroupConfiguration.insertGroupConfig(mutationBatch,
                engineName, groupName, enablingChannel);
        // Register group with engine configuration
        mutationBatch.withRow(ColumnFamilyEngineConfigurationToGroups.CF,
                engineName).putColumn(groupName, "");
        mutationBatch.execute();
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
        MutationBatch mutationBatch = keyspace.prepareMutationBatch()
                .withConsistencyLevel(writeMetaDataConsistencyLevel);
        deleteGroup(mutationBatch, groupConfig);
        mutationBatch.execute();
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
    private void deleteGroup(MutationBatch mutationBatch,
            GroupConfig groupConfig) throws Exception {
        CassandraGroupConfig cassandraGroupConfig = narrowGroupConfig(groupConfig);
        for (ChannelConfig channelConfig : getChannels(groupConfig)) {
            deleteChannel(mutationBatch, channelConfig);
        }
        Pair<String, String> groupKey = ColumnFamilyGroupConfiguration.getKey(
                cassandraGroupConfig.getEngineName(),
                cassandraGroupConfig.getName());
        mutationBatch.withRow(ColumnFamilyGroupConfiguration.CF, groupKey)
                .delete();
        mutationBatch.withRow(ColumnFamilyGroupConfigurationToChannels.CF,
                groupKey).delete();
        // Also delete reference to group in engine configuration
        mutationBatch.withRow(ColumnFamilyEngineConfigurationToGroups.CF,
                cassandraGroupConfig.getEngineName()).deleteColumn(
                groupConfig.getName());
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
     * @throws ConnectionException
     *             if query in Cassandra database fails.
     * @see GroupConfig#getEnablingChannel()
     */
    public GroupConfig updateGroupEnablingChannel(GroupConfig groupConfig,
            String enablingChannelName) throws ConnectionException {
        CassandraGroupConfig cassandraGroupConfig = narrowGroupConfig(groupConfig);
        MutationBatch mutationBatch = keyspace.prepareMutationBatch()
                .withConsistencyLevel(writeMetaDataConsistencyLevel);
        mutationBatch.withRow(
                ColumnFamilyGroupConfiguration.CF,
                ColumnFamilyGroupConfiguration.getKey(
                        cassandraGroupConfig.getEngineName(),
                        cassandraGroupConfig.getName())).putColumn(
                ColumnFamilyGroupConfiguration.COLUMN_ENABLING_CHANNEL,
                enablingChannelName);
        mutationBatch.execute();
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
     * @throws ConnectionException
     *             if query in Cassandra database fails.
     */
    public ChannelConfig createChannel(GroupConfig groupConfig,
            String channelName, SampleMode sampleMode)
            throws ConnectionException {
        CassandraGroupConfig cassandraGroupConfig = narrowGroupConfig(groupConfig);
        String engineName = cassandraGroupConfig.getEngineName();
        String groupName = cassandraGroupConfig.getName();
        // Insert new configuration into the database
        MutationBatch mutationBatch = keyspace.prepareMutationBatch()
                .withConsistencyLevel(writeMetaDataConsistencyLevel);
        ColumnFamilyChannelConfiguration.insertChannelConfig(mutationBatch,
                engineName, groupName, channelName, sampleMode);
        // Register channel with group configuration.
        Pair<String, String> groupKey = ColumnFamilyGroupConfigurationToChannels
                .getKey(engineName, groupName);
        mutationBatch.withRow(ColumnFamilyGroupConfigurationToChannels.CF,
                groupKey).putColumn(channelName, "");
        mutationBatch.execute();
        // We do not create the channel configuration object directly, because
        // we want to add the last sample time, if it already existed in the
        // database.
        return new CassandraChannelConfig(engineName, groupName, channelName,
                sampleMode, sampleStore);
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
     * @throws ConnectionException
     *             if query in Cassandra database fails.
     */
    public void deleteChannel(ChannelConfig channelConfig)
            throws ConnectionException {
        MutationBatch mutationBatch = keyspace.prepareMutationBatch()
                .withConsistencyLevel(writeMetaDataConsistencyLevel);
        deleteChannel(mutationBatch, channelConfig);
        mutationBatch.execute();
    }

    /**
     * Deletes a channel configuration from the database using the passed
     * mutator. Deleting a channel configuration will not automatically delete
     * the samples for the channel, however they are deleted the next time
     * {@link com.aquenos.csstudio.archive.cassandra.SampleStore#performCleanUp(java.util.HashSet, boolean)}
     * is executed.
     * 
     * @param mutationBatch
     *            mutationBatch to use for the delete operation.
     * @param channelConfig
     *            channel configuration to delete.
     */
    private void deleteChannel(MutationBatch mutationBatch,
            ChannelConfig channelConfig) {
        CassandraChannelConfig cassandraChannelConfig = narrowChannelConfig(channelConfig);
        String channelName = channelConfig.getName();
        // First we delete all compression levels for the channel.
        mutationBatch.withRow(
                ColumnFamilyChannelConfigurationToCompressionLevels.CF,
                channelName).delete();
        // Next we delete the channel configuration.
        mutationBatch.withRow(ColumnFamilyChannelConfiguration.CF, channelName)
                .delete();
        // Finally we delete the reference in the channel's group.
        Pair<String, String> groupKey = ColumnFamilyGroupConfigurationToChannels
                .getKey(cassandraChannelConfig.getEngineName(),
                        cassandraChannelConfig.getGroupName());
        mutationBatch.withRow(ColumnFamilyGroupConfigurationToChannels.CF,
                groupKey).delete();
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
     * @throws ConnectionException
     *             if query in Cassandra database fails.
     */
    public CompressionLevelConfig[] findCompressionLevelConfigs(
            String channelName) throws ConnectionException {
        ColumnList<BigInteger> columnList = keyspace
                .prepareQuery(
                        ColumnFamilyChannelConfigurationToCompressionLevels.CF)
                .setConsistencyLevel(readMetaDataConsistencyLevel)
                .getKey(channelName).execute().getResult();
        ArrayList<CompressionLevelConfig> configs = new ArrayList<CompressionLevelConfig>(
                columnList.size());
        for (Column<BigInteger> column : columnList) {
            configs.add(new CompressionLevelConfig(channelName, column
                    .getName().longValue(), column.getValue(
                    BigIntegerSerializer.get()).longValue()));
        }
        return configs.toArray(new CompressionLevelConfig[configs.size()]);
    }

    /**
     * Creates a compression level configuration in the database.
     * 
     * @param channelName
     *            name of the channel to create a compression-level for.
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
     * @throws ConnectionException
     *             if query in Cassandra database fails.
     */
    public CompressionLevelConfig createCompressionLevel(String channelName,
            long compressionPeriod, long retentionPeriod)
            throws ConnectionException {
        MutationBatch mutationBatch = keyspace.prepareMutationBatch()
                .withConsistencyLevel(writeMetaDataConsistencyLevel);
        mutationBatch.withRow(
                ColumnFamilyChannelConfigurationToCompressionLevels.CF,
                channelName).putColumn(BigInteger.valueOf(compressionPeriod),
                BigInteger.valueOf(retentionPeriod),
                BigIntegerSerializer.get(), null);
        mutationBatch.execute();
        return new CompressionLevelConfig(channelName, compressionPeriod,
                retentionPeriod);
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
        for (EngineConfig engineConfig : getEngines()) {
            engineConfigs.put(engineConfig.getName(), engineConfig);
            for (GroupConfig groupConfig : getGroups(engineConfig)) {
                groupConfigs.put(
                        new Pair<String, String>(engineConfig.getName(),
                                groupConfig.getName()), groupConfig);
                for (ChannelConfig channelConfig : getChannels(groupConfig)) {
                    channelConfigs.put(channelConfig.getName(), channelConfig);
                }
            }
            // Also add the special deactivate channels group which is not
            // referenced in the engine configuration.
            GroupConfig deactivatedChannelsGroupConfig = getDisabledChannelsGroup(engineConfig);
            groupConfigs.put(new Pair<String, String>(engineConfig.getName(),
                    SPECIAL_DISABLED_CHANNELS_GROUP_NAME),
                    deactivatedChannelsGroupConfig);
            // Also add the channels for this group
            for (ChannelConfig channelConfig : getChannels(deactivatedChannelsGroupConfig)) {
                channelConfigs.put(channelConfig.getName(), channelConfig);
            }
        }

        cleanUpEngines(engineConfigs, printStatus);
        cleanUpGroups(groupConfigs, printStatus);
        cleanUpChannels(channelConfigs, printStatus);
    }

    private void cleanUpEngines(HashMap<String, EngineConfig> engineConfigs,
            boolean printStatus) throws ConnectionException {
        cleanUpEngineOrChannelColumnFamily(engineConfigs,
                ColumnFamilyEngineConfiguration.CF, "engine configuration",
                printStatus);
        cleanUpEngineOrChannelColumnFamily(engineConfigs,
                ColumnFamilyEngineConfigurationToGroups.CF,
                "engine-to-group mappings for engine", printStatus);
    }

    private void cleanUpGroups(
            HashMap<Pair<String, String>, GroupConfig> groupConfigs,
            boolean printStatus) throws ConnectionException {
        cleanUpGroupColumnFamily(groupConfigs,
                ColumnFamilyGroupConfiguration.CF, "engine",
                "group configuration", printStatus);
        cleanUpGroupColumnFamily(groupConfigs,
                ColumnFamilyGroupConfigurationToChannels.CF, "engine",
                "group-to-channel mappings for group", printStatus);
    }

    private void cleanUpChannels(HashMap<String, ChannelConfig> channelConfigs,
            boolean printStatus) throws ConnectionException {
        cleanUpEngineOrChannelColumnFamily(channelConfigs,
                ColumnFamilyChannelConfiguration.CF, "channel configuration",
                printStatus);
        cleanUpEngineOrChannelColumnFamily(channelConfigs,
                ColumnFamilyChannelConfigurationToCompressionLevels.CF,
                "channel-to-compression-level mappings for channel",
                printStatus);
    }

    private void cleanUpEngineOrChannelColumnFamily(Map<String, ?> configs,
            ColumnFamily<String, ?> columnFamily, String configurationName,
            boolean printStatus) throws ConnectionException {
        // Look for all rows that have at least one column.
        Rows<String, ?> rows = keyspace.prepareQuery(columnFamily)
                .setConsistencyLevel(readMetaDataConsistencyLevel).getAllRows()
                .setRowLimit(5000)
                .withColumnRange(new RangeBuilder().setLimit(1).build())
                .execute().getResult();
        MutationBatch mutationBatch = keyspace.prepareMutationBatch()
                .withConsistencyLevel(writeMetaDataConsistencyLevel);
        for (Row<String, ?> row : rows) {
            String name = row.getKey();
            if (!configs.containsKey(name)) {
                if (printStatus) {
                    System.out.println("Removing invalid or dangling "
                            + configurationName + " \"" + name + "\")");
                }
                mutationBatch.withRow(columnFamily, name).delete();
            }

        }
        mutationBatch.execute();
    }

    private void cleanUpGroupColumnFamily(Map<Pair<String, String>, ?> configs,
            ColumnFamily<Pair<String, String>, ?> columnFamily,
            String firstConfigurationName, String secondConfigurationName,
            boolean printStatus) throws ConnectionException {
        // Look for all rows that have at least one column.
        Rows<Pair<String, String>, ?> rows = keyspace
                .prepareQuery(columnFamily)
                .setConsistencyLevel(readMetaDataConsistencyLevel).getAllRows()
                .setRowLimit(5000)
                .withColumnRange(new RangeBuilder().setLimit(1).build())
                .execute().getResult();
        MutationBatch mutationBatch = keyspace.prepareMutationBatch()
                .withConsistencyLevel(writeMetaDataConsistencyLevel);
        for (Row<Pair<String, String>, ?> row : rows) {
            Pair<String, String> rowKey = row.getKey();
            String firstName = rowKey.getFirst();
            String secondName = rowKey.getSecond();
            if (firstName == null || secondName == null
                    || !configs.containsKey(rowKey)) {
                if (printStatus) {
                    System.out.println("Removing dangling or invalid "
                            + secondConfigurationName + " \"" + secondName
                            + "\" (" + firstConfigurationName + " \""
                            + firstName + "\")");
                }
                mutationBatch.withRow(columnFamily, rowKey).delete();
            }

        }
        mutationBatch.execute();
    }

}
