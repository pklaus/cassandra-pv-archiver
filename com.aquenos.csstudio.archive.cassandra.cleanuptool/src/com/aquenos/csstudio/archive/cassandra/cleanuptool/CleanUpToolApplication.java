/*
 * Copyright 2012-2013 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.csstudio.archive.cassandra.cleanuptool;

import java.util.HashSet;
import java.util.UUID;

import org.csstudio.apputil.args.ArgParser;
import org.csstudio.apputil.args.BooleanOption;
import org.csstudio.apputil.args.IntegerOption;
import org.csstudio.apputil.args.StringOption;
import org.csstudio.archive.config.ChannelConfig;
import org.csstudio.archive.config.EngineConfig;
import org.csstudio.archive.config.GroupConfig;
import org.csstudio.logging.LogConfigurator;
import org.eclipse.equinox.app.IApplication;
import org.eclipse.equinox.app.IApplicationContext;

import com.aquenos.csstudio.archive.cassandra.CassandraArchivePreferences;
import com.aquenos.csstudio.archive.cassandra.SampleStore;
import com.aquenos.csstudio.archive.cassandra.util.Pair;
import com.aquenos.csstudio.archive.config.cassandra.CassandraArchiveConfig;
import com.aquenos.csstudio.archive.config.cassandra.CompressionLevelConfig;
import com.netflix.astyanax.AstyanaxConfiguration;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.SimpleAuthenticationCredentials;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

/**
 * Application that performs a clean-up on the configuration and sample column
 * families.
 * 
 * @author Sebastian Marsching
 * @see CassandraArchiveConfig#performCleanUp(boolean)
 * @see SampleStore#performCleanUp(HashSet, boolean)
 */
public class CleanUpToolApplication implements IApplication {

    @Override
    public Object start(final IApplicationContext context) throws Exception {
        // Handle command-line options
        final String args[] = (String[]) context.getArguments().get(
                "application.args");

        final ArgParser parser = new ArgParser();
        final BooleanOption help = new BooleanOption(parser, "-help",
                "show help");
        final StringOption cassandraHosts = new StringOption(parser,
                "-cassandra_hosts",
                "first-server.example.com,second-server.example.com",
                "Cassandra Hosts (comma-separated)",
                CassandraArchivePreferences.getHosts());
        final IntegerOption cassandraPort = new IntegerOption(parser,
                "-cassandra_port", "9160", "Cassandra Port",
                CassandraArchivePreferences.getPort());
        final StringOption cassandraKeyspace = new StringOption(parser,
                "-cassandra_keyspace", "cssArchive", "Cassandra Keyspace Name",
                CassandraArchivePreferences.getKeyspace());
        final StringOption cassandraUsername = new StringOption(parser,
                "-cassandra_username", "", "Cassandra Username",
                CassandraArchivePreferences.getUsername());
        final StringOption cassandraPassword = new StringOption(parser,
                "-cassandra_password", "", "Cassandra Password",
                CassandraArchivePreferences.getPassword());
        parser.addEclipseParameters();
        try {
            parser.parse(args);
        } catch (final Exception ex) {
            System.err.println(ex.getMessage());
            return IApplication.EXIT_OK;
        }
        if (help.get()) {
            System.out.println(parser.getHelp());
            return IApplication.EXIT_OK;
        }

        // Initialize logging
        LogConfigurator.configureFromPreferences();

        // We use a random identifier for the cluster, because there might be
        // several instances of the archive configuration. If we used the same
        // cluster for all of them, a cluster still being used might be shutdown
        // when one of them is closed.
        String uuid = UUID.randomUUID().toString();
        ConnectionPoolConfigurationImpl cpConfig = new ConnectionPoolConfigurationImpl(
                uuid).setSeeds(cassandraHosts.get()).setPort(
                cassandraPort.get());
        if (cassandraUsername.get() != null && cassandraPassword.get() != null) {
            cpConfig.setAuthenticationCredentials(new SimpleAuthenticationCredentials(
                    cassandraUsername.get(), cassandraPassword.get()));
        }
        AstyanaxConfiguration asConfig = new AstyanaxConfigurationImpl()
                .setDefaultReadConsistencyLevel(
                        CassandraArchivePreferences
                                .getReadDataConsistencyLevel())
                .setDefaultWriteConsistencyLevel(
                        CassandraArchivePreferences
                                .getWriteDataConsistencyLevel())
                .setRetryPolicy(CassandraArchivePreferences.getRetryPolicy())
                .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE);
        AstyanaxContext<Cluster> astyanaxContext = new AstyanaxContext.Builder()
                .forCluster(uuid).forKeyspace(cassandraKeyspace.get())
                .withConnectionPoolConfiguration(cpConfig)
                .withAstyanaxConfiguration(asConfig)
                .buildCluster(ThriftFamilyFactory.getInstance());
        astyanaxContext.start();
        Cluster cluster = astyanaxContext.getEntity();
        Keyspace keyspace = cluster.getKeyspace(cassandraKeyspace.get());

        SampleStore sampleStore = new SampleStore(cluster, keyspace,
                CassandraArchivePreferences.getReadDataConsistencyLevel(),
                CassandraArchivePreferences.getWriteDataConsistencyLevel(),
                CassandraArchivePreferences.getReadMetaDataConsistencyLevel(),
                CassandraArchivePreferences.getWriteMetaDataConsistencyLevel(),
                true, false);
        CassandraArchiveConfig config = sampleStore.getArchiveConfig();
        config.performCleanUp(true);
        HashSet<Pair<String, Long>> compressionLevelPeriods = new HashSet<Pair<String, Long>>();
        for (EngineConfig engineConfig : config.getEngines()) {
            for (GroupConfig groupConfig : config.getGroups(engineConfig)) {
                addCompressionLevelPeriodsForGroup(compressionLevelPeriods,
                        config, groupConfig);
            }
            // Also add the channels from the special disabled channels group.
            addCompressionLevelPeriodsForGroup(compressionLevelPeriods, config,
                    config.getDisabledChannelsGroup(engineConfig));
        }
        sampleStore.performCleanUp(compressionLevelPeriods, true);
        return IApplication.EXIT_OK;
    }

    private void addCompressionLevelPeriodsForGroup(
            HashSet<Pair<String, Long>> compressionLevelPeriods,
            CassandraArchiveConfig config, GroupConfig groupConfig)
            throws Exception {
        for (ChannelConfig channelConfig : config.getChannels(groupConfig)) {
            for (CompressionLevelConfig compressionLevelConfig : config
                    .findCompressionLevelConfigs(channelConfig.getName())) {
                compressionLevelPeriods.add(new Pair<String, Long>(
                        compressionLevelConfig.getChannelName(),
                        compressionLevelConfig.getCompressionPeriod()));
            }
            // Make sure "raw" compression level is always included
            compressionLevelPeriods.add(new Pair<String, Long>(channelConfig
                    .getName(), 0L));
        }
    }

    @Override
    public void stop() {
        // We ignore the stop signal.
    }

}
