/*
 * Copyright 2012 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.csstudio.archive.cassandra.cleanuptool;

import java.util.HashSet;
import java.util.TreeMap;

import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;

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

		CassandraHostConfigurator hostConfigurator = new CassandraHostConfigurator();
		hostConfigurator.setHosts(cassandraHosts.get());
		hostConfigurator.setPort(cassandraPort.get());
		Cluster cluster = HFactory.getOrCreateCluster("archiveCluster",
				hostConfigurator);
		TreeMap<String, String> credentials = new TreeMap<String, String>();
		if (cassandraUsername.get() != null && cassandraPassword.get() != null) {
			credentials.put("username", cassandraUsername.get());
			credentials.put("password", cassandraPassword.get());
		}
		Keyspace keyspace = HFactory.createKeyspace(cassandraKeyspace.get(),
				cluster,
				CassandraArchivePreferences.getConsistencyLevelPolicy(),
				CassandraArchivePreferences.getFailoverPolicy(), credentials);
		CassandraArchiveConfig config = new CassandraArchiveConfig(keyspace);
		config.performCleanUp(true);
		HashSet<Pair<String, String>> compressionLevelNames = new HashSet<Pair<String, String>>();
		for (EngineConfig engineConfig : config.getEngines()) {
			for (GroupConfig groupConfig : config.getGroups(engineConfig)) {
				for (ChannelConfig channelConfig : config
						.getChannels(groupConfig)) {
					for (CompressionLevelConfig compressionLevelConfig : config
							.findCompressionLevelConfigs(channelConfig
									.getName())) {
						compressionLevelNames.add(new Pair<String, String>(
								compressionLevelConfig.getChannelName(),
								compressionLevelConfig
										.getCompressionLevelName()));
					}
					// Make sure "raw" compression level is always included
					compressionLevelNames.add(new Pair<String, String>(
							channelConfig.getName(),
							CompressionLevelConfig.RAW_COMPRESSION_LEVEL_NAME));
				}
			}
		}
		SampleStore sampleStore = new SampleStore(keyspace);
		sampleStore.performCleanUp(compressionLevelNames, true);
		return IApplication.EXIT_OK;
	}

	@Override
	public void stop() {
		// We ignore the stop signal.
	}

}
