/*
 * Copyright 2012 aquenos GmbH.
 * Based on the archive config application for RDB archives
 * Copyright (c) 2011 Oak Ridge National Laboratory.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.csstudio.archive.config.cassandra;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.PrintStream;

import org.csstudio.apputil.args.ArgParser;
import org.csstudio.apputil.args.BooleanOption;
import org.csstudio.apputil.args.IntegerOption;
import org.csstudio.apputil.args.StringOption;
import org.csstudio.archive.config.EngineConfig;
import org.csstudio.logging.LogConfigurator;
import org.eclipse.equinox.app.IApplication;
import org.eclipse.equinox.app.IApplicationContext;

import com.aquenos.csstudio.archive.cassandra.CassandraArchivePreferences;
import com.aquenos.csstudio.archive.config.cassandra.internal.XMLExport;
import com.aquenos.csstudio.archive.config.cassandra.internal.XMLImport;
import com.aquenos.csstudio.archive.config.cassandra.internal.XMLImportException;

/**
 * [Headless] RCP command-line archive config tool
 * 
 * @author Kay Kasemir, Sebastian Marsching
 */
@SuppressWarnings("nls")
public class ArchiveConfigApplication implements IApplication {
	@Override
	public Object start(final IApplicationContext context) throws Exception {
		// Handle command-line options
		final String args[] = (String[]) context.getArguments().get(
				"application.args");

		final ArgParser parser = new ArgParser();
		final BooleanOption help = new BooleanOption(parser, "-help",
				"show help");
		final BooleanOption list = new BooleanOption(parser, "-list",
				"List engine names");
		final StringOption engine_name = new StringOption(parser, "-engine",
				"my_engine", "Engine Name", "");
		final StringOption filename = new StringOption(parser, "-config",
				"my_config.xml", "XML Engine config file", "");
		final BooleanOption do_export = new BooleanOption(parser, "-export",
				"export configuration as XML");
		final BooleanOption do_import = new BooleanOption(parser, "-import",
				"import configuration from XML");
		final BooleanOption do_delete = new BooleanOption(parser,
				"-delete_config", "Delete existing engine config");
		final StringOption engine_description = new StringOption(parser,
				"-description", "'My Engine'", "Engine Description", "Imported");
		final StringOption engine_host = new StringOption(parser, "-host",
				"my.host.org", "Engine Host", "localhost");
		final IntegerOption engine_port = new IntegerOption(parser, "-port",
				"4812", "Engine Port", 4812);
		final BooleanOption replace_engine = new BooleanOption(parser,
				"-replace_engine", "Replace existing engine config, or stop?");
		final BooleanOption steal_channels = new BooleanOption(parser,
				"-steal_channels",
				"Steal channels that are already in other engine");
		final StringOption cassandra_hosts = new StringOption(parser,
				"-cassandra_hosts",
				"first-server.example.com,second-server.example.com",
				"Cassandra Hosts (comma-separated)",
				CassandraArchivePreferences.getHosts());
		final IntegerOption cassandra_port = new IntegerOption(parser,
				"-cassandra_port", "9160", "Cassandra Port",
				CassandraArchivePreferences.getPort());
		final StringOption cassandra_keyspace = new StringOption(parser,
				"-cassandra_keyspace", "cssArchive", "Cassandra Keyspace Name",
				CassandraArchivePreferences.getKeyspace());
		final StringOption cassandra_username = new StringOption(parser,
				"-cassandra_username", "", "Cassandra Username",
				CassandraArchivePreferences.getUsername());
		final StringOption cassandra_password = new StringOption(parser,
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
		if (!list.get() && engine_name.get().length() <= 0) {
			System.err.println("Missing option " + engine_name.getOption());
			System.err.println(parser.getHelp());
			return IApplication.EXIT_OK;
		}

		LogConfigurator.configureFromPreferences();

		try {
			if (list.get()) {
				final CassandraArchiveConfig config = new CassandraArchiveConfig(
						cassandra_hosts.get(),
						cassandra_port.get(),
						cassandra_keyspace.get(),
						CassandraArchivePreferences.getConsistencyLevelPolicy(),
						CassandraArchivePreferences.getFailoverPolicy(),
						cassandra_username.get(), cassandra_password.get());
				final EngineConfig[] engines = config.getEngines();
				for (EngineConfig engine : engines)
					System.out.println(engine);
			}
			if (do_export.get()) {
				final PrintStream out;
				if (filename.get().isEmpty())
					out = System.out;
				else {
					out = new PrintStream(filename.get());
					System.out.println("Exporting config for engine "
							+ engine_name.get() + " to " + filename.get());
				}
				new XMLExport().export(out, cassandra_hosts.get(),
						cassandra_port.get(), cassandra_keyspace.get(),
						cassandra_username.get(), cassandra_password.get(),
						engine_name.get());
				if (out != System.out)
					out.close();
				return IApplication.EXIT_OK;
			} else if (do_delete.get()) {
				final CassandraArchiveConfig config = new CassandraArchiveConfig(
						cassandra_hosts.get(),
						cassandra_port.get(),
						cassandra_keyspace.get(),
						CassandraArchivePreferences.getConsistencyLevelPolicy(),
						CassandraArchivePreferences.getFailoverPolicy(),
						cassandra_username.get(), cassandra_password.get());
				try {
					final EngineConfig engine = config.findEngine(engine_name
							.get());
					if (engine == null)
						System.out.println("Engine config '"
								+ engine_name.get() + "' does not exist");
					else {
						config.deleteEngine(engine);
						System.out.println("Deleted engine config '"
								+ engine_name.get() + "'");
					}
				} finally {
					config.close();
				}
				return IApplication.EXIT_OK;
			} else if (do_import.get()) {
				if (filename.get().isEmpty()) {
					System.err
							.println("Missing option " + filename.getOption());
					System.err.println(parser.getHelp());
					return IApplication.EXIT_OK;
				}
				final String engine_url = "http://" + engine_host.get() + ":"
						+ engine_port.get() + "/main";
				final InputStream stream;
				try {
					stream = new FileInputStream(filename.get());
				} catch (FileNotFoundException ex) {
					System.out.println("Cannot open engine config file, "
							+ ex.getMessage());
					return IApplication.EXIT_OK;
				}
				System.out.println("Importing     : " + filename.get());
				System.out.println("Engine        : " + engine_name.get());
				System.out.println("Description   : "
						+ engine_description.get());
				System.out.println("URL           : " + engine_url);
				System.out.println("Replace engine: " + replace_engine.get());
				System.out.println("Steal channels: " + steal_channels.get());
				final XMLImport importer = new XMLImport(cassandra_hosts.get(),
						cassandra_port.get(), cassandra_keyspace.get(),
						cassandra_username.get(), cassandra_password.get(),
						replace_engine.get(), steal_channels.get());
				try {
					importer.parse(stream, engine_name.get(),
							engine_description.get(), engine_url);
				} catch (XMLImportException ex) { // Print message
					System.out.println(ex.getMessage());
					// Other exceptions result in a full stack trace
				} finally {
					importer.close();
				}
			}
		} catch (final Exception ex) {
			ex.printStackTrace();
		}
		return IApplication.EXIT_OK;
	}

	@Override
	public void stop() {
		// Ignored
	}
}
