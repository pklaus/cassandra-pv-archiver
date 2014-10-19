/*
 * Copyright 2012-2013 aquenos GmbH.
 * Based on the archive config application for RDB archives
 * Copyright (c) 2010 Oak Ridge National Laboratory.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.csstudio.archive.cassandra.configtool.internal;

import java.io.PrintStream;

import org.csstudio.archive.config.ArchiveConfig;
import org.csstudio.archive.config.ChannelConfig;
import org.csstudio.archive.config.EngineConfig;
import org.csstudio.archive.config.GroupConfig;
import org.csstudio.archive.config.SampleMode;
import org.csstudio.data.values.TimestampFactory;

import com.aquenos.csstudio.archive.cassandra.CassandraArchivePreferences;
import com.aquenos.csstudio.archive.config.cassandra.CassandraArchiveConfig;
import com.aquenos.csstudio.archive.config.cassandra.CompressionLevelConfig;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

/**
 * Export engine configuration as XML (to stdout)
 * 
 * @author Kay Kasemir, Sebastian Marsching
 */
@SuppressWarnings("nls")
public class XMLExport {
    private CassandraArchiveConfig config;

    /**
     * Export configuration
     * 
     * @param out
     *            {@link PrintStream}
     * @param rdb_url
     * @param rdb_user
     * @param rdb_password
     * @param rdb_schema
     * @param engine_name
     *            Name of engine configuration
     * @throws Exception
     *             on error
     */
    public void export(final PrintStream out, final String cassandra_hosts,
            final int cassandra_port, final String cassandra_keyspace,
            final String cassandra_username, final String cassandra_password,
            final String engine_name) throws Exception {
        config = new CassandraArchiveConfig(cassandra_hosts, cassandra_port,
                cassandra_keyspace,
                CassandraArchivePreferences.getReadDataConsistencyLevel(),
                CassandraArchivePreferences.getWriteDataConsistencyLevel(),
                CassandraArchivePreferences.getReadMetaDataConsistencyLevel(),
                CassandraArchivePreferences.getWriteMetaDataConsistencyLevel(),
                CassandraArchivePreferences.getRetryPolicy(),
                cassandra_username, cassandra_password, true);
        try {
            final EngineConfig engine = config.findEngine(engine_name);
            if (engine == null)
                throw new Exception("Unknown engine '" + engine_name + "'");
            out.println("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>");
            out.println("<!-- Created by ArchiveConfigTool -engine "
                    + engine_name + " -export");
            out.println("     " + TimestampFactory.now().toString());
            out.println(" -->");
            dumpEngine(out, config, engine);
        } finally {
            config.close();
        }
    }

    private void dumpEngine(final PrintStream out,
            final CassandraArchiveConfig config, final EngineConfig engine)
            throws Exception {
        out.println("<engineconfig>");
        final GroupConfig[] groups = config.getGroups(engine);
        for (GroupConfig group : groups) {
            dumpGroup(out, config, group);
        }
        // Include group with disabled channels if it contains channels.
        GroupConfig disabledChannelsGroup = config
                .getDisabledChannelsGroup(engine);
        if (config.getChannels(disabledChannelsGroup).length > 0) {
            dumpGroup(out, config, disabledChannelsGroup);
        }
        out.println("</engineconfig>");
    }

    private void dumpGroup(final PrintStream out, final ArchiveConfig config,
            final GroupConfig group) throws Exception {
        out.println("  <group>");
        out.println("    <name>" + group.getName() + "</name>");
        final ChannelConfig[] channels = config.getChannels(group);
        for (ChannelConfig channel : channels)
            dumpChannel(out, channel);
        out.println("  </group>");
    }

    private void dumpChannel(final PrintStream out, final ChannelConfig channel)
            throws ConnectionException {
        out.println("    <channel>");
        out.println("      <name>" + channel.getName() + "</name>");
        final SampleMode mode = channel.getSampleMode();
        out.println("      <period>" + mode.getPeriod() + "</period>");
        if (mode.isMonitor()) {
            if (mode.getDelta() != 0.0)
                out.println("      <monitor>" + mode.getDelta() + "</monitor>");
            else
                out.println("      <monitor/>");
        } else
            out.println("      <scan/>");
        for (CompressionLevelConfig compressionLevelConfig : config
                .findCompressionLevelConfigs(channel.getName())) {
            long compression_period = compressionLevelConfig
                    .getCompressionPeriod();
            long retention_period = compressionLevelConfig.getRetentionPeriod();
            if (compression_period != 0L || retention_period != 0L) {
                out.print("      <compression-level");
                out.print(" compression-period=\"" + compression_period + "\"");
                if (retention_period != 0) {
                    out.print(" retention-period=\"" + retention_period + "\"");
                }
                out.println("/>");
            }
        }
        out.println("    </channel>");
    }
}
