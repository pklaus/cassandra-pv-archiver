/*
 * Copyright 2017 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.web.admin.controller.internal;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.aquenos.cassandra.pvarchiver.server.archiving.AddChannelCommand;
import com.aquenos.cassandra.pvarchiver.server.archiving.AddOrUpdateChannelCommand;
import com.aquenos.cassandra.pvarchiver.server.archiving.ArchiveConfigurationCommand;
import com.aquenos.cassandra.pvarchiver.server.archiving.ArchiveServerConfigurationXmlImport;
import com.aquenos.cassandra.pvarchiver.server.archiving.ArchivingService;
import com.aquenos.cassandra.pvarchiver.server.archiving.ChannelInformationCache;
import com.aquenos.cassandra.pvarchiver.server.archiving.ChannelStatus;
import com.aquenos.cassandra.pvarchiver.server.archiving.RemoveChannelCommand;
import com.aquenos.cassandra.pvarchiver.server.archiving.UpdateChannelCommand;
import com.aquenos.cassandra.pvarchiver.server.cluster.ClusterManagementService;
import com.aquenos.cassandra.pvarchiver.server.database.ChannelMetaDataDAO;
import com.aquenos.cassandra.pvarchiver.server.database.ChannelMetaDataDAO.ChannelConfiguration;
import com.aquenos.cassandra.pvarchiver.server.internode.InterNodeCommunicationService;
import com.aquenos.cassandra.pvarchiver.server.util.FutureUtils;
import com.aquenos.cassandra.pvarchiver.server.web.admin.controller.ApiController;
import com.aquenos.cassandra.pvarchiver.server.web.admin.controller.UiController;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * <p>
 * Utility functions for importing and exporting a configuration file.
 * </p>
 * 
 * <p>
 * This class exists in order to share code between {@link ApiController} and
 * {@link UiController}.
 * </p>
 * 
 * @author Sebastian Marsching
 *
 */
public class ConfigurationImportExportUtils {

    private final static Function<ChannelStatus, ChannelConfiguration> CHANNEL_STATUS_TO_CONFIGURATION = new Function<ChannelStatus, ChannelConfiguration>() {
        @Override
        public ChannelConfiguration apply(ChannelStatus input) {
            if (input == null) {
                return null;
            } else {
                return input.getChannelConfiguration();
            }
        }
    };

    private ConfigurationImportExportUtils() {
    }

    /**
     * <p>
     * Returns all channel configurations for a specific server.
     * </p>
     * 
     * <p>
     * This method gathers all channel configurations for the specified server.
     * If the specified server is the local server, it gets this information
     * from the {@link ArchivingService}. If the specified server is a remote
     * server, this server is queried through the
     * {@link InterNodeCommunicationService}. If either of the two is not
     * available, it falls back to reading the configuration from the database
     * through the {@link ChannelMetaDataDAO}. If this fails as well,
     * <code>null</code> is returned.
     * </p>
     * 
     * @param serverId
     *            ID of the server for which the channel configuration shall be
     *            returned.
     * @param archivingService
     *            archiving service used for getting the configuration of the
     *            local server.
     * @param channelMetaDataDAO
     *            channel meta-data DAO used for reading the configuration from
     *            the database.
     * @param clusterManagementService
     *            cluster management service used to get information about other
     *            servers in the cluster.
     * @param interNodeCommunicationService
     *            inter-node communication service used to contact a remote
     *            server.
     * @return all channel configurations for the specified server or
     *         <code>null</code> if this information cannot be retrieved for
     *         some reason.
     */
    public static List<ChannelConfiguration> getChannelConfigurationForExport(
            UUID serverId, ArchivingService archivingService,
            ChannelMetaDataDAO channelMetaDataDAO,
            ClusterManagementService clusterManagementService,
            InterNodeCommunicationService interNodeCommunicationService) {
        UUID thisServerId = clusterManagementService.getThisServer()
                .getServerId();
        List<ChannelStatus> channelStatusList = null;
        try {
            if (thisServerId.equals(serverId)) {
                channelStatusList = archivingService
                        .getChannelStatusForAllChannels();
            } else {
                String targetServerUrl = clusterManagementService
                        .getInterNodeCommunicationUrl(serverId);
                channelStatusList = FutureUtils
                        .getUnchecked(interNodeCommunicationService
                                .getArchivingStatus(targetServerUrl));
            }
        } catch (RuntimeException e) {
            // There are many reasons why we might not be able to get the
            // status. The archiving service might not be initialized yet, the
            // target server might not be online, etc.
            // We do not treat this as an error. Instead we fall back to reading
            // the configuration from the database.
        }
        List<ChannelConfiguration> channelConfigurationList = null;
        if (channelStatusList == null) {
            try {
                channelConfigurationList = ImmutableList
                        .copyOf(FutureUtils.getUnchecked(channelMetaDataDAO
                                .getChannelsByServer(serverId)));
            } catch (RuntimeException e) {
                // If the database is not available, we might not be able to get
                // the configuration either. We still do not want to throw an
                // exception, which would result in an error response but
                // instead signal the problem through the model so that the view
                // can render an appropriate message.
            }
        } else {
            // The channel status includes the configuration, so we can simply
            // transform the list.
            channelConfigurationList = Lists.transform(channelStatusList,
                    CHANNEL_STATUS_TO_CONFIGURATION);
        }
        return channelConfigurationList;
    }

    /**
     * <p>
     * Generates the configuration commands needed to import the configuration
     * for an archive server.
     * </p>
     * 
     * <p>
     * This method takes care of retrieving the current list of channels for the
     * server and deciding (based on the specified options) which channels have
     * to be added, updated, or removed.
     * </p>
     * 
     * <p>
     * Information about existing channels is only used when
     * <code>removeChannels</code> is set. In this case it is needed in order to
     * determine which channels exist in the database, but not in the
     * configuration.
     * </p>
     * 
     * @param xmlImport
     *            configuration that shall be imported.
     * @param addChannels
     *            tells whether channels should be added to the server
     *            configuration. If <code>true</code> channels that exist in the
     *            configuration file but not on the server are added. If
     *            <code>false</code> channels that exist in the configuration
     *            file but not on the server are not added.
     * @param removeChannels
     *            tells whether channels that only exist on the server should be
     *            removed. If <code>true</code>, channels that exist on the
     *            server but not in the configuration file are removed. If
     *            <code>false</code>, channels that exist on the server but not
     *            in the configuration file are simply ignored.
     * @param updateChannels
     *            tells whether channels on the server should be updated. If
     *            <code>true</code>, channels that exist in the configuration
     *            file and on the server are updated with the configuration
     *            specified in the configuration file. If <code>false</code>,
     *            channels in the configuration file that also exist on the
     *            server are not touched.
     * @param serverId
     *            ID of the server for which the configuration shall be
     *            imported.
     * @param channelInformationCache
     *            channel information cache used to retrieve information about
     *            existing channels.
     * @param channelMetaDataDAO
     *            channel meta-data DAO used to retrieve information about
     *            existing channels when the the channel information cache does
     *            not have cached information.
     * @return configuration commands that effectively import the specified
     *         configuration for the specified server.
     * @throws RuntimeException
     *             if the commands cannot be generated. Typically, this is
     *             caused by problems accessing the database.
     */
    public static List<ArchiveConfigurationCommand> generateConfigurationCommands(
            ArchiveServerConfigurationXmlImport xmlImport, boolean addChannels,
            boolean removeChannels, boolean updateChannels, UUID serverId,
            ChannelInformationCache channelInformationCache,
            ChannelMetaDataDAO channelMetaDataDAO) {
        HashSet<String> channelNamesInConfiguration = new HashSet<String>();
        ArrayList<ArchiveConfigurationCommand> commands = new ArrayList<ArchiveConfigurationCommand>(
                xmlImport.getChannels().size());
        if (addChannels && updateChannels) {
            for (ChannelConfiguration channel : xmlImport.getChannels()) {
                channelNamesInConfiguration.add(channel.getChannelName());
                commands.add(new AddOrUpdateChannelCommand(
                        channel.getChannelName(),
                        channel.getControlSystemType(),
                        channel.getDecimationLevelToRetentionPeriod().keySet(),
                        channel.getDecimationLevelToRetentionPeriod(),
                        channel.isEnabled(), channel.getOptions(), serverId));
            }
        } else if (addChannels) {
            for (ChannelConfiguration channel : xmlImport.getChannels()) {
                channelNamesInConfiguration.add(channel.getChannelName());
                commands.add(new AddChannelCommand(channel.getChannelName(),
                        channel.getControlSystemType(),
                        channel.getDecimationLevelToRetentionPeriod().keySet(),
                        channel.getDecimationLevelToRetentionPeriod(),
                        channel.isEnabled(), channel.getOptions(), serverId));
            }
        } else if (updateChannels) {
            for (ChannelConfiguration channel : xmlImport.getChannels()) {
                channelNamesInConfiguration.add(channel.getChannelName());
                commands.add(new UpdateChannelCommand(channel.getChannelName(),
                        channel.getControlSystemType(), serverId,
                        channel.getDecimationLevelToRetentionPeriod().keySet(),
                        null, null,
                        channel.getDecimationLevelToRetentionPeriod(),
                        channel.isEnabled(), channel.getOptions(), null, null));
            }
        } else {
            for (ChannelConfiguration channel : xmlImport.getChannels()) {
                channelNamesInConfiguration.add(channel.getChannelName());
            }
        }
        if (removeChannels) {
            Set<String> existingChannelNames = null;
            try {
                existingChannelNames = channelInformationCache
                        .getChannels(serverId).keySet();
            } catch (IllegalStateException e) {
                // If we cannot get the information from the cache, this is
                // not a fatal error. We still can try to get it from the
                // database directly.
            }
            if (existingChannelNames == null) {
                existingChannelNames = ImmutableSet.copyOf(Iterables.transform(
                        FutureUtils.getUnchecked(channelMetaDataDAO
                                .getChannelsByServer(serverId)),
                        new Function<ChannelConfiguration, String>() {
                            @Override
                            public String apply(ChannelConfiguration input) {
                                return input.getChannelName();
                            }
                        }));
            }
            if (existingChannelNames != null) {
                for (String channelName : Sets.difference(existingChannelNames,
                        channelNamesInConfiguration)) {
                    commands.add(
                            new RemoveChannelCommand(channelName, serverId));
                }
            }
        }
        return commands;
    }

}
