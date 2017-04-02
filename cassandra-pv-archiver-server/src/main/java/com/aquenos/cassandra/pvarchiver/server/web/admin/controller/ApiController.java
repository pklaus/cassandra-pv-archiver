/*
 * Copyright 2015-2017 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.web.admin.controller;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;

import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.annotation.Secured;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.aquenos.cassandra.pvarchiver.common.CustomUrlCodec;
import com.aquenos.cassandra.pvarchiver.controlsystem.ControlSystemSupport;
import com.aquenos.cassandra.pvarchiver.server.archiving.AddChannelCommand;
import com.aquenos.cassandra.pvarchiver.server.archiving.AddOrUpdateChannelCommand;
import com.aquenos.cassandra.pvarchiver.server.archiving.ArchiveConfigurationCommand;
import com.aquenos.cassandra.pvarchiver.server.archiving.ArchiveConfigurationCommandResult;
import com.aquenos.cassandra.pvarchiver.server.archiving.ArchiveConfigurationService;
import com.aquenos.cassandra.pvarchiver.server.archiving.ArchiveServerConfigurationXmlExport;
import com.aquenos.cassandra.pvarchiver.server.archiving.ArchiveServerConfigurationXmlImport;
import com.aquenos.cassandra.pvarchiver.server.archiving.ArchivingService;
import com.aquenos.cassandra.pvarchiver.server.archiving.ChannelInformationCache;
import com.aquenos.cassandra.pvarchiver.server.archiving.ChannelStatus;
import com.aquenos.cassandra.pvarchiver.server.archiving.RemoveChannelCommand;
import com.aquenos.cassandra.pvarchiver.server.archiving.UpdateChannelCommand;
import com.aquenos.cassandra.pvarchiver.server.cluster.ClusterManagementService;
import com.aquenos.cassandra.pvarchiver.server.cluster.ServerStatus;
import com.aquenos.cassandra.pvarchiver.server.controlsystem.ControlSystemSupportRegistry;
import com.aquenos.cassandra.pvarchiver.server.database.CassandraProvider;
import com.aquenos.cassandra.pvarchiver.server.database.ChannelMetaDataDAO;
import com.aquenos.cassandra.pvarchiver.server.database.ChannelMetaDataDAO.ChannelConfiguration;
import com.aquenos.cassandra.pvarchiver.server.database.ChannelMetaDataDAO.ChannelInformation;
import com.aquenos.cassandra.pvarchiver.server.internode.InterNodeCommunicationService;
import com.aquenos.cassandra.pvarchiver.server.util.FutureUtils;
import com.aquenos.cassandra.pvarchiver.server.web.admin.controller.internal.ConfigurationImportExportUtils;
import com.aquenos.cassandra.pvarchiver.server.web.admin.controller.wsapi.ChannelResponse;
import com.aquenos.cassandra.pvarchiver.server.web.admin.controller.wsapi.ChannelsAllResponse;
import com.aquenos.cassandra.pvarchiver.server.web.admin.controller.wsapi.ChannelsByServerExportResponse;
import com.aquenos.cassandra.pvarchiver.server.web.admin.controller.wsapi.ChannelsByServerImportRequest;
import com.aquenos.cassandra.pvarchiver.server.web.admin.controller.wsapi.ChannelsByServerImportResponse;
import com.aquenos.cassandra.pvarchiver.server.web.admin.controller.wsapi.ChannelsByServerResponse;
import com.aquenos.cassandra.pvarchiver.server.web.admin.controller.wsapi.ClusterStatusResponse;
import com.aquenos.cassandra.pvarchiver.server.web.admin.controller.wsapi.RunArchiveConfigurationCommandsRequest;
import com.aquenos.cassandra.pvarchiver.server.web.admin.controller.wsapi.RunArchiveConfigurationCommandsResponse;
import com.aquenos.cassandra.pvarchiver.server.web.admin.controller.wsapi.ServerStatusResponse;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Controller handling requests to the administrative API. This controller
 * implements a REST-style API using the JSON protocol and exposes it at the
 * <code>/admin/api</code> URI.
 * 
 * @author Sebastian Marsching
 */
@Controller
@RequestMapping("/admin/api")
public class ApiController {

    private ArchiveConfigurationService archiveConfigurationService;
    private ArchivingService archivingService;
    private CassandraProvider cassandraProvider;
    private ChannelInformationCache channelInformationCache;
    private ChannelMetaDataDAO channelMetaDataDAO;
    private ClusterManagementService clusterManagementService;
    private ControlSystemSupportRegistry controlSystemSupportRegistry;
    private InterNodeCommunicationService interNodeCommunicationService;

    /**
     * Sets the archive configuration service used by this controller. The
     * archive configuration service is used to make changes to the archive
     * configuration when requested by the user. Typically, this method is
     * called automatically by the Spring container.
     * 
     * @param archiveConfigurationService
     *            archive configuration service for this server.
     */
    @Autowired
    public void setArchiveConfigurationService(
            ArchiveConfigurationService archiveConfigurationService) {
        this.archiveConfigurationService = archiveConfigurationService;
    }

    /**
     * Sets the archiving service. The archiving service is used to display the
     * current archiving status. Typically, this method is called automatically
     * by the Spring container.
     * 
     * @param archivingService
     *            archiving service for this server.
     */
    @Autowired
    public void setArchivingService(ArchivingService archivingService) {
        this.archivingService = archivingService;
    }

    /**
     * Sets the Cassandra provider used by this controller. The Cassandra
     * provider is used to get information about the server's connection to the
     * database. Typically, this method is called automatically by the Spring
     * container.
     * 
     * @param cassandraProvider
     *            Cassandra provider used by the archiving server.
     */
    @Autowired
    public void setCassandraProvider(CassandraProvider cassandraProvider) {
        this.cassandraProvider = cassandraProvider;
    }

    /**
     * Sets the channel information cache. The cache is used for reading
     * {@link ChannelInformation} objects from the database without having to
     * query the database every time. Typically, this method is called
     * automatically by the Spring container.
     * 
     * @param channelInformationCache
     *            channel information cache for getting
     *            {@link ChannelInformation} objects.
     */
    @Autowired
    public void setChannelInformationCache(
            ChannelInformationCache channelInformationCache) {
        this.channelInformationCache = channelInformationCache;
    }

    /**
     * Sets the DAO for reading and modifying meta-data related to channels. The
     * controller uses this DAO to read information about channels if the
     * service usually used for retrieving this information is temporarily not
     * available. Typically, this method is called automatically by the Spring
     * container.
     * 
     * @param channelMetaDataDAO
     *            channel meta-data DAO to be used by this object.
     */
    @Autowired
    public void setChannelMetaDataDAO(ChannelMetaDataDAO channelMetaDataDAO) {
        this.channelMetaDataDAO = channelMetaDataDAO;
    }

    /**
     * Sets the cluster management service used by this controller. The cluster
     * management service is used to get information about the cluster status
     * and to remove servers from the cluster when requested by the user.
     * Typically, this method is called automatically by the Spring container.
     * 
     * @param clusterManagementService
     *            cluster manager used by the archiving server.
     */
    @Autowired
    public void setClusterManagementService(
            ClusterManagementService clusterManagementService) {
        this.clusterManagementService = clusterManagementService;
    }

    /**
     * Sets the control-system support registry. The registry is used to gain
     * access to the control-system supports available on this archiving server.
     * Typically, this method is called automatically by the Spring container.
     * 
     * @param controlSystemSupportRegistry
     *            control-system support registry for this archiving server.
     */
    @Autowired
    public void setControlSystemSupportRegistry(
            ControlSystemSupportRegistry controlSystemSupportRegistry) {
        this.controlSystemSupportRegistry = controlSystemSupportRegistry;
    }

    /**
     * Sets the inter-node communication service. The inter-node communication
     * service is needed to communicate with other servers in the cluster when
     * displaying status information for them. Typically, this method is called
     * automatically by the Spring container.
     * 
     * @param interNodeCommunicationService
     *            inter-node communication service.
     */
    @Autowired
    public void setInterNodeCommunicationService(
            InterNodeCommunicationService interNodeCommunicationService) {
        this.interNodeCommunicationService = interNodeCommunicationService;
    }

    /**
     * Returns the details (configuration and status) for a single channel.
     * 
     * @param serverId
     *            ID of the server that is supposed to own the channel.
     * @param channelName
     *            name of the channel
     * @param response
     *            HTTP servlet response. The response is used to send the
     *            appropriate status code in case the requested information
     *            cannot be gathered successfully.
     * @return channel details. May be <code>null</code> if the request could
     *         not be processed.
     */
    @RequestMapping(value = "/1.0/channels/by-server/{serverId}/by-name/{channelName}", method = RequestMethod.GET)
    @ResponseBody
    public ChannelResponse channelByServerDetails(@PathVariable String serverId,
            @PathVariable String channelName, HttpServletResponse response) {
        UUID serverIdAsUuid;
        try {
            serverIdAsUuid = UUID.fromString(serverId);
        } catch (IllegalArgumentException e) {
            // If the server ID is not a valid UUID, we treat this like the
            // resource did not exist.
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            return null;
        }
        return channelDetails(serverIdAsUuid, channelName, response);
    }

    /**
     * Returns the details (configuration and status) for a single channel.
     * 
     * @param channelName
     *            name of the channel
     * @param response
     *            HTTP servlet response. The response is used to send the
     *            appropriate status code in case the requested information
     *            cannot be gathered successfully.
     * @return channel details. May be <code>null</code> if the request could
     *         not be processed.
     */
    @RequestMapping(value = "/1.0/channels/all/by-name/{channelName}", method = RequestMethod.GET)
    @ResponseBody
    public ChannelResponse channelDetails(@PathVariable String channelName,
            HttpServletResponse response) {
        return channelDetails(null, channelName, response);
    }

    /**
     * Returns the list of all channels. This means all channels, regardless of
     * the server they belong to.
     * 
     * @param response
     *            HTTP servlet response. The response is used to send the
     *            appropriate status code in case the requested information
     *            cannot be gathered successfully.
     * @return list of all channels.
     */
    @RequestMapping(value = "/1.0/channels/all", method = RequestMethod.GET)
    @ResponseBody
    public ChannelsAllResponse channelsAll(HttpServletResponse response) {
        try {
            SortedMap<String, ChannelInformation> channels = channelInformationCache
                    .getChannels();
            // Control-system types and server IDs are likely to be repeated a
            // lot of times. We cache the lookup results to minimize the number
            // of lookups needed (doing the lookup in another component might
            // involve acquiring a mutex).
            Map<String, String> controlSystemNames = new HashMap<String, String>();
            Map<UUID, String> serverNames = new HashMap<UUID, String>();
            List<ChannelsAllResponse.ChannelItem> channelItems = new ArrayList<>(
                    channels.size());
            for (ChannelInformation channelInformation : channels.values()) {
                String controlSystemType = channelInformation
                        .getControlSystemType();
                String controlSystemName = controlSystemNames
                        .get(controlSystemType);
                if (controlSystemName == null) {
                    ControlSystemSupport<?> controlSystemSupport = controlSystemSupportRegistry
                            .getControlSystemSupport(
                                    channelInformation.getControlSystemType());
                    if (controlSystemSupport == null) {
                        controlSystemName = controlSystemType;
                    } else {
                        controlSystemName = controlSystemSupport.getName();
                    }
                    controlSystemNames.put(controlSystemType,
                            controlSystemName);
                }
                UUID serverId = channelInformation.getServerId();
                String serverName = serverNames.get(serverId);
                if (serverName == null) {
                    ServerStatus serverStatus = clusterManagementService
                            .getServer(serverId);
                    if (serverStatus == null) {
                        serverName = "unknown";
                    } else {
                        serverName = serverStatus.getServerName();
                    }
                    serverNames.put(serverId, serverName);
                }
                channelItems.add(new ChannelsAllResponse.ChannelItem(
                        channelInformation.getChannelDataId(),
                        channelInformation.getChannelName(), controlSystemName,
                        controlSystemType,
                        channelInformation.getDecimationLevels(), serverId,
                        serverName));
            }
            return new ChannelsAllResponse(channelItems);
        } catch (IllegalStateException e) {
            // If we could not get the information because the service is
            // currently not available, we indicate this by sending error code
            // 503.
            response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
            return null;
        }
    }

    /**
     * Returns the list of channels for the specified server.
     * 
     * @param serverId
     *            ID of the server for which the channels are to be returned.
     * @param response
     *            HTTP servlet response. The response is used to send the
     *            appropriate status code in case the requested information
     *            cannot be gathered successfully.
     * @return list of channels for the specified server.
     */
    @RequestMapping(value = "/1.0/channels/by-server/{serverId}", method = RequestMethod.GET)
    @ResponseBody
    public ChannelsByServerResponse channelsByServer(
            @PathVariable String serverId, HttpServletResponse response) {
        UUID serverIdAsUuid;
        try {
            serverIdAsUuid = UUID.fromString(serverId);
        } catch (IllegalArgumentException e) {
            // If the server ID is not a valid UUID, we treat this like the
            // resource did not exist.
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            return null;
        }
        UUID thisServerId = clusterManagementService.getThisServer()
                .getServerId();
        List<ChannelStatus> channelStatusList = null;
        try {
            if (thisServerId.equals(serverIdAsUuid)) {
                channelStatusList = archivingService
                        .getChannelStatusForAllChannels();
            } else {
                String targetServerUrl = clusterManagementService
                        .getInterNodeCommunicationUrl(serverIdAsUuid);
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
        List<ChannelsByServerResponse.ChannelItem> channelItems;
        // Control-system types are likely to be repeated a lot of times. We
        // cache the lookup results to minimize the number of lookups needed
        // (doing the lookup in another component might involve acquiring a
        // mutex).
        Map<String, String> controlSystemNames = new HashMap<String, String>();
        if (channelStatusList == null) {
            List<ChannelConfiguration> channelConfigurationList = null;
            try {
                channelConfigurationList = ImmutableList
                        .copyOf(FutureUtils.getUnchecked(channelMetaDataDAO
                                .getChannelsByServer(serverIdAsUuid)));
            } catch (RuntimeException e) {
                // If the database is not available, we send an appropriate
                // error code.
                response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
                return null;
            }
            channelItems = new ArrayList<>(channelConfigurationList.size());
            for (ChannelConfiguration configuration : channelConfigurationList) {
                String controlSystemType = configuration.getControlSystemType();
                String controlSystemName = controlSystemNames
                        .get(controlSystemType);
                if (controlSystemName == null) {
                    ControlSystemSupport<?> controlSystemSupport = controlSystemSupportRegistry
                            .getControlSystemSupport(controlSystemType);
                    if (controlSystemSupport == null) {
                        controlSystemName = controlSystemType;
                    } else {
                        controlSystemName = controlSystemSupport.getName();
                    }
                    controlSystemNames.put(controlSystemType,
                            controlSystemName);
                }
                channelItems.add(new ChannelsByServerResponse.ChannelItem(
                        configuration.getChannelDataId(),
                        configuration.getChannelName(), controlSystemName,
                        controlSystemType,
                        configuration.getDecimationLevelToRetentionPeriod(),
                        configuration.isEnabled(), null,
                        configuration.getOptions(), null, null, null, null));
            }
        } else {
            channelItems = new ArrayList<>(channelStatusList.size());
            for (ChannelStatus status : channelStatusList) {
                ChannelConfiguration configuration = status
                        .getChannelConfiguration();
                String controlSystemType = configuration.getControlSystemType();
                String controlSystemName = controlSystemNames
                        .get(controlSystemType);
                if (controlSystemName == null) {
                    ControlSystemSupport<?> controlSystemSupport = controlSystemSupportRegistry
                            .getControlSystemSupport(controlSystemType);
                    if (controlSystemSupport == null) {
                        controlSystemName = controlSystemType;
                    } else {
                        controlSystemName = controlSystemSupport.getName();
                    }
                    controlSystemNames.put(controlSystemType,
                            controlSystemName);
                }
                channelItems.add(new ChannelsByServerResponse.ChannelItem(
                        configuration.getChannelDataId(),
                        configuration.getChannelName(), controlSystemName,
                        controlSystemType,
                        configuration.getDecimationLevelToRetentionPeriod(),
                        configuration.isEnabled(), status.getErrorMessage(),
                        configuration.getOptions(),
                        status.getState().name().toLowerCase(Locale.ENGLISH),
                        status.getTotalSamplesDropped(),
                        status.getTotalSamplesSkippedBack(),
                        status.getTotalSamplesWritten()));
            }
        }
        ServerStatus serverStatus = clusterManagementService
                .getServer(serverIdAsUuid);
        boolean serverExists = serverStatus != null || !channelItems.isEmpty();
        if (!serverExists) {
            // If the server does not exist at all, we want to indicate this
            // using a 404 response.
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            return null;
        }
        return new ChannelsByServerResponse(channelItems,
                channelStatusList != null);
    }

    /**
     * Returns the exported channel configuration for the specified server..
     * 
     * @param serverId
     *            ID of the server for which the configuration shall be
     *            returned.
     * @param response
     *            HTTP servlet response. The response is used to send the
     *            appropriate status code in case the requested information
     *            cannot be gathered successfully.
     * @return configuration file for the specified server.
     */
    @RequestMapping(value = "/1.0/channels/by-server/{serverId}/export", method = RequestMethod.GET)
    @ResponseBody
    public ChannelsByServerExportResponse channelsByServerExport(
            @PathVariable String serverId, HttpServletResponse response) {
        UUID serverIdAsUuid;
        try {
            serverIdAsUuid = UUID.fromString(serverId);
        } catch (IllegalArgumentException e) {
            // If the server ID is not a valid UUID, we treat this like the
            // resource did not exist.
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            return null;
        }
        List<ChannelConfiguration> channelConfigurationList = ConfigurationImportExportUtils
                .getChannelConfigurationForExport(serverIdAsUuid,
                        archivingService, channelMetaDataDAO,
                        clusterManagementService,
                        interNodeCommunicationService);
        ServerStatus serverStatus = clusterManagementService
                .getServer(serverIdAsUuid);
        if (channelConfigurationList == null) {
            // If we could not get the configuration, we cannot know whether the
            // server exists, so either way indicate that the server is
            // currently not available.
            response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
            return null;
        } else if (channelConfigurationList.isEmpty() && serverStatus == null) {
            // If the server ID is not known in the cluster and the list of
            // channels is empty, the server simply does not exist.
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            return null;
        } else {
            ArchiveServerConfigurationXmlExport xmlExport = new ArchiveServerConfigurationXmlExport(
                    channelConfigurationList);
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            xmlExport.serialize(outputStream, "UTF-8");
            return new ChannelsByServerExportResponse(
                    outputStream.toByteArray());
        }
    }

    /**
     * Imports the channel configuration for an archive server from a
     * configuration file.
     * 
     * @param serverId
     *            ID of the server for which the configuration shall be
     *            imported.
     * @param importRequest
     *            request object specifying the configuration file an other
     *            options that shall be used for the import operation.
     * @param response
     *            HTTP servlet response. The response is used to send the
     *            appropriate status code in case there is an error.
     * @return response object indicating the result of the import operation.
     */
    @RequestMapping(value = "/1.0/channels/by-server/{serverId}/import", method = RequestMethod.POST)
    @ResponseBody
    @Secured("ROLE_ADMIN")
    public ChannelsByServerImportResponse channelsByServerImport(
            @PathVariable String serverId,
            @RequestBody ChannelsByServerImportRequest importRequest,
            HttpServletResponse response) {
        UUID serverIdAsUuid;
        try {
            serverIdAsUuid = UUID.fromString(serverId);
        } catch (IllegalArgumentException e) {
            // If the server ID is not a valid UUID, we treat this like the
            // resource did not exist.
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            return null;
        }
        String errorMessage = null;
        byte[] configFile = importRequest.getConfigurationFile();
        boolean addChannels = importRequest.isAddChannels();
        boolean removeChannels = importRequest.isRemoveChannels();
        boolean updateChannels = importRequest.isUpdateChannels();
        boolean simulate = importRequest.isSimulate();
        ArchiveServerConfigurationXmlImport xmlImport;
        if (configFile.length == 0) {
            errorMessage = "Configuration file must have non-zero length.";
            response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
            xmlImport = null;
        } else {
            InputStream inputStream = new ByteArrayInputStream(configFile);
            try {
                xmlImport = new ArchiveServerConfigurationXmlImport(
                        inputStream);
            } catch (RuntimeException e) {
                String exceptionMessage = e.getMessage();
                if (exceptionMessage == null) {
                    Throwable cause = e.getCause();
                    if (cause.getMessage() != null) {
                        exceptionMessage = cause.getMessage();
                    } else {
                        exceptionMessage = e.getClass().getName();
                    }
                }
                errorMessage = "Parsing error: " + exceptionMessage;
                response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                xmlImport = null;
            }
        }
        TreeSet<String> addOrUpdateSucceeded = null;
        TreeSet<String> removeSucceeded = null;
        TreeMap<String, String> addOrUpdateFailed = null;
        TreeMap<String, String> removeFailed = null;
        List<ArchiveConfigurationCommand> commands = null;
        if (xmlImport != null) {
            try {
                commands = ConfigurationImportExportUtils
                        .generateConfigurationCommands(xmlImport, addChannels,
                                removeChannels, updateChannels, serverIdAsUuid,
                                channelInformationCache, channelMetaDataDAO);
            } catch (RuntimeException e) {
                String exceptionMessage = e.getMessage();
                if (exceptionMessage == null) {
                    exceptionMessage = e.getClass().getName();
                }
                errorMessage = "Database access error: " + exceptionMessage;
                response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
            }
        }
        // We only continue if there was no error while creating the list of
        // commands.
        List<ArchiveConfigurationCommandResult> results = null;
        if (commands != null && !simulate) {
            try {
                results = FutureUtils.getUnchecked(archiveConfigurationService
                        .runConfigurationCommands(commands));
            } catch (RuntimeException e) {
                String exceptionMessage = e.getMessage();
                if (exceptionMessage == null) {
                    exceptionMessage = e.getClass().getName();
                }
                errorMessage = "Database access error: " + exceptionMessage;
                response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
            }
        }
        if (results != null) {
            addOrUpdateSucceeded = new TreeSet<String>();
            removeSucceeded = new TreeSet<String>();
            addOrUpdateFailed = new TreeMap<String, String>();
            removeFailed = new TreeMap<String, String>();
            for (ArchiveConfigurationCommandResult result : results) {
                switch (result.getCommand().getCommandType()) {
                case ADD_CHANNEL:
                    if (result.isSuccess()) {
                        addOrUpdateSucceeded
                                .add(((AddChannelCommand) result.getCommand())
                                        .getChannelName());
                    } else {
                        addOrUpdateFailed.put(
                                ((AddChannelCommand) result.getCommand())
                                        .getChannelName(),
                                result.getErrorMessage());
                    }
                    break;
                case ADD_OR_UPDATE_CHANNEL:
                    if (result.isSuccess()) {
                        addOrUpdateSucceeded
                                .add(((AddOrUpdateChannelCommand) result
                                        .getCommand()).getChannelName());
                    } else {
                        addOrUpdateFailed.put(
                                ((AddOrUpdateChannelCommand) result
                                        .getCommand()).getChannelName(),
                                result.getErrorMessage());
                    }
                    break;
                case REMOVE_CHANNEL:
                    if (result.isSuccess()) {
                        removeSucceeded.add(
                                ((RemoveChannelCommand) result.getCommand())
                                        .getChannelName());
                    } else {
                        removeFailed.put(
                                ((RemoveChannelCommand) result.getCommand())
                                        .getChannelName(),
                                result.getErrorMessage());
                    }
                    break;
                case UPDATE_CHANNEL:
                    if (result.isSuccess()) {
                        addOrUpdateSucceeded.add(
                                ((UpdateChannelCommand) result.getCommand())
                                        .getChannelName());
                    } else {
                        addOrUpdateFailed.put(
                                ((UpdateChannelCommand) result.getCommand())
                                        .getChannelName(),
                                result.getErrorMessage());
                    }
                    break;
                default:
                    // We should never see any other commands because we can
                    // only get results for commands that we have sent.
                    break;
                }
            }
        }
        // If we are running in simulation mode, we do not execute in any
        // commands, but we simply act like all commands were successful.
        if (commands != null && simulate) {
            addOrUpdateSucceeded = new TreeSet<String>();
            removeSucceeded = new TreeSet<String>();
            for (ArchiveConfigurationCommand command : commands) {
                switch (command.getCommandType()) {
                case ADD_CHANNEL:
                    addOrUpdateSucceeded.add(
                            ((AddChannelCommand) command).getChannelName());
                    break;
                case ADD_OR_UPDATE_CHANNEL:
                    addOrUpdateSucceeded
                            .add(((AddOrUpdateChannelCommand) command)
                                    .getChannelName());
                    break;
                case REMOVE_CHANNEL:
                    removeSucceeded.add(
                            ((RemoveChannelCommand) command).getChannelName());
                    break;
                case UPDATE_CHANNEL:
                    addOrUpdateSucceeded.add(
                            ((UpdateChannelCommand) command).getChannelName());
                    break;
                default:
                    // We should never see any other commands because these are
                    // the only commands generated for a configuration file
                    // import.
                    break;
                }
            }
        }
        if ((addOrUpdateFailed != null && !addOrUpdateFailed.isEmpty())
                || (removeFailed != null && !removeFailed.isEmpty())) {
            // If changes for individual channels fail, this is considered an
            // internal error.
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        }
        return new ChannelsByServerImportResponse(addOrUpdateFailed,
                addOrUpdateSucceeded, errorMessage, removeFailed,
                removeSucceeded);
    }

    /**
     * Returns status information about the archiving cluster.
     * 
     * @param response
     *            HTTP servlet response. The response is used to send the
     *            appropriate status code in case there is an error.
     * @return status information about the cluster.
     */
    @RequestMapping(value = "/1.0/cluster-status")
    @ResponseBody
    public ClusterStatusResponse clusterStatus(HttpServletResponse response) {
        List<ServerStatus> serverList;
        try {
            serverList = clusterManagementService.getServers();
        } catch (RuntimeException e) {
            response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
            return null;
        }
        List<ClusterStatusResponse.ServerItem> transformedServerList = ImmutableList
                .copyOf(Lists.transform(serverList,
                        new Function<ServerStatus, ClusterStatusResponse.ServerItem>() {
                            @Override
                            public ClusterStatusResponse.ServerItem apply(
                                    ServerStatus input) {
                                return new ClusterStatusResponse.ServerItem(
                                        input.getLastOnlineTime(),
                                        input.isOnline(), input.getServerId(),
                                        input.getServerName());
                            }
                        }));
        return new ClusterStatusResponse(transformedServerList);
    }

    /**
     * Runs archive configuration commands.
     * 
     * @param request
     *            request object wrapping the list of archive configuration
     *            commands that shall be run.
     * @param response
     *            HTTP servlet response. The response is used to send the
     *            appropriate status code in case there is an error.
     * @return response object indicating the results of the executed commands.
     */
    @RequestMapping(value = "/1.0/run-archive-configuration-commands", method = RequestMethod.POST)
    @ResponseBody
    @Secured("ROLE_ADMIN")
    public RunArchiveConfigurationCommandsResponse runArchiveConfigurationCommands(
            @RequestBody RunArchiveConfigurationCommandsRequest request,
            HttpServletResponse response) {
        List<ArchiveConfigurationCommandResult> results;
        try {
            results = FutureUtils.getUnchecked(archiveConfigurationService
                    .runConfigurationCommands(request.getCommands()));
        } catch (RuntimeException e) {
            // The command objects have already been validated before entering
            // this method. Problems with individual channels do not result in
            // an exception, but are returned in the results.
            // This means that the only reason why we might receive an exception
            // here is an internal error, most likely due to the database being
            // unavailable. We signal this by returning the appropriate status
            // code and including an error message.
            String exceptionMessage = e.getMessage();
            if (exceptionMessage == null) {
                exceptionMessage = e.getClass().getName();
            }
            String errorMessage = "Database access error: " + exceptionMessage;
            response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
            return new RunArchiveConfigurationCommandsResponse(errorMessage,
                    null);
        }
        for (ArchiveConfigurationCommandResult result : results) {
            if (!result.isSuccess()) {
                response.setStatus(
                        HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                break;
            }
        }
        return new RunArchiveConfigurationCommandsResponse(null, results);
    }

    /**
     * Returns status information for this server.
     * 
     * @return status information for this server.
     */
    @RequestMapping(value = "/1.0/server-status/this-server")
    @ResponseBody
    public ServerStatusResponse serverStatus() {
        ServerStatus serverStatus = clusterManagementService.getThisServer();
        int channelsTotal = 0;
        int channelsDisconnected = 0;
        int channelsError = 0;
        try {
            for (ChannelStatus channelStatus : archivingService
                    .getChannelStatusForAllChannels()) {
                ++channelsTotal;
                switch (channelStatus.getState()) {
                case DISCONNECTED:
                    ++channelsDisconnected;
                    break;
                case ERROR:
                    ++channelsError;
                    break;
                default:
                    break;
                }
            }
        } catch (IllegalStateException e) {
            // When the archiving server has not been initialized yet, the
            // getChannelStatusForAllChannels() method throws an
            // IllegalStateException. We simply catch that exception and use
            // zero for all numbers concerning the channels.
        }
        long samplesDropped = archivingService.getNumberOfSamplesDropped();
        long samplesWritten = archivingService.getNumberOfSamplesWritten();
        String cassandraClusterName = null;
        String cassandraKeyspaceName = null;
        String cassandraError = null;
        try {
            cassandraClusterName = cassandraProvider.getCluster().getMetadata()
                    .getClusterName();
            cassandraKeyspaceName = cassandraProvider.getSession()
                    .getLoggedKeyspace();
        } catch (RuntimeException e) {
            cassandraError = e.getMessage();
            if (cassandraError == null) {
                cassandraError = e.getClass().getName();
            }
        }
        return new ServerStatusResponse(cassandraClusterName, cassandraError,
                cassandraKeyspaceName, channelsDisconnected, channelsError,
                channelsTotal, serverStatus.getServerId(),
                serverStatus.getLastOnlineTime(), serverStatus.getServerName(),
                serverStatus.isOnline(), samplesDropped, samplesWritten);
    }

    private ChannelResponse channelDetails(UUID serverIdFromRequest,
            String channelName, HttpServletResponse response) {
        // The channel name is encoded, so we have to decode it first.
        channelName = CustomUrlCodec.decode(channelName);
        // If the request was made for a specific server, we have to check
        // whether the specified server is actually the server that owns the
        // channel. If the request was made without specifying a server, we have
        // to find out which server the channel belongs to.
        boolean channelExists;
        ChannelInformation channelInformation;
        try {
            channelInformation = FutureUtils
                    .getUnchecked(channelInformationCache
                            .getChannelAsync(channelName, false));
            channelExists = channelInformation != null;
        } catch (RuntimeException e) {
            // If there is an error while getting the channel information, we
            // still want to carry on. We take care of setting the appropriate
            // response code later. If we cannot get the channel information, we
            // cannot really know whether the channel exists. We set
            // channelExists to true anyway, so that later we can detect this
            // situation: If channelExists is true but there is no channel
            // configuration available, there must have been a problem
            // retrieving the information.
            channelExists = true;
            channelInformation = null;
        }
        ChannelStatus channelStatus = null;
        if (channelInformation != null) {
            // The channel exists, but does it belong to the specified server we
            // treat this like the channel did not exist. There is no good
            // reason why a client would specify a server ID if it was
            // interested in the channel in general.
            if (serverIdFromRequest != null && !serverIdFromRequest
                    .equals(channelInformation.getServerId())) {
                response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                return null;
            }
            // Next, we want to get the channel's status. However, we do not
            // consider it fatal if we cannot get the status (most likely the
            // server is simply offline).
            try {
                UUID thisServerId = clusterManagementService.getThisServer()
                        .getServerId();
                if (thisServerId.equals(channelInformation.getServerId())) {
                    channelStatus = archivingService
                            .getChannelStatus(channelName);
                } else {
                    String targetServerUrl = clusterManagementService
                            .getInterNodeCommunicationUrl(
                                    channelInformation.getServerId());
                    channelStatus = FutureUtils
                            .getUnchecked(interNodeCommunicationService
                                    .getArchivingStatusForChannel(
                                            targetServerUrl, channelName));
                }
            } catch (RuntimeException e) {
                // There are many reasons why we might not be able to get the
                // status. The archiving service might not be initialized yet,
                // the target server might not be online, etc.
                // We do not treat this as an error. Instead we fall back to
                // reading the configuration from the database.
            }
        }
        ChannelConfiguration channelConfiguration = null;
        if (channelStatus != null) {
            // The channel status includes the configuration, so we can simply
            // use that information
            channelConfiguration = channelStatus.getChannelConfiguration();
        } else if (channelInformation != null) {
            try {
                channelConfiguration = FutureUtils
                        .getUnchecked(channelMetaDataDAO.getChannelByServer(
                                channelInformation.getServerId(), channelName));
            } catch (RuntimeException e) {
                // If getting the channel configuration fails, we still want to
                // carry on. We set the appropriate response code later.
            }
        }
        if (!channelExists) {
            // If the channel does not exist at all, we want to indicate this
            // using a 404 response.
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            return null;
        } else if (channelConfiguration == null) {
            // If the channel exists (or could exist), but we do not have the
            // configuration, there was a problem with getting the information.
            // We indicate this by sending error code 503.
            response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
            return null;
        }
        String controlSystemName = null;
        ControlSystemSupport<?> controlSystemSupport = controlSystemSupportRegistry
                .getControlSystemSupport(
                        channelConfiguration.getControlSystemType());
        if (controlSystemSupport == null) {
            controlSystemName = channelConfiguration.getControlSystemType();
        } else {
            controlSystemName = controlSystemSupport.getName();
        }
        UUID serverId = channelConfiguration.getServerId();
        String serverName = "unknown";
        ServerStatus serverStatus = clusterManagementService
                .getServer(serverId);
        if (serverStatus != null) {
            serverName = serverStatus.getServerName();
        }
        if (channelStatus == null) {
            return new ChannelResponse(channelConfiguration.getChannelDataId(),
                    channelName, controlSystemName,
                    channelConfiguration.getControlSystemType(),
                    channelConfiguration.getDecimationLevelToRetentionPeriod(),
                    channelConfiguration.isEnabled(), null,
                    channelConfiguration.getOptions(), serverId, serverName,
                    null, null, null, null);
        } else {
            return new ChannelResponse(channelConfiguration.getChannelDataId(),
                    channelName, controlSystemName,
                    channelConfiguration.getControlSystemType(),
                    channelConfiguration.getDecimationLevelToRetentionPeriod(),
                    channelConfiguration.isEnabled(),
                    channelStatus.getErrorMessage(),
                    channelConfiguration.getOptions(), serverId, serverName,
                    channelStatus.getState().name().toLowerCase(Locale.ENGLISH),
                    channelStatus.getTotalSamplesDropped(),
                    channelStatus.getTotalSamplesSkippedBack(),
                    channelStatus.getTotalSamplesWritten());
        }
    }

}
