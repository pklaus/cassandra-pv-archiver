/*
 * Copyright 2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.web.internode.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.aquenos.cassandra.pvarchiver.common.CustomUrlCodec;
import com.aquenos.cassandra.pvarchiver.server.archiving.ArchiveConfigurationCommandResult;
import com.aquenos.cassandra.pvarchiver.server.archiving.ArchiveConfigurationService;
import com.aquenos.cassandra.pvarchiver.server.archiving.ArchivingService;
import com.aquenos.cassandra.pvarchiver.server.archiving.ChannelInformationCache;
import com.aquenos.cassandra.pvarchiver.server.archiving.ChannelStatus;
import com.aquenos.cassandra.pvarchiver.server.internode.ArchivingStatusResponse;
import com.aquenos.cassandra.pvarchiver.server.internode.CurrentSystemTimeResponse;
import com.aquenos.cassandra.pvarchiver.server.internode.RunArchiveConfigurationCommandsRequest;
import com.aquenos.cassandra.pvarchiver.server.internode.RunArchiveConfigurationCommandsResponse;
import com.aquenos.cassandra.pvarchiver.server.internode.UpdateChannelInformationRequest;
import com.aquenos.cassandra.pvarchiver.server.util.FutureUtils;
import com.google.common.base.Function;
import com.google.common.util.concurrent.Futures;

/**
 * <p>
 * Controller handling requests to the inter-node API. This controller
 * implements a REST-style API using the JSON protocol and exposes it at the
 * <code>/inter-node/api</code> URI.
 * </p>
 * 
 * <p>
 * This controller does not check the authentication or authorization of
 * requests. These checks are expected to be made before a request is passed to
 * this controller.
 * </p>
 * 
 * @author Sebastian Marsching
 */
@Controller
@RequestMapping("/inter-node/api")
public class ApiController {

    private ArchiveConfigurationService archiveConfigurationService;
    private ArchivingService archivingService;
    private ChannelInformationCache channelInformationCache;
    private final Function<List<ArchiveConfigurationCommandResult>, RunArchiveConfigurationCommandsResponse> createRunArchiveConfigurationCommandsResponse = new Function<List<ArchiveConfigurationCommandResult>, RunArchiveConfigurationCommandsResponse>() {
        @Override
        public RunArchiveConfigurationCommandsResponse apply(
                List<ArchiveConfigurationCommandResult> input) {
            return new RunArchiveConfigurationCommandsResponse(input);
        }
    };

    /**
     * Sets the archive configuration service used by this controller. The
     * archive configuration service is used to execute configuration commands
     * that have been received from other servers in the cluster. Typically,
     * this method is called automatically by the Spring container.
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
     * Sets the archiving service. The archiving service is used to provide
     * status information when requested. Typically, this method is called
     * automatically by the Spring container.
     * 
     * @param archivingService
     *            archiving service for this server.
     */
    @Autowired
    public void setArchivingService(ArchivingService archivingService) {
        this.archivingService = archivingService;
    }

    /**
     * Sets the channel information cache. The cache is updated when this
     * controller receives updated channel information. Typically, this method
     * is called automatically by the Spring container.
     * 
     * @param channelInformationCache
     *            channel information cache for this server.
     */
    @Autowired
    public void setChannelInformationCache(
            ChannelInformationCache channelInformationCache) {
        this.channelInformationCache = channelInformationCache;
    }

    /**
     * Returns a response with the archiving status of all channels handled by
     * this server.
     * 
     * @return archiving status of all channels wrapped in an object suitable
     *         for JSON serialization.
     */
    @RequestMapping(value = "/1.0/archiving-status", method = RequestMethod.GET)
    @ResponseBody
    public ArchivingStatusResponse archivingStatus() {
        return new ArchivingStatusResponse(
                archivingService.getChannelStatusForAllChannels());
    }

    /**
     * Returns the archiving status of the specified channel. This method
     * returns <code>null</code> if the specified channel is not known by this
     * server's archiving service.
     * 
     * @param channelName
     *            name of the channel.
     * @return archiving status of the specified channel or <code>null</code> if
     *         the specified channel is not known.
     */
    @RequestMapping(value = "/1.0/archiving-status-for-channel/{channelName}", method = RequestMethod.GET)
    @ResponseBody
    public ChannelStatus archivingStatusForChannel(
            @PathVariable String channelName) {
        // The channel name is encoded, so we have to decode it first.
        channelName = CustomUrlCodec.decode(channelName);
        // We rather return null than returning a 404 error when the channel
        // cannot be found. This way, the client can correctly distinguish
        // whether the channel was not found or there was a problem with the
        // request (e.g. the web-service URL was wrong).
        return archivingService.getChannelStatus(channelName);
    }

    /**
     * Returns a response with the current system time (as returned by
     * {@link System#currentTimeMillis()}).
     * 
     * @return current system time wrapped in an object suitable for JSON
     *         serialization.
     */
    @RequestMapping(value = "/1.0/current-system-time", method = RequestMethod.GET)
    @ResponseBody
    public CurrentSystemTimeResponse currentSystemTime() {
        return new CurrentSystemTimeResponse(System.currentTimeMillis());
    }

    /**
     * Runs archive configuration commands on this server. This URI is only used
     * by other servers in the same cluster. Typically, the future returned by
     * this method will not throw an exception but signal failure of individual
     * commands by a corresponding result instead. However, if there is some
     * general error that prevents all commands from being executed, the future
     * might throw an exception.
     * 
     * @param request
     *            request object wrapping the commands that shall be executed by
     *            this server's {@link ArchiveConfigurationService}.
     * @return response to the <code>request</code> exposed through a future.
     *         This allows the container to reuse the calling thread while
     *         waiting for the operation to finish.
     */
    @RequestMapping(value = "/1.0/run-archive-configuration-commands", method = RequestMethod.POST)
    @ResponseBody
    public ListenableFuture<RunArchiveConfigurationCommandsResponse> runArchiveConfigurationCommands(
            @RequestBody RunArchiveConfigurationCommandsRequest request) {
        return FutureUtils.asSpringListenableFuture(Futures.transform(
                archiveConfigurationService.runConfigurationCommands(request
                        .getCommands()),
                createRunArchiveConfigurationCommandsResponse));
    }

    /**
     * Updates the channel cache and forwards the update to other servers if
     * requested. This URI is only used by other servers in the same cluster.
     * The future returned by this method completes when the local cache has
     * been updated and the update has been forwarded to the specified servers.
     * The future only indicates failure if there is a general problem with the
     * update. If the forwarding to individual servers fails, this does not
     * result in the returned future to fail.
     * 
     * @param request
     *            channel-information update request.
     * @return future that completes when the update operation completes. This
     *         allows the container to reuse the calling thread while waiting
     *         for the operation to finish.
     */
    @RequestMapping(value = "/1.0/update-channel-information", method = RequestMethod.POST)
    @ResponseBody
    public ListenableFuture<Void> updateChannelInformation(
            @RequestBody UpdateChannelInformationRequest request) {
        return FutureUtils.asSpringListenableFuture(channelInformationCache
                .processUpdate(request.getChannelInformationUpdates(),
                        request.getMissingChannels(),
                        request.getForwardToServers(), request.getTimeStamp()));
    }

}
