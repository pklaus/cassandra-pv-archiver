/*
 * Copyright 2015-2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.internode;

import java.net.URI;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.context.event.EventListener;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.Base64Utils;
import org.springframework.web.client.AsyncRestTemplate;
import org.springframework.web.util.DefaultUriTemplateHandler;

import com.aquenos.cassandra.pvarchiver.common.CustomUrlCodec;
import com.aquenos.cassandra.pvarchiver.server.archiving.ArchiveConfigurationCommand;
import com.aquenos.cassandra.pvarchiver.server.archiving.ArchiveConfigurationCommandResult;
import com.aquenos.cassandra.pvarchiver.server.archiving.ChannelStatus;
import com.aquenos.cassandra.pvarchiver.server.database.ChannelMetaDataDAO.ChannelInformation;
import com.aquenos.cassandra.pvarchiver.server.util.FutureUtils;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * Standard implementation of the {@link InterNodeCommunicationService}.
 * 
 * @author Sebastian Marsching
 */
public class InterNodeCommunicationServiceImpl implements
        ApplicationEventPublisherAware, InterNodeCommunicationService,
        SmartInitializingSingleton {

    private static final Charset UTF8_CHARSET = Charset.forName("UTF-8");

    private static <T> HttpEntity<T> prepareHttpEntity(T body,
            Pair<String, String> usernameAndPassword) {
        HttpHeaders headers = new HttpHeaders();
        headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set(
                "Authorization",
                "Basic "
                        + Base64Utils.encodeToString((usernameAndPassword
                                .getLeft() + ":" + usernameAndPassword
                                .getRight()).getBytes(UTF8_CHARSET)));
        return new HttpEntity<T>(body, headers);
    }

    private ApplicationEventPublisher applicationEventPublisher;
    private AsyncRestTemplate asyncRestTemplate;
    private InterNodeCommunicationAuthenticationService authenticationService;
    private DefaultUriTemplateHandler uriTemplateHandler;

    /**
     * Creates the inter-node communication service. The created service is not
     * ready to be used yet. Its dependencies have to be injected via the
     * various setter methods and the initialization has to be finished by
     * calling {@link #afterSingletonsInstantiated()} before using this service.
     */
    public InterNodeCommunicationServiceImpl() {
        this.uriTemplateHandler = new DefaultUriTemplateHandler();
        this.uriTemplateHandler.setStrictEncoding(true);
    }

    @Override
    public void afterSingletonsInstantiated() {
        // When the authentication service is already initialized, we might
        // never receive an
        // InterNodeCommunicationAuthenticationServiceInitializedEvent. In this
        // case, we have to send the
        // InterNodeCommunicationServiceInitializedEvent right now.
        if (authenticationService.isInitialized()) {
            applicationEventPublisher
                    .publishEvent(new InterNodeCommunicationServiceInitializedEvent(
                            this));
        }
    }

    /**
     * Handles
     * {@link InterNodeCommunicationAuthenticationServiceInitializedEvent}s.
     * These events signal that the authentication service used by this service
     * is ready for operation. This service reacts on these events by emitting
     * an {@link InterNodeCommunicationServiceInitializedEvent} in order to
     * signal that is is ready for operation.
     * 
     * @param event
     *            event signalling that the authentication service is ready for
     *            operation.
     */
    @EventListener
    public void onInterNodeCommunicationAuthenticationServiceInitializedEvent(
            InterNodeCommunicationAuthenticationServiceInitializedEvent event) {
        // We want to ignore events that are sent by a different service than
        // the one that we are using.
        if (event.getSource() != authenticationService) {
            return;
        }
        // When the authentication service is ready, this service is also ready
        // and we should send the corresponding event.
        applicationEventPublisher
                .publishEvent(new InterNodeCommunicationServiceInitializedEvent(
                        this));
    }

    @Override
    public void setApplicationEventPublisher(
            ApplicationEventPublisher applicationEventPublisher) {
        this.applicationEventPublisher = applicationEventPublisher;
    }

    /**
     * Sets the async REST template to be used for making HTTP requests to
     * remote servers. Unlike most other injection methods, this method is not
     * marked to be autowired because the async REST template requires specific
     * configuration.
     * 
     * @param asyncRestTemplate
     *            async REST template to be used for making requests to remote
     *            servers.
     */
    public void setAsyncRestTemplate(AsyncRestTemplate asyncRestTemplate) {
        this.asyncRestTemplate = asyncRestTemplate;
    }

    /**
     * Sets the inter-node-communication authentication service used by this
     * service. The authentication service is used for generating the username /
     * password pair that is sent to other servers as part of requests. This
     * information is needed so that the target servers can verify that the
     * request originates from another server in the same cluster. Typically,
     * this method is called automatically by the Spring container.
     * 
     * @param authenticationService
     *            inter-node-communication authentication service for this
     *            server.
     */
    @Autowired
    public void setAuthenticationService(
            InterNodeCommunicationAuthenticationService authenticationService) {
        this.authenticationService = authenticationService;
    }

    @Override
    public ListenableFuture<List<ChannelStatus>> getArchivingStatus(
            String targetBaseUrl) {
        try {
            Preconditions.checkNotNull(targetBaseUrl,
                    "The targetBaseUrl must not be null.");
            String targetUrl = targetBaseUrl + "/api/1.0/archiving-status";
            Pair<String, String> usernameAndPassword = authenticationService
                    .getUsernameAndPassword(InterNodeCommunicationAuthenticationService.Role.PRIVILEGED);
            ListenableFuture<ResponseEntity<ArchivingStatusResponse>> future = FutureUtils
                    .asListenableFuture(asyncRestTemplate.exchange(targetUrl,
                            HttpMethod.GET,
                            prepareHttpEntity(null, usernameAndPassword),
                            ArchivingStatusResponse.class));
            return Futures
                    .transform(
                            future,
                            new Function<ResponseEntity<ArchivingStatusResponse>, List<ChannelStatus>>() {
                                @Override
                                public List<ChannelStatus> apply(
                                        ResponseEntity<ArchivingStatusResponse> input) {
                                    ArchivingStatusResponse response = input
                                            .getBody();
                                    if (response == null) {
                                        throw new RuntimeException(
                                                "The server response did not include a usable body.");
                                    }
                                    return response.getChannelStatusList();
                                }
                            });
        } catch (Throwable t) {
            return Futures.immediateFailedFuture(t);
        }
    }

    @Override
    public ListenableFuture<ChannelStatus> getArchivingStatusForChannel(
            String targetBaseUrl, String channelName) {
        try {
            Preconditions.checkNotNull(targetBaseUrl,
                    "The targetBaseUrl must not be null.");
            Preconditions.checkNotNull(channelName,
                    "The channel name must not be null.");
            Preconditions.checkArgument(!channelName.isEmpty(),
                    "The channelName must not be emnpty.");
            String targetUrlString = targetBaseUrl
                    + "/api/1.0/archiving-status-for-channel/{channelName}";
            URI targetUrl = uriTemplateHandler.expand(targetUrlString,
                    CustomUrlCodec.encode(channelName));
            Pair<String, String> usernameAndPassword = authenticationService
                    .getUsernameAndPassword(InterNodeCommunicationAuthenticationService.Role.PRIVILEGED);
            ListenableFuture<ResponseEntity<ChannelStatus>> future = FutureUtils
                    .asListenableFuture(asyncRestTemplate.exchange(targetUrl,
                            HttpMethod.GET,
                            prepareHttpEntity(null, usernameAndPassword),
                            ChannelStatus.class));
            return Futures
                    .transform(
                            future,
                            new Function<ResponseEntity<ChannelStatus>, ChannelStatus>() {
                                @Override
                                public ChannelStatus apply(
                                        ResponseEntity<ChannelStatus> input) {
                                    return input.getBody();
                                }
                            });
        } catch (Throwable t) {
            return Futures.immediateFailedFuture(t);
        }
    }

    @Override
    public ListenableFuture<Long> getCurrentSystemTime(String targetBaseUrl) {
        try {
            Preconditions.checkNotNull(targetBaseUrl,
                    "The targetBaseUrl must not be null.");
            String targetUrl = targetBaseUrl + "/api/1.0/current-system-time";
            Pair<String, String> usernameAndPassword = authenticationService
                    .getUsernameAndPassword(InterNodeCommunicationAuthenticationService.Role.UNPRIVILEGED);
            ListenableFuture<ResponseEntity<CurrentSystemTimeResponse>> future = FutureUtils
                    .asListenableFuture(asyncRestTemplate.exchange(targetUrl,
                            HttpMethod.GET,
                            prepareHttpEntity(null, usernameAndPassword),
                            CurrentSystemTimeResponse.class));

            return Futures
                    .transform(
                            future,
                            new Function<ResponseEntity<CurrentSystemTimeResponse>, Long>() {
                                @Override
                                public Long apply(
                                        ResponseEntity<CurrentSystemTimeResponse> input) {
                                    CurrentSystemTimeResponse response = input
                                            .getBody();
                                    if (response == null) {
                                        throw new RuntimeException(
                                                "The server response did not include a usable body.");
                                    }
                                    return response
                                            .getCurrentSystemTimeMilliseconds();
                                }
                            });
        } catch (Throwable t) {
            return Futures.immediateFailedFuture(t);
        }
    }

    @Override
    public boolean isInitialized() {
        // This service is ready when its authentication service is ready.
        return authenticationService.isInitialized();
    }

    @Override
    public ListenableFuture<List<ArchiveConfigurationCommandResult>> runArchiveConfigurationCommands(
            String targetBaseUrl,
            List<? extends ArchiveConfigurationCommand> commands) {
        try {
            Preconditions.checkNotNull(targetBaseUrl,
                    "The targetBaseUrl must not be null.");
            Preconditions.checkNotNull(commands,
                    "The commands list must not be null.");
            String targetUrl = targetBaseUrl
                    + "/api/1.0/run-archive-configuration-commands";
            Pair<String, String> usernameAndPassword = authenticationService
                    .getUsernameAndPassword(InterNodeCommunicationAuthenticationService.Role.PRIVILEGED);
            ListenableFuture<ResponseEntity<RunArchiveConfigurationCommandsResponse>> future = FutureUtils
                    .asListenableFuture(asyncRestTemplate.postForEntity(
                            targetUrl,
                            prepareHttpEntity(
                                    new RunArchiveConfigurationCommandsRequest(
                                            commands), usernameAndPassword),
                            RunArchiveConfigurationCommandsResponse.class));
            return Futures
                    .transform(
                            future,
                            new Function<ResponseEntity<RunArchiveConfigurationCommandsResponse>, List<ArchiveConfigurationCommandResult>>() {
                                @Override
                                public List<ArchiveConfigurationCommandResult> apply(
                                        ResponseEntity<RunArchiveConfigurationCommandsResponse> input) {
                                    RunArchiveConfigurationCommandsResponse response = input
                                            .getBody();
                                    if (response == null) {
                                        throw new RuntimeException(
                                                "The server response did not include a usable body.");
                                    }
                                    return response.getResults();
                                }
                            });
        } catch (Throwable t) {
            return Futures.immediateFailedFuture(t);
        }
    }

    @Override
    public ListenableFuture<Void> updateChannelInformation(
            String targetBaseUrl,
            List<ChannelInformation> channelInformationUpdates,
            List<String> missingChannels, List<UUID> forwardToServers,
            long timeStamp) {
        try {
            Preconditions.checkNotNull(targetBaseUrl,
                    "The targetBaseUrl must not be null.");
            Preconditions.checkNotNull(channelInformationUpdates,
                    "The channelInformationUpdates list must not be null.");
            Preconditions.checkNotNull(missingChannels,
                    "The missingChannels list must not be null.");
            // If there are no updates, there is no need to notify the remote
            // server.
            if (channelInformationUpdates.isEmpty()
                    && missingChannels.isEmpty()) {
                return Futures.immediateFuture(null);
            }
            String targetUrl = targetBaseUrl
                    + "/api/1.0/update-channel-information";
            Pair<String, String> usernameAndPassword = authenticationService
                    .getUsernameAndPassword(InterNodeCommunicationAuthenticationService.Role.PRIVILEGED);
            ListenableFuture<ResponseEntity<Void>> future = FutureUtils
                    .asListenableFuture(asyncRestTemplate.postForEntity(
                            targetUrl,
                            prepareHttpEntity(
                                    new UpdateChannelInformationRequest(
                                            channelInformationUpdates,
                                            forwardToServers, missingChannels,
                                            timeStamp), usernameAndPassword),
                            Void.class));
            return Futures.transform(future,
                    new Function<ResponseEntity<Void>, Void>() {
                        @Override
                        public Void apply(ResponseEntity<Void> input) {
                            if (input.getStatusCode().is2xxSuccessful()) {
                                return null;
                            } else {
                                throw new RuntimeException(
                                        "The server response status code "
                                                + input.getStatusCode().value()
                                                + " did not indicate success.");
                            }
                        }
                    });
        } catch (Throwable t) {
            return Futures.immediateFailedFuture(t);
        }
    }

}
