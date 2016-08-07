/*
 * Copyright 2015-2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.internode;

import java.util.List;
import java.util.UUID;

import com.aquenos.cassandra.pvarchiver.server.archiving.ArchiveConfigurationCommand;
import com.aquenos.cassandra.pvarchiver.server.archiving.ArchiveConfigurationCommandResult;
import com.aquenos.cassandra.pvarchiver.server.archiving.ChannelStatus;
import com.aquenos.cassandra.pvarchiver.server.database.ChannelMetaDataDAO.ChannelInformation;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * <p>
 * Service providing access to other nodes in the archiving cluster. This server
 * can be used to send commands to other nodes when communication between two
 * (or more) nodes is required in order to perform an operation. The service
 * contacts the other server directly through the end-point URL published in the
 * database. This way, a communication link between two servers can be
 * established without having to poll the database constantly.
 * </p>
 * 
 * <p>
 * When an instance of this service has been fully initialized and is ready for
 * operation, it emits an {@link InterNodeCommunicationServiceInitializedEvent}.
 * Typically, such an even is pubished through the application context in which
 * the service is running. The initialization status can also be queried through
 * the {@link #isInitialized()} method.
 * </p>
 * 
 * @author Sebastian Marsching
 */
public interface InterNodeCommunicationService {

    /**
     * <p>
     * Queries a remote server for its archiving status. If the request is not
     * successful, the returned future will throw one of the following
     * exceptions:
     * </p>
     * 
     * <ul>
     * <li>{@link NullPointerException} if <code>targetBaseUrl</code> is
     * <code>null</code>.</li>
     * <li>{@link RuntimeException} if the communication with the target server
     * fails (e.g. the target server does not respond or responds with an
     * invalid response).</li>
     * </ul>
     * 
     * <p>
     * The returned future returns a list containing the status of all channels
     * known by the target server. The list is never <code>null</code>, but it
     * might be empty if the target server does not have any channels. The
     * channels are ordered by the natural order of their names.
     * </p>
     * 
     * @param targetBaseUrl
     *            base URL on which the target server shall be contacted. Must
     *            not include a trailing slash.
     * @return future that completes when the response from the server has been
     *         received. On success, the future will return a list of the status
     *         of all the channels of the target server. On failure, the future
     *         will throw an exception.
     */
    ListenableFuture<List<ChannelStatus>> getArchivingStatus(
            String targetBaseUrl);

    /**
     * <p>
     * Queries a remote server for the archiving status of one of its channels.
     * If the request is not successful, the returned future will throw one of
     * the following exceptions:
     * </p>
     * 
     * <ul>
     * <li>{@link IllegalArgumentException} if <code>channelName</code>is empty.
     * </li>
     * <li>{@link NullPointerException} if <code>targetBaseUrl</code> or
     * <code>channelName</code> is <code>null</code>.</li>
     * <li>{@link RuntimeException} if the communication with the target server
     * fails (e.g. the target server does not respond or responds with an
     * invalid response).</li>
     * </ul>
     * 
     * <p>
     * The returned future returns <code>null</code> if the specified channel is
     * not known by the target server's archiving service (e.g. because it does
     * not exist or is registered with a different server).
     * </p>
     * 
     * @param targetBaseUrl
     *            base URL on which the target server shall be contacted. Must
     *            not include a trailing slash.
     * @param channelName
     *            name of the channel to be queried.
     * @return future that completes when the response from the server has been
     *         received. On success, the future will return the status of the
     *         queried channel or <code>null</code> if the channel is not known
     *         by the target server. On failure, the future will throw an
     *         exception.
     */
    ListenableFuture<ChannelStatus> getArchivingStatusForChannel(
            String targetBaseUrl, String channelName);

    /**
     * <p>
     * Queries a remote server for its current system time (as returned by
     * {@link System#currentTimeMillis()}). If the request is not successful,
     * the returned future will throw one of the following exceptions:
     * </p>
     * 
     * <ul>
     * <li>{@link NullPointerException} if <code>targetBaseUrl</code> is
     * <code>null</code>.</li>
     * <li>{@link RuntimeException} if the communication with the target server
     * fails (e.g. the target server does not respond or responds with an
     * invalid response).</li>
     * </ul>
     * 
     * @param targetBaseUrl
     *            base URL on which the target server shall be contacted. Must
     *            not include a trailing slash.
     * @return future that completes when the response from the server has been
     *         received. On success, the future will return the current system
     *         time on the queried server. On failure, the future will throw an
     *         exception.
     */
    ListenableFuture<Long> getCurrentSystemTime(String targetBaseUrl);

    /**
     * <p>
     * Tells whether this communication service has initialized completely and
     * is ready for operation. The communication service is ready as soon as all
     * of its dependencies are ready.
     * </p>
     * 
     * <p>
     * When this service changes to the initialized state, it publishes an
     * {@link InterNodeCommunicationServiceInitializedEvent} to the application
     * context.
     * </p>
     * 
     * @return <code>true</code> if initialization has completed and this
     *         communication service can be used, <code>false</code> otherwise.
     */
    boolean isInitialized();

    /**
     * <p>
     * Runs a list of archive-configuration commands on the specified remote
     * server. If the execution of individual commands on the target server
     * fails, the corresponding {@link ArchiveConfigurationCommandResult} will
     * reflect this error. However, if the whole communication process fails,
     * the future returned by this method will throw one of the following
     * exceptions:
     * </p>
     * 
     * <ul>
     * <li>{@link NullPointerException} if <code>targetBaseUrl</code> or
     * <code>commands</code> is <code>null</code> or <code>commands</code>
     * contains <code>null</code> elements.</li>
     * <li>{@link RuntimeException} if the communication with the target server
     * fails (e.g. the target server does not respond or responds with an
     * unexpected error).</li>
     * </ul>
     * 
     * @param targetBaseUrl
     *            base URL on which the target server shall be contacted. Must
     *            not include a trailing slash.
     * @param commands
     *            list of commands that shall be sent to the target server.
     * @return future that completes when the whole operation has finished. On
     *         success, the future will return a list of
     *         {@link ArchiveConfigurationCommandResult}s that are returned in
     *         the same order as the commands specified in <code>commands</code>
     *         . If one or several individual commands fail, the corresponding
     *         result will reflect this failure. However, if the whole operation
     *         fails due to an unexpected problem, the future will throw an
     *         exception.
     */
    ListenableFuture<List<ArchiveConfigurationCommandResult>> runArchiveConfigurationCommands(
            String targetBaseUrl,
            List<? extends ArchiveConfigurationCommand> commands);

    /**
     * <p>
     * Updates the channel information cache of a remote server. The specified
     * updates are sent to the remote server and it is asked to also notify the
     * servers listed in <code>forwardToServers</code>. On failure, the future
     * returned by this method throws one of the following exceptions:
     * </p>
     * 
     * <ul>
     * <li>{@link NullPointerException} if <code>targetBaseUrl</code>,
     * <code>channelInformationUpdates</code>, or <code>missingChannels</code>
     * is <code>null</code> or <code>channelInformationUpdates</code>,
     * <code>missingChannels</code>, or <code>forwardToServers</code> contains
     * <code>null</code> elements.</li>
     * <li>{@link RuntimeException} if the communication with the target server
     * fails (e.g. the target server does not respond or responds with an
     * unexpected error).</li>
     * </ul>
     * 
     * @param targetBaseUrl
     *            base URL on which the target server shall be contacted. Must
     *            not include a trailing slash.
     * @param channelInformationUpdates
     *            updated channel information objects.
     * @param missingChannels
     *            names of channels that are now missing (the channel
     *            information is <code>null</code>, which typically means that
     *            they have been deleted).
     * @param forwardToServers
     *            servers to which the target server should forward this update.
     *            This parameter may be <code>null</code> if the target server
     *            shall not forward this update.
     * @param timeStamp
     *            time stamp (as returned by {@link System#currentTimeMillis()})
     *            of the point in time right before the information that is part
     *            of this update was retrieved from the database. This
     *            information is used for conflict resolution on the target
     *            server.
     * @return future that completes when the operation has finished. On
     *         failure, the future throws an exception.
     */
    ListenableFuture<Void> updateChannelInformation(String targetBaseUrl,
            List<ChannelInformation> channelInformationUpdates,
            List<String> missingChannels, List<UUID> forwardToServers,
            long timeStamp);

}
