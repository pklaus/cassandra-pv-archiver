/*
 * Copyright 2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.archiving;

import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.UUID;

import com.aquenos.cassandra.pvarchiver.server.database.ChannelMetaDataDAO.ChannelInformation;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * <p>
 * Cache for {@link ChannelInformation} objects.
 * </p>
 * 
 * <p>
 * Reading {@link ChannelInformation} objects from the database is rather
 * expensive in terms of latency, in particular if many of them are needed.
 * </p>
 * 
 * <p>
 * The cache can be queried through the {@link #getChannel(String)},
 * {@link #getChannels()}, {@link #getChannels(UUID)}, and
 * {@link #getChannelsSortedByServer()} methods. All these methods throw an
 * exception if the cache is not ready for being queried.
 * </p>
 * 
 * <p>
 * The {@link #getChannelAsync(String, boolean)} method can be used if the
 * database should be queried directly if the cache is not ready or if an update
 * of the cache should be forced before returning the channel information. Due
 * to the potential database access, this operation can take a considerable
 * amount of time to complete and thus works in an asynchronous fashion.
 * </p>
 * 
 * <p>
 * The {@link #updateChannels(Iterable)} method should be called when the
 * configuration of channels has been modified. The
 * {@link ArchiveConfigurationService} takes care of calling this method.
 * Finally, the {@link #processUpdate(List, List, Iterable, long)} method is
 * called by remote servers if they updated their cache because of configuration
 * changes, forwarding the updated information to this server.
 * </p>
 * 
 * @author Sebastian Marsching
 */
public interface ChannelInformationCache {

    /**
     * <p>
     * Returns the channel information for the specified channel name or
     * <code>null</code> if the channel does not exist.
     * </p>
     * 
     * <p>
     * The cache works in a way that it should be updated when channels are
     * modifies, so in general it should return up-to-date information about
     * channels. However, due to the asynchronous nature of cache updates, it is
     * possible that this method returns obsolete information. If it is
     * important that the most up-to-date information is returned, the
     * {@link #getChannelAsync(String, boolean)} method should be preferred.
     * </p>
     * 
     * <p>
     * This method throws an {@link IllegalStateException} if the cache is not
     * ready. Reasons for the cache not being ready might be that it has not
     * been properly initialized yet or that updates have failed and thus the
     * cache has been invalidated in order to avoid serving obsolete
     * information.
     * </p>
     * 
     * @param channelName
     *            name of the channel for which the channel information shall be
     *            retrieved.
     * @return channel information for the specified channel name or
     *         <code>null</code> if the channel does not exist.
     * @throws IllegalStateException
     *             if the cache is not ready to be queried.
     * @throws NullPointerException
     *             if <code>channelName</code> is <code>null</code>.
     */
    ChannelInformation getChannel(String channelName);

    /**
     * <p>
     * Returns the channel information for the specified channel name or
     * <code>null</code> if the channel does not exist. The operation completes
     * asynchronously (without blocking), so the channel information is returned
     * through a future.
     * </p>
     * 
     * <p>
     * If the cache is not ready to be queried, this method queries the database
     * directly. If the <code>forceUpdate</code> flag is <code>true</code>, the
     * database is always queried directly and the information in the cache is
     * updated with the information retrieved from the database.
     * </p>
     * 
     * <p>
     * The future returned by this method might throw the following exceptions:
     * </p>
     * 
     * <ul>
     * <li>{@link NullPointerException} if the <code>channelName</code> is
     * <code>null</code>.</li>
     * <li>{@link RuntimeException} if the database is queried and that query
     * fails.</li>
     * </ul>
     * 
     * @param channelName
     *            name of the channel for which the channel information shall be
     *            retrieved.
     * @param forceUpdate
     *            <code>true</code> if the database shall always be queried,
     *            even if the cache is ready. <code>false</code> if the
     *            information from the cache should be used if the cache is
     *            ready.
     * @return channel information for the specified channel name, exposed
     *         through a listenable future.
     */
    ListenableFuture<ChannelInformation> getChannelAsync(
            final String channelName, boolean forceUpdate);

    /**
     * <p>
     * Returns a map containing all channels in the cluster. The channel names
     * are used as keys and the corresponding channel information objects are
     * stored as values.
     * </p>
     * 
     * <p>
     * All the information is served from the cache. The cache works in a way
     * that it should be updated when channels are modifies, so in general it
     * should return up-to-date information about channels. However, due to the
     * asynchronous nature of cache updates, it is possible that this method
     * returns obsolete information.
     * </p>
     * 
     * <p>
     * This method throws an {@link IllegalStateException} if the cache is not
     * ready. Reasons for the cache not being ready might be that it has not
     * been properly initialized yet or that updates have failed and thus the
     * cache has been invalidated in order to avoid serving obsolete
     * information.
     * </p>
     * 
     * @return map containing all channels that exist in the cluster.
     * @throws IllegalStateException
     *             if the cache is not ready to be queried.
     */
    SortedMap<String, ChannelInformation> getChannels();

    /**
     * <p>
     * Returns a map containing all channels for the specified server. The
     * channel names are used as keys and the corresponding channel information
     * objects are stored as values. If the specified server is unknown, an
     * empty map is returned.
     * </p>
     * 
     * <p>
     * All the information is served from the cache. The cache works in a way
     * that it should be updated when channels are modifies, so in general it
     * should return up-to-date information about channels. However, due to the
     * asynchronous nature of cache updates, it is possible that this method
     * returns obsolete information.
     * </p>
     * 
     * <p>
     * This method throws an {@link IllegalStateException} if the cache is not
     * ready. Reasons for the cache not being ready might be that it has not
     * been properly initialized yet or that updates have failed and thus the
     * cache has been invalidated in order to avoid serving obsolete
     * information.
     * </p>
     * 
     * @param serverId
     *            id of the server for which the channels shall be returned.
     * @return map containing all channels for the specified server.
     * @throws IllegalStateException
     *             if the cache is not ready to be queried.
     * @throws NullPointerException
     *             if the <code>serverId</code> is <code>null</code>.
     */
    SortedMap<String, ChannelInformation> getChannels(UUID serverId);

    /**
     * <p>
     * Returns a map containing all channels in the cluster. The channels are
     * aggregated by their associated server, having one map for each server ID.
     * </p>
     * 
     * <p>
     * All the information is served from the cache. The cache works in a way
     * that it should be updated when channels are modifies, so in general it
     * should return up-to-date information about channels. However, due to the
     * asynchronous nature of cache updates, it is possible that this method
     * returns obsolete information.
     * </p>
     * 
     * <p>
     * This method throws an {@link IllegalStateException} if the cache is not
     * ready. Reasons for the cache not being ready might be that it has not
     * been properly initialized yet or that updates have failed and thus the
     * cache has been invalidated in order to avoid serving obsolete
     * information.
     * </p>
     * 
     * @return map containing all channels that exist in the cluster aggregated
     *         by server. The returned map contains an entry for each known
     *         server that has at least one channel. Each of this entries has a
     *         map as its value that maps channel names to the corresponding
     *         channel information objects.
     * @throws IllegalStateException
     *             if the cache is not ready to be queried.
     */
    Map<UUID, SortedMap<String, ChannelInformation>> getChannelsSortedByServer();

    /**
     * <p>
     * Process an update that has been received from a remote server. This
     * method should only be called by remote servers (through the corresponding
     * web-service API). It updates the cache with the information supplied in
     * the request, merging the entries in the cache with the entries from the
     * update. The cache is updated synchronously, but the servers specified in
     * <code>forwardToServers</code> are notified asynchronously.
     * </p>
     * 
     * <p>
     * The notification of other servers works in a cascaded way: Only a limited
     * number of servers from the specified list are notified directly. Those
     * servers are in turn asked to notify a subset of the remaining servers.
     * This way, each server only has to handle a limited number of
     * notifications, even if the cluster is very large.
     * </p>
     * 
     * <p>
     * The returned future throws a {@link NullPointerException} if
     * <code>channelInformationUpdates</code>, <code>missingChannels</code>, or
     * <code>forwardToServers</code> is <code>null</code> or they contain
     * <code>null</code> elements.
     * </p>
     * 
     * @param channelInformationUpdates
     *            updated channel-information objects.
     * @param missingChannels
     *            channels that are missing now (the channel information is
     *            <code>null</code> - typically because the channel has been
     *            deleted).
     * @param forwardToServers
     *            list of servers which should be notified with the information
     *            from the update.
     * @param remoteTime
     *            time stamp (as returned by {@link System#currentTimeMillis()}
     *            of the point in time before the channel information that is
     *            sent with this update was retrieved from the database. This
     *            information is used when resolving conflicting updates for the
     *            same channel.
     * @return listenable future that finishes when the cache has been updated
     *         and all servers have been notified (or the notification attempts
     *         have failed).
     */
    ListenableFuture<Void> processUpdate(
            List<ChannelInformation> channelInformationUpdates,
            List<String> missingChannels, Iterable<UUID> forwardToServers,
            long remoteTime);

    /**
     * <p>
     * Updates the channel information for the specified channel name. This
     * method does not only update the local cache but also forwards the updated
     * information to all remote servers by calling their
     * {@link #processUpdate(List, List, Iterable, long)} method with the
     * updated information.
     * </p>
     * 
     * <p>
     * This method is intended for the {@link ArchiveConfigurationService} which
     * calls it when it modifies channel configurations.
     * </p>
     * 
     * <p>
     * The future returned by this method might throw the following exceptions:
     * </p>
     * 
     * <ul>
     * <li>{@link NullPointerException} if the <code>channelNames</code> is
     * <code>null</code> or contains <code>null</code> elements.</li>
     * <li>{@link RuntimeException} if one of the database queries needed for
     * the update fails.</li>
     * </ul>
     * 
     * @param channelNames
     *            names of the channels to be updated.
     * @return listenable future that completes when the local cache has been
     *         updated and the update has been forwarded to the remote servers.
     */
    ListenableFuture<Void> updateChannels(Iterable<String> channelNames);

}
