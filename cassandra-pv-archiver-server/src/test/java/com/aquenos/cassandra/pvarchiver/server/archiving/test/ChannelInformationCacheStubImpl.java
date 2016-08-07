/*
 * Copyright 2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.archiving.test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;

import com.aquenos.cassandra.pvarchiver.server.archiving.ChannelInformationCache;
import com.aquenos.cassandra.pvarchiver.server.database.ChannelMetaDataDAO.ChannelInformation;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * Simple stub implementation of the {@link ChannelInformationCache}. This
 * implementation only implements the methods that read data. Methods that would
 * modify data throw an {@link UnsupportedOperationException}.
 * 
 * @author Sebastian Marsching
 */
public class ChannelInformationCacheStubImpl implements ChannelInformationCache {

    private SortedMap<String, ChannelInformation> byName;
    private Map<UUID, SortedMap<String, ChannelInformation>> byServer;

    /**
     * Creates a channel-information cache that contains the specified
     * {@link ChannelInformation} objects.
     * 
     * @param channelInformations
     *            channel-information objects to be provided by this cache.
     * @throws IllegalArgumentException
     *             if the same channel name is used by more than one
     *             channel-information object.
     * @throws NullPointerException
     *             if <code>channelInformations</code> is <code>null</code> or
     *             contains <code>null</code> elements.
     */
    public ChannelInformationCacheStubImpl(
            ChannelInformation... channelInformations) {
        TreeMap<String, ChannelInformation> byName = new TreeMap<String, ChannelInformation>();
        HashMap<UUID, SortedMap<String, ChannelInformation>> byServer = new HashMap<UUID, SortedMap<String, ChannelInformation>>();
        for (ChannelInformation channelInformation : channelInformations) {
            String channelName = channelInformation.getChannelName();
            if (byName.put(channelName, channelInformation) != null) {
                throw new IllegalArgumentException("Duplicate channel name: "
                        + channelName);
            }
            SortedMap<String, ChannelInformation> serverMap = byServer
                    .get(channelInformation.getServerId());
            if (serverMap == null) {
                serverMap = new TreeMap<String, ChannelInformation>();
                byServer.put(channelInformation.getServerId(), serverMap);
            }
            serverMap.put(channelName, channelInformation);
        }
        this.byName = Collections.unmodifiableSortedMap(byName);
        this.byServer = Collections.unmodifiableMap(byServer);
    }

    @Override
    public ChannelInformation getChannel(String channelName) {
        return byName.get(channelName);
    }

    @Override
    public ListenableFuture<ChannelInformation> getChannelAsync(
            String channelName, boolean forceUpdate) {
        return Futures.immediateFuture(getChannel(channelName));
    }

    @Override
    public SortedMap<String, ChannelInformation> getChannels() {
        return byName;
    }

    @Override
    public SortedMap<String, ChannelInformation> getChannels(UUID serverId) {
        return byServer.get(serverId);
    }

    @Override
    public Map<UUID, SortedMap<String, ChannelInformation>> getChannelsSortedByServer() {
        return byServer;
    }

    @Override
    public ListenableFuture<Void> processUpdate(
            List<ChannelInformation> channelInformationUpdates,
            List<String> missingChannels, Iterable<UUID> forwardToServers,
            long remoteTime) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListenableFuture<Void> updateChannels(Iterable<String> channelNames) {
        throw new UnsupportedOperationException();
    }

}
