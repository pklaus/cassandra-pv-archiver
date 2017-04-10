/*
 * Copyright 2015-2017 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.database;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Future;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.ClassRule;
import org.junit.Test;

import com.aquenos.cassandra.pvarchiver.server.database.ChannelMetaDataDAO.ChannelConfiguration;
import com.aquenos.cassandra.pvarchiver.server.database.ChannelMetaDataDAO.ChannelInformation;
import com.aquenos.cassandra.pvarchiver.server.database.ChannelMetaDataDAO.ChannelOperation;
import com.aquenos.cassandra.pvarchiver.server.database.ChannelMetaDataDAO.SampleBucketInformation;
import com.google.common.base.Functions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;

/**
 * Tests for the {@link ChannelMetaDataDAOImpl}.
 * 
 * @author Sebastian Marsching
 */
public class ChannelMetaDataDAOImplTest {

    /**
     * Cassandra provider that provides access to the embedded Cassandra server.
     */
    @ClassRule
    public static CassandraProviderStub cassandraProvider = new CassandraProviderStub();

    private static Map<Integer, Long> decimationLevelSetToCurrentBucketStartTimeMap(
            Set<Integer> decimationLevels) {
        return Maps.asMap(decimationLevels, Functions.constant(-1L));
    }

    private ChannelMetaDataDAOImpl dao;

    private final UUID serverId1;
    private final UUID serverId2;
    private final UUID serverId3;
    private final ChannelInformation channelInfo1;
    private final ChannelConfiguration channelConfig1;
    private final ChannelInformation channelInfo2;
    private final ChannelConfiguration channelConfig2;
    private final ChannelInformation channelInfo3;
    private final ChannelConfiguration channelConfig3;
    private final ChannelInformation channelInfo4;
    private final ChannelConfiguration channelConfig4;

    /**
     * Creates the test suite.
     */
    public ChannelMetaDataDAOImplTest() {
        dao = new ChannelMetaDataDAOImpl();
        dao.setCassandraProvider(cassandraProvider);
        dao.afterSingletonsInstantiated();
        serverId1 = UUID.randomUUID();
        serverId2 = UUID.randomUUID();
        serverId3 = UUID.randomUUID();
        Map<Integer, Integer> decimationLevelToRetentionPeriods_0 = new HashMap<Integer, Integer>();
        decimationLevelToRetentionPeriods_0.put(0, 86400);
        Set<Integer> decimationLevels_0 = decimationLevelToRetentionPeriods_0
                .keySet();
        Map<Integer, Integer> decimationLevelToRetentionPeriods_0_5_30 = new HashMap<Integer, Integer>();
        decimationLevelToRetentionPeriods_0_5_30.put(0, 31536000);
        decimationLevelToRetentionPeriods_0_5_30.put(5, 157680000);
        decimationLevelToRetentionPeriods_0_5_30.put(30, 0);
        Set<Integer> decimationLevels_0_5_30 = decimationLevelToRetentionPeriods_0_5_30
                .keySet();
        Map<String, String> options1 = new HashMap<String, String>();
        options1.put("my_option", "foo");
        options1.put("other_option", "bar");
        Map<String, String> options2 = new HashMap<String, String>();
        options1.put("other_option", "");
        Map<String, String> options3 = new HashMap<String, String>();
        channelInfo1 = new ChannelInformation(UUID.randomUUID(), "test1",
                "my_cs", decimationLevels_0_5_30, serverId1);
        channelConfig1 = new ChannelConfiguration(
                channelInfo1.getChannelDataId(), channelInfo1.getChannelName(),
                channelInfo1.getControlSystemType(),
                decimationLevelSetToCurrentBucketStartTimeMap(
                        channelInfo1.getDecimationLevels()),
                decimationLevelToRetentionPeriods_0_5_30, true, options1,
                channelInfo1.getServerId());
        channelInfo2 = new ChannelInformation(UUID.randomUUID(), "test2",
                "my_cs", decimationLevels_0, serverId2);
        channelConfig2 = new ChannelConfiguration(
                channelInfo2.getChannelDataId(), channelInfo2.getChannelName(),
                channelInfo2.getControlSystemType(),
                decimationLevelSetToCurrentBucketStartTimeMap(
                        channelInfo2.getDecimationLevels()),
                decimationLevelToRetentionPeriods_0, false, options1,
                channelInfo2.getServerId());
        channelInfo3 = new ChannelInformation(UUID.randomUUID(), "test3",
                "other_cs", decimationLevels_0_5_30, serverId3);
        channelConfig3 = new ChannelConfiguration(
                channelInfo3.getChannelDataId(), channelInfo3.getChannelName(),
                channelInfo3.getControlSystemType(),
                decimationLevelSetToCurrentBucketStartTimeMap(
                        channelInfo3.getDecimationLevels()),
                decimationLevelToRetentionPeriods_0_5_30, true, options2,
                channelInfo3.getServerId());
        channelInfo4 = new ChannelInformation(UUID.randomUUID(), "test4",
                "other_cs", decimationLevels_0, serverId1);
        channelConfig4 = new ChannelConfiguration(
                channelInfo4.getChannelDataId(), channelInfo4.getChannelName(),
                channelInfo4.getControlSystemType(),
                decimationLevelSetToCurrentBucketStartTimeMap(
                        channelInfo4.getDecimationLevels()),
                decimationLevelToRetentionPeriods_0, true, options3,
                channelInfo4.getServerId());
    }

    private void createChannel(ChannelConfiguration channelConfig) {
        Futures.getUnchecked(dao.createChannel(channelConfig.getChannelName(),
                channelConfig.getChannelDataId(),
                channelConfig.getControlSystemType(),
                channelConfig.getDecimationLevelToRetentionPeriod().keySet(),
                channelConfig.getDecimationLevelToRetentionPeriod(),
                channelConfig.isEnabled(), channelConfig.getOptions(),
                channelConfig.getServerId()));
    }

    private static List<SampleBucketInformation> listForSampleBuckets(
            Future<? extends Iterable<? extends SampleBucketInformation>> sampleBucketsFuture) {
        return Lists.newArrayList(Futures.getUnchecked(sampleBucketsFuture));
    }

    private static void sleepTtl(int ttlSeconds) {
        // We sleep slightly longer than the specified time to ensure that we do
        // not get in trouble with rounding problems etc.
        try {
            Thread.sleep(ttlSeconds * 1000L + 500L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Tests adding and removing channels.
     */
    @Test
    public void testCreateAndDeleteChannels() {
        assertEquals(0,
                Iterables.size(Futures.getUnchecked(dao.getChannels())));
        assertEquals(0, Iterables.size(
                Futures.getUnchecked(dao.getChannelsByServer(serverId1))));
        createChannel(channelConfig1);
        assertEquals(channelInfo1, Futures
                .getUnchecked(dao.getChannel(channelInfo1.getChannelName())));
        assertEquals(channelConfig1,
                Futures.getUnchecked(
                        dao.getChannelByServer(channelConfig1.getServerId(),
                                channelConfig1.getChannelName())));
        assertEquals(1,
                Iterables.size(Futures.getUnchecked(dao.getChannels())));
        assertEquals(1, Iterables.size(
                Futures.getUnchecked(dao.getChannelsByServer(serverId1))));
        assertEquals(0, Iterables.size(
                Futures.getUnchecked(dao.getChannelsByServer(serverId2))));
        createChannel(channelConfig2);
        assertEquals(channelInfo2, Futures
                .getUnchecked(dao.getChannel(channelInfo2.getChannelName())));
        assertEquals(channelConfig2,
                Futures.getUnchecked(
                        dao.getChannelByServer(channelConfig2.getServerId(),
                                channelConfig2.getChannelName())));
        assertEquals(2,
                Iterables.size(Futures.getUnchecked(dao.getChannels())));
        assertEquals(1, Iterables.size(
                Futures.getUnchecked(dao.getChannelsByServer(serverId2))));
        assertEquals(0, Iterables.size(
                Futures.getUnchecked(dao.getChannelsByServer(serverId3))));
        createChannel(channelConfig3);
        assertEquals(channelInfo3, Futures
                .getUnchecked(dao.getChannel(channelInfo3.getChannelName())));
        assertEquals(channelConfig3,
                Futures.getUnchecked(
                        dao.getChannelByServer(channelConfig3.getServerId(),
                                channelConfig3.getChannelName())));
        assertEquals(3,
                Iterables.size(Futures.getUnchecked(dao.getChannels())));
        assertEquals(1, Iterables.size(
                Futures.getUnchecked(dao.getChannelsByServer(serverId3))));
        assertEquals(1, Iterables.size(
                Futures.getUnchecked(dao.getChannelsByServer(serverId1))));
        createChannel(channelConfig4);
        assertEquals(channelInfo4, Futures
                .getUnchecked(dao.getChannel(channelInfo4.getChannelName())));
        assertEquals(channelConfig4,
                Futures.getUnchecked(
                        dao.getChannelByServer(channelConfig4.getServerId(),
                                channelConfig4.getChannelName())));
        assertEquals(4,
                Iterables.size(Futures.getUnchecked(dao.getChannels())));
        assertEquals(2, Iterables.size(
                Futures.getUnchecked(dao.getChannelsByServer(serverId1))));
        Futures.getUnchecked(dao.deleteChannel(channelConfig2.getChannelName(),
                channelConfig2.getServerId()));
        assertEquals(3,
                Iterables.size(Futures.getUnchecked(dao.getChannels())));
        assertEquals(0, Iterables.size(
                Futures.getUnchecked(dao.getChannelsByServer(serverId2))));
        Futures.getUnchecked(dao.deleteChannel(channelConfig1.getChannelName(),
                channelConfig1.getServerId()));
        assertEquals(2,
                Iterables.size(Futures.getUnchecked(dao.getChannels())));
        assertEquals(1, Iterables.size(
                Futures.getUnchecked(dao.getChannelsByServer(serverId1))));
        Futures.getUnchecked(dao.deleteChannel(channelConfig3.getChannelName(),
                channelConfig3.getServerId()));
        assertEquals(1,
                Iterables.size(Futures.getUnchecked(dao.getChannels())));
        assertEquals(0, Iterables.size(
                Futures.getUnchecked(dao.getChannelsByServer(serverId3))));
        Futures.getUnchecked(dao.deleteChannel(channelConfig4.getChannelName(),
                channelConfig4.getServerId()));
        assertEquals(0,
                Iterables.size(Futures.getUnchecked(dao.getChannels())));
        assertEquals(0, Iterables.size(
                Futures.getUnchecked(dao.getChannelsByServer(serverId1))));
    }

    /**
     * Tests adding and removing decimation levels.
     */
    @Test
    public void testCreateAndDeleteDecimationLevels() {
        createChannel(channelConfig1);
        Set<Integer> moreDecimationLevels = Sets.newHashSet(0, 5, 30, 300, 900);
        Map<Integer, Long> moreDecimationLevelsBucketStartTimeMap = Maps
                .asMap(moreDecimationLevels, Functions.constant(-1L));
        Map<Integer, Integer> moreDecimationLevelsRetentionPeriods = new HashMap<Integer, Integer>();
        moreDecimationLevelsRetentionPeriods
                .putAll(channelConfig1.getDecimationLevelToRetentionPeriod());
        moreDecimationLevelsRetentionPeriods.put(300, 0);
        moreDecimationLevelsRetentionPeriods.put(900, 0);
        ChannelConfiguration channelConfig1MoreDecimationLevels = new ChannelConfiguration(
                channelConfig1.getChannelDataId(),
                channelConfig1.getChannelName(),
                channelConfig1.getControlSystemType(),
                moreDecimationLevelsBucketStartTimeMap,
                moreDecimationLevelsRetentionPeriods,
                channelConfig1.isEnabled(), channelConfig1.getOptions(),
                channelConfig1.getServerId());
        ChannelInformation channelInfo1MoreDecimationLevels = new ChannelInformation(
                channelInfo1.getChannelDataId(), channelInfo1.getChannelName(),
                channelInfo1.getControlSystemType(), moreDecimationLevels,
                channelInfo1.getServerId());
        Futures.getUnchecked(dao.createChannelDecimationLevels(
                channelConfig1.getChannelName(), channelConfig1.getServerId(),
                Sets.newHashSet(300, 900),
                Collections.<Integer, Integer> emptyMap()));
        assertEquals(channelInfo1MoreDecimationLevels,
                Futures.getUnchecked(dao.getChannel(
                        channelInfo1MoreDecimationLevels.getChannelName())));
        assertEquals(channelConfig1MoreDecimationLevels,
                Futures.getUnchecked(dao.getChannelByServer(
                        channelConfig1MoreDecimationLevels.getServerId(),
                        channelConfig1MoreDecimationLevels.getChannelName())));
        Set<Integer> lessDecimationLevels = Sets.newHashSet(0, 30, 300);
        Map<Integer, Long> lessDecimationLevelsBucketStartTimeMap = Maps
                .asMap(lessDecimationLevels, Functions.constant(-1L));
        Map<Integer, Integer> lessDecimationLevelsRetentionPeriods = new HashMap<Integer, Integer>(
                moreDecimationLevelsRetentionPeriods);
        lessDecimationLevelsRetentionPeriods.remove(5);
        lessDecimationLevelsRetentionPeriods.remove(900);
        ChannelConfiguration channelConfig1LessDecimationLevels = new ChannelConfiguration(
                channelConfig1.getChannelDataId(),
                channelConfig1.getChannelName(),
                channelConfig1.getControlSystemType(),
                lessDecimationLevelsBucketStartTimeMap,
                lessDecimationLevelsRetentionPeriods,
                channelConfig1.isEnabled(), channelConfig1.getOptions(),
                channelConfig1.getServerId());
        ChannelInformation channelInfo1LessDecimationLevels = new ChannelInformation(
                channelInfo1.getChannelDataId(), channelInfo1.getChannelName(),
                channelInfo1.getControlSystemType(), lessDecimationLevels,
                channelInfo1.getServerId());
        Futures.getUnchecked(dao.deleteChannelDecimationLevels(
                channelConfig1.getChannelName(), channelConfig1.getServerId(),
                Sets.newHashSet(5, 900)));
        assertEquals(channelInfo1LessDecimationLevels,
                Futures.getUnchecked(dao.getChannel(
                        channelInfo1LessDecimationLevels.getChannelName())));
        assertEquals(channelConfig1LessDecimationLevels,
                Futures.getUnchecked(dao.getChannelByServer(
                        channelConfig1LessDecimationLevels.getServerId(),
                        channelConfig1LessDecimationLevels.getChannelName())));
        Futures.getUnchecked(dao.deleteChannel(channelConfig1.getChannelName(),
                channelConfig1.getServerId()));
    }

    /**
     * Tests adding and removing pending channel operations.
     */
    @Test
    public void testCreateAndDeletePendingChannelOperations() {
        ChannelOperation operation1 = new ChannelOperation("test1", "someData",
                UUID.randomUUID(), "someType", UUID.randomUUID());
        ChannelOperation operation2 = new ChannelOperation("test2", null,
                UUID.randomUUID(), "otherType", operation1.getServerId());
        ChannelOperation operation3 = new ChannelOperation("test1", "otherData",
                UUID.randomUUID(), "someType", UUID.randomUUID());
        ChannelOperation operation4 = new ChannelOperation("test1", "someData",
                UUID.randomUUID(), "otherTypeType", operation3.getServerId());
        // A TTL of 600 seconds is long enough so that no entry should vanish
        // while running the test.
        int ttl = 600;
        Pair<Boolean, UUID> result;
        assertEquals(0, Iterables.size(Futures.getUnchecked(
                dao.getPendingChannelOperations(operation1.getServerId()))));
        assertEquals(0, Iterables.size(Futures.getUnchecked(
                dao.getPendingChannelOperations(operation3.getServerId()))));
        result = Futures.getUnchecked(dao.createPendingChannelOperationRelaxed(
                operation1.getServerId(), operation1.getChannelName(),
                operation1.getOperationId(), operation1.getOperationType(),
                operation1.getOperationData(), ttl));
        assertTrue(result.getLeft());
        assertNull(result.getRight());
        assertEquals(1, Iterables.size(Futures.getUnchecked(
                dao.getPendingChannelOperations(operation1.getServerId()))));
        assertEquals(0, Iterables.size(Futures.getUnchecked(
                dao.getPendingChannelOperations(operation3.getServerId()))));
        assertEquals(operation1,
                Futures.getUnchecked(
                        dao.getPendingChannelOperation(operation1.getServerId(),
                                operation1.getChannelName())));
        assertNull(Futures.getUnchecked(dao.getPendingChannelOperation(
                operation2.getServerId(), operation2.getChannelName())));
        assertNull(Futures.getUnchecked(dao.getPendingChannelOperation(
                operation3.getServerId(), operation3.getChannelName())));
        result = Futures.getUnchecked(dao.createPendingChannelOperationRelaxed(
                operation2.getServerId(), operation2.getChannelName(),
                operation2.getOperationId(), operation2.getOperationType(),
                operation2.getOperationData(), ttl));
        assertTrue(result.getLeft());
        assertNull(result.getRight());
        assertEquals(2, Iterables.size(Futures.getUnchecked(
                dao.getPendingChannelOperations(operation1.getServerId()))));
        assertEquals(0, Iterables.size(Futures.getUnchecked(
                dao.getPendingChannelOperations(operation3.getServerId()))));
        assertEquals(operation1,
                Futures.getUnchecked(
                        dao.getPendingChannelOperation(operation1.getServerId(),
                                operation1.getChannelName())));
        assertEquals(operation2,
                Futures.getUnchecked(
                        dao.getPendingChannelOperation(operation2.getServerId(),
                                operation2.getChannelName())));
        assertNull(Futures.getUnchecked(dao.getPendingChannelOperation(
                operation3.getServerId(), operation3.getChannelName())));
        result = Futures.getUnchecked(dao.createPendingChannelOperationRelaxed(
                operation3.getServerId(), operation3.getChannelName(),
                operation3.getOperationId(), operation3.getOperationType(),
                operation3.getOperationData(), ttl));
        assertTrue(result.getLeft());
        assertNull(result.getRight());
        assertEquals(2, Iterables.size(Futures.getUnchecked(
                dao.getPendingChannelOperations(operation1.getServerId()))));
        assertEquals(1, Iterables.size(Futures.getUnchecked(
                dao.getPendingChannelOperations(operation3.getServerId()))));
        assertEquals(operation1,
                Futures.getUnchecked(
                        dao.getPendingChannelOperation(operation1.getServerId(),
                                operation1.getChannelName())));
        assertEquals(operation2,
                Futures.getUnchecked(
                        dao.getPendingChannelOperation(operation2.getServerId(),
                                operation2.getChannelName())));
        assertEquals(operation3,
                Futures.getUnchecked(
                        dao.getPendingChannelOperation(operation3.getServerId(),
                                operation3.getChannelName())));
        // We should not be able to create an operation if there is already an
        // operation for the same server and channel. This should hold, even if
        // we try to create exactly the same operation.
        result = Futures.getUnchecked(dao.createPendingChannelOperation(
                operation3.getServerId(), operation3.getChannelName(),
                operation3.getOperationId(), operation3.getOperationType(),
                operation3.getOperationData(), ttl));
        assertFalse(result.getLeft());
        assertEquals(operation3.getOperationId(), result.getRight());
        result = Futures.getUnchecked(dao.createPendingChannelOperation(
                operation4.getServerId(), operation4.getChannelName(),
                operation4.getOperationId(), operation4.getOperationType(),
                operation4.getOperationData(), ttl));
        assertFalse(result.getLeft());
        assertEquals(operation3.getOperationId(), result.getRight());
        // However, creating an operation that exactly matches an existing one
        // should succeed if running in relaxed mode.
        result = Futures.getUnchecked(dao.createPendingChannelOperationRelaxed(
                operation3.getServerId(), operation3.getChannelName(),
                operation3.getOperationId(), operation3.getOperationType(),
                operation3.getOperationData(), ttl));
        assertTrue(result.getLeft());
        assertNull(result.getRight());
        // Creating an operation that does differ from the existing one in any
        // point (ID, type, data), should still fail.
        result = Futures.getUnchecked(dao.createPendingChannelOperationRelaxed(
                operation3.getServerId(), operation3.getChannelName(),
                UUID.randomUUID(), operation3.getOperationType(),
                operation3.getOperationData(), ttl));
        assertFalse(result.getLeft());
        assertEquals(operation3.getOperationId(), result.getRight());
        result = Futures.getUnchecked(dao.createPendingChannelOperationRelaxed(
                operation3.getServerId(), operation3.getChannelName(),
                operation3.getOperationId(), operation3.getOperationType(),
                "someOtherData", ttl));
        assertFalse(result.getLeft());
        assertEquals(operation3.getOperationId(), result.getRight());
        result = Futures.getUnchecked(dao.createPendingChannelOperationRelaxed(
                operation3.getServerId(), operation3.getChannelName(),
                operation3.getOperationId(), "someOtherType",
                operation3.getOperationData(), ttl));
        assertFalse(result.getLeft());
        assertEquals(operation3.getOperationId(), result.getRight());
        // Trying to delete using the wrong operation ID should fail and no
        // record should be deleted.
        result = Futures.getUnchecked(dao.deletePendingChannelOperation(
                operation1.getServerId(), operation1.getChannelName(),
                operation2.getOperationId()));
        assertFalse(result.getLeft());
        assertEquals(operation1.getOperationId(), result.getRight());
        assertEquals(2, Iterables.size(Futures.getUnchecked(
                dao.getPendingChannelOperations(operation1.getServerId()))));
        assertEquals(1, Iterables.size(Futures.getUnchecked(
                dao.getPendingChannelOperations(operation3.getServerId()))));
        result = Futures.getUnchecked(dao.deletePendingChannelOperation(
                operation1.getServerId(), operation1.getChannelName(),
                operation1.getOperationId()));
        assertTrue(result.getLeft());
        assertNull(result.getRight());
        assertEquals(1, Iterables.size(Futures.getUnchecked(
                dao.getPendingChannelOperations(operation1.getServerId()))));
        assertEquals(1, Iterables.size(Futures.getUnchecked(
                dao.getPendingChannelOperations(operation3.getServerId()))));
        // Trying to delete a non-existing operation should succeed if there is
        // no other operation for the server and channel.
        result = Futures.getUnchecked(dao.deletePendingChannelOperation(
                operation1.getServerId(), operation1.getChannelName(),
                operation1.getOperationId()));
        assertTrue(result.getLeft());
        assertNull(result.getRight());
        assertEquals(1, Iterables.size(Futures.getUnchecked(
                dao.getPendingChannelOperations(operation1.getServerId()))));
        assertEquals(1, Iterables.size(Futures.getUnchecked(
                dao.getPendingChannelOperations(operation3.getServerId()))));
        result = Futures.getUnchecked(dao.deletePendingChannelOperation(
                operation2.getServerId(), operation2.getChannelName(),
                operation2.getOperationId()));
        assertTrue(result.getLeft());
        assertNull(result.getRight());
        assertEquals(0, Iterables.size(Futures.getUnchecked(
                dao.getPendingChannelOperations(operation1.getServerId()))));
        assertEquals(1, Iterables.size(Futures.getUnchecked(
                dao.getPendingChannelOperations(operation3.getServerId()))));
        result = Futures.getUnchecked(dao.deletePendingChannelOperation(
                operation3.getServerId(), operation3.getChannelName(),
                operation3.getOperationId()));
        assertTrue(result.getLeft());
        assertNull(result.getRight());
        assertEquals(0, Iterables.size(Futures.getUnchecked(
                dao.getPendingChannelOperations(operation1.getServerId()))));
        assertEquals(0, Iterables.size(Futures.getUnchecked(
                dao.getPendingChannelOperations(operation3.getServerId()))));
    }

    /**
     * Tests adding and removing sample buckets.
     */
    @Test
    public void testCreateAndDeleteSampleBuckets() {
        createChannel(channelConfig1);
        ArrayList<SampleBucketInformation> referenceBuckets0 = new ArrayList<SampleBucketInformation>(
                7);
        referenceBuckets0.add(new SampleBucketInformation(136L, 14L,
                channelConfig1.getChannelDataId(),
                channelConfig1.getChannelName(), 0));
        referenceBuckets0.add(new SampleBucketInformation(247L, 137L,
                channelConfig1.getChannelDataId(),
                channelConfig1.getChannelName(), 0));
        referenceBuckets0.add(new SampleBucketInformation(256L, 248L,
                channelConfig1.getChannelDataId(),
                channelConfig1.getChannelName(), 0));
        referenceBuckets0.add(new SampleBucketInformation(257L, 257L,
                channelConfig1.getChannelDataId(),
                channelConfig1.getChannelName(), 0));
        referenceBuckets0.add(new SampleBucketInformation(15999L, 258L,
                channelConfig1.getChannelDataId(),
                channelConfig1.getChannelName(), 0));
        referenceBuckets0.add(new SampleBucketInformation(21999L, 16000L,
                channelConfig1.getChannelDataId(),
                channelConfig1.getChannelName(), 0));
        referenceBuckets0.add(new SampleBucketInformation(Long.MAX_VALUE,
                22000L, channelConfig1.getChannelDataId(),
                channelConfig1.getChannelName(), 0));
        ArrayList<SampleBucketInformation> referenceBuckets5 = new ArrayList<SampleBucketInformation>(
                2);
        referenceBuckets5.add(new SampleBucketInformation(136L, 14L,
                channelConfig1.getChannelDataId(),
                channelConfig1.getChannelName(), 5));
        referenceBuckets5.add(new SampleBucketInformation(Long.MAX_VALUE, 137L,
                channelConfig1.getChannelDataId(),
                channelConfig1.getChannelName(), 5));
        Long precedingBucketStartTime = null;
        for (SampleBucketInformation bucket : referenceBuckets0) {
            // We create each bucket with an end time of Long.MAX_VALUE (this is
            // the same that we do in the real application). The end time is
            // updated when we insert the following bucket.
            Futures.getUnchecked(dao.createSampleBucket(bucket.getChannelName(),
                    bucket.getDecimationLevel(), bucket.getBucketStartTime(),
                    Long.MAX_VALUE, precedingBucketStartTime, true,
                    channelConfig1.getServerId()));
            precedingBucketStartTime = bucket.getBucketStartTime();
        }
        precedingBucketStartTime = null;
        for (SampleBucketInformation bucket : referenceBuckets5) {
            // We create each bucket with an end time of Long.MAX_VALUE (this is
            // the same that we do in the real application). The end time is
            // updated when we insert the following bucket.
            Futures.getUnchecked(dao.createSampleBucket(bucket.getChannelName(),
                    bucket.getDecimationLevel(), bucket.getBucketStartTime(),
                    Long.MAX_VALUE, precedingBucketStartTime, true,
                    channelConfig1.getServerId()));
            precedingBucketStartTime = bucket.getBucketStartTime();
        }
        // Check that the current bucket start time has been updated correctly.
        assertEquals(
                referenceBuckets0.get(6)
                        .getBucketStartTime(),
                (long) Futures
                        .getUnchecked(dao.getChannelByServer(
                                channelConfig1.getServerId(),
                                channelConfig1.getChannelName()))
                        .getDecimationLevelToCurrentBucketStartTime().get(0));
        assertEquals(
                referenceBuckets5.get(1)
                        .getBucketStartTime(),
                (long) Futures
                        .getUnchecked(dao.getChannelByServer(
                                channelConfig1.getServerId(),
                                channelConfig1.getChannelName()))
                        .getDecimationLevelToCurrentBucketStartTime().get(5));
        // Check the various ways to get sample buckets.
        assertEquals(referenceBuckets0, listForSampleBuckets(
                dao.getSampleBuckets(channelConfig1.getChannelName(), 0)));
        assertEquals(referenceBuckets5, listForSampleBuckets(
                dao.getSampleBuckets(channelConfig1.getChannelName(), 5)));
        assertEquals(referenceBuckets0.subList(1, 6),
                listForSampleBuckets(dao.getSampleBucketsInInterval(
                        channelConfig1.getChannelName(), 0, 15, 21999)));
        assertEquals(referenceBuckets0, Lists.newArrayList(
                Futures.getUnchecked(dao.getSampleBucketsInInterval(
                        channelConfig1.getChannelName(), 0, 14, 22000))));
        assertEquals(referenceBuckets0.subList(0, 6),
                listForSampleBuckets(dao.getSampleBucketsInInterval(
                        channelConfig1.getChannelName(), 0, 14, 21999)));
        assertEquals(referenceBuckets0.subList(1, 7),
                listForSampleBuckets(dao.getSampleBucketsInInterval(
                        channelConfig1.getChannelName(), 0, 15, 22000)));
        assertEquals(Lists.reverse(referenceBuckets0.subList(5, 7)),
                listForSampleBuckets(dao.getSampleBucketsInReverseOrder(
                        channelConfig1.getChannelName(), 0, 2)));
        assertEquals(Lists.reverse(referenceBuckets0),
                listForSampleBuckets(dao.getSampleBucketsInReverseOrder(
                        channelConfig1.getChannelName(), 0,
                        Integer.MAX_VALUE)));
        assertEquals(referenceBuckets0.subList(2, 7),
                listForSampleBuckets(dao.getSampleBucketsNewerThan(
                        channelConfig1.getChannelName(), 0, 138,
                        Integer.MAX_VALUE)));
        assertEquals(referenceBuckets0.subList(1, 7),
                listForSampleBuckets(dao.getSampleBucketsNewerThan(
                        channelConfig1.getChannelName(), 0, 137,
                        Integer.MAX_VALUE)));
        assertEquals(Lists.reverse(referenceBuckets0.subList(1, 4)),
                listForSampleBuckets(
                        dao.getSampleBucketsOlderThanInReverseOrder(
                                channelConfig1.getChannelName(), 0, 257, 3)));
        assertEquals(Lists.reverse(referenceBuckets0.subList(0, 5)),
                listForSampleBuckets(
                        dao.getSampleBucketsOlderThanInReverseOrder(
                                channelConfig1.getChannelName(), 0, 258,
                                Integer.MAX_VALUE)));
        // Delete an individual sample bucket.
        Futures.getUnchecked(
                dao.deleteSampleBucket(channelConfig1.getChannelName(), 0,
                        referenceBuckets0.get(6).getBucketStartTime(), true,
                        channelConfig1.getServerId()));
        referenceBuckets0.remove(6);
        assertEquals(-1L, (long) Futures
                .getUnchecked(
                        dao.getChannelByServer(channelConfig1.getServerId(),
                                channelConfig1.getChannelName()))
                .getDecimationLevelToCurrentBucketStartTime().get(0));
        assertEquals(referenceBuckets0, listForSampleBuckets(
                dao.getSampleBuckets(channelConfig1.getChannelName(), 0)));
        Futures.getUnchecked(
                dao.deleteSampleBucket(channelConfig1.getChannelName(), 0,
                        referenceBuckets0.get(3).getBucketStartTime(), true,
                        channelConfig1.getServerId()));
        referenceBuckets0.remove(3);
        assertEquals(referenceBuckets0, listForSampleBuckets(
                dao.getSampleBuckets(channelConfig1.getChannelName(), 0)));
        // Delete a decimation level - this includes all sample buckets.
        Futures.getUnchecked(dao.deleteChannelDecimationLevels(
                channelConfig1.getChannelName(), channelConfig1.getServerId(),
                Collections.singleton(5)));
        assertEquals(referenceBuckets0, listForSampleBuckets(
                dao.getSampleBuckets(channelConfig1.getChannelName(), 0)));
        assertEquals(0,
                listForSampleBuckets(dao
                        .getSampleBuckets(channelConfig1.getChannelName(), 5))
                                .size());
        // Delete the channel.
        Futures.getUnchecked(dao.deleteChannel(channelConfig1.getChannelName(),
                channelConfig1.getServerId()));
        assertEquals(0,
                listForSampleBuckets(dao
                        .getSampleBuckets(channelConfig1.getChannelName(), 0))
                                .size());
    }

    /**
     * Tests moving a channel from one server to another.
     */
    @Test
    public void testMoveChannel() {
        assertEquals(0,
                Iterables.size(Futures.getUnchecked(dao.getChannels())));
        assertEquals(0, Iterables.size(
                Futures.getUnchecked(dao.getChannelsByServer(serverId2))));
        createChannel(channelConfig2);
        assertEquals(channelInfo2, Futures
                .getUnchecked(dao.getChannel(channelInfo2.getChannelName())));
        assertEquals(channelConfig2,
                Futures.getUnchecked(
                        dao.getChannelByServer(channelConfig2.getServerId(),
                                channelConfig2.getChannelName())));
        assertEquals(1,
                Iterables.size(Futures.getUnchecked(dao.getChannels())));
        assertEquals(1, Iterables.size(
                Futures.getUnchecked(dao.getChannelsByServer(serverId2))));
        ChannelConfiguration channelConfig2Moved = new ChannelConfiguration(
                channelConfig2.getChannelDataId(),
                channelConfig2.getChannelName(),
                channelConfig2.getControlSystemType(),
                channelConfig2.getDecimationLevelToCurrentBucketStartTime(),
                channelConfig2.getDecimationLevelToRetentionPeriod(),
                channelConfig2.isEnabled(), channelConfig2.getOptions(),
                serverId3);
        ChannelInformation channelInfo2Moved = new ChannelInformation(
                channelInfo2.getChannelDataId(), channelInfo2.getChannelName(),
                channelInfo2.getControlSystemType(),
                channelInfo2.getDecimationLevels(),
                channelConfig2Moved.getServerId());
        assertNull(Futures.getUnchecked(
                dao.getChannelByServer(channelConfig2Moved.getServerId(),
                        channelConfig2Moved.getChannelName())));
        Futures.getUnchecked(dao.moveChannel(channelConfig2.getChannelName(),
                channelConfig2.getServerId(),
                channelConfig2Moved.getServerId()));
        assertEquals(channelInfo2Moved, Futures.getUnchecked(
                dao.getChannel(channelInfo2Moved.getChannelName())));
        assertEquals(channelConfig2Moved,
                Futures.getUnchecked(dao.getChannelByServer(
                        channelConfig2Moved.getServerId(),
                        channelConfig2Moved.getChannelName())));
        assertNull(Futures.getUnchecked(
                dao.getChannelByServer(channelConfig2.getServerId(),
                        channelConfig2.getChannelName())));
        assertEquals(1,
                Iterables.size(Futures.getUnchecked(dao.getChannels())));
        assertEquals(0, Iterables.size(
                Futures.getUnchecked(dao.getChannelsByServer(serverId2))));
        assertEquals(1, Iterables.size(
                Futures.getUnchecked(dao.getChannelsByServer(serverId3))));
        Futures.getUnchecked(
                dao.deleteChannel(channelConfig2Moved.getChannelName(),
                        channelConfig2Moved.getServerId()));
        assertNull(Futures.getUnchecked(
                dao.getChannel(channelInfo2Moved.getChannelName())));
        assertNull(Futures.getUnchecked(
                dao.getChannelByServer(channelConfig2Moved.getServerId(),
                        channelConfig2Moved.getChannelName())));
        assertEquals(0,
                Iterables.size(Futures.getUnchecked(dao.getChannels())));
        assertEquals(0, Iterables.size(
                Futures.getUnchecked(dao.getChannelsByServer(serverId3))));
    }

    /**
     * Tests the automatic removal of pending channel operations when the TTL
     * has passed.
     */
    @Test
    public void testPendingChannelOperationsTtl() {
        ChannelOperation operation = new ChannelOperation("test1", "someData",
                UUID.randomUUID(), "someType", UUID.randomUUID());
        Pair<Boolean, UUID> result;
        assertEquals(0, Iterables.size(Futures.getUnchecked(
                dao.getPendingChannelOperations(operation.getServerId()))));
        assertNull(Futures.getUnchecked(dao.getPendingChannelOperation(
                operation.getServerId(), operation.getChannelName())));
        // We create the operation with a TTL of only two seconds. This way, we
        // do not have to wait long until it disappears.
        result = Futures.getUnchecked(dao.createPendingChannelOperationRelaxed(
                operation.getServerId(), operation.getChannelName(),
                operation.getOperationId(), operation.getOperationType(),
                operation.getOperationData(), 2));
        assertTrue(result.getLeft());
        assertNull(result.getRight());
        sleepTtl(2);
        // The record that we just created should have vanished now.
        assertEquals(0, Iterables.size(Futures.getUnchecked(
                dao.getPendingChannelOperations(operation.getServerId()))));
        assertNull(Futures.getUnchecked(dao.getPendingChannelOperation(
                operation.getServerId(), operation.getChannelName())));
        // We create the record again with a longer TTL.
        result = Futures.getUnchecked(dao.createPendingChannelOperationRelaxed(
                operation.getServerId(), operation.getChannelName(),
                operation.getOperationId(), operation.getOperationType(),
                operation.getOperationData(), 60));
        assertTrue(result.getLeft());
        assertNull(result.getRight());
        sleepTtl(2);
        // The record that we just created should still exist.
        assertEquals(1, Iterables.size(Futures.getUnchecked(
                dao.getPendingChannelOperations(operation.getServerId()))));
        assertEquals(operation,
                Futures.getUnchecked(dao.getPendingChannelOperation(
                        operation.getServerId(), operation.getChannelName())));
        // We update the record with the same data, but a short TTL. It should
        // then vanish quickly.
        result = Futures.getUnchecked(dao.updatePendingChannelOperationRelaxed(
                operation.getServerId(), operation.getChannelName(),
                operation.getOperationId(), operation.getOperationId(),
                operation.getOperationType(), operation.getOperationData(), 2));
        assertTrue(result.getLeft());
        assertNull(result.getRight());
        sleepTtl(2);
        // The record that we just updated should have vanished now.
        assertEquals(0, Iterables.size(Futures.getUnchecked(
                dao.getPendingChannelOperations(operation.getServerId()))));
        assertNull(Futures.getUnchecked(dao.getPendingChannelOperation(
                operation.getServerId(), operation.getChannelName())));
        // Finally, we test the other way round: Creating a record with a short
        // TTL and then updating it with a long one. We use a slightly longer
        // "short" TTL, so that we can be sure to make the update in time.
        result = Futures.getUnchecked(dao.createPendingChannelOperationRelaxed(
                operation.getServerId(), operation.getChannelName(),
                operation.getOperationId(), operation.getOperationType(),
                operation.getOperationData(), 3));
        assertTrue(result.getLeft());
        assertNull(result.getRight());
        result = Futures.getUnchecked(dao.updatePendingChannelOperationRelaxed(
                operation.getServerId(), operation.getChannelName(),
                operation.getOperationId(), operation.getOperationId(),
                operation.getOperationType(), operation.getOperationData(),
                60));
        assertTrue(result.getLeft());
        assertNull(result.getRight());
        sleepTtl(3);
        // The record that we just updated should still exist.
        assertEquals(1, Iterables.size(Futures.getUnchecked(
                dao.getPendingChannelOperations(operation.getServerId()))));
        assertEquals(operation,
                Futures.getUnchecked(dao.getPendingChannelOperation(
                        operation.getServerId(), operation.getChannelName())));
        // We delete the record to leave a clean table.
        result = Futures.getUnchecked(dao.deletePendingChannelOperation(
                operation.getServerId(), operation.getChannelName(),
                operation.getOperationId()));
        assertTrue(result.getLeft());
        assertNull(result.getRight());
        assertEquals(0, Iterables.size(Futures.getUnchecked(
                dao.getPendingChannelOperations(operation.getServerId()))));
        assertNull(Futures.getUnchecked(dao.getPendingChannelOperation(
                operation.getServerId(), operation.getChannelName())));
    }

    /**
     * Tests renaming a channel.
     */
    @Test
    public void testRenameChannel() {
        assertEquals(0,
                Iterables.size(Futures.getUnchecked(dao.getChannels())));
        assertEquals(0, Iterables.size(
                Futures.getUnchecked(dao.getChannelsByServer(serverId1))));
        createChannel(channelConfig1);
        // Add a few sample buckets in order to test that they are moved with
        // the channel configuration.
        long bucketStartTime1 = 17L;
        long bucketStartTime2 = 22L;
        long bucketStartTime3 = 43L;
        long bucketStartTime4 = 123L;
        Futures.getUnchecked(dao.createSampleBucket(
                channelConfig1.getChannelName(), 5, bucketStartTime3,
                Long.MAX_VALUE, null, false, channelConfig1.getServerId()));
        Futures.getUnchecked(dao.createSampleBucket(
                channelConfig1.getChannelName(), 5, bucketStartTime2, 42L, null,
                false, channelConfig1.getServerId()));
        Futures.getUnchecked(dao.createSampleBucket(
                channelConfig1.getChannelName(), 5, bucketStartTime4,
                Long.MAX_VALUE, 43L, false, channelConfig1.getServerId()));
        Futures.getUnchecked(dao.createSampleBucket(
                channelConfig1.getChannelName(), 0, bucketStartTime1,
                Long.MAX_VALUE, null, false, channelConfig1.getServerId()));
        assertEquals(channelInfo1, Futures
                .getUnchecked(dao.getChannel(channelInfo1.getChannelName())));
        assertEquals(channelConfig1,
                Futures.getUnchecked(
                        dao.getChannelByServer(channelConfig1.getServerId(),
                                channelConfig1.getChannelName())));
        assertEquals(1,
                Iterables.size(Futures.getUnchecked(dao.getChannels())));
        assertEquals(1, Iterables.size(
                Futures.getUnchecked(dao.getChannelsByServer(serverId1))));
        List<SampleBucketInformation> sampleBuckets0 = listForSampleBuckets(
                dao.getSampleBuckets(channelConfig1.getChannelName(), 0));
        List<SampleBucketInformation> sampleBuckets5 = listForSampleBuckets(
                dao.getSampleBuckets(channelConfig1.getChannelName(), 5));
        assertEquals(1, sampleBuckets0.size());
        assertEquals(bucketStartTime1,
                sampleBuckets0.get(0).getBucketStartTime());
        assertEquals(3, sampleBuckets5.size());
        assertEquals(bucketStartTime2,
                sampleBuckets5.get(0).getBucketStartTime());
        assertEquals(bucketStartTime3,
                sampleBuckets5.get(1).getBucketStartTime());
        assertEquals(bucketStartTime4,
                sampleBuckets5.get(2).getBucketStartTime());
        ChannelConfiguration channelConfig1Renamed = new ChannelConfiguration(
                channelConfig1.getChannelDataId(), "renamed_test1",
                channelConfig1.getControlSystemType(),
                channelConfig1.getDecimationLevelToCurrentBucketStartTime(),
                channelConfig1.getDecimationLevelToRetentionPeriod(),
                channelConfig1.isEnabled(), channelConfig1.getOptions(),
                channelConfig1.getServerId());
        ChannelInformation channelInfo1Renamed = new ChannelInformation(
                channelInfo1.getChannelDataId(),
                channelConfig1Renamed.getChannelName(),
                channelInfo1.getControlSystemType(),
                channelInfo1.getDecimationLevels(), channelInfo1.getServerId());
        assertNull(Futures.getUnchecked(
                dao.getChannel(channelInfo1Renamed.getChannelName())));
        assertNull(Futures.getUnchecked(
                dao.getChannelByServer(channelConfig1Renamed.getServerId(),
                        channelConfig1Renamed.getChannelName())));
        Futures.getUnchecked(dao.renameChannel(channelConfig1.getChannelName(),
                channelConfig1Renamed.getChannelName(),
                channelConfig1.getServerId()));
        assertEquals(channelInfo1Renamed, Futures.getUnchecked(
                dao.getChannel(channelInfo1Renamed.getChannelName())));
        assertEquals(channelConfig1Renamed,
                Futures.getUnchecked(dao.getChannelByServer(
                        channelConfig1Renamed.getServerId(),
                        channelConfig1Renamed.getChannelName())));
        assertNull(Futures
                .getUnchecked(dao.getChannel(channelInfo1.getChannelName())));
        assertNull(Futures.getUnchecked(
                dao.getChannelByServer(channelConfig1.getServerId(),
                        channelConfig1.getChannelName())));
        assertEquals(1,
                Iterables.size(Futures.getUnchecked(dao.getChannels())));
        assertEquals(1, Iterables.size(
                Futures.getUnchecked(dao.getChannelsByServer(serverId1))));
        assertEquals(0, Iterables.size(Futures.getUnchecked(
                dao.getSampleBuckets(channelConfig1.getChannelName(), 0))));
        assertEquals(0, Iterables.size(Futures.getUnchecked(
                dao.getSampleBuckets(channelConfig1.getChannelName(), 5))));
        sampleBuckets0 = Lists.newArrayList(Futures.getUnchecked(dao
                .getSampleBuckets(channelConfig1Renamed.getChannelName(), 0)));
        sampleBuckets5 = Lists.newArrayList(Futures.getUnchecked(dao
                .getSampleBuckets(channelConfig1Renamed.getChannelName(), 5)));
        assertEquals(1, sampleBuckets0.size());
        assertEquals(bucketStartTime1,
                sampleBuckets0.get(0).getBucketStartTime());
        assertEquals(3, sampleBuckets5.size());
        assertEquals(bucketStartTime2,
                sampleBuckets5.get(0).getBucketStartTime());
        assertEquals(bucketStartTime3,
                sampleBuckets5.get(1).getBucketStartTime());
        assertEquals(bucketStartTime4,
                sampleBuckets5.get(2).getBucketStartTime());
        Futures.getUnchecked(
                dao.deleteChannel(channelConfig1Renamed.getChannelName(),
                        channelConfig1Renamed.getServerId()));
        assertNull(Futures.getUnchecked(
                dao.getChannel(channelInfo1Renamed.getChannelName())));
        assertNull(Futures.getUnchecked(
                dao.getChannelByServer(channelConfig1Renamed.getServerId(),
                        channelConfig1Renamed.getChannelName())));
        assertEquals(0,
                Iterables.size(Futures.getUnchecked(dao.getChannels())));
        assertEquals(0, Iterables.size(
                Futures.getUnchecked(dao.getChannelsByServer(serverId1))));
    }

    /**
     * Tests updating a channel configuration.
     */
    @Test
    public void testUpdateChannel() {
        createChannel(channelConfig1);
        Map<String, String> modifiedOptions = new HashMap<String, String>();
        modifiedOptions.put("abc123", "def");
        modifiedOptions.put("xyz", "");
        modifiedOptions.put(" ", "!");
        Map<Integer, Integer> modifiedDecimationLevelToRetentionPeriod = new HashMap<Integer, Integer>(
                channelConfig1.getDecimationLevelToRetentionPeriod());
        modifiedDecimationLevelToRetentionPeriod.put(0, 172800);
        ChannelConfiguration modifiedChannelConfig1 = new ChannelConfiguration(
                channelConfig1.getChannelDataId(),
                channelConfig1.getChannelName(),
                channelConfig1.getControlSystemType(),
                channelConfig1.getDecimationLevelToCurrentBucketStartTime(),
                modifiedDecimationLevelToRetentionPeriod,
                !channelConfig1.isEnabled(), modifiedOptions,
                channelConfig1.getServerId());
        Futures.getUnchecked(dao.updateChannelConfiguration(
                modifiedChannelConfig1.getChannelName(),
                modifiedChannelConfig1.getServerId(),
                Collections.singletonMap(0, 172800),
                modifiedChannelConfig1.isEnabled(),
                modifiedChannelConfig1.getOptions()));
        assertEquals(modifiedChannelConfig1,
                Futures.getUnchecked(
                        dao.getChannelByServer(channelConfig1.getServerId(),
                                channelConfig1.getChannelName())));
        Futures.getUnchecked(dao.deleteChannel(channelConfig1.getChannelName(),
                channelConfig1.getServerId()));
    }

    /**
     * Tests updating a pending channel operation.
     */
    @Test
    public void testUpdatePendingChannelOperations() {
        ChannelOperation operation1 = new ChannelOperation("test1", "someData",
                UUID.randomUUID(), "someType", UUID.randomUUID());
        ChannelOperation operation2 = new ChannelOperation("test1", null,
                operation1.getOperationId(), "otherType",
                operation1.getServerId());
        ChannelOperation operation3 = new ChannelOperation("test1", "otherData",
                UUID.randomUUID(), "someType", operation1.getServerId());
        // A TTL of 600 seconds is long enough so that no entry should vanish
        // while running the test.
        int ttl = 600;
        Pair<Boolean, UUID> result;
        assertNull(Futures.getUnchecked(dao.getPendingChannelOperation(
                operation1.getServerId(), operation1.getChannelName())));
        // Updating an operation that does not exist should fail.
        result = Futures.getUnchecked(dao.updatePendingChannelOperationRelaxed(
                operation1.getServerId(), operation1.getChannelName(),
                operation1.getOperationId(), operation1.getOperationId(),
                operation1.getOperationType(), operation1.getOperationData(),
                ttl));
        assertFalse(result.getLeft());
        assertNull(result.getRight());
        assertNull(Futures.getUnchecked(dao.getPendingChannelOperation(
                operation1.getServerId(), operation1.getChannelName())));
        // Now we create the operation.
        result = Futures.getUnchecked(dao.createPendingChannelOperationRelaxed(
                operation1.getServerId(), operation1.getChannelName(),
                operation1.getOperationId(), operation1.getOperationType(),
                operation1.getOperationData(), ttl));
        assertTrue(result.getLeft());
        assertNull(result.getRight());
        assertEquals(operation1,
                Futures.getUnchecked(
                        dao.getPendingChannelOperation(operation1.getServerId(),
                                operation1.getChannelName())));
        // Updating the operation with the same ID and data should work.
        result = Futures.getUnchecked(dao.updatePendingChannelOperationRelaxed(
                operation1.getServerId(), operation1.getChannelName(),
                operation1.getOperationId(), operation1.getOperationId(),
                operation1.getOperationType(), operation1.getOperationData(),
                ttl));
        assertTrue(result.getLeft());
        assertNull(result.getRight());
        assertEquals(operation1,
                Futures.getUnchecked(
                        dao.getPendingChannelOperation(operation1.getServerId(),
                                operation1.getChannelName())));
        // Updating the operation with the same ID but different data should
        // work.
        result = Futures.getUnchecked(dao.updatePendingChannelOperationRelaxed(
                operation2.getServerId(), operation2.getChannelName(),
                operation1.getOperationId(), operation2.getOperationId(),
                operation2.getOperationType(), operation2.getOperationData(),
                ttl));
        assertTrue(result.getLeft());
        assertNull(result.getRight());
        assertEquals(operation2,
                Futures.getUnchecked(
                        dao.getPendingChannelOperation(operation2.getServerId(),
                                operation2.getChannelName())));
        // Updating the operation with a different ID and data should work as
        // well.
        result = Futures.getUnchecked(dao.updatePendingChannelOperationRelaxed(
                operation3.getServerId(), operation3.getChannelName(),
                operation2.getOperationId(), operation3.getOperationId(),
                operation3.getOperationType(), operation3.getOperationData(),
                ttl));
        assertTrue(result.getLeft());
        assertNull(result.getRight());
        assertEquals(operation3,
                Futures.getUnchecked(
                        dao.getPendingChannelOperation(operation3.getServerId(),
                                operation3.getChannelName())));
        // Updating the operation should fail if the wrong ID is specified for
        // the existing operation.
        result = Futures.getUnchecked(dao.updatePendingChannelOperationRelaxed(
                operation2.getServerId(), operation2.getChannelName(),
                operation1.getOperationId(), operation2.getOperationId(),
                operation2.getOperationType(), operation2.getOperationData(),
                ttl));
        assertFalse(result.getLeft());
        assertEquals(operation3.getOperationId(), result.getRight());
        assertEquals(operation3,
                Futures.getUnchecked(
                        dao.getPendingChannelOperation(operation3.getServerId(),
                                operation3.getChannelName())));
        // In strict mode, an update that specifies the existing data as the
        // new data should fail, if the old ID does not match the existing ID.
        result = Futures.getUnchecked(dao.updatePendingChannelOperation(
                operation3.getServerId(), operation3.getChannelName(),
                operation2.getOperationId(), operation3.getOperationId(),
                operation3.getOperationType(), operation3.getOperationData(),
                ttl));
        assertFalse(result.getLeft());
        assertEquals(operation3.getOperationId(), result.getRight());
        // In relaxed mode, the update should succeed if the new data matches
        // the existing data, but it should fail if any item is different.
        result = Futures.getUnchecked(dao.updatePendingChannelOperationRelaxed(
                operation3.getServerId(), operation3.getChannelName(),
                UUID.randomUUID(), operation3.getOperationId(),
                operation3.getOperationType(), operation3.getOperationData(),
                ttl));
        assertTrue(result.getLeft());
        assertNull(result.getRight());
        result = Futures.getUnchecked(dao.updatePendingChannelOperationRelaxed(
                operation3.getServerId(), operation3.getChannelName(),
                UUID.randomUUID(), UUID.randomUUID(),
                operation3.getOperationType(), operation3.getOperationData(),
                ttl));
        assertFalse(result.getLeft());
        assertEquals(operation3.getOperationId(), result.getRight());
        result = Futures.getUnchecked(dao.updatePendingChannelOperationRelaxed(
                operation3.getServerId(), operation3.getChannelName(),
                UUID.randomUUID(), operation3.getOperationId(), "someOtherType",
                operation3.getOperationData(), ttl));
        assertFalse(result.getLeft());
        assertEquals(operation3.getOperationId(), result.getRight());
        result = Futures.getUnchecked(dao.updatePendingChannelOperationRelaxed(
                operation3.getServerId(), operation3.getChannelName(),
                UUID.randomUUID(), operation3.getOperationId(),
                operation3.getOperationType(), "someOtherData", ttl));
        assertFalse(result.getLeft());
        assertEquals(operation3.getOperationId(), result.getRight());
        // Finally, we delete the operation in order to keep the table clean.
        result = Futures.getUnchecked(dao.deletePendingChannelOperation(
                operation3.getServerId(), operation3.getChannelName(),
                operation3.getOperationId()));
        assertTrue(result.getLeft());
        assertNull(result.getRight());
        assertNull(Futures.getUnchecked(dao.getPendingChannelOperation(
                operation3.getServerId(), operation3.getChannelName())));
    }

    /**
     * Tests that the various <code>getSampleBuckets</code> methods return an
     * empty iterable when no sample buckets have been created yet. This test is
     * specifically targeted to detect a bug where the list of sample buckets
     * would return an entry with a <code>null</code> bucket ID because a CQL
     * row that only contains data from the static columns would be used.
     */
    @Test
    public void testZeroSampleBuckets() {
        createChannel(channelConfig1);
        // Check that the current bucket ID is null.
        ChannelConfiguration channelConfig1FromDb = Futures.getUnchecked(
                dao.getChannelByServer(channelConfig1.getServerId(),
                        channelConfig1.getChannelName()));
        assertEquals(-1L, (long) channelConfig1FromDb
                .getDecimationLevelToCurrentBucketStartTime().get(0));
        // Check that the various versions of the getSampleBuckets(...) methods
        // return an empty iterator.
        assertTrue(Iterables.isEmpty(Futures.getUnchecked(
                dao.getSampleBuckets(channelConfig1.getChannelName()))));
        assertTrue(Iterables.isEmpty(Futures.getUnchecked(
                dao.getSampleBuckets(channelConfig1.getChannelName(), 0))));
        assertTrue(Iterables.isEmpty(Futures.getUnchecked(
                dao.getSampleBucketsInInterval(channelConfig1.getChannelName(),
                        0, Long.MIN_VALUE, Long.MAX_VALUE))));
        assertTrue(Iterables.isEmpty(
                Futures.getUnchecked(dao.getSampleBucketsInReverseOrder(
                        channelConfig1.getChannelName(), 0, 1))));
        assertTrue(Iterables.isEmpty(Futures.getUnchecked(
                dao.getSampleBucketsNewerThan(channelConfig1.getChannelName(),
                        0, Long.MIN_VALUE, 1))));
        assertTrue(Iterables.isEmpty(Futures
                .getUnchecked(dao.getSampleBucketsOlderThanInReverseOrder(
                        channelConfig1.getChannelName(), 0, Long.MAX_VALUE,
                        1))));
        Futures.getUnchecked(dao.deleteChannel(channelConfig1.getChannelName(),
                channelConfig1.getServerId()));
    }

}
