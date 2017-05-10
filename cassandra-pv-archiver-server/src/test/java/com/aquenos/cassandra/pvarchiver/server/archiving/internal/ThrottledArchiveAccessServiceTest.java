/*
 * Copyright 2017 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.archiving.internal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import com.aquenos.cassandra.pvarchiver.controlsystem.ControlSystemSupport;
import com.aquenos.cassandra.pvarchiver.controlsystem.Sample;
import com.aquenos.cassandra.pvarchiver.server.archiving.ArchiveAccessService;
import com.aquenos.cassandra.pvarchiver.server.archiving.ArchiveAccessServiceImpl;
import com.aquenos.cassandra.pvarchiver.server.archiving.TimeStampLimitMode;
import com.aquenos.cassandra.pvarchiver.server.archiving.internal.ThrottledArchiveAccessService.SkippableObjectResultSet;
import com.aquenos.cassandra.pvarchiver.server.archiving.test.ArchiveAccessServiceImplBuilder;
import com.aquenos.cassandra.pvarchiver.server.archiving.test.ArchiveAccessServiceImplBuilder.DecimationLevelBuilder;
import com.aquenos.cassandra.pvarchiver.server.archiving.test.ArchiveAccessServiceImplBuilder.SampleBucketBuilder;
import com.aquenos.cassandra.pvarchiver.server.database.ChannelMetaDataDAO.ChannelConfiguration;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * Tests for the {@link ThrottledArchiveAccessService}.
 * 
 * @author Sebastian Marsching
 */
public class ThrottledArchiveAccessServiceTest {

    private final ArchiveAccessService archiveAccessService;
    private final ChannelConfiguration channelConfig;
    private final String channelName = "myTest";
    private final ControlSystemSupport<Sample> controlSystemSupport;
    private final int totalNumberOfSamples;

    /**
     * Creates the test suite. This initializes the components that are
     * internally needed for running the tests.
     */
    public ThrottledArchiveAccessServiceTest() {
        ArchiveAccessServiceImplBuilder aasb = new ArchiveAccessServiceImplBuilder()
                .channelName(channelName);
        DecimationLevelBuilder dlb = aasb.decimationLevel(0);
        // We use separate sample buckets because this will effectively create
        // different pages in the result set.
        SampleBucketBuilder sbb;
        int totalNumberOfSamples = 0;
        sbb = dlb.sampleBucket(0L, 123456L);
        for (int i = 0; i < 1999; ++i) {
            sbb.sample(i + 123);
            ++totalNumberOfSamples;
        }
        sbb = dlb.sampleBucket(123457L, 999999L);
        for (int i = 0; i < 150; ++i) {
            sbb.sample(i + 123578L);
            ++totalNumberOfSamples;
        }
        sbb = dlb.sampleBucket(1000000L, Long.MAX_VALUE);
        for (int i = 0; i < 150; ++i) {
            sbb.sample(i + 1234567L);
            ++totalNumberOfSamples;
        }
        this.totalNumberOfSamples = totalNumberOfSamples;
        Pair<ArchiveAccessServiceImpl, ControlSystemSupport<Sample>> aasAndCss = aasb
                .buildWithControlSystemSupport();
        archiveAccessService = aasAndCss.getLeft();
        controlSystemSupport = aasAndCss.getRight();
        aasAndCss.getRight();
        channelConfig = new ChannelConfiguration(UUID.randomUUID(), channelName,
                controlSystemSupport.getId(), ImmutableMap.of(0, 1000000L),
                ImmutableMap.of(0, 0), true,
                Collections.<String, String> emptyMap(), UUID.randomUUID());
    }

    /**
     * Tests getting samples from the throttled archive access service. In
     * particular, this test verifies that the result set returned by the
     * service contains the right number of samples.
     * 
     * @throws Exception
     *             if there is an error.
     */
    @Test
    public void testGetSamples() throws Exception {
        ThreadPoolExecutor poolExecutor = new ThreadPoolExecutor(0, 2, 1000L,
                TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
        ThrottledArchiveAccessService throttledArchiveAccessService = new ThrottledArchiveAccessService(
                archiveAccessService, poolExecutor, 2, 5000);
        ListenableFuture<SkippableObjectResultSet<Sample>> samplesResultSetFuture = getAllSamples(
                throttledArchiveAccessService);
        // We have to wait a moment for the future to complete.
        SkippableObjectResultSet<Sample> samplesResultSet = samplesResultSetFuture
                .get(1000L, TimeUnit.MILLISECONDS);
        // We want to test the one(), getAvailableWithoutFetching() and
        // isExhausted() methods of the result set.
        for (int i = 0; i < totalNumberOfSamples; ++i) {
            // The throttled archive access service should keep track of the
            // number of samples that are kept in memory.
            assertEquals(samplesResultSet.getAvailableWithoutFetching(),
                    throttledArchiveAccessService.getCurrentSamplesInMemory());
            assertFalse(samplesResultSet.isExhausted());
            assertNotNull(samplesResultSet.one());
        }
        // After reading all samples, the result set should be exhausted.
        assertEquals(0, samplesResultSet.getAvailableWithoutFetching());
        assertTrue(samplesResultSet.isExhausted());
        assertTrue(samplesResultSet.isFullyFetched());
        // Calling isExhausted() might fetch more samples, so we test again.
        assertEquals(0, samplesResultSet.getAvailableWithoutFetching());
        assertEquals(0,
                throttledArchiveAccessService.getCurrentSamplesInMemory());
        assertEquals(0, throttledArchiveAccessService
                .getCurrentRunningFetchOperations());
        // We also want to test the all() method because it uses a different
        // implementation. First, we try on the now empty result set, then on a
        // fresh one.
        assertEquals(0, samplesResultSet.all().size());
        assertEquals(0,
                throttledArchiveAccessService.getCurrentSamplesInMemory());
        assertEquals(0, throttledArchiveAccessService
                .getCurrentRunningFetchOperations());
        samplesResultSetFuture = getAllSamples(throttledArchiveAccessService);
        samplesResultSet = samplesResultSetFuture.get(1000L,
                TimeUnit.MILLISECONDS);
        assertEquals(totalNumberOfSamples, samplesResultSet.all().size());
        assertEquals(0,
                throttledArchiveAccessService.getCurrentSamplesInMemory());
        assertEquals(0, throttledArchiveAccessService
                .getCurrentRunningFetchOperations());
        // Finally, we test the iterator.
        samplesResultSetFuture = getAllSamples(throttledArchiveAccessService);
        samplesResultSet = samplesResultSetFuture.get(1000L,
                TimeUnit.MILLISECONDS);
        assertEquals(totalNumberOfSamples, Iterables.size(samplesResultSet));
    }

    private ListenableFuture<SkippableObjectResultSet<Sample>> getAllSamples(
            ThrottledArchiveAccessService throttledArchiveAccessService) {
        return throttledArchiveAccessService.getSamples(channelConfig, 0, 0L,
                TimeStampLimitMode.AT_OR_AFTER, Long.MAX_VALUE,
                TimeStampLimitMode.AT_OR_BEFORE, controlSystemSupport);
    }

}
