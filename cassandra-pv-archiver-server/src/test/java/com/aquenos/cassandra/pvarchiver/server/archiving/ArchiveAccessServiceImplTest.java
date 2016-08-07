/*
 * Copyright 2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.archiving;

import static org.junit.Assert.*;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;

import com.aquenos.cassandra.pvarchiver.controlsystem.Sample;
import com.aquenos.cassandra.pvarchiver.server.archiving.test.ArchiveAccessServiceImplBuilder;
import com.aquenos.cassandra.pvarchiver.server.util.FutureUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * Tests for the {@link ArchiveAccessServiceImpl}.
 * 
 * @author Sebastian Marsching
 */
public class ArchiveAccessServiceImplTest {

    private static void assertTimeStampsMatch(
            Iterable<Long> expectedTimeStamps,
            Iterable<? extends Sample> actualSamples) {
        Iterator<Long> timeStampIterator = expectedTimeStamps.iterator();
        Iterator<? extends Sample> sampleIterator = actualSamples.iterator();
        while (timeStampIterator.hasNext() && sampleIterator.hasNext()) {
            long timeStamp = timeStampIterator.next();
            Sample sample = sampleIterator.next();
            assertEquals(timeStamp, sample.getTimeStamp());
        }
        assertEquals(
                "The number of samples does not match the number of time stamps.",
                timeStampIterator.hasNext(), sampleIterator.hasNext());
    }

    private static void assertTimeStampsMatch(
            Iterable<Long> expectedTimeStamps,
            ListenableFuture<? extends Iterable<? extends Sample>> actualSamples) {
        assertTimeStampsMatch(expectedTimeStamps,
                FutureUtils.getUnchecked(actualSamples));
    }

    private static void testGetSamplesAllLimitModes(
            ArchiveAccessServiceImpl aas, String channelName,
            int decimationLevel, long start, long end,
            Iterable<Long> expectedSampleTimeStamps) {
        assertTimeStampsMatch(expectedSampleTimeStamps, aas.getSamples(
                channelName, decimationLevel, start,
                TimeStampLimitMode.AT_OR_BEFORE, end,
                TimeStampLimitMode.AT_OR_AFTER));
        assertTimeStampsMatch(expectedSampleTimeStamps, aas.getSamples(
                channelName, decimationLevel, start,
                TimeStampLimitMode.AT_OR_AFTER, end,
                TimeStampLimitMode.AT_OR_BEFORE));
        assertTimeStampsMatch(expectedSampleTimeStamps, aas.getSamples(
                channelName, decimationLevel, start,
                TimeStampLimitMode.AT_OR_AFTER, end,
                TimeStampLimitMode.AT_OR_AFTER));
        assertTimeStampsMatch(expectedSampleTimeStamps, aas.getSamples(
                channelName, decimationLevel, start,
                TimeStampLimitMode.AT_OR_BEFORE, end,
                TimeStampLimitMode.AT_OR_BEFORE));
    }

    /**
     * Tests retrieving samples when there is only a single sample bucket.
     */
    @Test
    public void testSingleSampleBucket() {
        // First, we run the tests using raw samples.
        // Run test with samples in a bucket using regular limits and limits at
        // the edge of the possible range.
        testSingleSampleBucket(0, 137L, 789L,
                ImmutableList.of(145L, 227L, 475L, 512L, 600L));
        testSingleSampleBucket(0, 0L, 789L,
                ImmutableList.of(145L, 227L, 475L, 512L, 600L));
        testSingleSampleBucket(0, 137L, Long.MAX_VALUE,
                ImmutableList.of(145L, 227L, 475L, 512L, 600L));
        testSingleSampleBucket(0, 0L, Long.MAX_VALUE,
                ImmutableList.of(145L, 227L, 475L, 512L, 600L));
        // Run test with samples right at the edge of the start or end of the
        // bucket.
        testSingleSampleBucket(0, 137L, 789L,
                ImmutableList.of(137L, 145L, 227L, 475L, 512L, 600L));
        testSingleSampleBucket(0, 137L, 789L,
                ImmutableList.of(145L, 227L, 475L, 512L, 600L, 789L));
        testSingleSampleBucket(0, 137L, 789L,
                ImmutableList.of(137L, 145L, 227L, 475L, 512L, 600L, 789L));
        // Run the test with samples at the edge of the possible range.
        testSingleSampleBucket(0, 0L, Long.MAX_VALUE,
                ImmutableList.of(0L, 145L, 227L, 475L, 512L, 600L));
        testSingleSampleBucket(0, 0L, Long.MAX_VALUE,
                ImmutableList.of(145L, 227L, 475L, 512L, 600L, Long.MAX_VALUE));
        testSingleSampleBucket(0, 0L, Long.MAX_VALUE, ImmutableList.of(0L,
                145L, 227L, 475L, 512L, 600L, Long.MAX_VALUE));
        // We repeat the tests for decimated samples. Basically, this should not
        // make much of a difference unless some optimizations that take
        // advantage of the special spacing are implemented in the code reading
        // samples.
        // Run test with samples in a bucket using regular limits and limits at
        // the edge of the possible range.
        testSingleSampleBucket(30, 17L, 257L,
                ImmutableList.of(30L, 60L, 90L, 150L, 180L, 210L));
        testSingleSampleBucket(30, 0L, 257L,
                ImmutableList.of(30L, 60L, 90L, 150L, 180L, 210L));
        testSingleSampleBucket(30, 17L, Long.MAX_VALUE,
                ImmutableList.of(30L, 60L, 90L, 150L, 180L, 210L));
        testSingleSampleBucket(30, 0L, Long.MAX_VALUE,
                ImmutableList.of(30L, 60L, 90L, 150L, 180L, 210L));
        // Run test with samples right at the edge of the start or end of the
        // bucket.
        testSingleSampleBucket(30, 30L, 257L,
                ImmutableList.of(30L, 60L, 90L, 150L, 180L, 210L));
        testSingleSampleBucket(30, 17L, 210L,
                ImmutableList.of(30L, 60L, 90L, 150L, 180L, 210L));
        testSingleSampleBucket(30, 30L, 210L,
                ImmutableList.of(30L, 60L, 90L, 150L, 180L, 210L));
        // Run the test with samples at the edge of the possible range.
        testSingleSampleBucket(30, 0L, Long.MAX_VALUE,
                ImmutableList.of(0L, 30L, 60L, 90L, 150L, 180L, 210L));
        testSingleSampleBucket(30, 0L, Long.MAX_VALUE, ImmutableList.of(30L,
                60L, 90L, 150L, 180L, 210L, 9223372036854775800L));
        testSingleSampleBucket(30, 0L, Long.MAX_VALUE, ImmutableList.of(0L,
                30L, 60L, 90L, 150L, 180L, 210L, 9223372036854775800L));
    }

    /**
     * Tests retrieving raw samples when there are multiple sample buckets.
     */
    @Test
    public void testMultipleSampleBucketsRawSamples() {
        // For this test, we use samples spread across many sample buckets. We
        // also use some empty buckets. In particular, we have an empty one at
        // the start and at the end because this might trigger some special code
        // paths.
        List<Long> sampleTimeStamps1 = Collections.emptyList();
        List<Long> sampleTimeStamps2 = ImmutableList.of(17L, 255L, 378L);
        List<Long> sampleTimeStamps3 = ImmutableList.of(379L, 381L, 411L, 412L);
        List<Long> sampleTimeStamps4 = Collections.emptyList();
        List<Long> sampleTimeStamps5 = ImmutableList.of(477L);
        List<Long> sampleTimeStamps6 = ImmutableList.of(506L, 517L);
        List<Long> sampleTimeStamps7 = Collections.emptyList();
        List<Long> sampleTimeStamps8 = Collections.emptyList();
        List<Long> sampleTimeStamps9 = ImmutableList.of(603L, 799L);
        List<Long> sampleTimeStamps10 = Collections.emptyList();
        @SuppressWarnings("unchecked")
        List<Long> allSampleTimeStamps = ImmutableList.copyOf(Iterables.concat(
                sampleTimeStamps1, sampleTimeStamps2, sampleTimeStamps3,
                sampleTimeStamps4, sampleTimeStamps5, sampleTimeStamps6,
                sampleTimeStamps7, sampleTimeStamps8, sampleTimeStamps9,
                sampleTimeStamps10));
        String channelName = "testChannel";
        int decimationLevel = 0;
        ArchiveAccessServiceImplBuilder aasb = new ArchiveAccessServiceImplBuilder()
                .channelName(channelName);
        aasb.decimationLevel(decimationLevel).sampleBucket(2L, 14L)
                .samples(sampleTimeStamps1);
        aasb.decimationLevel(decimationLevel).sampleBucket(16L, 378L)
                .samples(sampleTimeStamps2);
        aasb.decimationLevel(decimationLevel).sampleBucket(379L, 415L)
                .samples(sampleTimeStamps3);
        aasb.decimationLevel(decimationLevel).sampleBucket(420L, 475L)
                .samples(sampleTimeStamps4);
        aasb.decimationLevel(decimationLevel).sampleBucket(476L, 477L)
                .samples(sampleTimeStamps5);
        aasb.decimationLevel(decimationLevel).sampleBucket(506L, 517L)
                .samples(sampleTimeStamps6);
        aasb.decimationLevel(decimationLevel).sampleBucket(519L, 519L)
                .samples(sampleTimeStamps7);
        aasb.decimationLevel(decimationLevel).sampleBucket(520L, 600L)
                .samples(sampleTimeStamps8);
        aasb.decimationLevel(decimationLevel).sampleBucket(601L, 799L)
                .samples(sampleTimeStamps9);
        aasb.decimationLevel(decimationLevel)
                .sampleBucket(800L, Long.MAX_VALUE).samples(sampleTimeStamps10);
        ArchiveAccessServiceImpl aas = aasb.build();
        long start, end;
        List<Long> expectedSampleTimeStamps;
        // A query across the whole range should return all samples.
        start = 0L;
        end = Long.MAX_VALUE;
        expectedSampleTimeStamps = allSampleTimeStamps;
        testGetSamplesAllLimitModes(aas, channelName, decimationLevel, start,
                end, expectedSampleTimeStamps);
        expectedSampleTimeStamps = allSampleTimeStamps;
        start = expectedSampleTimeStamps.get(0);
        end = expectedSampleTimeStamps.get(expectedSampleTimeStamps.size() - 1);
        testGetSamplesAllLimitModes(aas, channelName, decimationLevel, start,
                end, expectedSampleTimeStamps);
        // For a query with limits that exactly match the time stamps of
        // samples, the limit mode should not match.
        expectedSampleTimeStamps = allSampleTimeStamps.subList(2, 8);
        start = expectedSampleTimeStamps.get(0);
        end = expectedSampleTimeStamps.get(expectedSampleTimeStamps.size() - 1);
        testGetSamplesAllLimitModes(aas, channelName, decimationLevel, start,
                end, expectedSampleTimeStamps);
        // For a query with limits that do not exactly match the time stamps of
        // samples, the limit mode should matter.
        start = 377L;
        end = 602L;
        expectedSampleTimeStamps = allSampleTimeStamps.subList(1, 11);
        assertTimeStampsMatch(expectedSampleTimeStamps, aas.getSamples(
                channelName, decimationLevel, start,
                TimeStampLimitMode.AT_OR_BEFORE, end,
                TimeStampLimitMode.AT_OR_AFTER));
        expectedSampleTimeStamps = allSampleTimeStamps.subList(2, 10);
        assertTimeStampsMatch(expectedSampleTimeStamps, aas.getSamples(
                channelName, decimationLevel, start,
                TimeStampLimitMode.AT_OR_AFTER, end,
                TimeStampLimitMode.AT_OR_BEFORE));
        expectedSampleTimeStamps = allSampleTimeStamps.subList(2, 11);
        assertTimeStampsMatch(expectedSampleTimeStamps, aas.getSamples(
                channelName, decimationLevel, start,
                TimeStampLimitMode.AT_OR_AFTER, end,
                TimeStampLimitMode.AT_OR_AFTER));
        expectedSampleTimeStamps = allSampleTimeStamps.subList(1, 10);
        assertTimeStampsMatch(expectedSampleTimeStamps, aas.getSamples(
                channelName, decimationLevel, start,
                TimeStampLimitMode.AT_OR_BEFORE, end,
                TimeStampLimitMode.AT_OR_BEFORE));
        // We also want to test that AT_OR_BEFORE for the lower limit works
        // correctly when we have to traverse multiple empty buckets.
        start = 602L;
        end = 800L;
        expectedSampleTimeStamps = allSampleTimeStamps.subList(9, 12);
        assertTimeStampsMatch(expectedSampleTimeStamps, aas.getSamples(
                channelName, decimationLevel, start,
                TimeStampLimitMode.AT_OR_BEFORE, end,
                TimeStampLimitMode.AT_OR_BEFORE));
        // We also want to test that AT_OR_AFTER fort upper limit works
        // correctly when we have to travese multiple empty buckets.
        start = 506L;
        end = 518L;
        expectedSampleTimeStamps = allSampleTimeStamps.subList(8, 11);
        assertTimeStampsMatch(expectedSampleTimeStamps, aas.getSamples(
                channelName, decimationLevel, start,
                TimeStampLimitMode.AT_OR_AFTER, end,
                TimeStampLimitMode.AT_OR_AFTER));
    }

    /**
     * Tests retrieving samples for a decimation level when there are multiple
     * buckets.
     */
    @Test
    public void testMultipleSampleBucketsDecimatedSamples() {
        // For this test, we use samples spread across many sample buckets. We
        // also use some empty buckets. In particular, we have an empty one at
        // the start and at the end because this might trigger some special code
        // paths.
        List<Long> sampleTimeStamps1 = Collections.emptyList();
        List<Long> sampleTimeStamps2 = ImmutableList.of(30L, 60L, 150L);
        List<Long> sampleTimeStamps3 = ImmutableList.of(180L, 360L, 390L, 420L);
        List<Long> sampleTimeStamps4 = Collections.emptyList();
        List<Long> sampleTimeStamps5 = ImmutableList.of(480L);
        List<Long> sampleTimeStamps6 = ImmutableList.of(510L, 540L);
        List<Long> sampleTimeStamps7 = Collections.emptyList();
        List<Long> sampleTimeStamps8 = Collections.emptyList();
        List<Long> sampleTimeStamps9 = ImmutableList.of(570L, 780L);
        List<Long> sampleTimeStamps10 = Collections.emptyList();
        @SuppressWarnings("unchecked")
        List<Long> allSampleTimeStamps = ImmutableList.copyOf(Iterables.concat(
                sampleTimeStamps1, sampleTimeStamps2, sampleTimeStamps3,
                sampleTimeStamps4, sampleTimeStamps5, sampleTimeStamps6,
                sampleTimeStamps7, sampleTimeStamps8, sampleTimeStamps9,
                sampleTimeStamps10));
        String channelName = "testChannel";
        int decimationLevel = 30;
        ArchiveAccessServiceImplBuilder aasb = new ArchiveAccessServiceImplBuilder()
                .channelName(channelName);
        aasb.decimationLevel(decimationLevel).sampleBucket(0L, 29L)
                .samples(sampleTimeStamps1);
        aasb.decimationLevel(decimationLevel).sampleBucket(30L, 179L)
                .samples(sampleTimeStamps2);
        aasb.decimationLevel(decimationLevel).sampleBucket(180L, 420L)
                .samples(sampleTimeStamps3);
        aasb.decimationLevel(decimationLevel).sampleBucket(421L, 477L)
                .samples(sampleTimeStamps4);
        aasb.decimationLevel(decimationLevel).sampleBucket(478L, 500L)
                .samples(sampleTimeStamps5);
        aasb.decimationLevel(decimationLevel).sampleBucket(509L, 545L)
                .samples(sampleTimeStamps6);
        aasb.decimationLevel(decimationLevel).sampleBucket(546L, 546L)
                .samples(sampleTimeStamps7);
        aasb.decimationLevel(decimationLevel).sampleBucket(550L, 569L)
                .samples(sampleTimeStamps8);
        aasb.decimationLevel(decimationLevel).sampleBucket(570L, 799L)
                .samples(sampleTimeStamps9);
        aasb.decimationLevel(decimationLevel)
                .sampleBucket(800L, Long.MAX_VALUE).samples(sampleTimeStamps10);
        ArchiveAccessServiceImpl aas = aasb.build();
        long start, end;
        List<Long> expectedSampleTimeStamps;
        // A query across the whole range should return all samples.
        start = 0L;
        end = Long.MAX_VALUE;
        expectedSampleTimeStamps = allSampleTimeStamps;
        testGetSamplesAllLimitModes(aas, channelName, decimationLevel, start,
                end, expectedSampleTimeStamps);
        expectedSampleTimeStamps = allSampleTimeStamps;
        start = expectedSampleTimeStamps.get(0);
        end = expectedSampleTimeStamps.get(expectedSampleTimeStamps.size() - 1);
        testGetSamplesAllLimitModes(aas, channelName, decimationLevel, start,
                end, expectedSampleTimeStamps);
        // For a query with limits that exactly match the time stamps of
        // samples, the limit mode should not match.
        expectedSampleTimeStamps = allSampleTimeStamps.subList(2, 8);
        start = expectedSampleTimeStamps.get(0);
        end = expectedSampleTimeStamps.get(expectedSampleTimeStamps.size() - 1);
        testGetSamplesAllLimitModes(aas, channelName, decimationLevel, start,
                end, expectedSampleTimeStamps);
        // For a query with limits that do not exactly match the time stamps of
        // samples, the limit mode should matter.
        start = 75L;
        end = 560L;
        expectedSampleTimeStamps = allSampleTimeStamps.subList(1, 11);
        assertTimeStampsMatch(expectedSampleTimeStamps, aas.getSamples(
                channelName, decimationLevel, start,
                TimeStampLimitMode.AT_OR_BEFORE, end,
                TimeStampLimitMode.AT_OR_AFTER));
        expectedSampleTimeStamps = allSampleTimeStamps.subList(2, 10);
        assertTimeStampsMatch(expectedSampleTimeStamps, aas.getSamples(
                channelName, decimationLevel, start,
                TimeStampLimitMode.AT_OR_AFTER, end,
                TimeStampLimitMode.AT_OR_BEFORE));
        expectedSampleTimeStamps = allSampleTimeStamps.subList(2, 11);
        assertTimeStampsMatch(expectedSampleTimeStamps, aas.getSamples(
                channelName, decimationLevel, start,
                TimeStampLimitMode.AT_OR_AFTER, end,
                TimeStampLimitMode.AT_OR_AFTER));
        expectedSampleTimeStamps = allSampleTimeStamps.subList(1, 10);
        assertTimeStampsMatch(expectedSampleTimeStamps, aas.getSamples(
                channelName, decimationLevel, start,
                TimeStampLimitMode.AT_OR_BEFORE, end,
                TimeStampLimitMode.AT_OR_BEFORE));
        // We also want to test that AT_OR_BEFORE for the lower limit works
        // correctly when we have to traverse multiple empty buckets.
        start = 565L;
        end = 800L;
        expectedSampleTimeStamps = allSampleTimeStamps.subList(9, 12);
        assertTimeStampsMatch(expectedSampleTimeStamps, aas.getSamples(
                channelName, decimationLevel, start,
                TimeStampLimitMode.AT_OR_BEFORE, end,
                TimeStampLimitMode.AT_OR_BEFORE));
        // We also want to test that AT_OR_AFTER fort upper limit works
        // correctly when we have to travese multiple empty buckets.
        start = 485L;
        end = 541L;
        expectedSampleTimeStamps = allSampleTimeStamps.subList(8, 11);
        assertTimeStampsMatch(expectedSampleTimeStamps, aas.getSamples(
                channelName, decimationLevel, start,
                TimeStampLimitMode.AT_OR_AFTER, end,
                TimeStampLimitMode.AT_OR_AFTER));
    }

    private void testSingleSampleBucket(int decimationLevel,
            long bucketStartTime, long bucketEndTime,
            List<Long> sampleTimeStamps) {
        String channelName = "testChannel";
        ArchiveAccessServiceImplBuilder aasb = new ArchiveAccessServiceImplBuilder()
                .channelName(channelName);
        aasb.decimationLevel(decimationLevel)
                .sampleBucket(bucketStartTime, bucketEndTime)
                .samples(sampleTimeStamps);
        ArchiveAccessServiceImpl aas = aasb.build();
        long start, end;
        List<Long> expectedSampleTimeStamps;
        // Get all samples with limits that are outside the sample bucket using
        // different limit modes.
        start = bucketStartTime > 10 ? (bucketStartTime - 10) : 0L;
        end = bucketEndTime < (Long.MAX_VALUE - 10) ? (bucketEndTime + 10L)
                : Long.MAX_VALUE;
        expectedSampleTimeStamps = sampleTimeStamps;
        testGetSamplesAllLimitModes(aas, channelName, decimationLevel, start,
                end, expectedSampleTimeStamps);
        // Do the same using 0L for the start and Long.MAX_VALUE for the end.
        start = 0L;
        end = Long.MAX_VALUE;
        expectedSampleTimeStamps = sampleTimeStamps;
        testGetSamplesAllLimitModes(aas, channelName, decimationLevel, start,
                end, expectedSampleTimeStamps);
        // Get all samples with limits that are exactly matching the sample
        // bucket's limits using different limit modes.
        start = bucketStartTime;
        end = bucketEndTime;
        expectedSampleTimeStamps = sampleTimeStamps;
        testGetSamplesAllLimitModes(aas, channelName, decimationLevel, start,
                end, expectedSampleTimeStamps);
        // Get all samples from decimation level 0 with limits that are exactly
        // matching the time stamps of the first and last sample using different
        // limit modes.
        start = sampleTimeStamps.get(0);
        end = sampleTimeStamps.get(sampleTimeStamps.size() - 1);
        expectedSampleTimeStamps = sampleTimeStamps;
        testGetSamplesAllLimitModes(aas, channelName, decimationLevel, start,
                end, expectedSampleTimeStamps);
        // Get the center samples (all but the first and the last sample) using
        // different limit modes.
        start = sampleTimeStamps.get(1);
        end = sampleTimeStamps.get(sampleTimeStamps.size() - 2);
        expectedSampleTimeStamps = sampleTimeStamps.subList(1,
                sampleTimeStamps.size() - 1);
        testGetSamplesAllLimitModes(aas, channelName, decimationLevel, start,
                end, expectedSampleTimeStamps);
        // Search for exactly the first sample using different limit modes.
        start = sampleTimeStamps.get(0);
        end = start;
        expectedSampleTimeStamps = sampleTimeStamps.subList(0, 1);
        testGetSamplesAllLimitModes(aas, channelName, decimationLevel, start,
                end, expectedSampleTimeStamps);
        // Search for exactly the second sample using different limit modes.
        start = sampleTimeStamps.get(1);
        end = start;
        expectedSampleTimeStamps = sampleTimeStamps.subList(1, 2);
        testGetSamplesAllLimitModes(aas, channelName, decimationLevel, start,
                end, expectedSampleTimeStamps);
        // Search for exactly the last sample using different limit modes.
        start = sampleTimeStamps.get(sampleTimeStamps.size() - 1);
        end = start;
        expectedSampleTimeStamps = sampleTimeStamps.subList(
                sampleTimeStamps.size() - 1, sampleTimeStamps.size());
        testGetSamplesAllLimitModes(aas, channelName, decimationLevel, start,
                end, expectedSampleTimeStamps);
        // Search around the second sample using different limit modes. We
        // expect different results for different limit modes. For this test we
        // assume that there is a gap between the first and the second and
        // between the second and the third sample.
        start = sampleTimeStamps.get(1) - 1L;
        end = sampleTimeStamps.get(1) + 1L;
        expectedSampleTimeStamps = sampleTimeStamps.subList(0, 3);
        assertTimeStampsMatch(expectedSampleTimeStamps, aas.getSamples(
                channelName, decimationLevel, start,
                TimeStampLimitMode.AT_OR_BEFORE, end,
                TimeStampLimitMode.AT_OR_AFTER));
        expectedSampleTimeStamps = sampleTimeStamps.subList(1, 2);
        assertTimeStampsMatch(expectedSampleTimeStamps, aas.getSamples(
                channelName, decimationLevel, start,
                TimeStampLimitMode.AT_OR_AFTER, end,
                TimeStampLimitMode.AT_OR_BEFORE));
        expectedSampleTimeStamps = sampleTimeStamps.subList(1, 3);
        assertTimeStampsMatch(expectedSampleTimeStamps, aas.getSamples(
                channelName, decimationLevel, start,
                TimeStampLimitMode.AT_OR_AFTER, end,
                TimeStampLimitMode.AT_OR_AFTER));
        expectedSampleTimeStamps = sampleTimeStamps.subList(0, 2);
        assertTimeStampsMatch(expectedSampleTimeStamps, aas.getSamples(
                channelName, decimationLevel, start,
                TimeStampLimitMode.AT_OR_BEFORE, end,
                TimeStampLimitMode.AT_OR_BEFORE));
    }

}
