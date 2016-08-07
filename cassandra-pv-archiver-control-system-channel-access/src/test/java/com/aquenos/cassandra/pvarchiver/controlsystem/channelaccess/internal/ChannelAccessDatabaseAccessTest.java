/*
 * Copyright 2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.controlsystem.channelaccess.internal;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import org.junit.BeforeClass;
import org.junit.Test;

import com.aquenos.cassandra.pvarchiver.controlsystem.SampleBucketId;
import com.aquenos.cassandra.pvarchiver.controlsystem.SampleBucketState;
import com.aquenos.cassandra.pvarchiver.controlsystem.channelaccess.ChannelAccessSample;
import com.aquenos.cassandra.pvarchiver.tests.EmbeddedCassandraServer;
import com.aquenos.epics.jackie.common.value.ChannelAccessAlarmSeverity;
import com.aquenos.epics.jackie.common.value.ChannelAccessAlarmStatus;
import com.aquenos.epics.jackie.common.value.ChannelAccessValueFactory;
import com.datastax.driver.core.Session;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;

/**
 * Tests the {@link ChannelAccessDatabaseAccess}.
 * 
 * @author Sebastian Marsching
 */
public class ChannelAccessDatabaseAccessTest {

    private static ChannelAccessDatabaseAccess databaseAccess;
    private static ChannelAccessSampleValueAccess sampleValueAccess;

    /**
     * Creates the {@link ChannelAccessDatabaseAccess} instance that is tested.
     */
    @BeforeClass
    public static void initializeDatabaseAccess() {
        Session session = EmbeddedCassandraServer.getSession();
        sampleValueAccess = new ChannelAccessSampleValueAccess(session);
        databaseAccess = new ChannelAccessDatabaseAccess(session,
                sampleValueAccess);
    }

    private static List<ChannelAccessDisconnectedSample> createDisconnectedSamples(
            boolean originalSamples, long... timeStamps) {
        ArrayList<ChannelAccessDisconnectedSample> samples = new ArrayList<ChannelAccessDisconnectedSample>(
                timeStamps.length);
        for (long timeStamp : timeStamps) {
            samples.add(new ChannelAccessDisconnectedSample(timeStamp,
                    originalSamples));
        }
        return Collections.unmodifiableList(samples);
    }

    /**
     * Tests for all sample types that they are written and read correctly.
     */
    @Test
    public void testAllSampleTypes() {
        long epicsTimeOffsetNanoseconds = 631152000000000000L;
        SampleBucketId bucketId = new SampleBucketId(UUID.randomUUID(), 0, 0L);
        LinkedList<ChannelAccessSample> samples = new LinkedList<ChannelAccessSample>();
        // We have to create one single element and one multi- or zero-element
        // value for each type, because these types get serialized in a
        // different way. We save some of the samples because we can use them
        // later when we test aggregated samples.
        ChannelAccessRawSample scalarCharSample = sampleValueAccess
                .createRawSample(ChannelAccessValueFactory.createTimeChar(
                        new byte[] { 18 }, ChannelAccessAlarmStatus.NO_ALARM,
                        ChannelAccessAlarmSeverity.NO_ALARM, 14, 12345),
                        ChannelAccessValueFactory.createControlsChar(
                                new byte[0], ChannelAccessAlarmStatus.NO_ALARM,
                                ChannelAccessAlarmSeverity.NO_ALARM, "Volts",
                                (byte) 85, (byte) -85, (byte) 30, (byte) 25,
                                (byte) 10, (byte) 5, (byte) 35, (byte) 5,
                                Charsets.UTF_8));
        samples.add(scalarCharSample);
        samples.add(sampleValueAccess.createRawSample(ChannelAccessValueFactory
                .createTimeChar(new byte[] { 14, -37, 85 },
                        ChannelAccessAlarmStatus.NO_ALARM,
                        ChannelAccessAlarmSeverity.NO_ALARM, 15, 12345),
                ChannelAccessValueFactory.createControlsChar(new byte[0],
                        ChannelAccessAlarmStatus.NO_ALARM,
                        ChannelAccessAlarmSeverity.NO_ALARM, "Volts",
                        (byte) 85, (byte) -85, (byte) 30, (byte) 25, (byte) 10,
                        (byte) 5, (byte) 35, (byte) 5, Charsets.UTF_8)));
        samples.add(sampleValueAccess.createRawSample(ChannelAccessValueFactory
                .createTimeChar(new byte[0], ChannelAccessAlarmStatus.NO_ALARM,
                        ChannelAccessAlarmSeverity.NO_ALARM, 16, 12345),
                ChannelAccessValueFactory.createControlsChar(new byte[0],
                        ChannelAccessAlarmStatus.NO_ALARM,
                        ChannelAccessAlarmSeverity.NO_ALARM, "Volts",
                        (byte) 85, (byte) -85, (byte) 30, (byte) 25, (byte) 10,
                        (byte) 5, (byte) 35, (byte) 5, Charsets.UTF_8)));
        ChannelAccessRawSample scalarDoubleSample = sampleValueAccess
                .createRawSample(ChannelAccessValueFactory.createTimeDouble(
                        new double[] { 42.0 },
                        ChannelAccessAlarmStatus.BAD_SUB,
                        ChannelAccessAlarmSeverity.INVALID_ALARM, 17, 123),
                        ChannelAccessValueFactory.createControlsDouble(
                                new double[0],
                                ChannelAccessAlarmStatus.NO_ALARM,
                                ChannelAccessAlarmSeverity.NO_ALARM, (short) 3,
                                "mA", 128.0, -5.0, 255.0, 200.0, -10.0, -15.0,
                                55.0, 5, Charsets.UTF_8));
        samples.add(scalarDoubleSample);
        samples.add(sampleValueAccess.createRawSample(ChannelAccessValueFactory
                .createTimeDouble(new double[] { 42.0, -18.5, Double.NaN,
                        Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY },
                        ChannelAccessAlarmStatus.BAD_SUB,
                        ChannelAccessAlarmSeverity.INVALID_ALARM, 18, 123),
                ChannelAccessValueFactory.createControlsDouble(new double[0],
                        ChannelAccessAlarmStatus.NO_ALARM,
                        ChannelAccessAlarmSeverity.NO_ALARM, (short) 3, "mA",
                        128.0, -5.0, 255.0, 200.0, -10.0, -15.0, 55.0, 5,
                        Charsets.UTF_8)));
        samples.add(sampleValueAccess.createRawSample(ChannelAccessValueFactory
                .createTimeDouble(new double[0],
                        ChannelAccessAlarmStatus.BAD_SUB,
                        ChannelAccessAlarmSeverity.INVALID_ALARM, 19, 123),
                ChannelAccessValueFactory.createControlsDouble(new double[0],
                        ChannelAccessAlarmStatus.NO_ALARM,
                        ChannelAccessAlarmSeverity.NO_ALARM, (short) 3, "mA",
                        128.0, -5.0, 255.0, 200.0, -10.0, -15.0, 55.0, 5,
                        Charsets.UTF_8)));
        samples.add(sampleValueAccess.createRawSample(ChannelAccessValueFactory
                .createTimeEnum(new short[] { 3 },
                        ChannelAccessAlarmStatus.STATE,
                        ChannelAccessAlarmSeverity.MINOR_ALARM, 20, 451),
                ChannelAccessValueFactory.createControlsEnum(new short[0],
                        ChannelAccessAlarmStatus.NO_ALARM,
                        ChannelAccessAlarmSeverity.NO_ALARM,
                        ImmutableList.of("abc", "def"), Charsets.UTF_8)));
        samples.add(sampleValueAccess.createRawSample(ChannelAccessValueFactory
                .createTimeEnum(new short[] { 3, -5 },
                        ChannelAccessAlarmStatus.STATE,
                        ChannelAccessAlarmSeverity.MINOR_ALARM, 21, 451),
                ChannelAccessValueFactory.createControlsEnum(new short[0],
                        ChannelAccessAlarmStatus.NO_ALARM,
                        ChannelAccessAlarmSeverity.NO_ALARM,
                        ImmutableList.of("abc"), Charsets.UTF_8)));
        samples.add(sampleValueAccess.createRawSample(ChannelAccessValueFactory
                .createTimeEnum(new short[0], ChannelAccessAlarmStatus.STATE,
                        ChannelAccessAlarmSeverity.MINOR_ALARM, 22, 451),
                ChannelAccessValueFactory.createControlsEnum(new short[0],
                        ChannelAccessAlarmStatus.NO_ALARM,
                        ChannelAccessAlarmSeverity.NO_ALARM,
                        Collections.<String> emptyList(), Charsets.UTF_8)));
        ChannelAccessRawSample scalarFloatSample = sampleValueAccess
                .createRawSample(ChannelAccessValueFactory.createTimeFloat(
                        new float[] { 42.0f }, ChannelAccessAlarmStatus.HIHI,
                        ChannelAccessAlarmSeverity.MAJOR_ALARM, 23, 0),
                        ChannelAccessValueFactory.createControlsFloat(
                                new float[0],
                                ChannelAccessAlarmStatus.NO_ALARM,
                                ChannelAccessAlarmSeverity.NO_ALARM, (short) 1,
                                "V", 128.0f, -5.0f, 255.0f, 200.0f, -10.0f,
                                -15.0f, 55.0f, 5, Charsets.UTF_8));
        samples.add(scalarFloatSample);
        samples.add(sampleValueAccess.createRawSample(ChannelAccessValueFactory
                .createTimeFloat(new float[] { 42.0f, -23.0f, 128.5f, -99.0f,
                        Float.NaN, Float.NEGATIVE_INFINITY,
                        Float.POSITIVE_INFINITY },
                        ChannelAccessAlarmStatus.HIHI,
                        ChannelAccessAlarmSeverity.MAJOR_ALARM, 24, 0),
                ChannelAccessValueFactory.createControlsFloat(new float[0],
                        ChannelAccessAlarmStatus.NO_ALARM,
                        ChannelAccessAlarmSeverity.NO_ALARM, (short) 1, "V",
                        128.0f, -5.0f, 255.0f, 200.0f, -10.0f, -15.0f, 55.0f,
                        5, Charsets.UTF_8)));
        samples.add(sampleValueAccess.createRawSample(ChannelAccessValueFactory
                .createTimeFloat(new float[0], ChannelAccessAlarmStatus.HIHI,
                        ChannelAccessAlarmSeverity.MAJOR_ALARM, 25, 0),
                ChannelAccessValueFactory.createControlsFloat(new float[0],
                        ChannelAccessAlarmStatus.NO_ALARM,
                        ChannelAccessAlarmSeverity.NO_ALARM, (short) 1, "V",
                        128.0f, -5.0f, 255.0f, 200.0f, -10.0f, -15.0f, 55.0f,
                        5, Charsets.UTF_8)));
        ChannelAccessRawSample scalarLongSample = sampleValueAccess
                .createRawSample(ChannelAccessValueFactory.createTimeLong(
                        new int[] { 223323 },
                        ChannelAccessAlarmStatus.NO_ALARM,
                        ChannelAccessAlarmSeverity.NO_ALARM, 26, 420),
                        ChannelAccessValueFactory.createControlsLong(
                                new int[0], ChannelAccessAlarmStatus.NO_ALARM,
                                ChannelAccessAlarmSeverity.NO_ALARM, "MHz", 85,
                                -85, 30, 25, 10, 5, 35, 5, Charsets.UTF_8));
        samples.add(scalarLongSample);
        samples.add(sampleValueAccess.createRawSample(ChannelAccessValueFactory
                .createTimeLong(new int[] { 223323, -99 },
                        ChannelAccessAlarmStatus.NO_ALARM,
                        ChannelAccessAlarmSeverity.NO_ALARM, 27, 420),
                ChannelAccessValueFactory.createControlsLong(new int[0],
                        ChannelAccessAlarmStatus.NO_ALARM,
                        ChannelAccessAlarmSeverity.NO_ALARM, "MHz", 85, -85,
                        30, 25, 10, 5, 35, 5, Charsets.UTF_8)));
        samples.add(sampleValueAccess.createRawSample(ChannelAccessValueFactory
                .createTimeLong(new int[0], ChannelAccessAlarmStatus.NO_ALARM,
                        ChannelAccessAlarmSeverity.NO_ALARM, 28, 420),
                ChannelAccessValueFactory.createControlsLong(new int[0],
                        ChannelAccessAlarmStatus.NO_ALARM,
                        ChannelAccessAlarmSeverity.NO_ALARM, "MHz", 85, -85,
                        30, 25, 10, 5, 35, 5, Charsets.UTF_8)));
        ChannelAccessRawSample scalarShortSample = sampleValueAccess
                .createRawSample(ChannelAccessValueFactory.createTimeShort(
                        new short[] { 761 }, ChannelAccessAlarmStatus.LOW,
                        ChannelAccessAlarmSeverity.MINOR_ALARM, 29, 99),
                        ChannelAccessValueFactory.createControlsShort(
                                new short[0],
                                ChannelAccessAlarmStatus.NO_ALARM,
                                ChannelAccessAlarmSeverity.NO_ALARM, "Ampere",
                                (short) 85, (short) -85, (short) 30,
                                (short) 25, (short) 10, (short) 5, (short) 35,
                                (short) 5, Charsets.UTF_8));
        samples.add(scalarShortSample);
        samples.add(sampleValueAccess.createRawSample(ChannelAccessValueFactory
                .createTimeShort(new short[] { 55, -37, 128, Short.MAX_VALUE },
                        ChannelAccessAlarmStatus.LOW,
                        ChannelAccessAlarmSeverity.MINOR_ALARM, 30, 99),
                ChannelAccessValueFactory.createControlsShort(new short[0],
                        ChannelAccessAlarmStatus.NO_ALARM,
                        ChannelAccessAlarmSeverity.NO_ALARM, "Ampere",
                        (short) 85, (short) -85, (short) 30, (short) 25,
                        (short) 10, (short) 5, (short) 35, (short) 5,
                        Charsets.UTF_8)));
        samples.add(sampleValueAccess.createRawSample(ChannelAccessValueFactory
                .createTimeShort(new short[0], ChannelAccessAlarmStatus.LOW,
                        ChannelAccessAlarmSeverity.MINOR_ALARM, 31, 99),
                ChannelAccessValueFactory.createControlsShort(new short[0],
                        ChannelAccessAlarmStatus.NO_ALARM,
                        ChannelAccessAlarmSeverity.NO_ALARM, "Ampere",
                        (short) 85, (short) -85, (short) 30, (short) 25,
                        (short) 10, (short) 5, (short) 35, (short) 5,
                        Charsets.UTF_8)));
        samples.add(sampleValueAccess.createRawSample(ChannelAccessValueFactory
                .createTimeString(ImmutableList.of("abc"),
                        ChannelAccessAlarmStatus.COMM,
                        ChannelAccessAlarmSeverity.MAJOR_ALARM, 32, 0,
                        Charsets.UTF_8), ChannelAccessValueFactory
                .createControlsString(Collections.<String> emptyList(),
                        ChannelAccessAlarmStatus.NO_ALARM,
                        ChannelAccessAlarmSeverity.NO_ALARM, Charsets.UTF_8)));
        samples.add(sampleValueAccess.createRawSample(ChannelAccessValueFactory
                .createTimeString(ImmutableList.of("abc", "def", "xyz123"),
                        ChannelAccessAlarmStatus.COMM,
                        ChannelAccessAlarmSeverity.MAJOR_ALARM, 33, 0,
                        Charsets.UTF_8), ChannelAccessValueFactory
                .createControlsString(Collections.<String> emptyList(),
                        ChannelAccessAlarmStatus.NO_ALARM,
                        ChannelAccessAlarmSeverity.NO_ALARM, Charsets.UTF_8)));
        samples.add(sampleValueAccess.createRawSample(ChannelAccessValueFactory
                .createTimeString(Collections.<String> emptyList(),
                        ChannelAccessAlarmStatus.COMM,
                        ChannelAccessAlarmSeverity.MAJOR_ALARM, 34, 0,
                        Charsets.UTF_8), ChannelAccessValueFactory
                .createControlsString(Collections.<String> emptyList(),
                        ChannelAccessAlarmStatus.NO_ALARM,
                        ChannelAccessAlarmSeverity.NO_ALARM, Charsets.UTF_8)));
        samples.add(sampleValueAccess.createAggregatedScalarCharSample(
                epicsTimeOffsetNanoseconds + 103000000000L, 34.2, 2.7,
                (byte) -4, (byte) 99, ChannelAccessAlarmSeverity.MAJOR_ALARM,
                ChannelAccessAlarmStatus.HIHI, 0.95, scalarCharSample));
        samples.add(sampleValueAccess.createAggregatedScalarDoubleSample(
                epicsTimeOffsetNanoseconds + 104000000000L, 42.8, 9.6, -4.2,
                99.78, ChannelAccessAlarmSeverity.MINOR_ALARM,
                ChannelAccessAlarmStatus.LOW, 1.0, scalarDoubleSample));
        samples.add(sampleValueAccess.createAggregatedScalarFloatSample(
                epicsTimeOffsetNanoseconds + 105000000000L, -3.42, 4.7, -38.2f,
                77.4f, ChannelAccessAlarmSeverity.INVALID_ALARM,
                ChannelAccessAlarmStatus.CALC, 0.01, scalarFloatSample));
        samples.add(sampleValueAccess.createAggregatedScalarLongSample(
                epicsTimeOffsetNanoseconds + 106000000000L, 77.3, 3.1, -42,
                11500, ChannelAccessAlarmSeverity.NO_ALARM,
                ChannelAccessAlarmStatus.NO_ALARM, 0.42, scalarLongSample));
        samples.add(sampleValueAccess.createAggregatedScalarShortSample(
                epicsTimeOffsetNanoseconds + 107000000000L, 113.2, 99.3,
                (short) 15, (short) 2500,
                ChannelAccessAlarmSeverity.MAJOR_ALARM,
                ChannelAccessAlarmStatus.LOLO, 0.23, scalarShortSample));
        // We also want to test the special "disabled" and "disconnected" sample
        // types.
        samples.add(new ChannelAccessDisabledSample(
                epicsTimeOffsetNanoseconds + 405000000000L, true));
        samples.add(new ChannelAccessDisconnectedSample(
                epicsTimeOffsetNanoseconds + 405000000001L, true));
        // Write all samples.
        for (ChannelAccessSample sample : samples) {
            Futures.getUnchecked(databaseAccess.writeSample(sample, bucketId,
                    123));
        }
        // Read all samples and compare them.
        assertArrayEquals(
                samples.toArray(),
                ImmutableList.copyOf(
                        Futures.getUnchecked(databaseAccess.getSamples(
                                bucketId, 0L, Long.MAX_VALUE, -1, false)))
                        .toArray());
        // Delete the sample bucket to avoid interference with other tests.
        Futures.getUnchecked(databaseAccess.deleteSamples(bucketId));
    }

    /**
     * Tests getting samples with various queries.
     */
    @Test
    public void testGetSamples() {
        SampleBucketId bucketId = new SampleBucketId(UUID.randomUUID(), 0, 0L);
        List<ChannelAccessDisconnectedSample> samples = createDisconnectedSamples(
                true, 17L, 29L, 30L, 35L, 55L, 100L);
        // Before writing the samples, the sample bucket should be empty.
        assertEquals(0, Iterables.size(Futures.getUnchecked(databaseAccess
                .getSamples(bucketId, 0L, Long.MAX_VALUE, -1, false))));
        // Write all samples.
        for (ChannelAccessDisconnectedSample sample : samples) {
            Futures.getUnchecked(databaseAccess.writeSample(sample, bucketId,
                    123));
        }
        // Get all samples.
        assertEquals(samples, ImmutableList.copyOf(Futures
                .getUnchecked(databaseAccess.getSamples(bucketId, 0L,
                        Long.MAX_VALUE, -1, false))));
        // Get all samples in reverse order.
        assertEquals(Lists.reverse(samples), ImmutableList.copyOf(Futures
                .getUnchecked(databaseAccess.getSamples(bucketId, 0L,
                        Long.MAX_VALUE, -1, true))));
        // Get first three samples.
        assertEquals(samples.subList(0, 3), ImmutableList.copyOf(Futures
                .getUnchecked(databaseAccess.getSamples(bucketId, 0L, 30L, -1,
                        false))));
        assertEquals(samples.subList(0, 3), ImmutableList.copyOf(Futures
                .getUnchecked(databaseAccess.getSamples(bucketId, 17L, 34L, -1,
                        false))));
        assertEquals(samples.subList(0, 3), ImmutableList.copyOf(Futures
                .getUnchecked(databaseAccess.getSamples(bucketId, 0L,
                        Long.MAX_VALUE, 3, false))));
        // Get second, third, and fourth sample.
        assertEquals(samples.subList(1, 4), ImmutableList.copyOf(Futures
                .getUnchecked(databaseAccess.getSamples(bucketId, 18L, 35L, -1,
                        false))));
        assertEquals(samples.subList(1, 4), ImmutableList.copyOf(Futures
                .getUnchecked(databaseAccess.getSamples(bucketId, 18L,
                        Long.MAX_VALUE, 3, false))));
        // Get last two samples.
        assertEquals(samples.subList(4, 6), ImmutableList.copyOf(Futures
                .getUnchecked(databaseAccess.getSamples(bucketId, 55L,
                        Long.MAX_VALUE, -1, false))));
        assertEquals(samples.subList(4, 6), ImmutableList.copyOf(Futures
                .getUnchecked(databaseAccess.getSamples(bucketId, 55L,
                        Long.MAX_VALUE, 2, false))));
        assertEquals(samples.subList(4, 6), ImmutableList.copyOf(Futures
                .getUnchecked(databaseAccess.getSamples(bucketId, 55L,
                        Long.MAX_VALUE, 3, false))));
        // Get first three samples in reverse order.
        assertEquals(Lists.reverse(samples.subList(0, 3)),
                ImmutableList.copyOf(Futures.getUnchecked(databaseAccess
                        .getSamples(bucketId, 0L, 30L, -1, true))));
        assertEquals(Lists.reverse(samples.subList(0, 3)),
                ImmutableList.copyOf(Futures.getUnchecked(databaseAccess
                        .getSamples(bucketId, 0L, 30L, 3, true))));
        assertEquals(Lists.reverse(samples.subList(0, 3)),
                ImmutableList.copyOf(Futures.getUnchecked(databaseAccess
                        .getSamples(bucketId, 0L, 30L, 4, true))));
        // Get third and fourth sample in reverse order.
        assertEquals(Lists.reverse(samples.subList(2, 4)),
                ImmutableList.copyOf(Futures.getUnchecked(databaseAccess
                        .getSamples(bucketId, 30L, 37L, -1, true))));
        assertEquals(Lists.reverse(samples.subList(2, 4)),
                ImmutableList.copyOf(Futures.getUnchecked(databaseAccess
                        .getSamples(bucketId, 0L, 37L, 2, true))));
        // Delete the sample bucket and check that there are no samples found
        // any longer.
        Futures.getUnchecked(databaseAccess.deleteSamples(bucketId));
        assertEquals(0, Iterables.size(Futures.getUnchecked(databaseAccess
                .getSamples(bucketId, 0L, Long.MAX_VALUE, -1, false))));
    }

    /**
     * Tests that samples written to different sample buckets do not interfere
     * with each other.
     */
    @Test
    public void testSampleBucketsDoNotInterfere() {
        // We create four different sample buckets. This way we can test that
        // sample buckets sharing the channel data-ID, the bucket start-time, or
        // the decimation level do not interfere with each other unless all
        // three identifiers match. We simply use "disconnected" samples for all
        // these tests because they are the easiest ones to create.
        SampleBucketId bucketId1 = new SampleBucketId(UUID.randomUUID(), 0, 15L);
        SampleBucketId bucketId2 = new SampleBucketId(UUID.randomUUID(),
                bucketId1.getDecimationLevel(), bucketId1.getBucketStartTime());
        SampleBucketId bucketId3 = new SampleBucketId(
                bucketId1.getChannelDataId(), 5, bucketId1.getBucketStartTime());
        SampleBucketId bucketId4 = new SampleBucketId(
                bucketId1.getChannelDataId(), bucketId1.getDecimationLevel(),
                900L);
        List<ChannelAccessDisconnectedSample> samples1 = createDisconnectedSamples(
                true, 17L, 29L, 30L, 35L, 55L, 100L);
        List<ChannelAccessDisconnectedSample> samples2 = createDisconnectedSamples(
                true, 19L, 28L, 31L, 37L, 52L, 101L);
        List<ChannelAccessDisconnectedSample> samples3 = createDisconnectedSamples(
                false, 670L, 695L, 700L, 705L, 755L, 955L);
        List<ChannelAccessDisconnectedSample> samples4 = createDisconnectedSamples(
                true, 900L, 902L, 903L);
        // Before writing the samples, the sample buckets should be empty.
        assertEquals(0, Iterables.size(Futures.getUnchecked(databaseAccess
                .getSamples(bucketId1, 0L, Long.MAX_VALUE, -1, false))));
        assertEquals(0, Iterables.size(Futures.getUnchecked(databaseAccess
                .getSamples(bucketId2, 0L, Long.MAX_VALUE, -1, false))));
        assertEquals(0, Iterables.size(Futures.getUnchecked(databaseAccess
                .getSamples(bucketId3, 0L, Long.MAX_VALUE, -1, false))));
        assertEquals(0, Iterables.size(Futures.getUnchecked(databaseAccess
                .getSamples(bucketId4, 0L, Long.MAX_VALUE, -1, false))));
        // Write all samples.
        for (ChannelAccessDisconnectedSample sample : samples1) {
            Futures.getUnchecked(databaseAccess.writeSample(sample, bucketId1,
                    123));
        }
        for (ChannelAccessDisconnectedSample sample : samples2) {
            Futures.getUnchecked(databaseAccess.writeSample(sample, bucketId2,
                    321));
        }
        for (ChannelAccessDisconnectedSample sample : samples3) {
            Futures.getUnchecked(databaseAccess.writeSample(sample, bucketId3,
                    789));
        }
        for (ChannelAccessDisconnectedSample sample : samples4) {
            Futures.getUnchecked(databaseAccess.writeSample(sample, bucketId4,
                    456));
        }
        // Get all samples. We run this test for all four sample buckets in
        // order to check that there is no interference between the different
        // sample buckets.
        assertEquals(samples1, ImmutableList.copyOf(Futures
                .getUnchecked(databaseAccess.getSamples(bucketId1, 0L,
                        Long.MAX_VALUE, -1, false))));
        assertEquals(samples2, ImmutableList.copyOf(Futures
                .getUnchecked(databaseAccess.getSamples(bucketId2, 0L,
                        Long.MAX_VALUE, -1, false))));
        assertEquals(samples3, ImmutableList.copyOf(Futures
                .getUnchecked(databaseAccess.getSamples(bucketId3, 0L,
                        Long.MAX_VALUE, -1, false))));
        assertEquals(samples4, ImmutableList.copyOf(Futures
                .getUnchecked(databaseAccess.getSamples(bucketId4, 0L,
                        Long.MAX_VALUE, -1, false))));
        // We also check that the meta-data for the different sample buckets
        // does not interfere with each other.
        assertEquals(new SampleBucketState(123, 100L),
                Futures.getUnchecked(databaseAccess
                        .getSampleBucketState(bucketId1)));
        assertEquals(new SampleBucketState(321, 101L),
                Futures.getUnchecked(databaseAccess
                        .getSampleBucketState(bucketId2)));
        assertEquals(new SampleBucketState(789, 955L),
                Futures.getUnchecked(databaseAccess
                        .getSampleBucketState(bucketId3)));
        assertEquals(new SampleBucketState(456, 903L),
                Futures.getUnchecked(databaseAccess
                        .getSampleBucketState(bucketId4)));
        // Delete two of the sample buckets. This should mean that these sample
        // buckets (and the corresponding meta-data) is empty now while the
        // other ones have not been touched.
        Futures.getUnchecked(databaseAccess.deleteSamples(bucketId3));
        Futures.getUnchecked(databaseAccess.deleteSamples(bucketId4));
        assertEquals(samples1, ImmutableList.copyOf(Futures
                .getUnchecked(databaseAccess.getSamples(bucketId1, 0L,
                        Long.MAX_VALUE, -1, false))));
        assertEquals(samples2, ImmutableList.copyOf(Futures
                .getUnchecked(databaseAccess.getSamples(bucketId2, 0L,
                        Long.MAX_VALUE, -1, false))));
        assertEquals(0, Iterables.size(Futures.getUnchecked(databaseAccess
                .getSamples(bucketId3, 0L, Long.MAX_VALUE, -1, false))));
        assertEquals(0, Iterables.size(Futures.getUnchecked(databaseAccess
                .getSamples(bucketId4, 0L, Long.MAX_VALUE, -1, false))));
        assertEquals(new SampleBucketState(123, 100L),
                Futures.getUnchecked(databaseAccess
                        .getSampleBucketState(bucketId1)));
        assertEquals(new SampleBucketState(321, 101L),
                Futures.getUnchecked(databaseAccess
                        .getSampleBucketState(bucketId2)));
        assertEquals(new SampleBucketState(0, 0L),
                Futures.getUnchecked(databaseAccess
                        .getSampleBucketState(bucketId3)));
        assertEquals(new SampleBucketState(0, 0L),
                Futures.getUnchecked(databaseAccess
                        .getSampleBucketState(bucketId4)));
        // Delete the remaining two sample buckets and check that they have been
        // deleted.
        Futures.getUnchecked(databaseAccess.deleteSamples(bucketId1));
        Futures.getUnchecked(databaseAccess.deleteSamples(bucketId2));
        assertEquals(0, Iterables.size(Futures.getUnchecked(databaseAccess
                .getSamples(bucketId1, 0L, Long.MAX_VALUE, -1, false))));
        assertEquals(0, Iterables.size(Futures.getUnchecked(databaseAccess
                .getSamples(bucketId2, 0L, Long.MAX_VALUE, -1, false))));
        assertEquals(new SampleBucketState(0, 0L),
                Futures.getUnchecked(databaseAccess
                        .getSampleBucketState(bucketId1)));
        assertEquals(new SampleBucketState(0, 0L),
                Futures.getUnchecked(databaseAccess
                        .getSampleBucketState(bucketId2)));
    }

    /**
     * Tests whether the state of a sample bucket is updated correctly when
     * writing a sample.
     */
    @Test
    public void testUpdateBucketState() {
        SampleBucketId bucketId = new SampleBucketId(UUID.randomUUID(), 0, 0L);
        // Before writing the first sample, the bucket's state should have
        // default values.
        assertEquals(new SampleBucketState(0, 0L),
                Futures.getUnchecked(databaseAccess
                        .getSampleBucketState(bucketId)));
        // When writing the first sample, the bucket's state should be updated
        // accordingly.
        Futures.getUnchecked(databaseAccess.writeSample(
                new ChannelAccessDisconnectedSample(155L, true), bucketId, 108));
        assertEquals(new SampleBucketState(108, 155L),
                Futures.getUnchecked(databaseAccess
                        .getSampleBucketState(bucketId)));
        // When writing a second sample, the bucket's state should be updated
        // accordingly.
        Futures.getUnchecked(databaseAccess
                .writeSample(new ChannelAccessDisconnectedSample(4013L, true),
                        bucketId, 270));
        assertEquals(new SampleBucketState(270, 4013L),
                Futures.getUnchecked(databaseAccess
                        .getSampleBucketState(bucketId)));
        // After deleting the sample bucket, the bucket's state should have
        // default values again.
        Futures.getUnchecked(databaseAccess.deleteSamples(bucketId));
        assertEquals(new SampleBucketState(0, 0L),
                Futures.getUnchecked(databaseAccess
                        .getSampleBucketState(bucketId)));
    }

}
