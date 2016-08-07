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

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;

import org.apache.commons.lang3.ArrayUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aquenos.cassandra.pvarchiver.controlsystem.channelaccess.ChannelAccessSample;
import com.aquenos.cassandra.pvarchiver.controlsystem.channelaccess.ChannelAccessSampleType;
import com.aquenos.cassandra.pvarchiver.tests.EmbeddedCassandraServer;
import com.aquenos.epics.jackie.common.value.ChannelAccessAlarmOnlyString;
import com.aquenos.epics.jackie.common.value.ChannelAccessAlarmSeverity;
import com.aquenos.epics.jackie.common.value.ChannelAccessAlarmStatus;
import com.aquenos.epics.jackie.common.value.ChannelAccessControlsChar;
import com.aquenos.epics.jackie.common.value.ChannelAccessControlsDouble;
import com.aquenos.epics.jackie.common.value.ChannelAccessControlsFloat;
import com.aquenos.epics.jackie.common.value.ChannelAccessControlsLong;
import com.aquenos.epics.jackie.common.value.ChannelAccessControlsShort;
import com.aquenos.epics.jackie.common.value.ChannelAccessGraphicsEnum;
import com.aquenos.epics.jackie.common.value.ChannelAccessTimeChar;
import com.aquenos.epics.jackie.common.value.ChannelAccessTimeDouble;
import com.aquenos.epics.jackie.common.value.ChannelAccessTimeEnum;
import com.aquenos.epics.jackie.common.value.ChannelAccessTimeFloat;
import com.aquenos.epics.jackie.common.value.ChannelAccessTimeLong;
import com.aquenos.epics.jackie.common.value.ChannelAccessTimeShort;
import com.aquenos.epics.jackie.common.value.ChannelAccessTimeString;
import com.aquenos.epics.jackie.common.value.ChannelAccessValueFactory;
import com.datastax.driver.core.Session;
import com.google.common.base.Charsets;

/**
 * Tests for the {@link ChannelAccessSampleDecimator}.
 * 
 * @author Sebastian Marsching
 */
public class ChannelAccessSampleDecimatorTest {

    private static final long EPICS_EPOCH_NANOSECONDS = 631152000000000000L;
    private static final long ONE_BILLION = 1000000000L;

    private static ChannelAccessSampleValueAccess sampleValueAccess;

    /**
     * Creates the {@link ChannelAccessSampleValueAccess} instance that is
     * needed for the tests.
     */
    @BeforeClass
    public static void initializeSampleValueAccess() {
        Session session = EmbeddedCassandraServer.getSession();
        sampleValueAccess = new ChannelAccessSampleValueAccess(session);
    }

    private static int archiveTimeStampToEpicsTimeStampNanoseconds(
            long archiveTimeStamp) {
        return (int) (archiveTimeStamp % ONE_BILLION);
    }

    private static int archiveTimeStampToEpicsTimeStampSeconds(
            long archiveTimeStamp) {
        return (int) ((archiveTimeStamp - EPICS_EPOCH_NANOSECONDS) / ONE_BILLION);
    }

    private static ChannelAccessSample calculateDecimatedSample(
            long intervalStartTime, long intervalLength,
            Iterable<ChannelAccessSample> sourceSamples) {
        ChannelAccessSampleDecimator decimator = new ChannelAccessSampleDecimator(
                "someChannel", intervalStartTime, intervalLength,
                sampleValueAccess);
        for (ChannelAccessSample sourceSample : sourceSamples) {
            decimator.processSample(sourceSample);
        }
        decimator.buildDecimatedSample();
        return decimator.getDecimatedSample();
    }

    private static ChannelAccessRawSample createScalarCharSample(
            long timeStamp, byte value,
            ChannelAccessAlarmSeverity alarmSeverity,
            ChannelAccessAlarmStatus alarmStatus, String units) {
        // In order to keep things simple, we only set the engineering units on
        // the controls value. The value elements and alarm state are not used
        // anyway. The limits are used, but for testing that the correct
        // meta-data is used, the engineering units are sufficient. We have
        // separate tests for verifying that the limits are serialized and
        // deserialized correctly.
        ChannelAccessControlsChar controlsValue = ChannelAccessValueFactory
                .createControlsChar(ArrayUtils.EMPTY_BYTE_ARRAY,
                        ChannelAccessAlarmStatus.NO_ALARM,
                        ChannelAccessAlarmSeverity.NO_ALARM, units, (byte) 0,
                        (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0,
                        (byte) 0, (byte) 0, Charsets.UTF_8);
        ChannelAccessTimeChar timeValue = ChannelAccessValueFactory
                .createTimeChar(new byte[] { value }, alarmStatus,
                        alarmSeverity,
                        archiveTimeStampToEpicsTimeStampSeconds(timeStamp),
                        archiveTimeStampToEpicsTimeStampNanoseconds(timeStamp));
        return sampleValueAccess.createRawSample(timeValue, controlsValue);
    }

    private static ChannelAccessRawSample createScalarDoubleSample(
            long timeStamp, double value,
            ChannelAccessAlarmSeverity alarmSeverity,
            ChannelAccessAlarmStatus alarmStatus, String units) {
        // In order to keep things simple, we only set the engineering units on
        // the controls value. The value elements and alarm state are not use
        // anyway. The limits are used, but for testing that the correct
        // meta-data is used, the engineering units are sufficient. We have
        // separate tests for verifying that the limits are serialized and
        // deserialized correctly.
        ChannelAccessControlsDouble controlsValue = ChannelAccessValueFactory
                .createControlsDouble(ArrayUtils.EMPTY_DOUBLE_ARRAY,
                        ChannelAccessAlarmStatus.NO_ALARM,
                        ChannelAccessAlarmSeverity.NO_ALARM, (short) 0, units,
                        0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, Charsets.UTF_8);
        ChannelAccessTimeDouble timeValue = ChannelAccessValueFactory
                .createTimeDouble(new double[] { value }, alarmStatus,
                        alarmSeverity,
                        archiveTimeStampToEpicsTimeStampSeconds(timeStamp),
                        archiveTimeStampToEpicsTimeStampNanoseconds(timeStamp));
        return sampleValueAccess.createRawSample(timeValue, controlsValue);
    }

    private static ChannelAccessRawSample createScalarFloatSample(
            long timeStamp, float value,
            ChannelAccessAlarmSeverity alarmSeverity,
            ChannelAccessAlarmStatus alarmStatus, String units) {
        // In order to keep things simple, we only set the engineering units on
        // the controls value. The value elements and alarm state are not used
        // anyway. The limits are used, but for testing that the correct
        // meta-data is used, the engineering units are sufficient. We have
        // separate tests for verifying that the limits are serialized and
        // deserialized correctly.
        ChannelAccessControlsFloat controlsValue = ChannelAccessValueFactory
                .createControlsFloat(ArrayUtils.EMPTY_FLOAT_ARRAY,
                        ChannelAccessAlarmStatus.NO_ALARM,
                        ChannelAccessAlarmSeverity.NO_ALARM, (short) 0, units,
                        0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f,
                        Charsets.UTF_8);
        ChannelAccessTimeFloat timeValue = ChannelAccessValueFactory
                .createTimeFloat(new float[] { value }, alarmStatus,
                        alarmSeverity,
                        archiveTimeStampToEpicsTimeStampSeconds(timeStamp),
                        archiveTimeStampToEpicsTimeStampNanoseconds(timeStamp));
        return sampleValueAccess.createRawSample(timeValue, controlsValue);
    }

    private static ChannelAccessRawSample createScalarEnumSample(
            long timeStamp, short value,
            ChannelAccessAlarmSeverity alarmSeverity,
            ChannelAccessAlarmStatus alarmStatus, String[] labels) {
        // In order to keep things simple, we only set the labels on the
        // controls value. The value elements and alarm state are not used
        // anyway.
        ChannelAccessGraphicsEnum controlsValue = ChannelAccessValueFactory
                .createControlsEnum(ArrayUtils.EMPTY_SHORT_ARRAY,
                        ChannelAccessAlarmStatus.NO_ALARM,
                        ChannelAccessAlarmSeverity.NO_ALARM,
                        Arrays.asList(labels), Charsets.UTF_8);
        ChannelAccessTimeEnum timeValue = ChannelAccessValueFactory
                .createTimeEnum(new short[] { value }, alarmStatus,
                        alarmSeverity,
                        archiveTimeStampToEpicsTimeStampSeconds(timeStamp),
                        archiveTimeStampToEpicsTimeStampNanoseconds(timeStamp));
        return sampleValueAccess.createRawSample(timeValue, controlsValue);
    }

    private static ChannelAccessRawSample createScalarLongSample(
            long timeStamp, int value,
            ChannelAccessAlarmSeverity alarmSeverity,
            ChannelAccessAlarmStatus alarmStatus, String units) {
        // In order to keep things simple, we only set the engineering units on
        // the controls value. The value elements and alarm state are not used
        // anyway. The limits are used, but for testing that the correct
        // meta-data is used, the engineering units are sufficient. We have
        // separate tests for verifying that the limits are serialized and
        // deserialized correctly.
        ChannelAccessControlsLong controlsValue = ChannelAccessValueFactory
                .createControlsLong(ArrayUtils.EMPTY_INT_ARRAY,
                        ChannelAccessAlarmStatus.NO_ALARM,
                        ChannelAccessAlarmSeverity.NO_ALARM, units, 0, 0, 0, 0,
                        0, 0, 0, 0, Charsets.UTF_8);
        ChannelAccessTimeLong timeValue = ChannelAccessValueFactory
                .createTimeLong(new int[] { value }, alarmStatus,
                        alarmSeverity,
                        archiveTimeStampToEpicsTimeStampSeconds(timeStamp),
                        archiveTimeStampToEpicsTimeStampNanoseconds(timeStamp));
        return sampleValueAccess.createRawSample(timeValue, controlsValue);
    }

    private static ChannelAccessRawSample createScalarShortSample(
            long timeStamp, short value,
            ChannelAccessAlarmSeverity alarmSeverity,
            ChannelAccessAlarmStatus alarmStatus, String units) {
        // In order to keep things simple, we only set the engineering units on
        // the controls value. The value elements and alarm state are not used
        // anyway. The limits are used, but for testing that the correct
        // meta-data is used, the engineering units are sufficient. We have
        // separate tests for verifying that the limits are serialized and
        // deserialized correctly.
        ChannelAccessControlsShort controlsValue = ChannelAccessValueFactory
                .createControlsShort(ArrayUtils.EMPTY_SHORT_ARRAY,
                        ChannelAccessAlarmStatus.NO_ALARM,
                        ChannelAccessAlarmSeverity.NO_ALARM, units, (short) 0,
                        (short) 0, (short) 0, (short) 0, (short) 0, (short) 0,
                        (short) 0, (short) 0, Charsets.UTF_8);
        ChannelAccessTimeShort timeValue = ChannelAccessValueFactory
                .createTimeShort(new short[] { value }, alarmStatus,
                        alarmSeverity,
                        archiveTimeStampToEpicsTimeStampSeconds(timeStamp),
                        archiveTimeStampToEpicsTimeStampNanoseconds(timeStamp));
        return sampleValueAccess.createRawSample(timeValue, controlsValue);
    }

    private static ChannelAccessRawSample createScalarStringSample(
            long timeStamp, String value,
            ChannelAccessAlarmSeverity alarmSeverity,
            ChannelAccessAlarmStatus alarmStatus) {
        // We only create the controls value because it is mandatory to have
        // one. Neither the alarm state nor the value is actually used.
        ChannelAccessAlarmOnlyString controlsValue = ChannelAccessValueFactory
                .createControlsString(Collections.<String> emptyList(),
                        ChannelAccessAlarmStatus.NO_ALARM,
                        ChannelAccessAlarmSeverity.NO_ALARM, Charsets.UTF_8);
        ChannelAccessTimeString timeValue = ChannelAccessValueFactory
                .createTimeString(Collections.singletonList(value),
                        alarmStatus, alarmSeverity,
                        archiveTimeStampToEpicsTimeStampSeconds(timeStamp),
                        archiveTimeStampToEpicsTimeStampNanoseconds(timeStamp),
                        Charsets.UTF_8);
        return sampleValueAccess.createRawSample(timeValue, controlsValue);
    }

    private static void verifyAggregatedScalarCharSample(
            ChannelAccessAggregatedSample actualSample, long expectedTimeStamp,
            double expectedMean, double expectedStd, byte expectedMin,
            byte expectedMax, ChannelAccessAlarmSeverity expectedAlarmSeverity,
            ChannelAccessAlarmStatus expectedAlarmStatus,
            double expectedCoveredPeriodFraction,
            ChannelAccessUdtSample expectedMetaData) {
        assertEquals(ChannelAccessSampleType.AGGREGATED_SCALAR_CHAR,
                actualSample.getType());
        assertEquals(expectedTimeStamp, actualSample.getTimeStamp());
        assertEquals(expectedMean,
                sampleValueAccess.deserializeMeanColumn(actualSample), 0.01);
        assertEquals(expectedStd,
                sampleValueAccess.deserializeStdColumn(actualSample), 0.01);
        assertEquals(expectedMin,
                sampleValueAccess.deserializeByteMinColumn(actualSample));
        assertEquals(expectedMax,
                sampleValueAccess.deserializeByteMaxColumn(actualSample));
        assertEquals(expectedCoveredPeriodFraction,
                sampleValueAccess
                        .deserializeCoveredPeriodFractionColumn(actualSample),
                0.01);
        ChannelAccessControlsChar expectedControlsValue;
        if (expectedMetaData instanceof ChannelAccessAggregatedSample) {
            expectedControlsValue = (ChannelAccessControlsChar) sampleValueAccess
                    .deserializeAggregatedSampleMetaData((ChannelAccessAggregatedSample) expectedMetaData);
        } else {
            expectedControlsValue = (ChannelAccessControlsChar) sampleValueAccess
                    .deserializeSample((ChannelAccessRawSample) expectedMetaData);
        }
        expectedControlsValue.setValue(ArrayUtils.EMPTY_BYTE_ARRAY);
        expectedControlsValue.setAlarmSeverity(expectedAlarmSeverity);
        expectedControlsValue.setAlarmStatus(expectedAlarmStatus);
        assertEquals(expectedControlsValue,
                sampleValueAccess
                        .deserializeAggregatedSampleMetaData(actualSample));
    }

    private static void verifyAggregatedScalarDoubleSample(
            ChannelAccessAggregatedSample actualSample, long expectedTimeStamp,
            double expectedMean, double expectedStd, double expectedMin,
            double expectedMax,
            ChannelAccessAlarmSeverity expectedAlarmSeverity,
            ChannelAccessAlarmStatus expectedAlarmStatus,
            double expectedCoveredPeriodFraction,
            ChannelAccessUdtSample expectedMetaData) {
        assertEquals(ChannelAccessSampleType.AGGREGATED_SCALAR_DOUBLE,
                actualSample.getType());
        assertEquals(expectedTimeStamp, actualSample.getTimeStamp());
        assertEquals(expectedMean,
                sampleValueAccess.deserializeMeanColumn(actualSample), 0.01);
        assertEquals(expectedStd,
                sampleValueAccess.deserializeStdColumn(actualSample), 0.01);
        assertEquals(expectedMin,
                sampleValueAccess.deserializeDoubleMinColumn(actualSample), 0.0);
        assertEquals(expectedMax,
                sampleValueAccess.deserializeDoubleMaxColumn(actualSample), 0.0);
        assertEquals(expectedCoveredPeriodFraction,
                sampleValueAccess
                        .deserializeCoveredPeriodFractionColumn(actualSample),
                0.01);
        ChannelAccessControlsDouble expectedControlsValue;
        if (expectedMetaData instanceof ChannelAccessAggregatedSample) {
            expectedControlsValue = (ChannelAccessControlsDouble) sampleValueAccess
                    .deserializeAggregatedSampleMetaData((ChannelAccessAggregatedSample) expectedMetaData);
        } else {
            expectedControlsValue = (ChannelAccessControlsDouble) sampleValueAccess
                    .deserializeSample((ChannelAccessRawSample) expectedMetaData);
        }
        expectedControlsValue.setValue(ArrayUtils.EMPTY_DOUBLE_ARRAY);
        expectedControlsValue.setAlarmSeverity(expectedAlarmSeverity);
        expectedControlsValue.setAlarmStatus(expectedAlarmStatus);
        assertEquals(expectedControlsValue,
                sampleValueAccess
                        .deserializeAggregatedSampleMetaData(actualSample));
    }

    private static void verifyAggregatedScalarFloatSample(
            ChannelAccessAggregatedSample actualSample, long expectedTimeStamp,
            double expectedMean, double expectedStd, float expectedMin,
            float expectedMax,
            ChannelAccessAlarmSeverity expectedAlarmSeverity,
            ChannelAccessAlarmStatus expectedAlarmStatus,
            double expectedCoveredPeriodFraction,
            ChannelAccessUdtSample expectedMetaData) {
        assertEquals(ChannelAccessSampleType.AGGREGATED_SCALAR_FLOAT,
                actualSample.getType());
        assertEquals(expectedTimeStamp, actualSample.getTimeStamp());
        assertEquals(expectedMean,
                sampleValueAccess.deserializeMeanColumn(actualSample), 0.01);
        assertEquals(expectedStd,
                sampleValueAccess.deserializeStdColumn(actualSample), 0.01);
        assertEquals(expectedMin,
                sampleValueAccess.deserializeFloatMinColumn(actualSample), 0.0f);
        assertEquals(expectedMax,
                sampleValueAccess.deserializeFloatMaxColumn(actualSample), 0.0f);
        assertEquals(expectedCoveredPeriodFraction,
                sampleValueAccess
                        .deserializeCoveredPeriodFractionColumn(actualSample),
                0.01);
        ChannelAccessControlsFloat expectedControlsValue;
        if (expectedMetaData instanceof ChannelAccessAggregatedSample) {
            expectedControlsValue = (ChannelAccessControlsFloat) sampleValueAccess
                    .deserializeAggregatedSampleMetaData((ChannelAccessAggregatedSample) expectedMetaData);
        } else {
            expectedControlsValue = (ChannelAccessControlsFloat) sampleValueAccess
                    .deserializeSample((ChannelAccessRawSample) expectedMetaData);
        }
        expectedControlsValue.setValue(ArrayUtils.EMPTY_FLOAT_ARRAY);
        expectedControlsValue.setAlarmSeverity(expectedAlarmSeverity);
        expectedControlsValue.setAlarmStatus(expectedAlarmStatus);
        assertEquals(expectedControlsValue,
                sampleValueAccess
                        .deserializeAggregatedSampleMetaData(actualSample));
    }

    private static void verifyAggregatedScalarLongSample(
            ChannelAccessAggregatedSample actualSample, long expectedTimeStamp,
            double expectedMean, double expectedStd, int expectedMin,
            int expectedMax, ChannelAccessAlarmSeverity expectedAlarmSeverity,
            ChannelAccessAlarmStatus expectedAlarmStatus,
            double expectedCoveredPeriodFraction,
            ChannelAccessUdtSample expectedMetaData) {
        assertEquals(ChannelAccessSampleType.AGGREGATED_SCALAR_LONG,
                actualSample.getType());
        assertEquals(expectedTimeStamp, actualSample.getTimeStamp());
        assertEquals(expectedMean,
                sampleValueAccess.deserializeMeanColumn(actualSample), 0.01);
        assertEquals(expectedStd,
                sampleValueAccess.deserializeStdColumn(actualSample), 0.01);
        assertEquals(expectedMin,
                sampleValueAccess.deserializeLongMinColumn(actualSample));
        assertEquals(expectedMax,
                sampleValueAccess.deserializeLongMaxColumn(actualSample));
        assertEquals(expectedCoveredPeriodFraction,
                sampleValueAccess
                        .deserializeCoveredPeriodFractionColumn(actualSample),
                0.01);
        ChannelAccessControlsLong expectedControlsValue;
        if (expectedMetaData instanceof ChannelAccessAggregatedSample) {
            expectedControlsValue = (ChannelAccessControlsLong) sampleValueAccess
                    .deserializeAggregatedSampleMetaData((ChannelAccessAggregatedSample) expectedMetaData);
        } else {
            expectedControlsValue = (ChannelAccessControlsLong) sampleValueAccess
                    .deserializeSample((ChannelAccessRawSample) expectedMetaData);
        }
        expectedControlsValue.setValue(ArrayUtils.EMPTY_INT_ARRAY);
        expectedControlsValue.setAlarmSeverity(expectedAlarmSeverity);
        expectedControlsValue.setAlarmStatus(expectedAlarmStatus);
        assertEquals(expectedControlsValue,
                sampleValueAccess
                        .deserializeAggregatedSampleMetaData(actualSample));
    }

    private static void verifyAggregatedScalarShortSample(
            ChannelAccessAggregatedSample actualSample, long expectedTimeStamp,
            double expectedMean, double expectedStd, short expectedMin,
            short expectedMax,
            ChannelAccessAlarmSeverity expectedAlarmSeverity,
            ChannelAccessAlarmStatus expectedAlarmStatus,
            double expectedCoveredPeriodFraction,
            ChannelAccessUdtSample expectedMetaData) {
        assertEquals(ChannelAccessSampleType.AGGREGATED_SCALAR_SHORT,
                actualSample.getType());
        assertEquals(expectedTimeStamp, actualSample.getTimeStamp());
        assertEquals(expectedMean,
                sampleValueAccess.deserializeMeanColumn(actualSample), 0.01);
        assertEquals(expectedStd,
                sampleValueAccess.deserializeStdColumn(actualSample), 0.01);
        assertEquals(expectedMin,
                sampleValueAccess.deserializeShortMinColumn(actualSample));
        assertEquals(expectedMax,
                sampleValueAccess.deserializeShortMaxColumn(actualSample));
        assertEquals(expectedCoveredPeriodFraction,
                sampleValueAccess
                        .deserializeCoveredPeriodFractionColumn(actualSample),
                0.01);
        ChannelAccessControlsShort expectedControlsValue;
        if (expectedMetaData instanceof ChannelAccessAggregatedSample) {
            expectedControlsValue = (ChannelAccessControlsShort) sampleValueAccess
                    .deserializeAggregatedSampleMetaData((ChannelAccessAggregatedSample) expectedMetaData);
        } else {
            expectedControlsValue = (ChannelAccessControlsShort) sampleValueAccess
                    .deserializeSample((ChannelAccessRawSample) expectedMetaData);
        }
        expectedControlsValue.setValue(ArrayUtils.EMPTY_SHORT_ARRAY);
        expectedControlsValue.setAlarmSeverity(expectedAlarmSeverity);
        expectedControlsValue.setAlarmStatus(expectedAlarmStatus);
        assertEquals(expectedControlsValue,
                sampleValueAccess
                        .deserializeAggregatedSampleMetaData(actualSample));
    }

    /**
     * Runs some basic tests with source samples of type
     * <code>AGGREGATED_SCALAR_CHAR</code>:
     */
    @Test
    public void testAggregatedScalarChar() {
        ChannelAccessRawSample metaDataSample = createScalarCharSample(
                EPICS_EPOCH_NANOSECONDS, (byte) 0,
                ChannelAccessAlarmSeverity.NO_ALARM,
                ChannelAccessAlarmStatus.NO_ALARM, "abc");
        ChannelAccessRawSample otherMetaDataSample = createScalarCharSample(
                EPICS_EPOCH_NANOSECONDS, (byte) 0,
                ChannelAccessAlarmSeverity.NO_ALARM,
                ChannelAccessAlarmStatus.NO_ALARM, "def");
        LinkedList<ChannelAccessSample> sourceSamples = new LinkedList<ChannelAccessSample>();
        ChannelAccessAggregatedSample decimatedSample;
        // Run the test with exactly one sample that has a time-stamp before the
        // period for which a decimated sample shall be calculated.
        sourceSamples.clear();
        sourceSamples.add(sampleValueAccess.createAggregatedScalarCharSample(
                79L * ONE_BILLION, 22.3, 4.71, (byte) -5, (byte) 99,
                ChannelAccessAlarmSeverity.MINOR_ALARM,
                ChannelAccessAlarmStatus.HIGH, 0.9, metaDataSample));
        decimatedSample = (ChannelAccessAggregatedSample) calculateDecimatedSample(
                90L * ONE_BILLION, 30L * ONE_BILLION, sourceSamples);
        verifyAggregatedScalarCharSample(decimatedSample, 90L * ONE_BILLION,
                22.3, 4.71, (byte) -5, (byte) 99,
                ChannelAccessAlarmSeverity.MINOR_ALARM,
                ChannelAccessAlarmStatus.HIGH, 0.9, metaDataSample);
        // Run the test with exactly one sample that has a time-stamp exactly at
        // the edge of the period for which a decimated sample shall be
        // calculated.
        sourceSamples.clear();
        sourceSamples.add(sampleValueAccess.createAggregatedScalarCharSample(
                600L * ONE_BILLION, -44.74, 22.55, (byte) -17, (byte) 127,
                ChannelAccessAlarmSeverity.NO_ALARM,
                ChannelAccessAlarmStatus.NO_ALARM, 1.0, metaDataSample));
        decimatedSample = (ChannelAccessAggregatedSample) calculateDecimatedSample(
                600L * ONE_BILLION, 300L * ONE_BILLION, sourceSamples);
        verifyAggregatedScalarCharSample(decimatedSample, 600L * ONE_BILLION,
                -44.74, 22.55, (byte) -17, (byte) 127,
                ChannelAccessAlarmSeverity.NO_ALARM,
                ChannelAccessAlarmStatus.NO_ALARM, 1.0, metaDataSample);
        // Run the test with three samples in order to see that the aggregation
        // process works correctly.
        sourceSamples.clear();
        sourceSamples.add(sampleValueAccess.createAggregatedScalarCharSample(
                57L * ONE_BILLION, 40.0, 5.0, (byte) -17, (byte) 66,
                ChannelAccessAlarmSeverity.NO_ALARM,
                ChannelAccessAlarmStatus.NO_ALARM, 1.0, metaDataSample));
        sourceSamples.add(sampleValueAccess.createAggregatedScalarCharSample(
                125L * ONE_BILLION, 0.0, 2.0, (byte) -64, (byte) 15,
                ChannelAccessAlarmSeverity.INVALID_ALARM,
                ChannelAccessAlarmStatus.COMM, 0.5, otherMetaDataSample));
        sourceSamples.add(sampleValueAccess.createAggregatedScalarCharSample(
                175L * ONE_BILLION, 20.0, 3.0, (byte) 5, (byte) 95,
                ChannelAccessAlarmSeverity.MINOR_ALARM,
                ChannelAccessAlarmStatus.LOW, 1.0, otherMetaDataSample));
        decimatedSample = (ChannelAccessAggregatedSample) calculateDecimatedSample(
                100L * ONE_BILLION, 100L * ONE_BILLION, sourceSamples);
        verifyAggregatedScalarCharSample(decimatedSample, 100L * ONE_BILLION,
                20.0, 16.7132, (byte) -64, (byte) 95,
                ChannelAccessAlarmSeverity.INVALID_ALARM,
                ChannelAccessAlarmStatus.COMM, 0.75, metaDataSample);
    }

    /**
     * Runs some basic tests with source samples of type
     * <code>AGGREGATED_SCALAR_DOUBLE</code>:
     */
    @Test
    public void testAggregatedScalarDouble() {
        ChannelAccessRawSample metaDataSample = createScalarDoubleSample(0L,
                0.0, ChannelAccessAlarmSeverity.NO_ALARM,
                ChannelAccessAlarmStatus.NO_ALARM, "Abc");
        ChannelAccessRawSample otherMetaDataSample = createScalarDoubleSample(
                0L, 0.0, ChannelAccessAlarmSeverity.NO_ALARM,
                ChannelAccessAlarmStatus.NO_ALARM, "zyx");
        LinkedList<ChannelAccessSample> sourceSamples = new LinkedList<ChannelAccessSample>();
        ChannelAccessAggregatedSample decimatedSample;
        // Run the test with exactly one sample that has a time-stamp before the
        // period for which a decimated sample shall be calculated.
        sourceSamples.clear();
        sourceSamples.add(sampleValueAccess.createAggregatedScalarDoubleSample(
                79L * ONE_BILLION, 22.3, 4.71, -15.0, 199.0,
                ChannelAccessAlarmSeverity.NO_ALARM,
                ChannelAccessAlarmStatus.NO_ALARM, 1.0, metaDataSample));
        decimatedSample = (ChannelAccessAggregatedSample) calculateDecimatedSample(
                90L * ONE_BILLION, 30L * ONE_BILLION, sourceSamples);
        verifyAggregatedScalarDoubleSample(decimatedSample, 90L * ONE_BILLION,
                22.3, 4.71, -15.0, 199.0, ChannelAccessAlarmSeverity.NO_ALARM,
                ChannelAccessAlarmStatus.NO_ALARM, 1.0, metaDataSample);
        // Run the test with exactly one sample that has a time-stamp exactly at
        // the edge of the period for which a decimated sample shall be
        // calculated.
        sourceSamples.clear();
        sourceSamples.add(sampleValueAccess.createAggregatedScalarDoubleSample(
                600L * ONE_BILLION, -44.74, 22.55, -17.0, 127.0,
                ChannelAccessAlarmSeverity.INVALID_ALARM,
                ChannelAccessAlarmStatus.LINK, 0.99, otherMetaDataSample));
        decimatedSample = (ChannelAccessAggregatedSample) calculateDecimatedSample(
                600L * ONE_BILLION, 300L * ONE_BILLION, sourceSamples);
        verifyAggregatedScalarDoubleSample(decimatedSample, 600L * ONE_BILLION,
                -44.74, 22.55, -17.0, 127.0,
                ChannelAccessAlarmSeverity.INVALID_ALARM,
                ChannelAccessAlarmStatus.LINK, 0.99, otherMetaDataSample);
        // Run the test with three samples in order to see that the aggregation
        // process works correctly.
        sourceSamples.clear();
        sourceSamples.add(sampleValueAccess.createAggregatedScalarDoubleSample(
                57L * ONE_BILLION, 40.0, 5.0, -17.0, 66.0,
                ChannelAccessAlarmSeverity.NO_ALARM,
                ChannelAccessAlarmStatus.NO_ALARM, 1.0, metaDataSample));
        sourceSamples.add(sampleValueAccess.createAggregatedScalarDoubleSample(
                125L * ONE_BILLION, 0.0, 2.0, -64.0, 15.0,
                ChannelAccessAlarmSeverity.MINOR_ALARM,
                ChannelAccessAlarmStatus.COMM, 0.5, otherMetaDataSample));
        sourceSamples.add(sampleValueAccess.createAggregatedScalarDoubleSample(
                175L * ONE_BILLION, 20.0, 3.0, 5.0, 95.0,
                ChannelAccessAlarmSeverity.MINOR_ALARM,
                ChannelAccessAlarmStatus.LOW, 1.0, otherMetaDataSample));
        decimatedSample = (ChannelAccessAggregatedSample) calculateDecimatedSample(
                100L * ONE_BILLION, 100L * ONE_BILLION, sourceSamples);
        verifyAggregatedScalarDoubleSample(decimatedSample, 100L * ONE_BILLION,
                20.0, 16.7132, -64.0, 95.0,
                ChannelAccessAlarmSeverity.MINOR_ALARM,
                ChannelAccessAlarmStatus.COMM, 0.75, metaDataSample);
    }

    /**
     * Runs some basic tests with source samples of type
     * <code>AGGREGATED_SCALAR_FLOAT</code>:
     */
    @Test
    public void testAggregatedScalarFloat() {
        ChannelAccessRawSample metaDataSample = createScalarFloatSample(0L,
                0.0f, ChannelAccessAlarmSeverity.NO_ALARM,
                ChannelAccessAlarmStatus.NO_ALARM, "xyz");
        ChannelAccessRawSample otherMetaDataSample = createScalarFloatSample(
                0L, 0.0f, ChannelAccessAlarmSeverity.NO_ALARM,
                ChannelAccessAlarmStatus.NO_ALARM, "12345");
        LinkedList<ChannelAccessSample> sourceSamples = new LinkedList<ChannelAccessSample>();
        ChannelAccessAggregatedSample decimatedSample;
        // Run the test with exactly one sample that has a time-stamp before the
        // period for which a decimated sample shall be calculated.
        sourceSamples.clear();
        sourceSamples.add(sampleValueAccess.createAggregatedScalarFloatSample(
                79L * ONE_BILLION, 22.3, 4.71, -15.0f, 199.0f,
                ChannelAccessAlarmSeverity.MINOR_ALARM,
                ChannelAccessAlarmStatus.LOW, 0.125, metaDataSample));
        decimatedSample = (ChannelAccessAggregatedSample) calculateDecimatedSample(
                90L * ONE_BILLION, 30L * ONE_BILLION, sourceSamples);
        verifyAggregatedScalarFloatSample(decimatedSample, 90L * ONE_BILLION,
                22.3, 4.71, -15.0f, 199.0f,
                ChannelAccessAlarmSeverity.MINOR_ALARM,
                ChannelAccessAlarmStatus.LOW, 0.125, metaDataSample);
        // Run the test with exactly one sample that has a time-stamp exactly at
        // the edge of the period for which a decimated sample shall be
        // calculated.
        sourceSamples.clear();
        sourceSamples.add(sampleValueAccess.createAggregatedScalarFloatSample(
                600L * ONE_BILLION, -44.74, 22.55, -17.0f, 127.0f,
                ChannelAccessAlarmSeverity.NO_ALARM,
                ChannelAccessAlarmStatus.NO_ALARM, 1.0, otherMetaDataSample));
        decimatedSample = (ChannelAccessAggregatedSample) calculateDecimatedSample(
                600L * ONE_BILLION, 300L * ONE_BILLION, sourceSamples);
        verifyAggregatedScalarFloatSample(decimatedSample, 600L * ONE_BILLION,
                -44.74, 22.55, -17.0f, 127.0f,
                ChannelAccessAlarmSeverity.NO_ALARM,
                ChannelAccessAlarmStatus.NO_ALARM, 1.0, otherMetaDataSample);
        // Run the test with three samples in order to see that the aggregation
        // process works correctly.
        sourceSamples.clear();
        sourceSamples.add(sampleValueAccess.createAggregatedScalarFloatSample(
                57L * ONE_BILLION, 40.0, 5.0, -17.0f, 66.0f,
                ChannelAccessAlarmSeverity.NO_ALARM,
                ChannelAccessAlarmStatus.NO_ALARM, 1.0, metaDataSample));
        sourceSamples.add(sampleValueAccess.createAggregatedScalarFloatSample(
                125L * ONE_BILLION, 0.0, 2.0, -64.0f, 15.0f,
                ChannelAccessAlarmSeverity.MINOR_ALARM,
                ChannelAccessAlarmStatus.COMM, 1.0, otherMetaDataSample));
        sourceSamples.add(sampleValueAccess.createAggregatedScalarFloatSample(
                175L * ONE_BILLION, 20.0, 3.0, 5.0f, 95.0f,
                ChannelAccessAlarmSeverity.MINOR_ALARM,
                ChannelAccessAlarmStatus.LOW, 1.0, otherMetaDataSample));
        decimatedSample = (ChannelAccessAggregatedSample) calculateDecimatedSample(
                100L * ONE_BILLION, 100L * ONE_BILLION, sourceSamples);
        verifyAggregatedScalarFloatSample(decimatedSample, 100L * ONE_BILLION,
                15.0, 16.8967, -64.0f, 95.0f,
                ChannelAccessAlarmSeverity.MINOR_ALARM,
                ChannelAccessAlarmStatus.COMM, 1.0, metaDataSample);
    }

    /**
     * Runs some basic tests with source samples of type
     * <code>AGGREGATED_SCALAR_LONG</code>:
     */
    @Test
    public void testAggregatedScalarLong() {
        ChannelAccessRawSample metaDataSample = createScalarLongSample(0L, 0,
                ChannelAccessAlarmSeverity.NO_ALARM,
                ChannelAccessAlarmStatus.NO_ALARM, "A");
        ChannelAccessRawSample otherMetaDataSample = createScalarLongSample(0L,
                0, ChannelAccessAlarmSeverity.NO_ALARM,
                ChannelAccessAlarmStatus.NO_ALARM, "V");
        LinkedList<ChannelAccessSample> sourceSamples = new LinkedList<ChannelAccessSample>();
        ChannelAccessAggregatedSample decimatedSample;
        // Run the test with exactly one sample that has a time-stamp before the
        // period for which a decimated sample shall be calculated.
        sourceSamples.clear();
        sourceSamples.add(sampleValueAccess.createAggregatedScalarLongSample(
                79L * ONE_BILLION, 22.3, 4.71, -5, 99,
                ChannelAccessAlarmSeverity.INVALID_ALARM,
                ChannelAccessAlarmStatus.WRITE, 0.9, metaDataSample));
        decimatedSample = (ChannelAccessAggregatedSample) calculateDecimatedSample(
                90L * ONE_BILLION, 30L * ONE_BILLION, sourceSamples);
        verifyAggregatedScalarLongSample(decimatedSample, 90L * ONE_BILLION,
                22.3, 4.71, -5, 99, ChannelAccessAlarmSeverity.INVALID_ALARM,
                ChannelAccessAlarmStatus.WRITE, 0.9, metaDataSample);
        // Run the test with exactly one sample that has a time-stamp exactly at
        // the edge of the period for which a decimated sample shall be
        // calculated.
        sourceSamples.clear();
        sourceSamples.add(sampleValueAccess.createAggregatedScalarLongSample(
                600L * ONE_BILLION, -44.74, 22.55, Integer.MIN_VALUE,
                Integer.MAX_VALUE, ChannelAccessAlarmSeverity.NO_ALARM,
                ChannelAccessAlarmStatus.NO_ALARM, 1.0, metaDataSample));
        decimatedSample = (ChannelAccessAggregatedSample) calculateDecimatedSample(
                600L * ONE_BILLION, 300L * ONE_BILLION, sourceSamples);
        verifyAggregatedScalarLongSample(decimatedSample, 600L * ONE_BILLION,
                -44.74, 22.55, Integer.MIN_VALUE, Integer.MAX_VALUE,
                ChannelAccessAlarmSeverity.NO_ALARM,
                ChannelAccessAlarmStatus.NO_ALARM, 1.0, metaDataSample);
        // Run the test with three samples in order to see that the aggregation
        // process works correctly.
        sourceSamples.clear();
        sourceSamples.add(sampleValueAccess.createAggregatedScalarLongSample(
                57L * ONE_BILLION, 40.0, 5.0, -17, 66,
                ChannelAccessAlarmSeverity.NO_ALARM,
                ChannelAccessAlarmStatus.NO_ALARM, 1.0, metaDataSample));
        sourceSamples.add(sampleValueAccess.createAggregatedScalarLongSample(
                125L * ONE_BILLION, 0.0, 2.0, -64, 15,
                ChannelAccessAlarmSeverity.INVALID_ALARM,
                ChannelAccessAlarmStatus.COMM, 0.5, otherMetaDataSample));
        sourceSamples.add(sampleValueAccess.createAggregatedScalarLongSample(
                175L * ONE_BILLION, 20.0, 3.0, 5, 95,
                ChannelAccessAlarmSeverity.MINOR_ALARM,
                ChannelAccessAlarmStatus.LOW, 1.0, otherMetaDataSample));
        decimatedSample = (ChannelAccessAggregatedSample) calculateDecimatedSample(
                100L * ONE_BILLION, 100L * ONE_BILLION, sourceSamples);
        verifyAggregatedScalarLongSample(decimatedSample, 100L * ONE_BILLION,
                20.0, 16.7132, -64, 95,
                ChannelAccessAlarmSeverity.INVALID_ALARM,
                ChannelAccessAlarmStatus.COMM, 0.75, metaDataSample);
    }

    /**
     * Runs some basic tests with source samples of type
     * <code>AGGREGATED_SCALAR_SHORT</code>:
     */
    @Test
    public void testAggregatedScalarShort() {
        ChannelAccessRawSample metaDataSample = createScalarShortSample(0L,
                (short) 0, ChannelAccessAlarmSeverity.NO_ALARM,
                ChannelAccessAlarmStatus.NO_ALARM, "abc");
        ChannelAccessRawSample otherMetaDataSample = createScalarShortSample(
                0L, (short) 0, ChannelAccessAlarmSeverity.NO_ALARM,
                ChannelAccessAlarmStatus.NO_ALARM, "def");
        LinkedList<ChannelAccessSample> sourceSamples = new LinkedList<ChannelAccessSample>();
        ChannelAccessAggregatedSample decimatedSample;
        // Run the test with exactly one sample that has a time-stamp before the
        // period for which a decimated sample shall be calculated.
        sourceSamples.clear();
        sourceSamples.add(sampleValueAccess.createAggregatedScalarShortSample(
                79L * ONE_BILLION, 22.3, 4.71, (short) -5, (short) 99,
                ChannelAccessAlarmSeverity.MINOR_ALARM,
                ChannelAccessAlarmStatus.HIGH, 0.9, metaDataSample));
        decimatedSample = (ChannelAccessAggregatedSample) calculateDecimatedSample(
                90L * ONE_BILLION, 30L * ONE_BILLION, sourceSamples);
        verifyAggregatedScalarShortSample(decimatedSample, 90L * ONE_BILLION,
                22.3, 4.71, (short) -5, (short) 99,
                ChannelAccessAlarmSeverity.MINOR_ALARM,
                ChannelAccessAlarmStatus.HIGH, 0.9, metaDataSample);
        // Run the test with exactly one sample that has a time-stamp exactly at
        // the edge of the period for which a decimated sample shall be
        // calculated.
        sourceSamples.clear();
        sourceSamples.add(sampleValueAccess.createAggregatedScalarShortSample(
                600L * ONE_BILLION, -44.74, 22.55, (short) -17, (short) 127,
                ChannelAccessAlarmSeverity.NO_ALARM,
                ChannelAccessAlarmStatus.NO_ALARM, 1.0, metaDataSample));
        decimatedSample = (ChannelAccessAggregatedSample) calculateDecimatedSample(
                600L * ONE_BILLION, 300L * ONE_BILLION, sourceSamples);
        verifyAggregatedScalarShortSample(decimatedSample, 600L * ONE_BILLION,
                -44.74, 22.55, (short) -17, (short) 127,
                ChannelAccessAlarmSeverity.NO_ALARM,
                ChannelAccessAlarmStatus.NO_ALARM, 1.0, metaDataSample);
        // Run the test with three samples in order to see that the aggregation
        // process works correctly.
        sourceSamples.clear();
        sourceSamples.add(sampleValueAccess.createAggregatedScalarShortSample(
                57L * ONE_BILLION, 40.0, 5.0, (short) -17, (short) 66,
                ChannelAccessAlarmSeverity.NO_ALARM,
                ChannelAccessAlarmStatus.NO_ALARM, 1.0, metaDataSample));
        sourceSamples.add(sampleValueAccess.createAggregatedScalarShortSample(
                125L * ONE_BILLION, 0.0, 2.0, (short) -64, (short) 15,
                ChannelAccessAlarmSeverity.NO_ALARM,
                ChannelAccessAlarmStatus.NO_ALARM, 0.5, otherMetaDataSample));
        sourceSamples.add(sampleValueAccess.createAggregatedScalarShortSample(
                175L * ONE_BILLION, 20.0, 3.0, (short) 5, (short) 95,
                ChannelAccessAlarmSeverity.NO_ALARM,
                ChannelAccessAlarmStatus.NO_ALARM, 1.0, otherMetaDataSample));
        decimatedSample = (ChannelAccessAggregatedSample) calculateDecimatedSample(
                100L * ONE_BILLION, 100L * ONE_BILLION, sourceSamples);
        verifyAggregatedScalarShortSample(decimatedSample, 100L * ONE_BILLION,
                20.0, 16.7132, (short) -64, (short) 95,
                ChannelAccessAlarmSeverity.NO_ALARM,
                ChannelAccessAlarmStatus.NO_ALARM, 0.75, metaDataSample);
    }

    /**
     * Tests mixed aggregatable and disabled samples. This test verifies that if
     * a period contains disabled samples and samples of different aggregatable
     * types, the aggregatable type that covers the greatest fraction of the
     * interval is used.
     */
    @Test
    public void testMixedAggregatableAndDisabled() {
        LinkedList<ChannelAccessSample> sourceSamples = new LinkedList<ChannelAccessSample>();
        ChannelAccessRawSample firstUsedSourceSample;
        ChannelAccessAggregatedSample decimatedSample;
        sourceSamples.add(new ChannelAccessDisabledSample(99L, true));
        firstUsedSourceSample = createScalarDoubleSample(
                EPICS_EPOCH_NANOSECONDS + 110L * ONE_BILLION, 50.0,
                ChannelAccessAlarmSeverity.NO_ALARM,
                ChannelAccessAlarmStatus.NO_ALARM, "xyz");
        sourceSamples.add(firstUsedSourceSample);
        sourceSamples.add(createScalarLongSample(EPICS_EPOCH_NANOSECONDS + 120L
                * ONE_BILLION, 42, ChannelAccessAlarmSeverity.MAJOR_ALARM,
                ChannelAccessAlarmStatus.HIHI, ""));
        sourceSamples.add(createScalarDoubleSample(EPICS_EPOCH_NANOSECONDS
                + 150L * ONE_BILLION, 20.0,
                ChannelAccessAlarmSeverity.MINOR_ALARM,
                ChannelAccessAlarmStatus.LOW, "abc"));
        sourceSamples.add(new ChannelAccessDisabledSample(
                EPICS_EPOCH_NANOSECONDS + 170L * ONE_BILLION, true));
        decimatedSample = (ChannelAccessAggregatedSample) calculateDecimatedSample(
                EPICS_EPOCH_NANOSECONDS + 100L * ONE_BILLION,
                100L * ONE_BILLION, sourceSamples);
        verifyAggregatedScalarDoubleSample(decimatedSample,
                EPICS_EPOCH_NANOSECONDS + 100L * ONE_BILLION, 30.0, 14.1421,
                20.0, 50.0, ChannelAccessAlarmSeverity.MINOR_ALARM,
                ChannelAccessAlarmStatus.LOW, 0.3, firstUsedSourceSample);
    }

    /**
     * Tests mixed aggregatable and disconnected samples. This test verifies
     * that if a period contains disconnected samples and samples of different
     * aggregatable types, the aggregatable type that covers the greatest
     * fraction of the interval is used.
     */
    @Test
    public void testMixedAggregatableAndDisconnected() {
        LinkedList<ChannelAccessSample> sourceSamples = new LinkedList<ChannelAccessSample>();
        ChannelAccessRawSample firstUsedSourceSample;
        ChannelAccessAggregatedSample decimatedSample;
        sourceSamples.add(new ChannelAccessDisconnectedSample(99L, true));
        firstUsedSourceSample = createScalarDoubleSample(
                EPICS_EPOCH_NANOSECONDS + 110L * ONE_BILLION, 50.0,
                ChannelAccessAlarmSeverity.NO_ALARM,
                ChannelAccessAlarmStatus.NO_ALARM, "xyz");
        sourceSamples.add(firstUsedSourceSample);
        sourceSamples.add(createScalarLongSample(EPICS_EPOCH_NANOSECONDS + 120L
                * ONE_BILLION, 42, ChannelAccessAlarmSeverity.MAJOR_ALARM,
                ChannelAccessAlarmStatus.HIHI, ""));
        sourceSamples.add(createScalarDoubleSample(EPICS_EPOCH_NANOSECONDS
                + 150L * ONE_BILLION, 20.0,
                ChannelAccessAlarmSeverity.MINOR_ALARM,
                ChannelAccessAlarmStatus.LOW, "abc"));
        sourceSamples.add(new ChannelAccessDisconnectedSample(
                EPICS_EPOCH_NANOSECONDS + 170L * ONE_BILLION, true));
        decimatedSample = (ChannelAccessAggregatedSample) calculateDecimatedSample(
                EPICS_EPOCH_NANOSECONDS + 100L * ONE_BILLION,
                100L * ONE_BILLION, sourceSamples);
        verifyAggregatedScalarDoubleSample(decimatedSample,
                EPICS_EPOCH_NANOSECONDS + 100L * ONE_BILLION, 30.0, 14.1421,
                20.0, 50.0, ChannelAccessAlarmSeverity.MINOR_ALARM,
                ChannelAccessAlarmStatus.LOW, 0.3, firstUsedSourceSample);
    }

    /**
     * Runs some basic tests with source samples of type
     * <code>SCALAR_ENUM</code> and <code>SCALAR_STRING</code>. This single test
     * is sufficient for testing all non-aggregatable sample types because the
     * code path is identical.
     */
    @Test
    public void testMixedNonAggregatable() {
        LinkedList<ChannelAccessSample> sourceSamples = new LinkedList<ChannelAccessSample>();
        ChannelAccessRawSample firstSourceSample;
        ChannelAccessRawSample decimatedSample;
        // Run the test with exactly one sample that has a time-stamp before the
        // period for which a decimated sample shall be calculated.
        sourceSamples.clear();
        firstSourceSample = createScalarEnumSample(EPICS_EPOCH_NANOSECONDS
                + 79L * ONE_BILLION, (short) 2,
                ChannelAccessAlarmSeverity.MINOR_ALARM,
                ChannelAccessAlarmStatus.STATE, new String[] { "Yes", "No",
                        "Maybe" });
        sourceSamples.add(firstSourceSample);
        decimatedSample = (ChannelAccessRawSample) calculateDecimatedSample(
                EPICS_EPOCH_NANOSECONDS + 90L * ONE_BILLION, 30L * ONE_BILLION,
                sourceSamples);
        assertEquals(EPICS_EPOCH_NANOSECONDS + 90L * ONE_BILLION,
                decimatedSample.getTimeStamp());
        assertEquals(firstSourceSample.getType(), decimatedSample.getType());
        assertEquals(firstSourceSample.getValue(), decimatedSample.getValue());
        assertFalse(decimatedSample.isOriginalSample());
        // Run the test with exactly one sample that has a time-stamp exactly at
        // the edge of the period for which a decimated sample shall be
        // calculated.
        sourceSamples.clear();
        firstSourceSample = createScalarStringSample(EPICS_EPOCH_NANOSECONDS
                + 90L * ONE_BILLION, "Cassandra",
                ChannelAccessAlarmSeverity.NO_ALARM,
                ChannelAccessAlarmStatus.NO_ALARM);
        sourceSamples.add(firstSourceSample);
        decimatedSample = (ChannelAccessRawSample) calculateDecimatedSample(
                EPICS_EPOCH_NANOSECONDS + 90L * ONE_BILLION, 30L * ONE_BILLION,
                sourceSamples);
        assertEquals(EPICS_EPOCH_NANOSECONDS + 90L * ONE_BILLION,
                decimatedSample.getTimeStamp());
        assertEquals(firstSourceSample.getType(), decimatedSample.getType());
        assertEquals(firstSourceSample.getValue(), decimatedSample.getValue());
        assertFalse(decimatedSample.isOriginalSample());
        // Run the test with three samples in order to check whether the first
        // one is selected as expected.
        sourceSamples.clear();
        firstSourceSample = createScalarEnumSample(EPICS_EPOCH_NANOSECONDS
                + 175L * ONE_BILLION, (short) 2,
                ChannelAccessAlarmSeverity.NO_ALARM,
                ChannelAccessAlarmStatus.NO_ALARM, new String[] { "Yes", "No",
                        "Maybe" });
        sourceSamples.add(firstSourceSample);
        sourceSamples.add(createScalarStringSample(EPICS_EPOCH_NANOSECONDS
                + 201L * ONE_BILLION, "Cassandra",
                ChannelAccessAlarmSeverity.INVALID_ALARM,
                ChannelAccessAlarmStatus.COMM));
        sourceSamples.add(createScalarEnumSample(EPICS_EPOCH_NANOSECONDS + 239L
                * ONE_BILLION, (short) 2, ChannelAccessAlarmSeverity.NO_ALARM,
                ChannelAccessAlarmStatus.NO_ALARM, new String[] { "Yes", "No",
                        "Maybe" }));
        decimatedSample = (ChannelAccessRawSample) calculateDecimatedSample(
                EPICS_EPOCH_NANOSECONDS + 180L * ONE_BILLION,
                60L * ONE_BILLION, sourceSamples);
        assertEquals(EPICS_EPOCH_NANOSECONDS + 180L * ONE_BILLION,
                decimatedSample.getTimeStamp());
        assertEquals(firstSourceSample.getType(), decimatedSample.getType());
        assertEquals(firstSourceSample.getValue(), decimatedSample.getValue());
        assertFalse(decimatedSample.isOriginalSample());
    }

    /**
     * Runs some basic tests with source samples of type
     * <code>SCALAR_CHAR</code>:
     */
    @Test
    public void testScalarChar() {
        LinkedList<ChannelAccessSample> sourceSamples = new LinkedList<ChannelAccessSample>();
        ChannelAccessRawSample firstSourceSample;
        ChannelAccessAggregatedSample decimatedSample;
        // Run the test with exactly one sample that has a time-stamp before the
        // period for which a decimated sample shall be calculated.
        sourceSamples.clear();
        firstSourceSample = createScalarCharSample(EPICS_EPOCH_NANOSECONDS
                + 79L * ONE_BILLION, (byte) 22,
                ChannelAccessAlarmSeverity.MINOR_ALARM,
                ChannelAccessAlarmStatus.HIGH, "abc");
        sourceSamples.add(firstSourceSample);
        decimatedSample = (ChannelAccessAggregatedSample) calculateDecimatedSample(
                EPICS_EPOCH_NANOSECONDS + 90L * ONE_BILLION, 30L * ONE_BILLION,
                sourceSamples);
        verifyAggregatedScalarCharSample(decimatedSample,
                EPICS_EPOCH_NANOSECONDS + 90L * ONE_BILLION, 22.0, 0.0,
                (byte) 22, (byte) 22, ChannelAccessAlarmSeverity.MINOR_ALARM,
                ChannelAccessAlarmStatus.HIGH, 1.0, firstSourceSample);
        // Run the test with exactly one sample that has a time-stamp exactly at
        // the edge of the period for which a decimated sample shall be
        // calculated.
        sourceSamples.clear();
        firstSourceSample = createScalarCharSample(EPICS_EPOCH_NANOSECONDS
                + 600L * ONE_BILLION, (byte) -45,
                ChannelAccessAlarmSeverity.NO_ALARM,
                ChannelAccessAlarmStatus.NO_ALARM, "xyz");
        sourceSamples.add(firstSourceSample);
        decimatedSample = (ChannelAccessAggregatedSample) calculateDecimatedSample(
                EPICS_EPOCH_NANOSECONDS + 600L * ONE_BILLION,
                300L * ONE_BILLION, sourceSamples);
        verifyAggregatedScalarCharSample(decimatedSample,
                EPICS_EPOCH_NANOSECONDS + 600L * ONE_BILLION, -45.0, 0.0,
                (byte) -45, (byte) -45, ChannelAccessAlarmSeverity.NO_ALARM,
                ChannelAccessAlarmStatus.NO_ALARM, 1.0, firstSourceSample);
        // Run the test with three samples in order to see that the aggregation
        // process works correctly.
        sourceSamples.clear();
        firstSourceSample = createScalarCharSample(EPICS_EPOCH_NANOSECONDS
                + 57L * ONE_BILLION, (byte) 40,
                ChannelAccessAlarmSeverity.NO_ALARM,
                ChannelAccessAlarmStatus.NO_ALARM, "123");
        sourceSamples.add(firstSourceSample);
        sourceSamples.add(createScalarCharSample(EPICS_EPOCH_NANOSECONDS + 125L
                * ONE_BILLION, (byte) 0,
                ChannelAccessAlarmSeverity.INVALID_ALARM,
                ChannelAccessAlarmStatus.COMM, "abc"));
        sourceSamples.add(createScalarCharSample(EPICS_EPOCH_NANOSECONDS + 175L
                * ONE_BILLION, (byte) 20,
                ChannelAccessAlarmSeverity.MINOR_ALARM,
                ChannelAccessAlarmStatus.LOW, "def"));
        decimatedSample = (ChannelAccessAggregatedSample) calculateDecimatedSample(
                EPICS_EPOCH_NANOSECONDS + 100L * ONE_BILLION,
                100L * ONE_BILLION, sourceSamples);
        verifyAggregatedScalarCharSample(decimatedSample,
                EPICS_EPOCH_NANOSECONDS + 100L * ONE_BILLION, 15.0, 16.583,
                (byte) 0, (byte) 40, ChannelAccessAlarmSeverity.INVALID_ALARM,
                ChannelAccessAlarmStatus.COMM, 1.0, firstSourceSample);
    }

    /**
     * Runs some basic tests with source samples of type
     * <code>SCALAR_DOUBLE</code>:
     */
    @Test
    public void testScalarDouble() {
        LinkedList<ChannelAccessSample> sourceSamples = new LinkedList<ChannelAccessSample>();
        ChannelAccessRawSample firstSourceSample;
        ChannelAccessAggregatedSample decimatedSample;
        // Run the test with exactly one sample that has a time-stamp before the
        // period for which a decimated sample shall be calculated.
        sourceSamples.clear();
        firstSourceSample = createScalarDoubleSample(EPICS_EPOCH_NANOSECONDS
                + 79L * ONE_BILLION, 22.3,
                ChannelAccessAlarmSeverity.MINOR_ALARM,
                ChannelAccessAlarmStatus.HIGH, "xyz");
        sourceSamples.add(firstSourceSample);
        decimatedSample = (ChannelAccessAggregatedSample) calculateDecimatedSample(
                EPICS_EPOCH_NANOSECONDS + 90L * ONE_BILLION, 30L * ONE_BILLION,
                sourceSamples);
        verifyAggregatedScalarDoubleSample(decimatedSample,
                EPICS_EPOCH_NANOSECONDS + 90L * ONE_BILLION, 22.3, 0.0, 22.3,
                22.3, ChannelAccessAlarmSeverity.MINOR_ALARM,
                ChannelAccessAlarmStatus.HIGH, 1.0, firstSourceSample);
        // Run the test with exactly one sample that has a time-stamp exactly at
        // the edge of the period for which a decimated sample shall be
        // calculated.
        sourceSamples.clear();
        firstSourceSample = createScalarDoubleSample(EPICS_EPOCH_NANOSECONDS
                + 600L * ONE_BILLION, -45.0,
                ChannelAccessAlarmSeverity.NO_ALARM,
                ChannelAccessAlarmStatus.NO_ALARM, "123");
        sourceSamples.add(firstSourceSample);
        decimatedSample = (ChannelAccessAggregatedSample) calculateDecimatedSample(
                EPICS_EPOCH_NANOSECONDS + 600L * ONE_BILLION,
                300L * ONE_BILLION, sourceSamples);
        verifyAggregatedScalarDoubleSample(decimatedSample,
                EPICS_EPOCH_NANOSECONDS + 600L * ONE_BILLION, -45.0, 0.0,
                -45.0, -45.0, ChannelAccessAlarmSeverity.NO_ALARM,
                ChannelAccessAlarmStatus.NO_ALARM, 1.0, firstSourceSample);
        // Run the test with three samples in order to see that the aggregation
        // process works correctly.
        sourceSamples.clear();
        firstSourceSample = createScalarDoubleSample(EPICS_EPOCH_NANOSECONDS
                + 57L * ONE_BILLION, 40.0, ChannelAccessAlarmSeverity.NO_ALARM,
                ChannelAccessAlarmStatus.NO_ALARM, "xyz");
        sourceSamples.add(firstSourceSample);
        sourceSamples.add(createScalarDoubleSample(EPICS_EPOCH_NANOSECONDS
                + 140L * ONE_BILLION, 10.0,
                ChannelAccessAlarmSeverity.MINOR_ALARM,
                ChannelAccessAlarmStatus.COMM, "abc"));
        sourceSamples.add(createScalarDoubleSample(EPICS_EPOCH_NANOSECONDS
                + 170L * ONE_BILLION, -20.0,
                ChannelAccessAlarmSeverity.MAJOR_ALARM,
                ChannelAccessAlarmStatus.LOLO, "def"));
        decimatedSample = (ChannelAccessAggregatedSample) calculateDecimatedSample(
                EPICS_EPOCH_NANOSECONDS + 100L * ONE_BILLION,
                100L * ONE_BILLION, sourceSamples);
        verifyAggregatedScalarDoubleSample(decimatedSample,
                EPICS_EPOCH_NANOSECONDS + 100L * ONE_BILLION, 13.0, 24.91987,
                -20.0, 40.0, ChannelAccessAlarmSeverity.MAJOR_ALARM,
                ChannelAccessAlarmStatus.LOLO, 1.0, firstSourceSample);
    }

    /**
     * Runs some basic tests with source samples of type
     * <code>SCALAR_FLOAT</code>:
     */
    @Test
    public void testScalarFloat() {
        LinkedList<ChannelAccessSample> sourceSamples = new LinkedList<ChannelAccessSample>();
        ChannelAccessRawSample firstSourceSample;
        ChannelAccessAggregatedSample decimatedSample;
        // Run the test with exactly one sample that has a time-stamp before the
        // period for which a decimated sample shall be calculated.
        sourceSamples.clear();
        firstSourceSample = createScalarFloatSample(EPICS_EPOCH_NANOSECONDS
                + 79L * ONE_BILLION, 22.3f,
                ChannelAccessAlarmSeverity.MINOR_ALARM,
                ChannelAccessAlarmStatus.HIGH, "xyz");
        sourceSamples.add(firstSourceSample);
        decimatedSample = (ChannelAccessAggregatedSample) calculateDecimatedSample(
                EPICS_EPOCH_NANOSECONDS + 90L * ONE_BILLION, 30L * ONE_BILLION,
                sourceSamples);
        verifyAggregatedScalarFloatSample(decimatedSample,
                EPICS_EPOCH_NANOSECONDS + 90L * ONE_BILLION, 22.3, 0.0, 22.3f,
                22.3f, ChannelAccessAlarmSeverity.MINOR_ALARM,
                ChannelAccessAlarmStatus.HIGH, 1.0, firstSourceSample);
        // Run the test with exactly one sample that has a time-stamp exactly at
        // the edge of the period for which a decimated sample shall be
        // calculated.
        sourceSamples.clear();
        firstSourceSample = createScalarFloatSample(EPICS_EPOCH_NANOSECONDS
                + 600L * ONE_BILLION, -45.0f,
                ChannelAccessAlarmSeverity.NO_ALARM,
                ChannelAccessAlarmStatus.NO_ALARM, "123");
        sourceSamples.add(firstSourceSample);
        decimatedSample = (ChannelAccessAggregatedSample) calculateDecimatedSample(
                EPICS_EPOCH_NANOSECONDS + 600L * ONE_BILLION,
                300L * ONE_BILLION, sourceSamples);
        verifyAggregatedScalarFloatSample(decimatedSample,
                EPICS_EPOCH_NANOSECONDS + 600L * ONE_BILLION, -45.0, 0.0,
                -45.0f, -45.0f, ChannelAccessAlarmSeverity.NO_ALARM,
                ChannelAccessAlarmStatus.NO_ALARM, 1.0, firstSourceSample);
        // Run the test with three samples in order to see that the aggregation
        // process works correctly.
        sourceSamples.clear();
        firstSourceSample = createScalarFloatSample(EPICS_EPOCH_NANOSECONDS
                + 57L * ONE_BILLION, 40.0f,
                ChannelAccessAlarmSeverity.NO_ALARM,
                ChannelAccessAlarmStatus.NO_ALARM, "xyz");
        sourceSamples.add(firstSourceSample);
        sourceSamples.add(createScalarFloatSample(EPICS_EPOCH_NANOSECONDS
                + 140L * ONE_BILLION, 10.0f,
                ChannelAccessAlarmSeverity.MINOR_ALARM,
                ChannelAccessAlarmStatus.COMM, "abc"));
        sourceSamples.add(createScalarFloatSample(EPICS_EPOCH_NANOSECONDS
                + 170L * ONE_BILLION, -20.0f,
                ChannelAccessAlarmSeverity.MINOR_ALARM,
                ChannelAccessAlarmStatus.LOW, "def"));
        decimatedSample = (ChannelAccessAggregatedSample) calculateDecimatedSample(
                EPICS_EPOCH_NANOSECONDS + 100L * ONE_BILLION,
                100L * ONE_BILLION, sourceSamples);
        verifyAggregatedScalarFloatSample(decimatedSample,
                EPICS_EPOCH_NANOSECONDS + 100L * ONE_BILLION, 13.0, 24.91987,
                -20.0f, 40.0f, ChannelAccessAlarmSeverity.MINOR_ALARM,
                ChannelAccessAlarmStatus.COMM, 1.0, firstSourceSample);
    }

    /**
     * Runs some basic tests with source samples of type
     * <code>SCALAR_LONG</code>:
     */
    @Test
    public void testScalarLong() {
        LinkedList<ChannelAccessSample> sourceSamples = new LinkedList<ChannelAccessSample>();
        ChannelAccessRawSample firstSourceSample;
        ChannelAccessAggregatedSample decimatedSample;
        // Run the test with exactly one sample that has a time-stamp before the
        // period for which a decimated sample shall be calculated.
        sourceSamples.clear();
        firstSourceSample = createScalarLongSample(EPICS_EPOCH_NANOSECONDS
                + 79L * ONE_BILLION, 22,
                ChannelAccessAlarmSeverity.MINOR_ALARM,
                ChannelAccessAlarmStatus.HIGH, "abc");
        sourceSamples.add(firstSourceSample);
        decimatedSample = (ChannelAccessAggregatedSample) calculateDecimatedSample(
                EPICS_EPOCH_NANOSECONDS + 90L * ONE_BILLION, 30L * ONE_BILLION,
                sourceSamples);
        verifyAggregatedScalarLongSample(decimatedSample,
                EPICS_EPOCH_NANOSECONDS + 90L * ONE_BILLION, 22.0, 0.0, 22, 22,
                ChannelAccessAlarmSeverity.MINOR_ALARM,
                ChannelAccessAlarmStatus.HIGH, 1.0, firstSourceSample);
        // Run the test with exactly one sample that has a time-stamp exactly at
        // the edge of the period for which a decimated sample shall be
        // calculated.
        sourceSamples.clear();
        firstSourceSample = createScalarLongSample(EPICS_EPOCH_NANOSECONDS
                + 600L * ONE_BILLION, -45, ChannelAccessAlarmSeverity.NO_ALARM,
                ChannelAccessAlarmStatus.NO_ALARM, "xyz");
        sourceSamples.add(firstSourceSample);
        decimatedSample = (ChannelAccessAggregatedSample) calculateDecimatedSample(
                EPICS_EPOCH_NANOSECONDS + 600L * ONE_BILLION,
                300L * ONE_BILLION, sourceSamples);
        verifyAggregatedScalarLongSample(decimatedSample,
                EPICS_EPOCH_NANOSECONDS + 600L * ONE_BILLION, -45.0, 0.0, -45,
                -45, ChannelAccessAlarmSeverity.NO_ALARM,
                ChannelAccessAlarmStatus.NO_ALARM, 1.0, firstSourceSample);
        // Run the test with three samples in order to see that the aggregation
        // process works correctly.
        sourceSamples.clear();
        firstSourceSample = createScalarLongSample(EPICS_EPOCH_NANOSECONDS
                + 57L * ONE_BILLION, 40, ChannelAccessAlarmSeverity.NO_ALARM,
                ChannelAccessAlarmStatus.NO_ALARM, "123");
        sourceSamples.add(firstSourceSample);
        sourceSamples.add(createScalarLongSample(EPICS_EPOCH_NANOSECONDS + 125L
                * ONE_BILLION, 0, ChannelAccessAlarmSeverity.INVALID_ALARM,
                ChannelAccessAlarmStatus.COMM, "abc"));
        sourceSamples.add(createScalarLongSample(EPICS_EPOCH_NANOSECONDS + 175L
                * ONE_BILLION, 20, ChannelAccessAlarmSeverity.MINOR_ALARM,
                ChannelAccessAlarmStatus.LOW, "def"));
        decimatedSample = (ChannelAccessAggregatedSample) calculateDecimatedSample(
                EPICS_EPOCH_NANOSECONDS + 100L * ONE_BILLION,
                100L * ONE_BILLION, sourceSamples);
        verifyAggregatedScalarLongSample(decimatedSample,
                EPICS_EPOCH_NANOSECONDS + 100L * ONE_BILLION, 15.0, 16.583, 0,
                40, ChannelAccessAlarmSeverity.INVALID_ALARM,
                ChannelAccessAlarmStatus.COMM, 1.0, firstSourceSample);
    }

    /**
     * Runs some basic tests with source samples of type
     * <code>SCALAR_SHORT</code>:
     */
    @Test
    public void testScalarShort() {
        LinkedList<ChannelAccessSample> sourceSamples = new LinkedList<ChannelAccessSample>();
        ChannelAccessRawSample firstSourceSample;
        ChannelAccessAggregatedSample decimatedSample;
        // Run the test with exactly one sample that has a time-stamp before the
        // period for which a decimated sample shall be calculated.
        sourceSamples.clear();
        firstSourceSample = createScalarShortSample(EPICS_EPOCH_NANOSECONDS
                + 79L * ONE_BILLION, (short) 22,
                ChannelAccessAlarmSeverity.MINOR_ALARM,
                ChannelAccessAlarmStatus.HIGH, "abc");
        sourceSamples.add(firstSourceSample);
        decimatedSample = (ChannelAccessAggregatedSample) calculateDecimatedSample(
                EPICS_EPOCH_NANOSECONDS + 90L * ONE_BILLION, 30L * ONE_BILLION,
                sourceSamples);
        verifyAggregatedScalarShortSample(decimatedSample,
                EPICS_EPOCH_NANOSECONDS + 90L * ONE_BILLION, 22.0, 0.0,
                (short) 22, (short) 22, ChannelAccessAlarmSeverity.MINOR_ALARM,
                ChannelAccessAlarmStatus.HIGH, 1.0, firstSourceSample);
        // Run the test with exactly one sample that has a time-stamp exactly at
        // the edge of the period for which a decimated sample shall be
        // calculated.
        sourceSamples.clear();
        firstSourceSample = createScalarShortSample(EPICS_EPOCH_NANOSECONDS
                + 600L * ONE_BILLION, (short) -45,
                ChannelAccessAlarmSeverity.NO_ALARM,
                ChannelAccessAlarmStatus.NO_ALARM, "xyz");
        sourceSamples.add(firstSourceSample);
        decimatedSample = (ChannelAccessAggregatedSample) calculateDecimatedSample(
                EPICS_EPOCH_NANOSECONDS + 600L * ONE_BILLION,
                300L * ONE_BILLION, sourceSamples);
        verifyAggregatedScalarShortSample(decimatedSample,
                EPICS_EPOCH_NANOSECONDS + 600L * ONE_BILLION, -45.0, 0.0,
                (short) -45, (short) -45, ChannelAccessAlarmSeverity.NO_ALARM,
                ChannelAccessAlarmStatus.NO_ALARM, 1.0, firstSourceSample);
        // Run the test with three samples in order to see that the aggregation
        // process works correctly.
        sourceSamples.clear();
        firstSourceSample = createScalarShortSample(EPICS_EPOCH_NANOSECONDS
                + 57L * ONE_BILLION, (short) 40,
                ChannelAccessAlarmSeverity.NO_ALARM,
                ChannelAccessAlarmStatus.NO_ALARM, "123");
        sourceSamples.add(firstSourceSample);
        sourceSamples.add(createScalarShortSample(EPICS_EPOCH_NANOSECONDS
                + 125L * ONE_BILLION, (short) 0,
                ChannelAccessAlarmSeverity.INVALID_ALARM,
                ChannelAccessAlarmStatus.COMM, "abc"));
        sourceSamples.add(createScalarShortSample(EPICS_EPOCH_NANOSECONDS
                + 175L * ONE_BILLION, (short) 20,
                ChannelAccessAlarmSeverity.MINOR_ALARM,
                ChannelAccessAlarmStatus.LOW, "def"));
        decimatedSample = (ChannelAccessAggregatedSample) calculateDecimatedSample(
                EPICS_EPOCH_NANOSECONDS + 100L * ONE_BILLION,
                100L * ONE_BILLION, sourceSamples);
        verifyAggregatedScalarShortSample(decimatedSample,
                EPICS_EPOCH_NANOSECONDS + 100L * ONE_BILLION, 15.0, 16.583,
                (short) 0, (short) 40,
                ChannelAccessAlarmSeverity.INVALID_ALARM,
                ChannelAccessAlarmStatus.COMM, 1.0, firstSourceSample);
    }

}
