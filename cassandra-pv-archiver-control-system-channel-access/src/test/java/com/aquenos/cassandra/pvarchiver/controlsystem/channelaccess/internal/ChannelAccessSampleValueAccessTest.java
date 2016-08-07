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

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aquenos.cassandra.pvarchiver.tests.EmbeddedCassandraServer;
import com.aquenos.epics.jackie.common.value.ChannelAccessAlarmOnlyString;
import com.aquenos.epics.jackie.common.value.ChannelAccessAlarmSeverity;
import com.aquenos.epics.jackie.common.value.ChannelAccessAlarmStatus;
import com.aquenos.epics.jackie.common.value.ChannelAccessControlsChar;
import com.aquenos.epics.jackie.common.value.ChannelAccessControlsDouble;
import com.aquenos.epics.jackie.common.value.ChannelAccessControlsFloat;
import com.aquenos.epics.jackie.common.value.ChannelAccessControlsLong;
import com.aquenos.epics.jackie.common.value.ChannelAccessControlsShort;
import com.aquenos.epics.jackie.common.value.ChannelAccessControlsValue;
import com.aquenos.epics.jackie.common.value.ChannelAccessGraphicsEnum;
import com.aquenos.epics.jackie.common.value.ChannelAccessTimeChar;
import com.aquenos.epics.jackie.common.value.ChannelAccessTimeDouble;
import com.aquenos.epics.jackie.common.value.ChannelAccessTimeEnum;
import com.aquenos.epics.jackie.common.value.ChannelAccessTimeFloat;
import com.aquenos.epics.jackie.common.value.ChannelAccessTimeLong;
import com.aquenos.epics.jackie.common.value.ChannelAccessTimeShort;
import com.aquenos.epics.jackie.common.value.ChannelAccessTimeString;
import com.aquenos.epics.jackie.common.value.ChannelAccessTimeValue;
import com.aquenos.epics.jackie.common.value.ChannelAccessValueFactory;
import com.datastax.driver.core.Session;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;

/**
 * Tests for the {@link ChannelAccessSampleValueAccess}.
 * 
 * @author Sebastian Marsching
 */
public class ChannelAccessSampleValueAccessTest {

    private static ChannelAccessSampleValueAccess sampleValueAccess;

    /**
     * Creates the {@link ChannelAccessSampleValueAccess} instance that is
     * tested.
     */
    @BeforeClass
    public static void initializeSampleValueAccess() {
        Session session = EmbeddedCassandraServer.getSession();
        sampleValueAccess = new ChannelAccessSampleValueAccess(session);
    }

    private static ChannelAccessControlsValue<?> mergeValuePair(
            ChannelAccessTimeValue<?> timeValue,
            ChannelAccessControlsValue<?> controlsValue) {
        assert (timeValue.getType().toSimpleType().equals(controlsValue
                .getType().toSimpleType()));
        switch (timeValue.getType().toSimpleType()) {
        case DBR_CHAR:
            byte[] charValue = new byte[timeValue.getValueSize()];
            ((ChannelAccessTimeChar) timeValue).getValue().get(charValue);
            ChannelAccessControlsChar newChar = (ChannelAccessControlsChar) controlsValue
                    .clone();
            newChar.setValue(charValue);
            newChar.setAlarmSeverity(timeValue.getAlarmSeverity());
            newChar.setAlarmStatus(timeValue.getAlarmStatus());
            return newChar;
        case DBR_DOUBLE:
            double[] doubleValue = new double[timeValue.getValueSize()];
            ((ChannelAccessTimeDouble) timeValue).getValue().get(doubleValue);
            ChannelAccessControlsDouble newDouble = (ChannelAccessControlsDouble) controlsValue
                    .clone();
            newDouble.setValue(doubleValue);
            newDouble.setAlarmSeverity(timeValue.getAlarmSeverity());
            newDouble.setAlarmStatus(timeValue.getAlarmStatus());
            return newDouble;
        case DBR_ENUM:
            short[] enumValue = new short[timeValue.getValueSize()];
            ((ChannelAccessTimeEnum) timeValue).getValue().get(enumValue);
            ChannelAccessGraphicsEnum newEnum = (ChannelAccessGraphicsEnum) controlsValue
                    .clone();
            newEnum.setValue(enumValue);
            newEnum.setAlarmSeverity(timeValue.getAlarmSeverity());
            newEnum.setAlarmStatus(timeValue.getAlarmStatus());
            return newEnum;
        case DBR_FLOAT:
            float[] floatValue = new float[timeValue.getValueSize()];
            ((ChannelAccessTimeFloat) timeValue).getValue().get(floatValue);
            ChannelAccessControlsFloat newFloat = (ChannelAccessControlsFloat) controlsValue
                    .clone();
            newFloat.setValue(floatValue);
            newFloat.setAlarmSeverity(timeValue.getAlarmSeverity());
            newFloat.setAlarmStatus(timeValue.getAlarmStatus());
            return newFloat;
        case DBR_LONG:
            int[] longValue = new int[timeValue.getValueSize()];
            ((ChannelAccessTimeLong) timeValue).getValue().get(longValue);
            ChannelAccessControlsLong newLong = (ChannelAccessControlsLong) controlsValue
                    .clone();
            newLong.setValue(longValue);
            newLong.setAlarmSeverity(timeValue.getAlarmSeverity());
            newLong.setAlarmStatus(timeValue.getAlarmStatus());
            return newLong;
        case DBR_SHORT:
            short[] shortValue = new short[timeValue.getValueSize()];
            ((ChannelAccessTimeShort) timeValue).getValue().get(shortValue);
            ChannelAccessControlsShort newShort = (ChannelAccessControlsShort) controlsValue
                    .clone();
            newShort.setValue(shortValue);
            newShort.setAlarmSeverity(timeValue.getAlarmSeverity());
            newShort.setAlarmStatus(timeValue.getAlarmStatus());
            return newShort;
        case DBR_STRING:
            Collection<String> stringValue = ((ChannelAccessTimeString) timeValue)
                    .getValue();
            ChannelAccessAlarmOnlyString newString = (ChannelAccessAlarmOnlyString) controlsValue
                    .clone();
            newString.setValue(stringValue);
            newString.setAlarmSeverity(timeValue.getAlarmSeverity());
            newString.setAlarmStatus(timeValue.getAlarmStatus());
            return newString;
        default:
            throw new RuntimeException("Unexpected type: "
                    + timeValue.getType());
        }
    }

    /**
     * Test creating and deserializing aggregated samples. This effectively
     * tests that the conversion from the statistical values and the meta data
     * to the respective Cassandra UDT values and back works correctly.
     */
    @Test
    public void testCreateAndDeserializeAggregatedSamples() {
        // For each sample type, we first have to create a raw sample that we
        // can use as the source of the meta-data. The content of the time value
        // should not matter at all. The content of the controls value only
        // matters as far as the meta-data is concerned. The value elements and
        // the alarm severity and status should not be used anyway. When
        // comparing the created sample, we have to modify the controls value to
        // represent the actual alarm state of the sample because otherwise the
        // controls value returned for the aggregated sample would not match.

        // Aggregated scalar char.
        ChannelAccessControlsChar charMetaData = ChannelAccessValueFactory
                .createControlsChar(ArrayUtils.EMPTY_BYTE_ARRAY,
                        ChannelAccessAlarmStatus.NO_ALARM,
                        ChannelAccessAlarmSeverity.NO_ALARM, "Volts",
                        (byte) 85, (byte) -85, (byte) 30, (byte) 25, (byte) 10,
                        (byte) 5, (byte) 35, (byte) 5, Charsets.UTF_8);
        ChannelAccessRawSample charMetaDataSample = sampleValueAccess
                .createRawSample(ChannelAccessValueFactory.createTimeChar(
                        ArrayUtils.EMPTY_BYTE_ARRAY,
                        ChannelAccessAlarmStatus.NO_ALARM,
                        ChannelAccessAlarmSeverity.NO_ALARM, 0, 0),
                        charMetaData);
        ChannelAccessAggregatedSample aggregatedScalarCharSample = sampleValueAccess
                .createAggregatedScalarCharSample(123L, 42.3, 5.4, (byte) -19,
                        (byte) 115, ChannelAccessAlarmSeverity.MAJOR_ALARM,
                        ChannelAccessAlarmStatus.HWLIMIT, 0.457,
                        charMetaDataSample);
        charMetaData.setAlarmSeverity(ChannelAccessAlarmSeverity.MAJOR_ALARM);
        charMetaData.setAlarmStatus(ChannelAccessAlarmStatus.HWLIMIT);
        assertEquals(123L, aggregatedScalarCharSample.getTimeStamp());
        assertEquals(42.3,
                sampleValueAccess
                        .deserializeMeanColumn(aggregatedScalarCharSample), 0.0);
        assertEquals(5.4,
                sampleValueAccess
                        .deserializeStdColumn(aggregatedScalarCharSample), 0.0);
        assertEquals(-19,
                sampleValueAccess
                        .deserializeByteMinColumn(aggregatedScalarCharSample));
        assertEquals(115,
                sampleValueAccess
                        .deserializeByteMaxColumn(aggregatedScalarCharSample));
        assertEquals(
                charMetaData,
                sampleValueAccess
                        .deserializeAggregatedSampleMetaData(aggregatedScalarCharSample));
        assertEquals(
                0.457,
                sampleValueAccess
                        .deserializeCoveredPeriodFractionColumn(aggregatedScalarCharSample),
                0.0);
        aggregatedScalarCharSample = sampleValueAccess
                .createAggregatedScalarCharSample(123L, 42.3, 5.4, (byte) -19,
                        (byte) 115, ChannelAccessAlarmSeverity.MAJOR_ALARM,
                        ChannelAccessAlarmStatus.HWLIMIT, 0.457,
                        aggregatedScalarCharSample);
        assertEquals(
                charMetaData,
                sampleValueAccess
                        .deserializeAggregatedSampleMetaData(aggregatedScalarCharSample));

        // Aggregated scalar double.
        ChannelAccessControlsDouble doubleMetaData = ChannelAccessValueFactory
                .createControlsDouble(ArrayUtils.EMPTY_DOUBLE_ARRAY,
                        ChannelAccessAlarmStatus.LOW,
                        ChannelAccessAlarmSeverity.MINOR_ALARM, (short) 3,
                        "Ampere", 23.5, -99.6, 30.0, 25.2, 10.4, 5.8, 35.1,
                        5.2, Charsets.UTF_8);
        ChannelAccessRawSample doubleMetaDataSample = sampleValueAccess
                .createRawSample(ChannelAccessValueFactory.createTimeDouble(
                        ArrayUtils.EMPTY_DOUBLE_ARRAY,
                        ChannelAccessAlarmStatus.NO_ALARM,
                        ChannelAccessAlarmSeverity.NO_ALARM, 0, 0),
                        doubleMetaData);
        ChannelAccessAggregatedSample aggregatedScalarDoubleSample = sampleValueAccess
                .createAggregatedScalarDoubleSample(321L, -42.7, 6.4, -199.0,
                        21.37, ChannelAccessAlarmSeverity.NO_ALARM,
                        ChannelAccessAlarmStatus.NO_ALARM, 1.0,
                        doubleMetaDataSample);
        doubleMetaData.setAlarmSeverity(ChannelAccessAlarmSeverity.NO_ALARM);
        doubleMetaData.setAlarmStatus(ChannelAccessAlarmStatus.NO_ALARM);
        assertEquals(321L, aggregatedScalarDoubleSample.getTimeStamp());
        assertEquals(-42.7,
                sampleValueAccess
                        .deserializeMeanColumn(aggregatedScalarDoubleSample),
                0.0);
        assertEquals(6.4,
                sampleValueAccess
                        .deserializeStdColumn(aggregatedScalarDoubleSample),
                0.0);
        assertEquals(
                -199.0,
                sampleValueAccess
                        .deserializeDoubleMinColumn(aggregatedScalarDoubleSample),
                0.0);
        assertEquals(
                21.37,
                sampleValueAccess
                        .deserializeDoubleMaxColumn(aggregatedScalarDoubleSample),
                0.0);
        assertEquals(
                doubleMetaData,
                sampleValueAccess
                        .deserializeAggregatedSampleMetaData(aggregatedScalarDoubleSample));
        assertEquals(
                1.0,
                sampleValueAccess
                        .deserializeCoveredPeriodFractionColumn(aggregatedScalarDoubleSample),
                0.0);
        aggregatedScalarDoubleSample = sampleValueAccess
                .createAggregatedScalarDoubleSample(321L, -42.7, 6.4, -199.0,
                        21.37, ChannelAccessAlarmSeverity.NO_ALARM,
                        ChannelAccessAlarmStatus.NO_ALARM, 1.0,
                        aggregatedScalarDoubleSample);
        assertEquals(
                doubleMetaData,
                sampleValueAccess
                        .deserializeAggregatedSampleMetaData(aggregatedScalarDoubleSample));

        // Aggregated scalar float.
        ChannelAccessControlsFloat floatMetaData = ChannelAccessValueFactory
                .createControlsFloat(ArrayUtils.EMPTY_FLOAT_ARRAY,
                        ChannelAccessAlarmStatus.NO_ALARM,
                        ChannelAccessAlarmSeverity.NO_ALARM, (short) 2,
                        "Kelvin", 23.7f, -98.6f, 35.0f, -25.2f, 10.4f, 5.9f,
                        35.12f, 5.2f, Charsets.UTF_8);
        ChannelAccessRawSample floatMetaDataSample = sampleValueAccess
                .createRawSample(ChannelAccessValueFactory.createTimeFloat(
                        ArrayUtils.EMPTY_FLOAT_ARRAY,
                        ChannelAccessAlarmStatus.NO_ALARM,
                        ChannelAccessAlarmSeverity.NO_ALARM, 0, 0),
                        floatMetaData);
        ChannelAccessAggregatedSample aggregatedScalarFloatSample = sampleValueAccess
                .createAggregatedScalarFloatSample(456L, -13.7, 64.23, -201.0f,
                        21.39f, ChannelAccessAlarmSeverity.INVALID_ALARM,
                        ChannelAccessAlarmStatus.BAD_SUB, 0.99,
                        floatMetaDataSample);
        floatMetaData
                .setAlarmSeverity(ChannelAccessAlarmSeverity.INVALID_ALARM);
        floatMetaData.setAlarmStatus(ChannelAccessAlarmStatus.BAD_SUB);
        assertEquals(456L, aggregatedScalarFloatSample.getTimeStamp());
        assertEquals(-13.7,
                sampleValueAccess
                        .deserializeMeanColumn(aggregatedScalarFloatSample),
                0.0);
        assertEquals(64.23,
                sampleValueAccess
                        .deserializeStdColumn(aggregatedScalarFloatSample), 0.0);
        assertEquals(
                -201.0f,
                sampleValueAccess
                        .deserializeFloatMinColumn(aggregatedScalarFloatSample),
                0.0f);
        assertEquals(
                21.39f,
                sampleValueAccess
                        .deserializeFloatMaxColumn(aggregatedScalarFloatSample),
                0.0f);
        assertEquals(
                0.99,
                sampleValueAccess
                        .deserializeCoveredPeriodFractionColumn(aggregatedScalarFloatSample),
                0.0);
        assertEquals(
                floatMetaData,
                sampleValueAccess
                        .deserializeAggregatedSampleMetaData(aggregatedScalarFloatSample));
        aggregatedScalarFloatSample = sampleValueAccess
                .createAggregatedScalarFloatSample(456L, -13.7, 64.23, -201.0f,
                        21.39f, ChannelAccessAlarmSeverity.INVALID_ALARM,
                        ChannelAccessAlarmStatus.BAD_SUB, 0.99,
                        aggregatedScalarFloatSample);
        assertEquals(
                floatMetaData,
                sampleValueAccess
                        .deserializeAggregatedSampleMetaData(aggregatedScalarFloatSample));

        // Aggregated scalar long.
        ChannelAccessControlsLong longMetaData = ChannelAccessValueFactory
                .createControlsLong(ArrayUtils.EMPTY_INT_ARRAY,
                        ChannelAccessAlarmStatus.NO_ALARM,
                        ChannelAccessAlarmSeverity.NO_ALARM, "Volts", 86, -87,
                        29, 25, 11, 5, 34, 4, Charsets.UTF_8);
        ChannelAccessRawSample longMetaDataSample = sampleValueAccess
                .createRawSample(ChannelAccessValueFactory.createTimeLong(
                        ArrayUtils.EMPTY_INT_ARRAY,
                        ChannelAccessAlarmStatus.NO_ALARM,
                        ChannelAccessAlarmSeverity.NO_ALARM, 0, 0),
                        longMetaData);
        ChannelAccessAggregatedSample aggregatedScalarLongSample = sampleValueAccess
                .createAggregatedScalarLongSample(789L, -99.9, 7.89, -55, 2147,
                        ChannelAccessAlarmSeverity.NO_ALARM,
                        ChannelAccessAlarmStatus.NO_ALARM, 0.1,
                        longMetaDataSample);
        longMetaData.setAlarmSeverity(ChannelAccessAlarmSeverity.NO_ALARM);
        longMetaData.setAlarmStatus(ChannelAccessAlarmStatus.NO_ALARM);
        assertEquals(789L, aggregatedScalarLongSample.getTimeStamp());
        assertEquals(-99.9,
                sampleValueAccess
                        .deserializeMeanColumn(aggregatedScalarLongSample), 0.0);
        assertEquals(7.89,
                sampleValueAccess
                        .deserializeStdColumn(aggregatedScalarLongSample), 0.0);
        assertEquals(-55,
                sampleValueAccess
                        .deserializeLongMinColumn(aggregatedScalarLongSample));
        assertEquals(2147,
                sampleValueAccess
                        .deserializeLongMaxColumn(aggregatedScalarLongSample));
        assertEquals(
                0.1,
                sampleValueAccess
                        .deserializeCoveredPeriodFractionColumn(aggregatedScalarLongSample),
                0.0);
        assertEquals(
                longMetaData,
                sampleValueAccess
                        .deserializeAggregatedSampleMetaData(aggregatedScalarLongSample));
        aggregatedScalarLongSample = sampleValueAccess
                .createAggregatedScalarLongSample(789L, -99.9, 7.89, -55, 2147,
                        ChannelAccessAlarmSeverity.NO_ALARM,
                        ChannelAccessAlarmStatus.NO_ALARM, 0.1,
                        aggregatedScalarLongSample);
        assertEquals(
                longMetaData,
                sampleValueAccess
                        .deserializeAggregatedSampleMetaData(aggregatedScalarLongSample));

        // Aggregated scalar short.
        ChannelAccessControlsShort shortMetaData = ChannelAccessValueFactory
                .createControlsShort(ArrayUtils.EMPTY_SHORT_ARRAY,
                        ChannelAccessAlarmStatus.NO_ALARM,
                        ChannelAccessAlarmSeverity.NO_ALARM, "Volts",
                        (short) 86, (short) -185, (short) 333, (short) -25,
                        (short) 15, (short) -50, (short) 350, (short) 5,
                        Charsets.UTF_8);
        ChannelAccessRawSample shortMetaDataSample = sampleValueAccess
                .createRawSample(ChannelAccessValueFactory.createTimeShort(
                        ArrayUtils.EMPTY_SHORT_ARRAY,
                        ChannelAccessAlarmStatus.NO_ALARM,
                        ChannelAccessAlarmSeverity.NO_ALARM, 0, 0),
                        shortMetaData);
        ChannelAccessAggregatedSample aggregatedScalarShortSample = sampleValueAccess
                .createAggregatedScalarShortSample(987L, -2.3, 54.8,
                        (short) -190, (short) 177,
                        ChannelAccessAlarmSeverity.MAJOR_ALARM,
                        ChannelAccessAlarmStatus.HIHI, 0.25,
                        shortMetaDataSample);
        shortMetaData.setAlarmSeverity(ChannelAccessAlarmSeverity.MAJOR_ALARM);
        shortMetaData.setAlarmStatus(ChannelAccessAlarmStatus.HIHI);
        assertEquals(987L, aggregatedScalarShortSample.getTimeStamp());
        assertEquals(-2.3,
                sampleValueAccess
                        .deserializeMeanColumn(aggregatedScalarShortSample),
                0.0);
        assertEquals(54.8,
                sampleValueAccess
                        .deserializeStdColumn(aggregatedScalarShortSample), 0.0);
        assertEquals(-190,
                sampleValueAccess
                        .deserializeShortMinColumn(aggregatedScalarShortSample));
        assertEquals(177,
                sampleValueAccess
                        .deserializeShortMaxColumn(aggregatedScalarShortSample));
        assertEquals(
                0.25,
                sampleValueAccess
                        .deserializeCoveredPeriodFractionColumn(aggregatedScalarShortSample),
                0.0);
        assertEquals(
                shortMetaData,
                sampleValueAccess
                        .deserializeAggregatedSampleMetaData(aggregatedScalarShortSample));
        aggregatedScalarShortSample = sampleValueAccess
                .createAggregatedScalarShortSample(987L, -2.3, 54.8,
                        (short) -190, (short) 177,
                        ChannelAccessAlarmSeverity.MAJOR_ALARM,
                        ChannelAccessAlarmStatus.HIHI, 0.25,
                        aggregatedScalarShortSample);
        assertEquals(
                shortMetaData,
                sampleValueAccess
                        .deserializeAggregatedSampleMetaData(aggregatedScalarShortSample));
    }

    /**
     * Tests creating and deserializing raw samples. This effectively tests that
     * the conversion from Channel Access values to the respective Cassandra UDT
     * values and back to Channel Access values works correctly.
     */
    @Test
    public void testCreateAndDeserializeRawSamples() {
        LinkedList<Pair<? extends ChannelAccessTimeValue<?>, ? extends ChannelAccessControlsValue<?>>> valuePairs = new LinkedList<Pair<? extends ChannelAccessTimeValue<?>, ? extends ChannelAccessControlsValue<?>>>();
        // We have to create one single element and one multi- or zero-element
        // value for each type, because these types get serialized in a
        // different way.
        valuePairs.add(Pair.of(ChannelAccessValueFactory.createTimeChar(
                new byte[] { 18 }, ChannelAccessAlarmStatus.NO_ALARM,
                ChannelAccessAlarmSeverity.NO_ALARM, 14, 12345),
                ChannelAccessValueFactory.createControlsChar(new byte[0],
                        ChannelAccessAlarmStatus.NO_ALARM,
                        ChannelAccessAlarmSeverity.NO_ALARM, "Volts",
                        (byte) 85, (byte) -85, (byte) 30, (byte) 25, (byte) 10,
                        (byte) 5, (byte) 35, (byte) 5, Charsets.UTF_8)));
        valuePairs.add(Pair.of(ChannelAccessValueFactory.createTimeChar(
                new byte[] { 14, -37, 85 }, ChannelAccessAlarmStatus.NO_ALARM,
                ChannelAccessAlarmSeverity.NO_ALARM, 15, 12345),
                ChannelAccessValueFactory.createControlsChar(new byte[0],
                        ChannelAccessAlarmStatus.NO_ALARM,
                        ChannelAccessAlarmSeverity.NO_ALARM, "Volts",
                        (byte) 85, (byte) -85, (byte) 30, (byte) 25, (byte) 10,
                        (byte) 5, (byte) 35, (byte) 5, Charsets.UTF_8)));
        valuePairs.add(Pair.of(ChannelAccessValueFactory.createTimeChar(
                new byte[0], ChannelAccessAlarmStatus.NO_ALARM,
                ChannelAccessAlarmSeverity.NO_ALARM, 16, 12345),
                ChannelAccessValueFactory.createControlsChar(new byte[0],
                        ChannelAccessAlarmStatus.NO_ALARM,
                        ChannelAccessAlarmSeverity.NO_ALARM, "Volts",
                        (byte) 85, (byte) -85, (byte) 30, (byte) 25, (byte) 10,
                        (byte) 5, (byte) 35, (byte) 5, Charsets.UTF_8)));
        valuePairs.add(Pair.of(ChannelAccessValueFactory.createTimeDouble(
                new double[] { 42.0 }, ChannelAccessAlarmStatus.BAD_SUB,
                ChannelAccessAlarmSeverity.INVALID_ALARM, 17, 123),
                ChannelAccessValueFactory.createControlsDouble(new double[0],
                        ChannelAccessAlarmStatus.NO_ALARM,
                        ChannelAccessAlarmSeverity.NO_ALARM, (short) 3, "mA",
                        128.0, -5.0, 255.0, 200.0, -10.0, -15.0, 55.0, 5,
                        Charsets.UTF_8)));
        valuePairs.add(Pair.of(ChannelAccessValueFactory.createTimeDouble(
                new double[] { 42.0, -18.5, Double.NaN,
                        Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY },
                ChannelAccessAlarmStatus.BAD_SUB,
                ChannelAccessAlarmSeverity.INVALID_ALARM, 18, 123),
                ChannelAccessValueFactory.createControlsDouble(new double[0],
                        ChannelAccessAlarmStatus.NO_ALARM,
                        ChannelAccessAlarmSeverity.NO_ALARM, (short) 3, "mA",
                        128.0, -5.0, 255.0, 200.0, -10.0, -15.0, 55.0, 5,
                        Charsets.UTF_8)));
        valuePairs.add(Pair.of(ChannelAccessValueFactory.createTimeDouble(
                new double[0], ChannelAccessAlarmStatus.BAD_SUB,
                ChannelAccessAlarmSeverity.INVALID_ALARM, 19, 123),
                ChannelAccessValueFactory.createControlsDouble(new double[0],
                        ChannelAccessAlarmStatus.NO_ALARM,
                        ChannelAccessAlarmSeverity.NO_ALARM, (short) 3, "mA",
                        128.0, -5.0, 255.0, 200.0, -10.0, -15.0, 55.0, 5,
                        Charsets.UTF_8)));
        valuePairs.add(Pair.of(ChannelAccessValueFactory.createTimeEnum(
                new short[] { 3 }, ChannelAccessAlarmStatus.STATE,
                ChannelAccessAlarmSeverity.MINOR_ALARM, 20, 451),
                ChannelAccessValueFactory.createControlsEnum(new short[0],
                        ChannelAccessAlarmStatus.NO_ALARM,
                        ChannelAccessAlarmSeverity.NO_ALARM,
                        ImmutableList.of("abc", "def"), Charsets.UTF_8)));
        valuePairs.add(Pair.of(ChannelAccessValueFactory.createTimeEnum(
                new short[] { 3, -5 }, ChannelAccessAlarmStatus.STATE,
                ChannelAccessAlarmSeverity.MINOR_ALARM, 21, 451),
                ChannelAccessValueFactory.createControlsEnum(new short[0],
                        ChannelAccessAlarmStatus.NO_ALARM,
                        ChannelAccessAlarmSeverity.NO_ALARM,
                        ImmutableList.of("abc"), Charsets.UTF_8)));
        valuePairs.add(Pair.of(ChannelAccessValueFactory.createTimeEnum(
                new short[0], ChannelAccessAlarmStatus.STATE,
                ChannelAccessAlarmSeverity.MINOR_ALARM, 22, 451),
                ChannelAccessValueFactory.createControlsEnum(new short[0],
                        ChannelAccessAlarmStatus.NO_ALARM,
                        ChannelAccessAlarmSeverity.NO_ALARM,
                        Collections.<String> emptyList(), Charsets.UTF_8)));
        valuePairs.add(Pair.of(ChannelAccessValueFactory.createTimeFloat(
                new float[] { 42.0f }, ChannelAccessAlarmStatus.HIHI,
                ChannelAccessAlarmSeverity.MAJOR_ALARM, 23, 0),
                ChannelAccessValueFactory.createControlsFloat(new float[0],
                        ChannelAccessAlarmStatus.NO_ALARM,
                        ChannelAccessAlarmSeverity.NO_ALARM, (short) 1, "V",
                        128.0f, -5.0f, 255.0f, 200.0f, -10.0f, -15.0f, 55.0f,
                        5, Charsets.UTF_8)));
        valuePairs.add(Pair.of(ChannelAccessValueFactory.createTimeFloat(
                new float[] { 42.0f, -23.0f, 128.5f, -99.0f, Float.NaN,
                        Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY },
                ChannelAccessAlarmStatus.HIHI,
                ChannelAccessAlarmSeverity.MAJOR_ALARM, 24, 0),
                ChannelAccessValueFactory.createControlsFloat(new float[0],
                        ChannelAccessAlarmStatus.NO_ALARM,
                        ChannelAccessAlarmSeverity.NO_ALARM, (short) 1, "V",
                        128.0f, -5.0f, 255.0f, 200.0f, -10.0f, -15.0f, 55.0f,
                        5, Charsets.UTF_8)));
        valuePairs.add(Pair.of(ChannelAccessValueFactory.createTimeFloat(
                new float[0], ChannelAccessAlarmStatus.HIHI,
                ChannelAccessAlarmSeverity.MAJOR_ALARM, 25, 0),
                ChannelAccessValueFactory.createControlsFloat(new float[0],
                        ChannelAccessAlarmStatus.NO_ALARM,
                        ChannelAccessAlarmSeverity.NO_ALARM, (short) 1, "V",
                        128.0f, -5.0f, 255.0f, 200.0f, -10.0f, -15.0f, 55.0f,
                        5, Charsets.UTF_8)));
        valuePairs.add(Pair.of(ChannelAccessValueFactory.createTimeLong(
                new int[] { 223323 }, ChannelAccessAlarmStatus.NO_ALARM,
                ChannelAccessAlarmSeverity.NO_ALARM, 26, 420),
                ChannelAccessValueFactory.createControlsLong(new int[0],
                        ChannelAccessAlarmStatus.NO_ALARM,
                        ChannelAccessAlarmSeverity.NO_ALARM, "MHz", 85, -85,
                        30, 25, 10, 5, 35, 5, Charsets.UTF_8)));
        valuePairs.add(Pair.of(ChannelAccessValueFactory.createTimeLong(
                new int[] { 223323, -99 }, ChannelAccessAlarmStatus.NO_ALARM,
                ChannelAccessAlarmSeverity.NO_ALARM, 27, 420),
                ChannelAccessValueFactory.createControlsLong(new int[0],
                        ChannelAccessAlarmStatus.NO_ALARM,
                        ChannelAccessAlarmSeverity.NO_ALARM, "MHz", 85, -85,
                        30, 25, 10, 5, 35, 5, Charsets.UTF_8)));
        valuePairs.add(Pair.of(ChannelAccessValueFactory.createTimeLong(
                new int[0], ChannelAccessAlarmStatus.NO_ALARM,
                ChannelAccessAlarmSeverity.NO_ALARM, 28, 420),
                ChannelAccessValueFactory.createControlsLong(new int[0],
                        ChannelAccessAlarmStatus.NO_ALARM,
                        ChannelAccessAlarmSeverity.NO_ALARM, "MHz", 85, -85,
                        30, 25, 10, 5, 35, 5, Charsets.UTF_8)));
        valuePairs.add(Pair.of(ChannelAccessValueFactory.createTimeShort(
                new short[] { 761 }, ChannelAccessAlarmStatus.LOW,
                ChannelAccessAlarmSeverity.MINOR_ALARM, 29, 99),
                ChannelAccessValueFactory.createControlsShort(new short[0],
                        ChannelAccessAlarmStatus.NO_ALARM,
                        ChannelAccessAlarmSeverity.NO_ALARM, "Ampere",
                        (short) 85, (short) -85, (short) 30, (short) 25,
                        (short) 10, (short) 5, (short) 35, (short) 5,
                        Charsets.UTF_8)));
        valuePairs.add(Pair.of(ChannelAccessValueFactory.createTimeShort(
                new short[] { 55, -37, 128, Short.MAX_VALUE },
                ChannelAccessAlarmStatus.LOW,
                ChannelAccessAlarmSeverity.MINOR_ALARM, 30, 99),
                ChannelAccessValueFactory.createControlsShort(new short[0],
                        ChannelAccessAlarmStatus.NO_ALARM,
                        ChannelAccessAlarmSeverity.NO_ALARM, "Ampere",
                        (short) 85, (short) -85, (short) 30, (short) 25,
                        (short) 10, (short) 5, (short) 35, (short) 5,
                        Charsets.UTF_8)));
        valuePairs.add(Pair.of(ChannelAccessValueFactory.createTimeShort(
                new short[0], ChannelAccessAlarmStatus.LOW,
                ChannelAccessAlarmSeverity.MINOR_ALARM, 31, 99),
                ChannelAccessValueFactory.createControlsShort(new short[0],
                        ChannelAccessAlarmStatus.NO_ALARM,
                        ChannelAccessAlarmSeverity.NO_ALARM, "Ampere",
                        (short) 85, (short) -85, (short) 30, (short) 25,
                        (short) 10, (short) 5, (short) 35, (short) 5,
                        Charsets.UTF_8)));
        valuePairs.add(Pair.of(ChannelAccessValueFactory.createTimeString(
                ImmutableList.of("abc"), ChannelAccessAlarmStatus.COMM,
                ChannelAccessAlarmSeverity.MAJOR_ALARM, 32, 0, Charsets.UTF_8),
                ChannelAccessValueFactory.createControlsString(
                        Collections.<String> emptyList(),
                        ChannelAccessAlarmStatus.NO_ALARM,
                        ChannelAccessAlarmSeverity.NO_ALARM, Charsets.UTF_8)));
        valuePairs.add(Pair.of(ChannelAccessValueFactory.createTimeString(
                ImmutableList.of("abc", "def", "xyz123"),
                ChannelAccessAlarmStatus.COMM,
                ChannelAccessAlarmSeverity.MAJOR_ALARM, 33, 0, Charsets.UTF_8),
                ChannelAccessValueFactory.createControlsString(
                        Collections.<String> emptyList(),
                        ChannelAccessAlarmStatus.NO_ALARM,
                        ChannelAccessAlarmSeverity.NO_ALARM, Charsets.UTF_8)));
        valuePairs.add(Pair.of(ChannelAccessValueFactory.createTimeString(
                Collections.<String> emptyList(),
                ChannelAccessAlarmStatus.COMM,
                ChannelAccessAlarmSeverity.MAJOR_ALARM, 34, 0, Charsets.UTF_8),
                ChannelAccessValueFactory.createControlsString(
                        Collections.<String> emptyList(),
                        ChannelAccessAlarmStatus.NO_ALARM,
                        ChannelAccessAlarmSeverity.NO_ALARM, Charsets.UTF_8)));
        for (Pair<? extends ChannelAccessTimeValue<?>, ? extends ChannelAccessControlsValue<?>> valuePair : valuePairs) {
            ChannelAccessRawSample sample = sampleValueAccess.createRawSample(
                    valuePair.getLeft(), valuePair.getRight());
            assertEquals(valuePair.getLeft().getTimeStamp()
                    .toNanosecondsEPICS().longValue() + 631152000000000000L,
                    sample.getTimeStamp());
            ChannelAccessControlsValue<?> expectedValue = mergeValuePair(
                    valuePair.getLeft(), valuePair.getRight());
            ChannelAccessControlsValue<?> actualValue = sampleValueAccess
                    .deserializeSample(sample);
            assertEquals(expectedValue, actualValue);
        }
    }

}
