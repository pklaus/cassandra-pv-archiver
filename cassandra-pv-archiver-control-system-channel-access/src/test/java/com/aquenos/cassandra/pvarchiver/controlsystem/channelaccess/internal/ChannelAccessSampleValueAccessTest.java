/*
 * Copyright 2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.controlsystem.channelaccess.internal;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Collection;
import java.util.Collections;

import org.apache.commons.lang3.ArrayUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aquenos.cassandra.pvarchiver.controlsystem.channelaccess.ChannelAccessSample;
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
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;

/**
 * Tests for the {@link ChannelAccessSampleValueAccess}.
 * 
 * @author Sebastian Marsching
 */
public class ChannelAccessSampleValueAccessTest {

    private static JsonFactory jsonFactory;
    private static ChannelAccessSampleValueAccess sampleValueAccess;

    /**
     * Creates the {@link JsonFactory} that is internally used for testing the
     * JSON serialization code.
     */
    @BeforeClass
    public static void initializeJsonFactory() {
        jsonFactory = new JsonFactory();
        // Enabling the duplicate detection allows us to catch errors early.
        jsonFactory.enable(JsonGenerator.Feature.STRICT_DUPLICATE_DETECTION);
        // If the code works correctly, it should close all objects, so that we
        // do not want this feature.
        jsonFactory.disable(JsonGenerator.Feature.AUTO_CLOSE_JSON_CONTENT);
    }

    /**
     * Creates the {@link ChannelAccessSampleValueAccess} instance that is
     * tested.
     */
    @BeforeClass
    public static void initializeSampleValueAccess() {
        Session session = EmbeddedCassandraServer.getSession();
        sampleValueAccess = new ChannelAccessSampleValueAccess(session);
    }

    private static ChannelAccessControlsChar createControlsChar() {
        return ChannelAccessValueFactory.createControlsChar(new byte[0],
                ChannelAccessAlarmStatus.NO_ALARM,
                ChannelAccessAlarmSeverity.NO_ALARM, "Volts", (byte) 85,
                (byte) -85, (byte) 30, (byte) 25, (byte) 10, (byte) 5,
                (byte) 35, (byte) 5, Charsets.UTF_8);
    }

    private static ChannelAccessControlsDouble createControlsDouble() {
        return ChannelAccessValueFactory.createControlsDouble(new double[0],
                ChannelAccessAlarmStatus.NO_ALARM,
                ChannelAccessAlarmSeverity.NO_ALARM, (short) 3, "mA", 128.0,
                -5.0, 255.0, 200.0, -10.0, -15.0, 55.0, 5, Charsets.UTF_8);
    }

    private static ChannelAccessGraphicsEnum createControlsEnum() {
        return ChannelAccessValueFactory.createControlsEnum(new short[0],
                ChannelAccessAlarmStatus.NO_ALARM,
                ChannelAccessAlarmSeverity.NO_ALARM,
                ImmutableList.of("abc", "def"), Charsets.UTF_8);
    }

    private static ChannelAccessControlsFloat createControlsFloat() {
        return ChannelAccessValueFactory
                .createControlsFloat(new float[0],
                        ChannelAccessAlarmStatus.NO_ALARM,
                        ChannelAccessAlarmSeverity.NO_ALARM, (short) 1, "V",
                        128.0f, -5.0f, 255.0f, 200.0f, -10.0f, -15.0f, 55.0f,
                        5, Charsets.UTF_8);
    }

    private static ChannelAccessControlsLong createControlsLong() {
        return ChannelAccessValueFactory.createControlsLong(new int[0],
                ChannelAccessAlarmStatus.NO_ALARM,
                ChannelAccessAlarmSeverity.NO_ALARM, "MHz", 85, -85, 30, 25,
                10, 5, 35, 5, Charsets.UTF_8);
    }

    private static ChannelAccessControlsShort createControlsShort() {
        return ChannelAccessValueFactory.createControlsShort(new short[0],
                ChannelAccessAlarmStatus.NO_ALARM,
                ChannelAccessAlarmSeverity.NO_ALARM, "Ampere", (short) 85,
                (short) -85, (short) 30, (short) 25, (short) 10, (short) 5,
                (short) 35, (short) 5, Charsets.UTF_8);
    }

    private static ChannelAccessAlarmOnlyString createControlsString() {
        return ChannelAccessValueFactory.createControlsString(
                Collections.<String> emptyList(),
                ChannelAccessAlarmStatus.NO_ALARM,
                ChannelAccessAlarmSeverity.NO_ALARM, Charsets.UTF_8);
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

    private static String serializeSampleToJson(ChannelAccessSample sample) {
        try {
            StringWriter stringWriter = new StringWriter();
            JsonGenerator jsonGenerator = jsonFactory
                    .createGenerator(stringWriter);
            jsonGenerator.writeStartObject();
            sampleValueAccess.serializeSampleToJsonV1(jsonGenerator, sample);
            jsonGenerator.writeEndObject();
            jsonGenerator.close();
            return stringWriter.getBuffer().toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void testCreateAndDeserializeRawSample(
            ChannelAccessTimeValue<?> timeValue,
            ChannelAccessControlsValue<?> controlsValue, String expectedJson) {
        ChannelAccessRawSample sample = sampleValueAccess.createRawSample(
                timeValue, controlsValue);
        assertEquals(
                timeValue.getTimeStamp().toNanosecondsEPICS().longValue() + 631152000000000000L,
                sample.getTimeStamp());
        ChannelAccessControlsValue<?> expectedValue = mergeValuePair(timeValue,
                controlsValue);
        ChannelAccessControlsValue<?> actualValue = sampleValueAccess
                .deserializeSample(sample);
        assertEquals(expectedValue, actualValue);
        // For the JSON serialization, we also test what happens when the
        // sample is not an "original" sample.
        ChannelAccessRawSample decimatedSample = new ChannelAccessRawSample(
                sample.getTimeStamp(), sample.getType(), sample.getValue(),
                false);
        // The "quality" in the JSON string depends on whether we have an
        // original or decimated sample, so we use a placeholder for it.
        assertEquals(expectedJson.replace("%quality%", "Original"),
                serializeSampleToJson(sample));
        assertEquals(expectedJson.replace("%quality%", "Interpolated"),
                serializeSampleToJson(decimatedSample));
    }

    /**
     * Test creating and deserializing aggregated scalar char samples. This
     * effectively tests that the conversion from the statistical values and the
     * meta data to the respective Cassandra UDT values and back works
     * correctly. This test also verifies that the conversion to JSON works as
     * intended. The latter test checks for actual equality of the JSON strings,
     * so minor modifications in the JSON serialization logic can yield negative
     * test results even though the code is actually compliant.
     */
    @Test
    public void testCreateAndDeserializeAggregatedScalarCharSample() {
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
                .createAggregatedScalarCharSample(123L, 42.25, 5.4, (byte) -19,
                        (byte) 115, ChannelAccessAlarmSeverity.MAJOR_ALARM,
                        ChannelAccessAlarmStatus.HWLIMIT, 0.4375,
                        charMetaDataSample);
        charMetaData.setAlarmSeverity(ChannelAccessAlarmSeverity.MAJOR_ALARM);
        charMetaData.setAlarmStatus(ChannelAccessAlarmStatus.HWLIMIT);
        assertEquals(123L, aggregatedScalarCharSample.getTimeStamp());
        assertEquals(42.25,
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
                0.4375,
                sampleValueAccess
                        .deserializeCoveredPeriodFractionColumn(aggregatedScalarCharSample),
                0.0);
        aggregatedScalarCharSample = sampleValueAccess
                .createAggregatedScalarCharSample(123L, 42.25, 5.4, (byte) -19,
                        (byte) 115, ChannelAccessAlarmSeverity.MAJOR_ALARM,
                        ChannelAccessAlarmStatus.HWLIMIT, 0.4375,
                        aggregatedScalarCharSample);
        assertEquals(
                charMetaData,
                sampleValueAccess
                        .deserializeAggregatedSampleMetaData(aggregatedScalarCharSample));
        assertEquals(
                "{\"time\":123,\"severity\":{\"level\":\"MAJOR\",\"hasValue\":true},\"status\":\"HWLIMIT\",\"quality\":\"Interpolated\",\"metaData\":{\"type\":\"numeric\",\"precision\":0,\"units\":\"Volts\",\"displayLow\":-85.0,\"displayHigh\":85.0,\"warnLow\":10.0,\"warnHigh\":25.0,\"alarmLow\":5.0,\"alarmHigh\":30.0},\"type\":\"minMaxDouble\",\"value\":[42.25],\"minimum\":-19.0,\"maximum\":115.0}",
                serializeSampleToJson(aggregatedScalarCharSample));
    }

    /**
     * Test creating and deserializing aggregated scalar double samples. This
     * effectively tests that the conversion from the statistical values and the
     * meta data to the respective Cassandra UDT values and back works
     * correctly. This test also verifies that the conversion to JSON works as
     * intended. The latter test checks for actual equality of the JSON strings,
     * so minor modifications in the JSON serialization logic can yield negative
     * test results even though the code is actually compliant.
     */
    @Test
    public void testCreateAndDeserializeAggregatedScalarDoubleSample() {
        ChannelAccessControlsDouble doubleMetaData = ChannelAccessValueFactory
                .createControlsDouble(ArrayUtils.EMPTY_DOUBLE_ARRAY,
                        ChannelAccessAlarmStatus.LOW,
                        ChannelAccessAlarmSeverity.MINOR_ALARM, (short) 3,
                        "Ampere", 23.5, -99.625, 30.0, 25.25, 10.375, 5.75,
                        35.125, 5.25, Charsets.UTF_8);
        ChannelAccessRawSample doubleMetaDataSample = sampleValueAccess
                .createRawSample(ChannelAccessValueFactory.createTimeDouble(
                        ArrayUtils.EMPTY_DOUBLE_ARRAY,
                        ChannelAccessAlarmStatus.NO_ALARM,
                        ChannelAccessAlarmSeverity.NO_ALARM, 0, 0),
                        doubleMetaData);
        ChannelAccessAggregatedSample aggregatedScalarDoubleSample = sampleValueAccess
                .createAggregatedScalarDoubleSample(321L, -42.75, 6.375,
                        -199.0, 21.375, ChannelAccessAlarmSeverity.NO_ALARM,
                        ChannelAccessAlarmStatus.NO_ALARM, 1.0,
                        doubleMetaDataSample);
        doubleMetaData.setAlarmSeverity(ChannelAccessAlarmSeverity.NO_ALARM);
        doubleMetaData.setAlarmStatus(ChannelAccessAlarmStatus.NO_ALARM);
        assertEquals(321L, aggregatedScalarDoubleSample.getTimeStamp());
        assertEquals(-42.75,
                sampleValueAccess
                        .deserializeMeanColumn(aggregatedScalarDoubleSample),
                0.0);
        assertEquals(6.375,
                sampleValueAccess
                        .deserializeStdColumn(aggregatedScalarDoubleSample),
                0.0);
        assertEquals(
                -199.0,
                sampleValueAccess
                        .deserializeDoubleMinColumn(aggregatedScalarDoubleSample),
                0.0);
        assertEquals(
                21.375,
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
                .createAggregatedScalarDoubleSample(321L, -42.75, 6.375,
                        -199.0, 21.375, ChannelAccessAlarmSeverity.NO_ALARM,
                        ChannelAccessAlarmStatus.NO_ALARM, 1.0,
                        aggregatedScalarDoubleSample);
        assertEquals(
                doubleMetaData,
                sampleValueAccess
                        .deserializeAggregatedSampleMetaData(aggregatedScalarDoubleSample));
        assertEquals(
                "{\"time\":321,\"severity\":{\"level\":\"OK\",\"hasValue\":true},\"status\":\"NO_ALARM\",\"quality\":\"Interpolated\",\"metaData\":{\"type\":\"numeric\",\"precision\":3,\"units\":\"Ampere\",\"displayLow\":-99.625,\"displayHigh\":23.5,\"warnLow\":10.375,\"warnHigh\":25.25,\"alarmLow\":5.75,\"alarmHigh\":30.0},\"type\":\"minMaxDouble\",\"value\":[-42.75],\"minimum\":-199.0,\"maximum\":21.375}",
                serializeSampleToJson(aggregatedScalarDoubleSample));
    }

    /**
     * Test creating and deserializing aggregated scalar float samples. This
     * effectively tests that the conversion from the statistical values and the
     * meta data to the respective Cassandra UDT values and back works
     * correctly. This test also verifies that the conversion to JSON works as
     * intended. The latter test checks for actual equality of the JSON strings,
     * so minor modifications in the JSON serialization logic can yield negative
     * test results even though the code is actually compliant.
     */
    @Test
    public void testCreateAndDeserializeAggregatedScalarFloatSample() {
        ChannelAccessControlsFloat floatMetaData = ChannelAccessValueFactory
                .createControlsFloat(ArrayUtils.EMPTY_FLOAT_ARRAY,
                        ChannelAccessAlarmStatus.NO_ALARM,
                        ChannelAccessAlarmSeverity.NO_ALARM, (short) 2,
                        "Kelvin", 23.75f, -98.625f, 35.0f, -25.25f, 10.375f,
                        5.875f, 35.125f, 5.25f, Charsets.UTF_8);
        ChannelAccessRawSample floatMetaDataSample = sampleValueAccess
                .createRawSample(ChannelAccessValueFactory.createTimeFloat(
                        ArrayUtils.EMPTY_FLOAT_ARRAY,
                        ChannelAccessAlarmStatus.NO_ALARM,
                        ChannelAccessAlarmSeverity.NO_ALARM, 0, 0),
                        floatMetaData);
        ChannelAccessAggregatedSample aggregatedScalarFloatSample = sampleValueAccess
                .createAggregatedScalarFloatSample(456L, -13.75, 64.25,
                        -201.0f, 21.296875f,
                        ChannelAccessAlarmSeverity.INVALID_ALARM,
                        ChannelAccessAlarmStatus.BAD_SUB, 0.99,
                        floatMetaDataSample);
        floatMetaData
                .setAlarmSeverity(ChannelAccessAlarmSeverity.INVALID_ALARM);
        floatMetaData.setAlarmStatus(ChannelAccessAlarmStatus.BAD_SUB);
        assertEquals(456L, aggregatedScalarFloatSample.getTimeStamp());
        assertEquals(-13.75,
                sampleValueAccess
                        .deserializeMeanColumn(aggregatedScalarFloatSample),
                0.0);
        assertEquals(64.25,
                sampleValueAccess
                        .deserializeStdColumn(aggregatedScalarFloatSample), 0.0);
        assertEquals(
                -201.0f,
                sampleValueAccess
                        .deserializeFloatMinColumn(aggregatedScalarFloatSample),
                0.0f);
        assertEquals(
                21.296875f,
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
                .createAggregatedScalarFloatSample(456L, -13.75, 64.25,
                        -201.0f, 21.296875f,
                        ChannelAccessAlarmSeverity.INVALID_ALARM,
                        ChannelAccessAlarmStatus.BAD_SUB, 0.99,
                        aggregatedScalarFloatSample);
        assertEquals(
                floatMetaData,
                sampleValueAccess
                        .deserializeAggregatedSampleMetaData(aggregatedScalarFloatSample));
        assertEquals(
                "{\"time\":456,\"severity\":{\"level\":\"INVALID\",\"hasValue\":true},\"status\":\"BAD_SUB\",\"quality\":\"Interpolated\",\"metaData\":{\"type\":\"numeric\",\"precision\":2,\"units\":\"Kelvin\",\"displayLow\":-98.625,\"displayHigh\":23.75,\"warnLow\":10.375,\"warnHigh\":-25.25,\"alarmLow\":5.875,\"alarmHigh\":35.0},\"type\":\"minMaxDouble\",\"value\":[-13.75],\"minimum\":-201.0,\"maximum\":21.296875}",
                serializeSampleToJson(aggregatedScalarFloatSample));
    }

    /**
     * Test creating and deserializing aggregated scalar long samples. This
     * effectively tests that the conversion from the statistical values and the
     * meta data to the respective Cassandra UDT values and back works
     * correctly. This test also verifies that the conversion to JSON works as
     * intended. The latter test checks for actual equality of the JSON strings,
     * so minor modifications in the JSON serialization logic can yield negative
     * test results even though the code is actually compliant.
     */
    @Test
    public void testCreateAndDeserializeAggregatedScalarLongSample() {
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
                .createAggregatedScalarLongSample(789L, -99.875, 7.875, -55,
                        2147, ChannelAccessAlarmSeverity.NO_ALARM,
                        ChannelAccessAlarmStatus.NO_ALARM, 0.1,
                        longMetaDataSample);
        longMetaData.setAlarmSeverity(ChannelAccessAlarmSeverity.NO_ALARM);
        longMetaData.setAlarmStatus(ChannelAccessAlarmStatus.NO_ALARM);
        assertEquals(789L, aggregatedScalarLongSample.getTimeStamp());
        assertEquals(-99.875,
                sampleValueAccess
                        .deserializeMeanColumn(aggregatedScalarLongSample), 0.0);
        assertEquals(7.875,
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
                .createAggregatedScalarLongSample(789L, -99.875, 7.875, -55,
                        2147, ChannelAccessAlarmSeverity.NO_ALARM,
                        ChannelAccessAlarmStatus.NO_ALARM, 0.1,
                        aggregatedScalarLongSample);
        assertEquals(
                longMetaData,
                sampleValueAccess
                        .deserializeAggregatedSampleMetaData(aggregatedScalarLongSample));
        assertEquals(
                "{\"time\":789,\"severity\":{\"level\":\"OK\",\"hasValue\":true},\"status\":\"NO_ALARM\",\"quality\":\"Interpolated\",\"metaData\":{\"type\":\"numeric\",\"precision\":0,\"units\":\"Volts\",\"displayLow\":-87.0,\"displayHigh\":86.0,\"warnLow\":11.0,\"warnHigh\":25.0,\"alarmLow\":5.0,\"alarmHigh\":29.0},\"type\":\"minMaxDouble\",\"value\":[-99.875],\"minimum\":-55.0,\"maximum\":2147.0}",
                serializeSampleToJson(aggregatedScalarLongSample));
    }

    /**
     * Test creating and deserializing aggregated scalar short samples. This
     * effectively tests that the conversion from the statistical values and the
     * meta data to the respective Cassandra UDT values and back works
     * correctly. This test also verifies that the conversion to JSON works as
     * intended. The latter test checks for actual equality of the JSON strings,
     * so minor modifications in the JSON serialization logic can yield negative
     * test results even though the code is actually compliant.
     */
    @Test
    public void testCreateAndDeserializeAggregatedScalarShortSample() {
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
                .createAggregatedScalarShortSample(987L, -2.375, 54.75,
                        (short) -190, (short) 177,
                        ChannelAccessAlarmSeverity.MAJOR_ALARM,
                        ChannelAccessAlarmStatus.HIHI, 0.25,
                        shortMetaDataSample);
        shortMetaData.setAlarmSeverity(ChannelAccessAlarmSeverity.MAJOR_ALARM);
        shortMetaData.setAlarmStatus(ChannelAccessAlarmStatus.HIHI);
        assertEquals(987L, aggregatedScalarShortSample.getTimeStamp());
        assertEquals(-2.375,
                sampleValueAccess
                        .deserializeMeanColumn(aggregatedScalarShortSample),
                0.0);
        assertEquals(54.75,
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
                .createAggregatedScalarShortSample(987L, -2.375, 54.75,
                        (short) -190, (short) 177,
                        ChannelAccessAlarmSeverity.MAJOR_ALARM,
                        ChannelAccessAlarmStatus.HIHI, 0.25,
                        aggregatedScalarShortSample);
        assertEquals(
                shortMetaData,
                sampleValueAccess
                        .deserializeAggregatedSampleMetaData(aggregatedScalarShortSample));
        assertEquals(
                "{\"time\":987,\"severity\":{\"level\":\"MAJOR\",\"hasValue\":true},\"status\":\"HIHI\",\"quality\":\"Interpolated\",\"metaData\":{\"type\":\"numeric\",\"precision\":0,\"units\":\"Volts\",\"displayLow\":-185.0,\"displayHigh\":86.0,\"warnLow\":15.0,\"warnHigh\":-25.0,\"alarmLow\":-50.0,\"alarmHigh\":333.0},\"type\":\"minMaxDouble\",\"value\":[-2.375],\"minimum\":-190.0,\"maximum\":177.0}",
                serializeSampleToJson(aggregatedScalarShortSample));
    }

    /**
     * Tests creating and deserializing char samples. This effectively tests
     * that the conversion from Channel Access values to the respective
     * Cassandra UDT values and back to Channel Access values works correctly.
     * This test also verifies that the conversion to JSON works as intended.
     * The latter test checks for actual equality of the JSON strings, so minor
     * modifications in the JSON serialization logic can yield negative test
     * results even though the code is actually compliant.
     */
    @Test
    public void testCreateAndDeserializeCharSample() {
        testCreateAndDeserializeRawSample(
                ChannelAccessValueFactory.createTimeChar(new byte[] { 18 },
                        ChannelAccessAlarmStatus.NO_ALARM,
                        ChannelAccessAlarmSeverity.NO_ALARM, 14, 12345),
                createControlsChar(),
                "{\"time\":631152014000012345,\"severity\":{\"level\":\"OK\",\"hasValue\":true},\"status\":\"NO_ALARM\",\"quality\":\"%quality%\",\"metaData\":{\"type\":\"numeric\",\"precision\":0,\"units\":\"Volts\",\"displayLow\":-85.0,\"displayHigh\":85.0,\"warnLow\":10.0,\"warnHigh\":25.0,\"alarmLow\":5.0,\"alarmHigh\":30.0},\"type\":\"long\",\"value\":[18]}");
        testCreateAndDeserializeRawSample(
                ChannelAccessValueFactory.createTimeChar(new byte[] { 14, -37,
                        85 }, ChannelAccessAlarmStatus.NO_ALARM,
                        ChannelAccessAlarmSeverity.NO_ALARM, 15, 12345),
                createControlsChar(),
                "{\"time\":631152015000012345,\"severity\":{\"level\":\"OK\",\"hasValue\":true},\"status\":\"NO_ALARM\",\"quality\":\"%quality%\",\"metaData\":{\"type\":\"numeric\",\"precision\":0,\"units\":\"Volts\",\"displayLow\":-85.0,\"displayHigh\":85.0,\"warnLow\":10.0,\"warnHigh\":25.0,\"alarmLow\":5.0,\"alarmHigh\":30.0},\"type\":\"long\",\"value\":[14,-37,85]}");
        testCreateAndDeserializeRawSample(
                ChannelAccessValueFactory.createTimeChar(new byte[0],
                        ChannelAccessAlarmStatus.NO_ALARM,
                        ChannelAccessAlarmSeverity.NO_ALARM, 16, 12345),
                createControlsChar(),
                "{\"time\":631152016000012345,\"severity\":{\"level\":\"OK\",\"hasValue\":true},\"status\":\"NO_ALARM\",\"quality\":\"%quality%\",\"metaData\":{\"type\":\"numeric\",\"precision\":0,\"units\":\"Volts\",\"displayLow\":-85.0,\"displayHigh\":85.0,\"warnLow\":10.0,\"warnHigh\":25.0,\"alarmLow\":5.0,\"alarmHigh\":30.0},\"type\":\"long\",\"value\":[]}");
    }

    /**
     * Tests creating and deserializing double samples. This effectively tests
     * that the conversion from Channel Access values to the respective
     * Cassandra UDT values and back to Channel Access values works correctly.
     * This test also verifies that the conversion to JSON works as intended.
     * The latter test checks for actual equality of the JSON strings, so minor
     * modifications in the JSON serialization logic can yield negative test
     * results even though the code is actually compliant.
     */
    @Test
    public void testCreateAndDeserializeDoubleSample() {
        testCreateAndDeserializeRawSample(
                ChannelAccessValueFactory.createTimeDouble(
                        new double[] { 42.0 },
                        ChannelAccessAlarmStatus.BAD_SUB,
                        ChannelAccessAlarmSeverity.INVALID_ALARM, 17, 123),
                createControlsDouble(),
                "{\"time\":631152017000000123,\"severity\":{\"level\":\"INVALID\",\"hasValue\":true},\"status\":\"BAD_SUB\",\"quality\":\"%quality%\",\"metaData\":{\"type\":\"numeric\",\"precision\":3,\"units\":\"mA\",\"displayLow\":-5.0,\"displayHigh\":128.0,\"warnLow\":-10.0,\"warnHigh\":200.0,\"alarmLow\":-15.0,\"alarmHigh\":255.0},\"type\":\"double\",\"value\":[42.0]}");
        testCreateAndDeserializeRawSample(
                ChannelAccessValueFactory.createTimeDouble(new double[] { 42.0,
                        -18.5, Double.NaN, Double.NEGATIVE_INFINITY,
                        Double.POSITIVE_INFINITY },
                        ChannelAccessAlarmStatus.BAD_SUB,
                        ChannelAccessAlarmSeverity.INVALID_ALARM, 18, 123),
                createControlsDouble(),
                "{\"time\":631152018000000123,\"severity\":{\"level\":\"INVALID\",\"hasValue\":true},\"status\":\"BAD_SUB\",\"quality\":\"%quality%\",\"metaData\":{\"type\":\"numeric\",\"precision\":3,\"units\":\"mA\",\"displayLow\":-5.0,\"displayHigh\":128.0,\"warnLow\":-10.0,\"warnHigh\":200.0,\"alarmLow\":-15.0,\"alarmHigh\":255.0},\"type\":\"double\",\"value\":[42.0,-18.5,\"NaN\",\"-Infinity\",\"Infinity\"]}");
        testCreateAndDeserializeRawSample(
                ChannelAccessValueFactory.createTimeDouble(new double[0],
                        ChannelAccessAlarmStatus.BAD_SUB,
                        ChannelAccessAlarmSeverity.INVALID_ALARM, 19, 123),
                createControlsDouble(),
                "{\"time\":631152019000000123,\"severity\":{\"level\":\"INVALID\",\"hasValue\":true},\"status\":\"BAD_SUB\",\"quality\":\"%quality%\",\"metaData\":{\"type\":\"numeric\",\"precision\":3,\"units\":\"mA\",\"displayLow\":-5.0,\"displayHigh\":128.0,\"warnLow\":-10.0,\"warnHigh\":200.0,\"alarmLow\":-15.0,\"alarmHigh\":255.0},\"type\":\"double\",\"value\":[]}");
    }

    /**
     * Tests creating and deserializing enum samples. This effectively tests
     * that the conversion from Channel Access values to the respective
     * Cassandra UDT values and back to Channel Access values works correctly.
     * This test also verifies that the conversion to JSON works as intended.
     * The latter test checks for actual equality of the JSON strings, so minor
     * modifications in the JSON serialization logic can yield negative test
     * results even though the code is actually compliant.
     */
    @Test
    public void testCreateAndDeserializeEnumSample() {
        testCreateAndDeserializeRawSample(
                ChannelAccessValueFactory.createTimeEnum(new short[] { 3 },
                        ChannelAccessAlarmStatus.STATE,
                        ChannelAccessAlarmSeverity.MINOR_ALARM, 20, 451),
                createControlsEnum(),
                "{\"time\":631152020000000451,\"severity\":{\"level\":\"MINOR\",\"hasValue\":true},\"status\":\"STATE\",\"quality\":\"%quality%\",\"metaData\":{\"type\":\"enum\",\"states\":[\"abc\",\"def\"]},\"type\":\"enum\",\"value\":[3]}");
        testCreateAndDeserializeRawSample(
                ChannelAccessValueFactory.createTimeEnum(new short[] { 3, -5 },
                        ChannelAccessAlarmStatus.STATE,
                        ChannelAccessAlarmSeverity.MINOR_ALARM, 21, 451),
                createControlsEnum(),
                "{\"time\":631152021000000451,\"severity\":{\"level\":\"MINOR\",\"hasValue\":true},\"status\":\"STATE\",\"quality\":\"%quality%\",\"metaData\":{\"type\":\"enum\",\"states\":[\"abc\",\"def\"]},\"type\":\"enum\",\"value\":[3,-5]}");
        testCreateAndDeserializeRawSample(
                ChannelAccessValueFactory.createTimeEnum(new short[0],
                        ChannelAccessAlarmStatus.STATE,
                        ChannelAccessAlarmSeverity.MINOR_ALARM, 22, 451),
                createControlsEnum(),
                "{\"time\":631152022000000451,\"severity\":{\"level\":\"MINOR\",\"hasValue\":true},\"status\":\"STATE\",\"quality\":\"%quality%\",\"metaData\":{\"type\":\"enum\",\"states\":[\"abc\",\"def\"]},\"type\":\"enum\",\"value\":[]}");
    }

    /**
     * Tests creating and deserializing float samples. This effectively tests
     * that the conversion from Channel Access values to the respective
     * Cassandra UDT values and back to Channel Access values works correctly.
     * This test also verifies that the conversion to JSON works as intended.
     * The latter test checks for actual equality of the JSON strings, so minor
     * modifications in the JSON serialization logic can yield negative test
     * results even though the code is actually compliant.
     */
    @Test
    public void testCreateAndDeserializeFloatSample() {
        testCreateAndDeserializeRawSample(
                ChannelAccessValueFactory.createTimeFloat(
                        new float[] { 42.0f }, ChannelAccessAlarmStatus.HIHI,
                        ChannelAccessAlarmSeverity.MAJOR_ALARM, 23, 0),
                createControlsFloat(),
                "{\"time\":631152023000000000,\"severity\":{\"level\":\"MAJOR\",\"hasValue\":true},\"status\":\"HIHI\",\"quality\":\"%quality%\",\"metaData\":{\"type\":\"numeric\",\"precision\":1,\"units\":\"V\",\"displayLow\":-5.0,\"displayHigh\":128.0,\"warnLow\":-10.0,\"warnHigh\":200.0,\"alarmLow\":-15.0,\"alarmHigh\":255.0},\"type\":\"double\",\"value\":[42.0]}");
        testCreateAndDeserializeRawSample(
                ChannelAccessValueFactory.createTimeFloat(new float[] { 42.0f,
                        -23.0f, 128.5f, -99.0f, Float.NaN,
                        Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY },
                        ChannelAccessAlarmStatus.HIHI,
                        ChannelAccessAlarmSeverity.MAJOR_ALARM, 24, 0),
                createControlsFloat(),
                "{\"time\":631152024000000000,\"severity\":{\"level\":\"MAJOR\",\"hasValue\":true},\"status\":\"HIHI\",\"quality\":\"%quality%\",\"metaData\":{\"type\":\"numeric\",\"precision\":1,\"units\":\"V\",\"displayLow\":-5.0,\"displayHigh\":128.0,\"warnLow\":-10.0,\"warnHigh\":200.0,\"alarmLow\":-15.0,\"alarmHigh\":255.0},\"type\":\"double\",\"value\":[42.0,-23.0,128.5,-99.0,\"NaN\",\"-Infinity\",\"Infinity\"]}");
        testCreateAndDeserializeRawSample(
                ChannelAccessValueFactory.createTimeFloat(new float[0],
                        ChannelAccessAlarmStatus.HIHI,
                        ChannelAccessAlarmSeverity.MAJOR_ALARM, 25, 0),
                createControlsFloat(),
                "{\"time\":631152025000000000,\"severity\":{\"level\":\"MAJOR\",\"hasValue\":true},\"status\":\"HIHI\",\"quality\":\"%quality%\",\"metaData\":{\"type\":\"numeric\",\"precision\":1,\"units\":\"V\",\"displayLow\":-5.0,\"displayHigh\":128.0,\"warnLow\":-10.0,\"warnHigh\":200.0,\"alarmLow\":-15.0,\"alarmHigh\":255.0},\"type\":\"double\",\"value\":[]}");
    }

    /**
     * Tests creating and deserializing long samples. This effectively tests
     * that the conversion from Channel Access values to the respective
     * Cassandra UDT values and back to Channel Access values works correctly.
     * This test also verifies that the conversion to JSON works as intended.
     * The latter test checks for actual equality of the JSON strings, so minor
     * modifications in the JSON serialization logic can yield negative test
     * results even though the code is actually compliant.
     */
    @Test
    public void testCreateAndDeserializeLongSample() {
        testCreateAndDeserializeRawSample(
                ChannelAccessValueFactory.createTimeLong(new int[] { 223323 },
                        ChannelAccessAlarmStatus.NO_ALARM,
                        ChannelAccessAlarmSeverity.NO_ALARM, 26, 420),
                createControlsLong(),
                "{\"time\":631152026000000420,\"severity\":{\"level\":\"OK\",\"hasValue\":true},\"status\":\"NO_ALARM\",\"quality\":\"%quality%\",\"metaData\":{\"type\":\"numeric\",\"precision\":0,\"units\":\"MHz\",\"displayLow\":-85.0,\"displayHigh\":85.0,\"warnLow\":10.0,\"warnHigh\":25.0,\"alarmLow\":5.0,\"alarmHigh\":30.0},\"type\":\"long\",\"value\":[223323]}");
        testCreateAndDeserializeRawSample(
                ChannelAccessValueFactory.createTimeLong(new int[] { 223323,
                        -99 }, ChannelAccessAlarmStatus.NO_ALARM,
                        ChannelAccessAlarmSeverity.NO_ALARM, 27, 420),
                createControlsLong(),
                "{\"time\":631152027000000420,\"severity\":{\"level\":\"OK\",\"hasValue\":true},\"status\":\"NO_ALARM\",\"quality\":\"%quality%\",\"metaData\":{\"type\":\"numeric\",\"precision\":0,\"units\":\"MHz\",\"displayLow\":-85.0,\"displayHigh\":85.0,\"warnLow\":10.0,\"warnHigh\":25.0,\"alarmLow\":5.0,\"alarmHigh\":30.0},\"type\":\"long\",\"value\":[223323,-99]}");
        testCreateAndDeserializeRawSample(
                ChannelAccessValueFactory.createTimeLong(new int[0],
                        ChannelAccessAlarmStatus.NO_ALARM,
                        ChannelAccessAlarmSeverity.NO_ALARM, 28, 420),
                createControlsLong(),
                "{\"time\":631152028000000420,\"severity\":{\"level\":\"OK\",\"hasValue\":true},\"status\":\"NO_ALARM\",\"quality\":\"%quality%\",\"metaData\":{\"type\":\"numeric\",\"precision\":0,\"units\":\"MHz\",\"displayLow\":-85.0,\"displayHigh\":85.0,\"warnLow\":10.0,\"warnHigh\":25.0,\"alarmLow\":5.0,\"alarmHigh\":30.0},\"type\":\"long\",\"value\":[]}");
    }

    /**
     * Tests creating and deserializing short samples. This effectively tests
     * that the conversion from Channel Access values to the respective
     * Cassandra UDT values and back to Channel Access values works correctly.
     * This test also verifies that the conversion to JSON works as intended.
     * The latter test checks for actual equality of the JSON strings, so minor
     * modifications in the JSON serialization logic can yield negative test
     * results even though the code is actually compliant.
     */
    @Test
    public void testCreateAndDeserializeShortSample() {
        testCreateAndDeserializeRawSample(
                ChannelAccessValueFactory.createTimeShort(new short[] { 761 },
                        ChannelAccessAlarmStatus.LOW,
                        ChannelAccessAlarmSeverity.MINOR_ALARM, 29, 99),
                createControlsShort(),
                "{\"time\":631152029000000099,\"severity\":{\"level\":\"MINOR\",\"hasValue\":true},\"status\":\"LOW\",\"quality\":\"%quality%\",\"metaData\":{\"type\":\"numeric\",\"precision\":0,\"units\":\"Ampere\",\"displayLow\":-85.0,\"displayHigh\":85.0,\"warnLow\":10.0,\"warnHigh\":25.0,\"alarmLow\":5.0,\"alarmHigh\":30.0},\"type\":\"long\",\"value\":[761]}");
        testCreateAndDeserializeRawSample(
                ChannelAccessValueFactory.createTimeShort(new short[] { 55,
                        -37, 128, Short.MAX_VALUE },
                        ChannelAccessAlarmStatus.LOW,
                        ChannelAccessAlarmSeverity.MINOR_ALARM, 30, 99),
                createControlsShort(),
                "{\"time\":631152030000000099,\"severity\":{\"level\":\"MINOR\",\"hasValue\":true},\"status\":\"LOW\",\"quality\":\"%quality%\",\"metaData\":{\"type\":\"numeric\",\"precision\":0,\"units\":\"Ampere\",\"displayLow\":-85.0,\"displayHigh\":85.0,\"warnLow\":10.0,\"warnHigh\":25.0,\"alarmLow\":5.0,\"alarmHigh\":30.0},\"type\":\"long\",\"value\":[55,-37,128,32767]}");
        testCreateAndDeserializeRawSample(
                ChannelAccessValueFactory.createTimeShort(new short[0],
                        ChannelAccessAlarmStatus.LOW,
                        ChannelAccessAlarmSeverity.MINOR_ALARM, 31, 99),
                createControlsShort(),
                "{\"time\":631152031000000099,\"severity\":{\"level\":\"MINOR\",\"hasValue\":true},\"status\":\"LOW\",\"quality\":\"%quality%\",\"metaData\":{\"type\":\"numeric\",\"precision\":0,\"units\":\"Ampere\",\"displayLow\":-85.0,\"displayHigh\":85.0,\"warnLow\":10.0,\"warnHigh\":25.0,\"alarmLow\":5.0,\"alarmHigh\":30.0},\"type\":\"long\",\"value\":[]}");
    }

    /**
     * Tests creating and deserializing string samples. This effectively tests
     * that the conversion from Channel Access values to the respective
     * Cassandra UDT values and back to Channel Access values works correctly.
     * This test also verifies that the conversion to JSON works as intended.
     * The latter test checks for actual equality of the JSON strings, so minor
     * modifications in the JSON serialization logic can yield negative test
     * results even though the code is actually compliant.
     */
    @Test
    public void testCreateAndDeserializeStringSamples() {
        testCreateAndDeserializeRawSample(
                ChannelAccessValueFactory.createTimeString(
                        ImmutableList.of("abc"), ChannelAccessAlarmStatus.COMM,
                        ChannelAccessAlarmSeverity.MAJOR_ALARM, 32, 0,
                        Charsets.UTF_8),
                createControlsString(),
                "{\"time\":631152032000000000,\"severity\":{\"level\":\"MAJOR\",\"hasValue\":true},\"status\":\"COMM\",\"quality\":\"%quality%\",\"type\":\"string\",\"value\":[\"abc\"]}");
        testCreateAndDeserializeRawSample(
                ChannelAccessValueFactory.createTimeString(
                        ImmutableList.of("abc", "def", "xyz123"),
                        ChannelAccessAlarmStatus.COMM,
                        ChannelAccessAlarmSeverity.MAJOR_ALARM, 33, 0,
                        Charsets.UTF_8),
                createControlsString(),
                "{\"time\":631152033000000000,\"severity\":{\"level\":\"MAJOR\",\"hasValue\":true},\"status\":\"COMM\",\"quality\":\"%quality%\",\"type\":\"string\",\"value\":[\"abc\",\"def\",\"xyz123\"]}");
        testCreateAndDeserializeRawSample(
                ChannelAccessValueFactory.createTimeString(
                        Collections.<String> emptyList(),
                        ChannelAccessAlarmStatus.COMM,
                        ChannelAccessAlarmSeverity.MAJOR_ALARM, 34, 0,
                        Charsets.UTF_8),
                createControlsString(),
                "{\"time\":631152034000000000,\"severity\":{\"level\":\"MAJOR\",\"hasValue\":true},\"status\":\"COMM\",\"quality\":\"%quality%\",\"type\":\"string\",\"value\":[]}");
    }

    /**
     * Tests the serialization of "disconnected" samples to JSON. This test
     * checks for actual equality of the JSON strings, so minor modifications in
     * the JSON serialization logic can yield negative test results even though
     * the code is actually compliant.
     */
    @Test
    public void testDisabledSampleToJson() {
        assertEquals(
                "{\"time\":82472,\"severity\":{\"level\":\"INVALID\",\"hasValue\":false},\"status\":\"Archive_Disabled\",\"quality\":\"Original\",\"type\":\"string\",\"value\":[\"Archive_Disabled\"]}",
                serializeSampleToJson(new ChannelAccessDisabledSample(82472L,
                        true)));
        assertEquals(
                "{\"time\":1542,\"severity\":{\"level\":\"INVALID\",\"hasValue\":false},\"status\":\"Archive_Disabled\",\"quality\":\"Interpolated\",\"type\":\"string\",\"value\":[\"Archive_Disabled\"]}",
                serializeSampleToJson(new ChannelAccessDisabledSample(1542L,
                        false)));
    }

    /**
     * Tests the serialization of "disconnected" samples to JSON. This test
     * checks for actual equality of the JSON strings, so minor modifications in
     * the JSON serialization logic can yield negative test results even though
     * the code is actually compliant.
     */
    @Test
    public void testDisconnectedSampleToJson() {
        assertEquals(
                "{\"time\":5423,\"severity\":{\"level\":\"INVALID\",\"hasValue\":false},\"status\":\"Disconnected\",\"quality\":\"Original\",\"type\":\"string\",\"value\":[\"Disconnected\"]}",
                serializeSampleToJson(new ChannelAccessDisconnectedSample(
                        5423L, true)));
        assertEquals(
                "{\"time\":8723,\"severity\":{\"level\":\"INVALID\",\"hasValue\":false},\"status\":\"Disconnected\",\"quality\":\"Interpolated\",\"type\":\"string\",\"value\":[\"Disconnected\"]}",
                serializeSampleToJson(new ChannelAccessDisconnectedSample(
                        8723L, false)));
    }

}
