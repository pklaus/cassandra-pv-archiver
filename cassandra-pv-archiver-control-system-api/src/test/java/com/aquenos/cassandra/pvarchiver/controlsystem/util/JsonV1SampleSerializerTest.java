/*
 * Copyright 2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.controlsystem.util;

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.StringWriter;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import com.aquenos.cassandra.pvarchiver.controlsystem.util.JsonV1SampleSerializer.Quality;
import com.aquenos.cassandra.pvarchiver.controlsystem.util.JsonV1SampleSerializer.Severity;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

/**
 * Tests the {@link JsonV1SampleSerializer}.
 * 
 * @author Sebastian Marsching
 */
public class JsonV1SampleSerializerTest {

    private JsonFactory jsonFactory;

    /**
     * Creates the test suite, initializing the internally used
     * {@link JsonFactory}.
     */
    public JsonV1SampleSerializerTest() {
        JsonFactory jsonFactory = new JsonFactory();
        // Enabling the duplicate detection allows us to catch errors early.
        jsonFactory.enable(JsonGenerator.Feature.STRICT_DUPLICATE_DETECTION);
        // If the code works correctly, it should close all objects, so that we
        // do not want this feature.
        jsonFactory.disable(JsonGenerator.Feature.AUTO_CLOSE_JSON_CONTENT);
        this.jsonFactory = jsonFactory;
    }

    /**
     * Tests the <code>serializeDoubleSample(...)</code> methods. This test
     * compares the actual JSON string that are being generated, so it depends
     * on implementation details and might produce false negatives if
     * serialization details in the JSON library or the serializer code change.
     * 
     * @throws IOException
     *             if there is an (unexpected) I/O error.
     */
    @Test
    public void testSerializeDoubleSample() throws IOException {
        Pair<JsonGenerator, StringWriter> jsonGeneratorAndStringWriter;
        String jsonString;
        jsonGeneratorAndStringWriter = prepareJsonGenerator();
        JsonV1SampleSerializer.serializeDoubleSample(
                jsonGeneratorAndStringWriter.getLeft(), 1472910282661419012L,
                42.99, Severity.OK, true, "OK", Quality.INTERPOLATED);
        jsonString = closeJsonGenerator(jsonGeneratorAndStringWriter);
        assertEquals(
                "{\"time\":1472910282661419012,\"severity\":{\"level\":\"OK\",\"hasValue\":true},\"status\":\"OK\",\"quality\":\"Interpolated\",\"type\":\"double\",\"value\":[42.99]}",
                jsonString);
        jsonGeneratorAndStringWriter = prepareJsonGenerator();
        JsonV1SampleSerializer.serializeDoubleSample(
                jsonGeneratorAndStringWriter.getLeft(), 1472910282661419000L,
                42.38, Severity.MAJOR, true, "OK", Quality.ORIGINAL, 2, "mA",
                -20.0, 50.0, -10.0, 100.0, Double.NEGATIVE_INFINITY,
                Double.POSITIVE_INFINITY);
        jsonString = closeJsonGenerator(jsonGeneratorAndStringWriter);
        assertEquals(
                "{\"time\":1472910282661419000,\"severity\":{\"level\":\"MAJOR\",\"hasValue\":true},\"status\":\"OK\",\"quality\":\"Original\",\"metaData\":{\"type\":\"numeric\",\"precision\":2,\"units\":\"mA\",\"displayLow\":-20.0,\"displayHigh\":50.0,\"warnLow\":-10.0,\"warnHigh\":100.0,\"alarmLow\":\"-Infinity\",\"alarmHigh\":\"Infinity\"},\"type\":\"double\",\"value\":[42.38]}",
                jsonString);
        jsonGeneratorAndStringWriter = prepareJsonGenerator();
        JsonV1SampleSerializer.serializeDoubleSample(
                jsonGeneratorAndStringWriter.getLeft(), 1472220282661419012L,
                new double[] { 42.99, 31.5 }, Severity.OK, false, "",
                Quality.ORIGINAL);
        jsonString = closeJsonGenerator(jsonGeneratorAndStringWriter);
        assertEquals(
                "{\"time\":1472220282661419012,\"severity\":{\"level\":\"OK\",\"hasValue\":false},\"status\":\"\",\"quality\":\"Original\",\"type\":\"double\",\"value\":[42.99,31.5]}",
                jsonString);
        jsonGeneratorAndStringWriter = prepareJsonGenerator();
        JsonV1SampleSerializer.serializeDoubleSample(
                jsonGeneratorAndStringWriter.getLeft(), 1472910282751419000L,
                new double[] { -14.32, Double.NaN }, Severity.INVALID, false,
                "Error", Quality.INTERPOLATED, 4, "V", -25.0, 51.0, -11.0,
                200.0, 35.3, 99.8);
        jsonString = closeJsonGenerator(jsonGeneratorAndStringWriter);
        assertEquals(
                "{\"time\":1472910282751419000,\"severity\":{\"level\":\"INVALID\",\"hasValue\":false},\"status\":\"Error\",\"quality\":\"Interpolated\",\"metaData\":{\"type\":\"numeric\",\"precision\":4,\"units\":\"V\",\"displayLow\":-25.0,\"displayHigh\":51.0,\"warnLow\":-11.0,\"warnHigh\":200.0,\"alarmLow\":35.3,\"alarmHigh\":99.8},\"type\":\"double\",\"value\":[-14.32,\"NaN\"]}",
                jsonString);
    }

    /**
     * Tests the <code>serializeEnumSample(...)</code> methods. This test
     * compares the actual JSON string that are being generated, so it depends
     * on implementation details and might produce false negatives if
     * serialization details in the JSON library or the serializer code change.
     * 
     * @throws IOException
     *             if there is an (unexpected) I/O error.
     */
    @Test
    public void testSerializeEnumSample() throws IOException {
        Pair<JsonGenerator, StringWriter> jsonGeneratorAndStringWriter;
        String jsonString;
        jsonGeneratorAndStringWriter = prepareJsonGenerator();
        JsonV1SampleSerializer.serializeEnumSample(
                jsonGeneratorAndStringWriter.getLeft(), 1522910282661419012L,
                3, Severity.OK, true, "OK", Quality.INTERPOLATED);
        jsonString = closeJsonGenerator(jsonGeneratorAndStringWriter);
        assertEquals(
                "{\"time\":1522910282661419012,\"severity\":{\"level\":\"OK\",\"hasValue\":true},\"status\":\"OK\",\"quality\":\"Interpolated\",\"type\":\"enum\",\"value\":[3]}",
                jsonString);
        jsonGeneratorAndStringWriter = prepareJsonGenerator();
        JsonV1SampleSerializer.serializeEnumSample(
                jsonGeneratorAndStringWriter.getLeft(), 1522910282661445612L,
                3, Severity.OK, true, "OK", Quality.ORIGINAL, new String[] {
                        "label 1", "label 2", "label 3" });
        jsonString = closeJsonGenerator(jsonGeneratorAndStringWriter);
        assertEquals(
                "{\"time\":1522910282661445612,\"severity\":{\"level\":\"OK\",\"hasValue\":true},\"status\":\"OK\",\"quality\":\"Original\",\"metaData\":{\"type\":\"enum\",\"states\":[\"label 1\",\"label 2\",\"label 3\"]},\"type\":\"enum\",\"value\":[3]}",
                jsonString);
        jsonGeneratorAndStringWriter = prepareJsonGenerator();
        JsonV1SampleSerializer.serializeEnumSample(
                jsonGeneratorAndStringWriter.getLeft(), 1522910282781419012L,
                new int[] { 2, 4 }, Severity.MINOR, true, "Not OK",
                Quality.ORIGINAL);
        jsonString = closeJsonGenerator(jsonGeneratorAndStringWriter);
        assertEquals(
                "{\"time\":1522910282781419012,\"severity\":{\"level\":\"MINOR\",\"hasValue\":true},\"status\":\"Not OK\",\"quality\":\"Original\",\"type\":\"enum\",\"value\":[2,4]}",
                jsonString);
        jsonGeneratorAndStringWriter = prepareJsonGenerator();
        JsonV1SampleSerializer.serializeEnumSample(
                jsonGeneratorAndStringWriter.getLeft(), 1522910282661445612L,
                new int[] { 0, 5, 3 }, Severity.INVALID, true, "unknown",
                Quality.ORIGINAL, new String[] { "Abc", "xyz", "123" });
        jsonString = closeJsonGenerator(jsonGeneratorAndStringWriter);
        assertEquals(
                "{\"time\":1522910282661445612,\"severity\":{\"level\":\"INVALID\",\"hasValue\":true},\"status\":\"unknown\",\"quality\":\"Original\",\"metaData\":{\"type\":\"enum\",\"states\":[\"Abc\",\"xyz\",\"123\"]},\"type\":\"enum\",\"value\":[0,5,3]}",
                jsonString);
    }

    /**
     * Tests the <code>serializeLongSample(...)</code> methods. This test
     * compares the actual JSON string that are being generated, so it depends
     * on implementation details and might produce false negatives if
     * serialization details in the JSON library or the serializer code change.
     * 
     * @throws IOException
     *             if there is an (unexpected) I/O error.
     */
    @Test
    public void testSerializeLongSample() throws IOException {
        Pair<JsonGenerator, StringWriter> jsonGeneratorAndStringWriter;
        String jsonString;
        jsonGeneratorAndStringWriter = prepareJsonGenerator();
        JsonV1SampleSerializer.serializeLongSample(
                jsonGeneratorAndStringWriter.getLeft(), 1442910282661419012L,
                42L, Severity.OK, true, "OK", Quality.INTERPOLATED);
        jsonString = closeJsonGenerator(jsonGeneratorAndStringWriter);
        assertEquals(
                "{\"time\":1442910282661419012,\"severity\":{\"level\":\"OK\",\"hasValue\":true},\"status\":\"OK\",\"quality\":\"Interpolated\",\"type\":\"long\",\"value\":[42]}",
                jsonString);
        jsonGeneratorAndStringWriter = prepareJsonGenerator();
        JsonV1SampleSerializer.serializeLongSample(
                jsonGeneratorAndStringWriter.getLeft(), 1472910282661417000L,
                199L, Severity.MAJOR, true, "OK", Quality.ORIGINAL, 0, "m",
                -20.0, 50.0, -10.0, 100.0, Double.NEGATIVE_INFINITY,
                Double.POSITIVE_INFINITY);
        jsonString = closeJsonGenerator(jsonGeneratorAndStringWriter);
        assertEquals(
                "{\"time\":1472910282661417000,\"severity\":{\"level\":\"MAJOR\",\"hasValue\":true},\"status\":\"OK\",\"quality\":\"Original\",\"metaData\":{\"type\":\"numeric\",\"precision\":0,\"units\":\"m\",\"displayLow\":-20.0,\"displayHigh\":50.0,\"warnLow\":-10.0,\"warnHigh\":100.0,\"alarmLow\":\"-Infinity\",\"alarmHigh\":\"Infinity\"},\"type\":\"long\",\"value\":[199]}",
                jsonString);
        jsonGeneratorAndStringWriter = prepareJsonGenerator();
        JsonV1SampleSerializer.serializeLongSample(
                jsonGeneratorAndStringWriter.getLeft(), 1442910202661419012L,
                new long[] { -5L, 32L }, Severity.OK, true, "OK",
                Quality.INTERPOLATED);
        jsonString = closeJsonGenerator(jsonGeneratorAndStringWriter);
        assertEquals(
                "{\"time\":1442910202661419012,\"severity\":{\"level\":\"OK\",\"hasValue\":true},\"status\":\"OK\",\"quality\":\"Interpolated\",\"type\":\"long\",\"value\":[-5,32]}",
                jsonString);
        jsonGeneratorAndStringWriter = prepareJsonGenerator();
        JsonV1SampleSerializer.serializeLongSample(
                jsonGeneratorAndStringWriter.getLeft(), 1472910282651417000L,
                new long[] { 19L, 155L, 21L }, Severity.MAJOR, true, "OK",
                Quality.ORIGINAL, 0, "m", -20.0, 50.0, -10.0, 100.0,
                Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY);
        jsonString = closeJsonGenerator(jsonGeneratorAndStringWriter);
        assertEquals(
                "{\"time\":1472910282651417000,\"severity\":{\"level\":\"MAJOR\",\"hasValue\":true},\"status\":\"OK\",\"quality\":\"Original\",\"metaData\":{\"type\":\"numeric\",\"precision\":0,\"units\":\"m\",\"displayLow\":-20.0,\"displayHigh\":50.0,\"warnLow\":-10.0,\"warnHigh\":100.0,\"alarmLow\":\"-Infinity\",\"alarmHigh\":\"Infinity\"},\"type\":\"long\",\"value\":[19,155,21]}",
                jsonString);
    }

    /**
     * Tests the <code>serializeMinMaxDoubleSample(...)</code> methods. This
     * test compares the actual JSON string that are being generated, so it
     * depends on implementation details and might produce false negatives if
     * serialization details in the JSON library or the serializer code change.
     * 
     * @throws IOException
     *             if there is an (unexpected) I/O error.
     */
    @Test
    public void testSerializeMinMaxDoubleSample() throws IOException {
        Pair<JsonGenerator, StringWriter> jsonGeneratorAndStringWriter;
        String jsonString;
        jsonGeneratorAndStringWriter = prepareJsonGenerator();
        JsonV1SampleSerializer.serializeMinMaxDoubleSample(
                jsonGeneratorAndStringWriter.getLeft(), 1472910282661419012L,
                42.99, 13.0, 49.8, Severity.OK, true, "OK",
                Quality.INTERPOLATED);
        jsonString = closeJsonGenerator(jsonGeneratorAndStringWriter);
        assertEquals(
                "{\"time\":1472910282661419012,\"severity\":{\"level\":\"OK\",\"hasValue\":true},\"status\":\"OK\",\"quality\":\"Interpolated\",\"type\":\"minMaxDouble\",\"value\":[42.99],\"minimum\":13.0,\"maximum\":49.8}",
                jsonString);
        jsonGeneratorAndStringWriter = prepareJsonGenerator();
        JsonV1SampleSerializer.serializeMinMaxDoubleSample(
                jsonGeneratorAndStringWriter.getLeft(), 1472910282661419000L,
                42.38, -17.2, 88.32, Severity.MAJOR, true, "OK",
                Quality.ORIGINAL, 2, "mA", -20.0, 50.0, -10.0, 100.0,
                Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY);
        jsonString = closeJsonGenerator(jsonGeneratorAndStringWriter);
        assertEquals(
                "{\"time\":1472910282661419000,\"severity\":{\"level\":\"MAJOR\",\"hasValue\":true},\"status\":\"OK\",\"quality\":\"Original\",\"metaData\":{\"type\":\"numeric\",\"precision\":2,\"units\":\"mA\",\"displayLow\":-20.0,\"displayHigh\":50.0,\"warnLow\":-10.0,\"warnHigh\":100.0,\"alarmLow\":\"-Infinity\",\"alarmHigh\":\"Infinity\"},\"type\":\"minMaxDouble\",\"value\":[42.38],\"minimum\":-17.2,\"maximum\":88.32}",
                jsonString);
        jsonGeneratorAndStringWriter = prepareJsonGenerator();
        JsonV1SampleSerializer.serializeMinMaxDoubleSample(
                jsonGeneratorAndStringWriter.getLeft(), 1472220282661419012L,
                new double[] { 42.99, 31.5 }, 0.0, 100.5, Severity.OK, false,
                "", Quality.ORIGINAL);
        jsonString = closeJsonGenerator(jsonGeneratorAndStringWriter);
        assertEquals(
                "{\"time\":1472220282661419012,\"severity\":{\"level\":\"OK\",\"hasValue\":false},\"status\":\"\",\"quality\":\"Original\",\"type\":\"minMaxDouble\",\"value\":[42.99,31.5],\"minimum\":0.0,\"maximum\":100.5}",
                jsonString);
        jsonGeneratorAndStringWriter = prepareJsonGenerator();
        JsonV1SampleSerializer.serializeMinMaxDoubleSample(
                jsonGeneratorAndStringWriter.getLeft(), 1472910282751419000L,
                new double[] { -14.32, Double.NaN }, -50.0, -2.0,
                Severity.INVALID, false, "Error", Quality.INTERPOLATED, 4, "V",
                -25.0, 51.0, -11.0, 200.0, 35.3, 99.8);
        jsonString = closeJsonGenerator(jsonGeneratorAndStringWriter);
        assertEquals(
                "{\"time\":1472910282751419000,\"severity\":{\"level\":\"INVALID\",\"hasValue\":false},\"status\":\"Error\",\"quality\":\"Interpolated\",\"metaData\":{\"type\":\"numeric\",\"precision\":4,\"units\":\"V\",\"displayLow\":-25.0,\"displayHigh\":51.0,\"warnLow\":-11.0,\"warnHigh\":200.0,\"alarmLow\":35.3,\"alarmHigh\":99.8},\"type\":\"minMaxDouble\",\"value\":[-14.32,\"NaN\"],\"minimum\":-50.0,\"maximum\":-2.0}",
                jsonString);
    }

    /**
     * Tests the <code>serializeStringSample(...)</code> methods. This test
     * compares the actual JSON string that are being generated, so it depends
     * on implementation details and might produce false negatives if
     * serialization details in the JSON library or the serializer code change.
     * 
     * @throws IOException
     *             if there is an (unexpected) I/O error.
     */
    @Test
    public void testSerializeStringSample() throws IOException {
        Pair<JsonGenerator, StringWriter> jsonGeneratorAndStringWriter;
        String jsonString;
        jsonGeneratorAndStringWriter = prepareJsonGenerator();
        JsonV1SampleSerializer.serializeStringSample(
                jsonGeneratorAndStringWriter.getLeft(), 1472910282361419012L,
                "foobar", Severity.OK, true, "OK", Quality.ORIGINAL);
        jsonString = closeJsonGenerator(jsonGeneratorAndStringWriter);
        assertEquals(
                "{\"time\":1472910282361419012,\"severity\":{\"level\":\"OK\",\"hasValue\":true},\"status\":\"OK\",\"quality\":\"Original\",\"type\":\"string\",\"value\":[\"foobar\"]}",
                jsonString);
        jsonGeneratorAndStringWriter = prepareJsonGenerator();
        JsonV1SampleSerializer.serializeStringSample(
                jsonGeneratorAndStringWriter.getLeft(), 1473220282651419012L,
                new String[] { "abc", "def 123" }, Severity.MINOR, false,
                "warning", Quality.ORIGINAL);
        jsonString = closeJsonGenerator(jsonGeneratorAndStringWriter);
        assertEquals(
                "{\"time\":1473220282651419012,\"severity\":{\"level\":\"MINOR\",\"hasValue\":false},\"status\":\"warning\",\"quality\":\"Original\",\"type\":\"string\",\"value\":[\"abc\",\"def 123\"]}",
                jsonString);
    }

    private String closeJsonGenerator(
            Pair<JsonGenerator, StringWriter> jsonGeneratorAndStringWriter)
            throws IOException {
        JsonGenerator jsonGenerator = jsonGeneratorAndStringWriter.getLeft();
        jsonGenerator.writeEndObject();
        jsonGeneratorAndStringWriter.getLeft().close();
        return jsonGeneratorAndStringWriter.getRight().getBuffer().toString();
    }

    private Pair<JsonGenerator, StringWriter> prepareJsonGenerator()
            throws IOException {
        StringWriter stringWriter = new StringWriter();
        JsonGenerator jsonGenerator = jsonFactory.createGenerator(stringWriter);
        jsonGenerator.writeStartObject();
        return Pair.of(jsonGenerator, stringWriter);
    }

}
