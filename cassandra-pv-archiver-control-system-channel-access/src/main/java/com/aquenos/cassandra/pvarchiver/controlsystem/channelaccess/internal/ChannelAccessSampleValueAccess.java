/*
 * Copyright 2016-2017 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.controlsystem.channelaccess.internal;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.ShortBuffer;
import java.util.Arrays;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;

import com.aquenos.cassandra.pvarchiver.controlsystem.channelaccess.ChannelAccessControlSystemSupport;
import com.aquenos.cassandra.pvarchiver.controlsystem.channelaccess.ChannelAccessSample;
import com.aquenos.cassandra.pvarchiver.controlsystem.channelaccess.ChannelAccessSampleType;
import com.aquenos.cassandra.pvarchiver.controlsystem.util.JsonV1SampleSerializer;
import com.aquenos.epics.jackie.common.util.NullTerminatedStringUtil;
import com.aquenos.epics.jackie.common.value.ChannelAccessAlarmOnlyString;
import com.aquenos.epics.jackie.common.value.ChannelAccessAlarmSeverity;
import com.aquenos.epics.jackie.common.value.ChannelAccessAlarmStatus;
import com.aquenos.epics.jackie.common.value.ChannelAccessAlarmValue;
import com.aquenos.epics.jackie.common.value.ChannelAccessControlsChar;
import com.aquenos.epics.jackie.common.value.ChannelAccessControlsDouble;
import com.aquenos.epics.jackie.common.value.ChannelAccessControlsFloat;
import com.aquenos.epics.jackie.common.value.ChannelAccessControlsLong;
import com.aquenos.epics.jackie.common.value.ChannelAccessControlsShort;
import com.aquenos.epics.jackie.common.value.ChannelAccessControlsValue;
import com.aquenos.epics.jackie.common.value.ChannelAccessFloatingPointGraphicsValue;
import com.aquenos.epics.jackie.common.value.ChannelAccessGraphicsEnum;
import com.aquenos.epics.jackie.common.value.ChannelAccessNumericGraphicsValue;
import com.aquenos.epics.jackie.common.value.ChannelAccessTimeChar;
import com.aquenos.epics.jackie.common.value.ChannelAccessTimeDouble;
import com.aquenos.epics.jackie.common.value.ChannelAccessTimeEnum;
import com.aquenos.epics.jackie.common.value.ChannelAccessTimeFloat;
import com.aquenos.epics.jackie.common.value.ChannelAccessTimeLong;
import com.aquenos.epics.jackie.common.value.ChannelAccessTimeShort;
import com.aquenos.epics.jackie.common.value.ChannelAccessTimeString;
import com.aquenos.epics.jackie.common.value.ChannelAccessTimeValue;
import com.aquenos.epics.jackie.common.value.ChannelAccessValueFactory;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.datastax.driver.core.schemabuilder.CreateType;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.datastax.driver.core.schemabuilder.UDTType;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.base.Charsets;

/**
 * <p>
 * Utility for dealing with Channel Access sample values. This class takes care
 * of creating the user-defined types in the database that are used for storing
 * sample values. It also provides methods for dealing with such values.
 * </p>
 * 
 * <p>
 * This class is intended for use by {@link ChannelAccessControlSystemSupport}
 * and its associated classes only.
 * </p>
 * 
 * @author Sebastian Marsching
 */
public class ChannelAccessSampleValueAccess {

    /**
     * Offset between EPICS and UNIX epoch (in seconds).
     */
    public static final long OFFSET_EPICS_EPOCH_TO_UNIX_EPOCH_SECONDS = 631152000L;

    /**
     * Length of an EPICS string (in bytes).
     */
    private static final int EPICS_STRING_SIZE = 40;

    private static final String UDT_AGGREGATED_SCALAR_CHAR = "channel_access_aggregated_scalar_char";
    private static final String UDT_AGGREGATED_SCALAR_DOUBLE = "channel_access_aggregated_scalar_double";
    private static final String UDT_AGGREGATED_SCALAR_FLOAT = "channel_access_aggregated_scalar_float";
    private static final String UDT_AGGREGATED_SCALAR_LONG = "channel_access_aggregated_scalar_long";
    private static final String UDT_AGGREGATED_SCALAR_SHORT = "channel_access_aggregated_scalar_short";
    private static final String UDT_ARRAY_CHAR = "channel_access_array_char";
    private static final String UDT_ARRAY_DOUBLE = "channel_access_array_double";
    private static final String UDT_ARRAY_ENUM = "channel_access_array_enum";
    private static final String UDT_ARRAY_FLOAT = "channel_access_array_float";
    private static final String UDT_ARRAY_LONG = "channel_access_array_long";
    private static final String UDT_ARRAY_SHORT = "channel_access_array_short";
    private static final String UDT_ARRAY_STRING = "channel_access_array_string";
    private static final String UDT_COLUMN_ALARM_SEVERITY = "alarm_severity";
    private static final String UDT_COLUMN_ALARM_STATUS = "alarm_status";
    private static final String UDT_COLUMN_COVERED_PERIOD_FRACTION = "covered_period_fraction";
    private static final String UDT_COLUMN_LABELS = "labels";
    private static final String UDT_COLUMN_LOWER_ALARM_LIMIT = "lower_alarm_limit";
    private static final String UDT_COLUMN_LOWER_CONTROL_LIMIT = "lower_control_limit";
    private static final String UDT_COLUMN_LOWER_DISPLAY_LIMIT = "lower_display_limit";
    private static final String UDT_COLUMN_LOWER_WARNING_LIMIT = "lower_warning_limit";
    private static final String UDT_COLUMN_MAX = "max";
    private static final String UDT_COLUMN_MEAN = "mean";
    private static final String UDT_COLUMN_MIN = "min";
    private static final String UDT_COLUMN_PRECISION = "precision";
    private static final String UDT_COLUMN_STD = "std";
    private static final String UDT_COLUMN_UNITS = "units";
    private static final String UDT_COLUMN_UPPER_ALARM_LIMIT = "upper_alarm_limit";
    private static final String UDT_COLUMN_UPPER_CONTROL_LIMIT = "upper_control_limit";
    private static final String UDT_COLUMN_UPPER_DISPLAY_LIMIT = "upper_display_limit";
    private static final String UDT_COLUMN_UPPER_WARNING_LIMIT = "upper_warning_limit";
    private static final String UDT_COLUMN_VALUE = "value";
    // This field is private, so we can use an array because we do not have to
    // worry about anyone modifying it.
    private static final String[] UDT_AGGREGATED_SCALAR_FLOATING_POINT_META_DATA_COLUMNS = new String[] {
            UDT_COLUMN_LOWER_ALARM_LIMIT, UDT_COLUMN_LOWER_CONTROL_LIMIT,
            UDT_COLUMN_LOWER_DISPLAY_LIMIT, UDT_COLUMN_LOWER_WARNING_LIMIT,
            UDT_COLUMN_PRECISION, UDT_COLUMN_UNITS,
            UDT_COLUMN_UPPER_ALARM_LIMIT, UDT_COLUMN_UPPER_CONTROL_LIMIT,
            UDT_COLUMN_UPPER_DISPLAY_LIMIT, UDT_COLUMN_UPPER_WARNING_LIMIT };
    // This field is private, so we can use an array because we do not have to
    // worry about anyone modifying it.
    private static final String[] UDT_AGGREGATED_SCALAR_INTEGER_META_DATA_COLUMNS = new String[] {
            UDT_COLUMN_LOWER_ALARM_LIMIT, UDT_COLUMN_LOWER_CONTROL_LIMIT,
            UDT_COLUMN_LOWER_DISPLAY_LIMIT, UDT_COLUMN_LOWER_WARNING_LIMIT,
            UDT_COLUMN_UNITS, UDT_COLUMN_UPPER_ALARM_LIMIT,
            UDT_COLUMN_UPPER_CONTROL_LIMIT, UDT_COLUMN_UPPER_DISPLAY_LIMIT,
            UDT_COLUMN_UPPER_WARNING_LIMIT };
    private static final String UDT_SCALAR_CHAR = "channel_access_scalar_char";
    private static final String UDT_SCALAR_DOUBLE = "channel_access_scalar_double";
    private static final String UDT_SCALAR_ENUM = "channel_access_scalar_enum";
    private static final String UDT_SCALAR_FLOAT = "channel_access_scalar_float";
    private static final String UDT_SCALAR_LONG = "channel_access_scalar_long";
    private static final String UDT_SCALAR_SHORT = "channel_access_scalar_short";
    private static final String UDT_SCALAR_STRING = "channel_access_scalar_string";

    private UserType aggregatedScalarChar;
    private UserType aggregatedScalarDouble;
    private UserType aggregatedScalarFloat;
    private UserType aggregatedScalarLong;
    private UserType aggregatedScalarShort;
    private UserType arrayChar;
    private UserType arrayDouble;
    private UserType arrayEnum;
    private UserType arrayFloat;
    private UserType arrayLong;
    private UserType arrayShort;
    private UserType arrayString;
    private UserType scalarChar;
    private UserType scalarDouble;
    private UserType scalarEnum;
    private UserType scalarFloat;
    private UserType scalarLong;
    private UserType scalarShort;
    private UserType scalarString;
    private Session session;

    private static CreateType addAggregatedScalarValueColumns(
            CreateType builder, DataType dataType) {
        return builder
                .addColumn(UDT_COLUMN_MEAN, DataType.cdouble())
                .addColumn(UDT_COLUMN_STD, DataType.cdouble())
                .addColumn(UDT_COLUMN_MIN, dataType)
                .addColumn(UDT_COLUMN_MAX, dataType)
                .addColumn(UDT_COLUMN_COVERED_PERIOD_FRACTION,
                        DataType.cdouble());
    }

    private static CreateType addAlarmColumns(CreateType builder) {
        return builder
                .addColumn(UDT_COLUMN_ALARM_SEVERITY, DataType.smallint())
                .addColumn(UDT_COLUMN_ALARM_STATUS, DataType.smallint());
    }

    private static CreateType addArrayValueColumn(CreateType builder) {
        return builder.addColumn(UDT_COLUMN_VALUE, DataType.blob());
    }

    private static CreateType addLabelsColumn(CreateType builder) {
        return builder.addColumn(UDT_COLUMN_LABELS,
                DataType.frozenList(DataType.text()));
    }

    private static CreateType addLimitColumns(CreateType builder,
            DataType dataType) {
        return builder.addColumn(UDT_COLUMN_LOWER_WARNING_LIMIT, dataType)
                .addColumn(UDT_COLUMN_UPPER_WARNING_LIMIT, dataType)
                .addColumn(UDT_COLUMN_LOWER_ALARM_LIMIT, dataType)
                .addColumn(UDT_COLUMN_UPPER_ALARM_LIMIT, dataType)
                .addColumn(UDT_COLUMN_LOWER_DISPLAY_LIMIT, dataType)
                .addColumn(UDT_COLUMN_UPPER_DISPLAY_LIMIT, dataType)
                .addColumn(UDT_COLUMN_LOWER_CONTROL_LIMIT, dataType)
                .addColumn(UDT_COLUMN_UPPER_CONTROL_LIMIT, dataType);
    }

    private static CreateType addUnitsColumn(CreateType builder) {
        return builder.addColumn(UDT_COLUMN_UNITS, DataType.text());
    }

    private static CreateType addPrecisionAndUnitsColumns(CreateType builder) {
        return addUnitsColumn(builder.addColumn(UDT_COLUMN_PRECISION,
                DataType.smallint()));
    }

    private static CreateType addScalarValueColumn(CreateType builder,
            DataType dataType) {
        return builder.addColumn(UDT_COLUMN_VALUE, dataType);
    }

    private static void serializeAggregatedSampleToJsonV1(
            JsonGenerator jsonGenerator, ChannelAccessAggregatedSample sample)
            throws IOException {
        long timeStamp = sample.getTimeStamp();
        UDTValue value = sample.getValue();
        short numericSeverity = value.getShort(UDT_COLUMN_ALARM_SEVERITY);
        JsonV1SampleSerializer.Severity severity;
        switch (numericSeverity) {
        case 0:
            severity = JsonV1SampleSerializer.Severity.OK;
            break;
        case 1:
            severity = JsonV1SampleSerializer.Severity.MINOR;
            break;
        case 2:
            severity = JsonV1SampleSerializer.Severity.MAJOR;
            break;
        default:
            severity = JsonV1SampleSerializer.Severity.INVALID;
            break;
        }
        short numericStatus = value.getShort(UDT_COLUMN_ALARM_STATUS);
        String status = ChannelAccessAlarmStatus.forAlarmStatusCode(
                numericStatus).toString();
        JsonV1SampleSerializer.Quality quality = JsonV1SampleSerializer.Quality.INTERPOLATED;
        int precision;
        String units;
        double mean, min, max, displayLow, displayHigh, warnLow, warnHigh, alarmLow, alarmHigh;
        switch (sample.getType()) {
        case AGGREGATED_SCALAR_CHAR:
            mean = value.getDouble(UDT_COLUMN_MEAN);
            min = value.getByte(UDT_COLUMN_MIN);
            max = value.getByte(UDT_COLUMN_MAX);
            precision = 0;
            units = value.getString(UDT_COLUMN_UNITS);
            displayLow = value.getByte(UDT_COLUMN_LOWER_DISPLAY_LIMIT);
            displayHigh = value.getByte(UDT_COLUMN_UPPER_DISPLAY_LIMIT);
            warnLow = value.getByte(UDT_COLUMN_LOWER_WARNING_LIMIT);
            warnHigh = value.getByte(UDT_COLUMN_UPPER_WARNING_LIMIT);
            alarmLow = value.getByte(UDT_COLUMN_LOWER_ALARM_LIMIT);
            alarmHigh = value.getByte(UDT_COLUMN_UPPER_ALARM_LIMIT);
            break;
        case AGGREGATED_SCALAR_DOUBLE:
            mean = value.getDouble(UDT_COLUMN_MEAN);
            min = value.getDouble(UDT_COLUMN_MIN);
            max = value.getDouble(UDT_COLUMN_MAX);
            precision = value.getShort(UDT_COLUMN_PRECISION);
            units = value.getString(UDT_COLUMN_UNITS);
            displayLow = value.getDouble(UDT_COLUMN_LOWER_DISPLAY_LIMIT);
            displayHigh = value.getDouble(UDT_COLUMN_UPPER_DISPLAY_LIMIT);
            warnLow = value.getDouble(UDT_COLUMN_LOWER_WARNING_LIMIT);
            warnHigh = value.getDouble(UDT_COLUMN_UPPER_WARNING_LIMIT);
            alarmLow = value.getDouble(UDT_COLUMN_LOWER_ALARM_LIMIT);
            alarmHigh = value.getDouble(UDT_COLUMN_UPPER_ALARM_LIMIT);
            break;
        case AGGREGATED_SCALAR_FLOAT:
            mean = value.getDouble(UDT_COLUMN_MEAN);
            min = value.getFloat(UDT_COLUMN_MIN);
            max = value.getFloat(UDT_COLUMN_MAX);
            precision = value.getShort(UDT_COLUMN_PRECISION);
            units = value.getString(UDT_COLUMN_UNITS);
            displayLow = value.getFloat(UDT_COLUMN_LOWER_DISPLAY_LIMIT);
            displayHigh = value.getFloat(UDT_COLUMN_UPPER_DISPLAY_LIMIT);
            warnLow = value.getFloat(UDT_COLUMN_LOWER_WARNING_LIMIT);
            warnHigh = value.getFloat(UDT_COLUMN_UPPER_WARNING_LIMIT);
            alarmLow = value.getFloat(UDT_COLUMN_LOWER_ALARM_LIMIT);
            alarmHigh = value.getFloat(UDT_COLUMN_UPPER_ALARM_LIMIT);
            break;
        case AGGREGATED_SCALAR_LONG:
            mean = value.getDouble(UDT_COLUMN_MEAN);
            min = value.getInt(UDT_COLUMN_MIN);
            max = value.getInt(UDT_COLUMN_MAX);
            precision = 0;
            units = value.getString(UDT_COLUMN_UNITS);
            displayLow = value.getInt(UDT_COLUMN_LOWER_DISPLAY_LIMIT);
            displayHigh = value.getInt(UDT_COLUMN_UPPER_DISPLAY_LIMIT);
            warnLow = value.getInt(UDT_COLUMN_LOWER_WARNING_LIMIT);
            warnHigh = value.getInt(UDT_COLUMN_UPPER_WARNING_LIMIT);
            alarmLow = value.getInt(UDT_COLUMN_LOWER_ALARM_LIMIT);
            alarmHigh = value.getInt(UDT_COLUMN_UPPER_ALARM_LIMIT);
            break;
        case AGGREGATED_SCALAR_SHORT:
            mean = value.getDouble(UDT_COLUMN_MEAN);
            min = value.getShort(UDT_COLUMN_MIN);
            max = value.getShort(UDT_COLUMN_MAX);
            precision = 0;
            units = value.getString(UDT_COLUMN_UNITS);
            displayLow = value.getShort(UDT_COLUMN_LOWER_DISPLAY_LIMIT);
            displayHigh = value.getShort(UDT_COLUMN_UPPER_DISPLAY_LIMIT);
            warnLow = value.getShort(UDT_COLUMN_LOWER_WARNING_LIMIT);
            warnHigh = value.getShort(UDT_COLUMN_UPPER_WARNING_LIMIT);
            alarmLow = value.getShort(UDT_COLUMN_LOWER_ALARM_LIMIT);
            alarmHigh = value.getShort(UDT_COLUMN_UPPER_ALARM_LIMIT);
            break;
        default:
            throw new RuntimeException("Unhandled sample type: "
                    + sample.getType());
        }
        JsonV1SampleSerializer.serializeMinMaxDoubleSample(jsonGenerator,
                timeStamp, mean, min, max, severity, true, status, quality,
                precision, units, displayLow, displayHigh, warnLow, warnHigh,
                alarmLow, alarmHigh);
    }

    private static void serializeRawSampleToJsonV1(JsonGenerator jsonGenerator,
            ChannelAccessRawSample sample) throws IOException {
        long timeStamp = sample.getTimeStamp();
        UDTValue value = sample.getValue();
        short numericSeverity = value.getShort(UDT_COLUMN_ALARM_SEVERITY);
        JsonV1SampleSerializer.Severity severity;
        switch (numericSeverity) {
        case 0:
            severity = JsonV1SampleSerializer.Severity.OK;
            break;
        case 1:
            severity = JsonV1SampleSerializer.Severity.MINOR;
            break;
        case 2:
            severity = JsonV1SampleSerializer.Severity.MAJOR;
            break;
        default:
            severity = JsonV1SampleSerializer.Severity.INVALID;
            break;
        }
        short numericStatus = value.getShort(UDT_COLUMN_ALARM_STATUS);
        String status = ChannelAccessAlarmStatus.forAlarmStatusCode(
                numericStatus).toString();
        JsonV1SampleSerializer.Quality quality = sample.isOriginalSample() ? JsonV1SampleSerializer.Quality.ORIGINAL
                : JsonV1SampleSerializer.Quality.INTERPOLATED;
        int precision;
        String units;
        double displayLow, displayHigh, warnLow, warnHigh, alarmLow, alarmHigh;
        switch (sample.getType()) {
        case ARRAY_CHAR:
        case SCALAR_CHAR:
            precision = 0;
            units = value.getString(UDT_COLUMN_UNITS);
            displayLow = value.getByte(UDT_COLUMN_LOWER_DISPLAY_LIMIT);
            displayHigh = value.getByte(UDT_COLUMN_UPPER_DISPLAY_LIMIT);
            warnLow = value.getByte(UDT_COLUMN_LOWER_WARNING_LIMIT);
            warnHigh = value.getByte(UDT_COLUMN_UPPER_WARNING_LIMIT);
            alarmLow = value.getByte(UDT_COLUMN_LOWER_ALARM_LIMIT);
            alarmHigh = value.getByte(UDT_COLUMN_UPPER_ALARM_LIMIT);
            break;
        case ARRAY_DOUBLE:
        case SCALAR_DOUBLE:
            precision = value.getShort(UDT_COLUMN_PRECISION);
            units = value.getString(UDT_COLUMN_UNITS);
            displayLow = value.getDouble(UDT_COLUMN_LOWER_DISPLAY_LIMIT);
            displayHigh = value.getDouble(UDT_COLUMN_UPPER_DISPLAY_LIMIT);
            warnLow = value.getDouble(UDT_COLUMN_LOWER_WARNING_LIMIT);
            warnHigh = value.getDouble(UDT_COLUMN_UPPER_WARNING_LIMIT);
            alarmLow = value.getDouble(UDT_COLUMN_LOWER_ALARM_LIMIT);
            alarmHigh = value.getDouble(UDT_COLUMN_UPPER_ALARM_LIMIT);
            break;
        case ARRAY_FLOAT:
        case SCALAR_FLOAT:
            precision = value.getShort(UDT_COLUMN_PRECISION);
            units = value.getString(UDT_COLUMN_UNITS);
            displayLow = value.getFloat(UDT_COLUMN_LOWER_DISPLAY_LIMIT);
            displayHigh = value.getFloat(UDT_COLUMN_UPPER_DISPLAY_LIMIT);
            warnLow = value.getFloat(UDT_COLUMN_LOWER_WARNING_LIMIT);
            warnHigh = value.getFloat(UDT_COLUMN_UPPER_WARNING_LIMIT);
            alarmLow = value.getFloat(UDT_COLUMN_LOWER_ALARM_LIMIT);
            alarmHigh = value.getFloat(UDT_COLUMN_UPPER_ALARM_LIMIT);
            break;
        case ARRAY_LONG:
        case SCALAR_LONG:
            precision = 0;
            units = value.getString(UDT_COLUMN_UNITS);
            displayLow = value.getInt(UDT_COLUMN_LOWER_DISPLAY_LIMIT);
            displayHigh = value.getInt(UDT_COLUMN_UPPER_DISPLAY_LIMIT);
            warnLow = value.getInt(UDT_COLUMN_LOWER_WARNING_LIMIT);
            warnHigh = value.getInt(UDT_COLUMN_UPPER_WARNING_LIMIT);
            alarmLow = value.getInt(UDT_COLUMN_LOWER_ALARM_LIMIT);
            alarmHigh = value.getInt(UDT_COLUMN_UPPER_ALARM_LIMIT);
            break;
        case ARRAY_SHORT:
        case SCALAR_SHORT:
            precision = 0;
            units = value.getString(UDT_COLUMN_UNITS);
            displayLow = value.getShort(UDT_COLUMN_LOWER_DISPLAY_LIMIT);
            displayHigh = value.getShort(UDT_COLUMN_UPPER_DISPLAY_LIMIT);
            warnLow = value.getShort(UDT_COLUMN_LOWER_WARNING_LIMIT);
            warnHigh = value.getShort(UDT_COLUMN_UPPER_WARNING_LIMIT);
            alarmLow = value.getShort(UDT_COLUMN_LOWER_ALARM_LIMIT);
            alarmHigh = value.getShort(UDT_COLUMN_UPPER_ALARM_LIMIT);
            break;
        default:
            precision = 0;
            units = null;
            displayLow = 0.0;
            displayHigh = 0.0;
            warnLow = 0.0;
            warnHigh = 0.0;
            alarmLow = 0.0;
            alarmHigh = 0.0;
            break;
        }
        switch (sample.getType()) {
        case ARRAY_CHAR:
            JsonV1SampleSerializer.serializeLongSample(jsonGenerator,
                    timeStamp, toLongArray(value.getBytes(UDT_COLUMN_VALUE)),
                    severity, true, status, quality, precision, units,
                    displayLow, displayHigh, warnLow, warnHigh, alarmLow,
                    alarmHigh);
            break;
        case ARRAY_DOUBLE:
            JsonV1SampleSerializer.serializeDoubleSample(jsonGenerator,
                    timeStamp, toDoubleArray(value.getBytes(UDT_COLUMN_VALUE)
                            .asDoubleBuffer()), severity, true, status,
                    quality, precision, units, displayLow, displayHigh,
                    warnLow, warnHigh, alarmLow, alarmHigh);
            break;
        case ARRAY_ENUM:
            JsonV1SampleSerializer
                    .serializeEnumSample(jsonGenerator, timeStamp,
                            toIntArray(value.getBytes(UDT_COLUMN_VALUE)
                                    .asShortBuffer()), severity, true, status,
                            quality,
                            value.getList(UDT_COLUMN_LABELS, String.class)
                                    .toArray(ArrayUtils.EMPTY_STRING_ARRAY));
            break;
        case ARRAY_FLOAT:
            JsonV1SampleSerializer.serializeDoubleSample(jsonGenerator,
                    timeStamp, toDoubleArray(value.getBytes(UDT_COLUMN_VALUE)
                            .asFloatBuffer()), severity, true, status, quality,
                    precision, units, displayLow, displayHigh, warnLow,
                    warnHigh, alarmLow, alarmHigh);
            break;
        case ARRAY_LONG:
            JsonV1SampleSerializer.serializeLongSample(jsonGenerator,
                    timeStamp, toLongArray(value.getBytes(UDT_COLUMN_VALUE)
                            .asIntBuffer()), severity, true, status, quality,
                    precision, units, displayLow, displayHigh, warnLow,
                    warnHigh, alarmLow, alarmHigh);
            break;
        case ARRAY_SHORT:
            JsonV1SampleSerializer.serializeLongSample(jsonGenerator,
                    timeStamp, toLongArray(value.getBytes(UDT_COLUMN_VALUE)
                            .asShortBuffer()), severity, true, status, quality,
                    precision, units, displayLow, displayHigh, warnLow,
                    warnHigh, alarmLow, alarmHigh);
            break;
        case ARRAY_STRING:
            JsonV1SampleSerializer.serializeStringSample(jsonGenerator,
                    timeStamp, toStringArray(value.getBytes(UDT_COLUMN_VALUE)),
                    severity, true, status, quality);
            break;
        case SCALAR_CHAR:
            JsonV1SampleSerializer.serializeLongSample(jsonGenerator,
                    timeStamp, value.getByte(UDT_COLUMN_VALUE), severity, true,
                    status, quality, precision, units, displayLow, displayHigh,
                    warnLow, warnHigh, alarmLow, alarmHigh);
            break;
        case SCALAR_DOUBLE:
            JsonV1SampleSerializer.serializeDoubleSample(jsonGenerator,
                    timeStamp, value.getDouble(UDT_COLUMN_VALUE), severity,
                    true, status, quality, precision, units, displayLow,
                    displayHigh, warnLow, warnHigh, alarmLow, alarmHigh);
            break;
        case SCALAR_ENUM:
            JsonV1SampleSerializer.serializeEnumSample(
                    jsonGenerator,
                    timeStamp,
                    value.getShort(UDT_COLUMN_VALUE),
                    severity,
                    true,
                    status,
                    quality,
                    value.getList(UDT_COLUMN_LABELS, String.class).toArray(
                            ArrayUtils.EMPTY_STRING_ARRAY));
            break;
        case SCALAR_FLOAT:
            JsonV1SampleSerializer.serializeDoubleSample(jsonGenerator,
                    timeStamp, value.getFloat(UDT_COLUMN_VALUE), severity,
                    true, status, quality, precision, units, displayLow,
                    displayHigh, warnLow, warnHigh, alarmLow, alarmHigh);
            break;
        case SCALAR_LONG:
            JsonV1SampleSerializer.serializeLongSample(jsonGenerator,
                    timeStamp, value.getInt(UDT_COLUMN_VALUE), severity, true,
                    status, quality, precision, units, displayLow, displayHigh,
                    warnLow, warnHigh, alarmLow, alarmHigh);
            break;
        case SCALAR_SHORT:
            JsonV1SampleSerializer.serializeLongSample(jsonGenerator,
                    timeStamp, value.getShort(UDT_COLUMN_VALUE), severity,
                    true, status, quality, precision, units, displayLow,
                    displayHigh, warnLow, warnHigh, alarmLow, alarmHigh);
            break;
        case SCALAR_STRING:
            JsonV1SampleSerializer.serializeStringSample(jsonGenerator,
                    timeStamp, value.getString(UDT_COLUMN_VALUE), severity,
                    true, status, quality);
            break;
        default:
            throw new RuntimeException("Unhandled sample type: "
                    + sample.getType());
        }
    }

    private static byte[] toByteArray(ByteBuffer byteBuffer) {
        if (byteBuffer.hasArray()) {
            return byteBuffer.array();
        } else {
            byte[] array = new byte[byteBuffer.remaining()];
            for (int i = 0; i < array.length; ++i) {
                array[i] = byteBuffer.get();
            }
            return array;
        }
    }

    private static double[] toDoubleArray(DoubleBuffer doubleBuffer) {
        // The passed double buffer is created from a byte buffer, so there is
        // no sense in trying doubleBuffer.array().
        double[] array = new double[doubleBuffer.remaining()];
        for (int i = 0; i < array.length; ++i) {
            array[i] = doubleBuffer.get();
        }
        return array;
    }

    private static double[] toDoubleArray(FloatBuffer floatBuffer) {
        double[] array = new double[floatBuffer.remaining()];
        for (int i = 0; i < array.length; ++i) {
            array[i] = floatBuffer.get();
        }
        return array;
    }

    private static float[] toFloatArray(FloatBuffer floatBuffer) {
        // The passed float buffer is created from a byte buffer, so there is
        // no sense in trying floatBuffer.array().
        float[] array = new float[floatBuffer.remaining()];
        for (int i = 0; i < array.length; ++i) {
            array[i] = floatBuffer.get();
        }
        return array;
    }

    private static int[] toIntArray(IntBuffer intBuffer) {
        // The passed int buffer is created from a byte buffer, so there is no
        // sense in trying intBuffer.array().
        int[] array = new int[intBuffer.remaining()];
        for (int i = 0; i < array.length; ++i) {
            array[i] = intBuffer.get();
        }
        return array;
    }

    private static int[] toIntArray(ShortBuffer shortBuffer) {
        int[] array = new int[shortBuffer.remaining()];
        for (int i = 0; i < array.length; ++i) {
            array[i] = shortBuffer.get();
        }
        return array;
    }

    private static long[] toLongArray(ByteBuffer byteBuffer) {
        long[] array = new long[byteBuffer.remaining()];
        for (int i = 0; i < array.length; ++i) {
            array[i] = byteBuffer.get();
        }
        return array;
    }

    private static long[] toLongArray(IntBuffer intBuffer) {
        long[] array = new long[intBuffer.remaining()];
        for (int i = 0; i < array.length; ++i) {
            array[i] = intBuffer.get();
        }
        return array;
    }

    private static long[] toLongArray(ShortBuffer shortBuffer) {
        long[] array = new long[shortBuffer.remaining()];
        for (int i = 0; i < array.length; ++i) {
            array[i] = shortBuffer.get();
        }
        return array;
    }

    private static short[] toShortArray(ShortBuffer shortBuffer) {
        // The passed short buffer is created from a byte buffer, so there is
        // no sense in trying shortBuffer.array().
        short[] array = new short[shortBuffer.remaining()];
        for (int i = 0; i < array.length; ++i) {
            array[i] = shortBuffer.get();
        }
        return array;
    }

    private static String[] toStringArray(ByteBuffer byteBuffer) {
        if (byteBuffer.remaining() % EPICS_STRING_SIZE != 0) {
            throw new RuntimeException(
                    "Invalid string sample: The number of bytes making up a string has to be a multiple of "
                            + EPICS_STRING_SIZE
                            + ", but the found string has "
                            + byteBuffer.remaining() + " bytes.");
        }
        String[] array = new String[byteBuffer.remaining() / EPICS_STRING_SIZE];
        byte[] buffer = new byte[EPICS_STRING_SIZE];
        for (int i = 0; i < array.length; ++i) {
            byteBuffer.get(buffer);
            array[i] = NullTerminatedStringUtil.nullTerminatedBytesToString(
                    buffer, Charsets.UTF_8);
        }
        return array;
    }

    /**
     * Creates the sample-value access for the specified session. This
     * implicitly creates the user-defined types needed for sample values if
     * they do not exist yet.
     * 
     * @param session
     *            session for accessing the database.
     * @throws NullPointerException
     *             if <code>session</code> is <code>null</code>.
     * @throws RuntimeException
     *             if there is a problem with the database connection (e.g. a
     *             user-defined type cannot be created).
     */
    public ChannelAccessSampleValueAccess(Session session) {
        this.session = session;
        createUserDefinedTypes();
    }

    /**
     * Creates an aggregated scalar char sample. The sample is created by taking
     * the meta-data (limits, units, etc.) from the specified
     * <code>metaDataSample</code> and adding the specified values to create a
     * new aggregated sample.
     * 
     * @param sampleTimeStamp
     *            time stamp of the newly created sample. The time is specified
     *            as the number of nanoseconds since epoch (January 1st, 1970,
     *            00:00:00 UTC).
     * @param mean
     *            mean of the aggregated samples. In mathematical terms this is
     *            ∑ p<sub>i</sub> x<sub>i</sub> where x<sub>i</sub> are the
     *            individual samples and p<sub>i</sub> = t<sub>sample</sub> /
     *            t<sub>total</sub>.
     * @param std
     *            standard deviation of the aggregated samples. In mathematical
     *            terms this is √(&lt;x<sup>2</sup>&gt; - &lt;x&gt;<sup>2</sup>)
     *            where &lt;x&gt; is the <code>mean</code> and
     *            &lt;x<sup>2</sup>&gt; = ∑ p<sub>i</sub>
     *            x<sub>i</sub><sup>2</sup> with x<sub>i</sub> being the
     *            individual samples and p<sub>i</sub> = t<sub>sample</sub> /
     *            t<sub>total</sub>.
     * @param minimum
     *            least value of any sample that participated in building the
     *            aggregated sample.
     * @param maximum
     *            greatest value of any sample that participated in building the
     *            aggregated sample.
     * @param alarmSeverity
     *            highest alarm severity encountered among all samples.
     * @param alarmStatus
     *            alarm status associated with the highest alarm severity as
     *            specified by <code>alarmSeverity</code>.
     * @param coveredPeriodFraction
     *            fraction of the total interval that is covered by the
     *            aggregated sample. For example, if an aggregated sample is
     *            generated for a period of 30 seconds, but the samples used to
     *            build this aggregated sample only cover 27 seconds of this
     *            period, the fraction is 0.9.
     * @param metaDataSample
     *            sample which is used as the source for the created sample's
     *            meta-data. The meta-data includes information not directly
     *            associated with the sample's value such as limits and display
     *            information. This sample must be of type
     *            <code>SCALAR_CHAR</code>, <code>ARRAY_CHAR</code> or
     *            <code>AGGREGATED_SCALAR_CHAR</code>.
     * @return new aggregated sample created from the specified parameters.
     */
    public ChannelAccessAggregatedSample createAggregatedScalarCharSample(
            long sampleTimeStamp, double mean, double std, byte minimum,
            byte maximum, ChannelAccessAlarmSeverity alarmSeverity,
            ChannelAccessAlarmStatus alarmStatus, double coveredPeriodFraction,
            ChannelAccessUdtSample metaDataSample) {
        // This method is only used internally, so we use assertions instead of
        // preconditions.
        assert (metaDataSample != null);
        assert (metaDataSample.getType().equals(
                ChannelAccessSampleType.SCALAR_CHAR)
                || metaDataSample.getType().equals(
                        ChannelAccessSampleType.ARRAY_CHAR) || metaDataSample
                .getType().equals(
                        ChannelAccessSampleType.AGGREGATED_SCALAR_CHAR));
        UDTValue oldUdtValue = metaDataSample.getValue();
        UDTValue newUdtValue = aggregatedScalarChar.newValue();
        newUdtValue.setDouble(UDT_COLUMN_MEAN, mean);
        newUdtValue.setDouble(UDT_COLUMN_STD, std);
        newUdtValue.setByte(UDT_COLUMN_MIN, minimum);
        newUdtValue.setByte(UDT_COLUMN_MAX, maximum);
        newUdtValue.setShort(UDT_COLUMN_ALARM_SEVERITY,
                alarmSeverity.toSeverityCode());
        newUdtValue.setShort(UDT_COLUMN_ALARM_STATUS,
                alarmStatus.toAlarmStatusCode());
        newUdtValue.setDouble(UDT_COLUMN_COVERED_PERIOD_FRACTION,
                coveredPeriodFraction);
        for (String columnName : UDT_AGGREGATED_SCALAR_INTEGER_META_DATA_COLUMNS) {
            newUdtValue.setBytesUnsafe(columnName,
                    oldUdtValue.getBytesUnsafe(columnName));
        }
        return new ChannelAccessAggregatedSample(sampleTimeStamp,
                ChannelAccessSampleType.AGGREGATED_SCALAR_CHAR, newUdtValue);
    }

    /**
     * Creates an aggregated scalar double sample. The sample is created by
     * taking the meta-data (limits, units, etc.) from the specified
     * <code>metaDataSample</code> and adding the specified values to create a
     * new aggregated sample.
     * 
     * @param sampleTimeStamp
     *            time stamp of the newly created sample. The time is specified
     *            as the number of nanoseconds since epoch (January 1st, 1970,
     *            00:00:00 UTC).
     * @param mean
     *            mean of the aggregated samples. In mathematical terms this is
     *            ∑ p<sub>i</sub> x<sub>i</sub> where x<sub>i</sub> are the
     *            individual samples and p<sub>i</sub> = t<sub>sample</sub> /
     *            t<sub>total</sub>.
     * @param std
     *            standard deviation of the aggregated samples. In mathematical
     *            terms this is √(&lt;x<sup>2</sup>&gt; - &lt;x&gt;<sup>2</sup>)
     *            where &lt;x&gt; is the <code>mean</code> and
     *            &lt;x<sup>2</sup>&gt; = ∑ p<sub>i</sub>
     *            x<sub>i</sub><sup>2</sup> with x<sub>i</sub> being the
     *            individual samples and p<sub>i</sub> = t<sub>sample</sub> /
     *            t<sub>total</sub>.
     * @param minimum
     *            least value of any sample that participated in building the
     *            aggregated sample.
     * @param maximum
     *            greatest value of any sample that participated in building the
     *            aggregated sample.
     * @param alarmSeverity
     *            highest alarm severity encountered among all samples.
     * @param alarmStatus
     *            alarm status associated with the highest alarm severity as
     *            specified by <code>alarmSeverity</code>.
     * @param coveredPeriodFraction
     *            fraction of the total interval that is covered by the
     *            aggregated sample. For example, if an aggregated sample is
     *            generated for a period of 30 seconds, but the samples used to
     *            build this aggregated sample only cover 27 seconds of this
     *            period, the fraction is 0.9.
     * @param metaDataSample
     *            sample which is used as the source for the created sample's
     *            meta-data. The meta-data includes information not directly
     *            associated with the sample's value such as limits and display
     *            information. This sample must be of type
     *            <code>SCALAR_DOUBLE</code>, <code>ARRAY_DOUBLE</code> or
     *            <code>AGGREGATED_SCALAR_DOUBLE</code>.
     * @return new aggregated sample created from the specified parameters.
     */
    public ChannelAccessAggregatedSample createAggregatedScalarDoubleSample(
            long sampleTimeStamp, double mean, double std, double minimum,
            double maximum, ChannelAccessAlarmSeverity alarmSeverity,
            ChannelAccessAlarmStatus alarmStatus, double coveredPeriodFraction,
            ChannelAccessUdtSample metaDataSample) {
        // This method is only used internally, so we use assertions instead of
        // preconditions.
        assert (metaDataSample != null);
        assert (metaDataSample.getType().equals(
                ChannelAccessSampleType.SCALAR_DOUBLE)
                || metaDataSample.getType().equals(
                        ChannelAccessSampleType.ARRAY_DOUBLE) || metaDataSample
                .getType().equals(
                        ChannelAccessSampleType.AGGREGATED_SCALAR_DOUBLE));
        UDTValue oldUdtValue = metaDataSample.getValue();
        UDTValue newUdtValue = aggregatedScalarDouble.newValue();
        newUdtValue.setDouble(UDT_COLUMN_MEAN, mean);
        newUdtValue.setDouble(UDT_COLUMN_STD, std);
        newUdtValue.setDouble(UDT_COLUMN_MIN, minimum);
        newUdtValue.setDouble(UDT_COLUMN_MAX, maximum);
        newUdtValue.setShort(UDT_COLUMN_ALARM_SEVERITY,
                alarmSeverity.toSeverityCode());
        newUdtValue.setShort(UDT_COLUMN_ALARM_STATUS,
                alarmStatus.toAlarmStatusCode());
        newUdtValue.setDouble(UDT_COLUMN_COVERED_PERIOD_FRACTION,
                coveredPeriodFraction);
        for (String columnName : UDT_AGGREGATED_SCALAR_FLOATING_POINT_META_DATA_COLUMNS) {
            newUdtValue.setBytesUnsafe(columnName,
                    oldUdtValue.getBytesUnsafe(columnName));
        }
        return new ChannelAccessAggregatedSample(sampleTimeStamp,
                ChannelAccessSampleType.AGGREGATED_SCALAR_DOUBLE, newUdtValue);
    }

    /**
     * Creates an aggregated scalar float sample. The sample is created by
     * taking the meta-data (limits, units, etc.) from the specified
     * <code>metaDataSample</code> and adding the specified values to create a
     * new aggregated sample.
     * 
     * @param sampleTimeStamp
     *            time stamp of the newly created sample. The time is specified
     *            as the number of nanoseconds since epoch (January 1st, 1970,
     *            00:00:00 UTC).
     * @param mean
     *            mean of the aggregated samples. In mathematical terms this is
     *            ∑ p<sub>i</sub> x<sub>i</sub> where x<sub>i</sub> are the
     *            individual samples and p<sub>i</sub> = t<sub>sample</sub> /
     *            t<sub>total</sub>.
     * @param std
     *            standard deviation of the aggregated samples. In mathematical
     *            terms this is √(&lt;x<sup>2</sup>&gt; - &lt;x&gt;<sup>2</sup>)
     *            where &lt;x&gt; is the <code>mean</code> and
     *            &lt;x<sup>2</sup>&gt; = ∑ p<sub>i</sub>
     *            x<sub>i</sub><sup>2</sup> with x<sub>i</sub> being the
     *            individual samples and p<sub>i</sub> = t<sub>sample</sub> /
     *            t<sub>total</sub>.
     * @param minimum
     *            least value of any sample that participated in building the
     *            aggregated sample.
     * @param maximum
     *            greatest value of any sample that participated in building the
     *            aggregated sample.
     * @param alarmSeverity
     *            highest alarm severity encountered among all samples.
     * @param alarmStatus
     *            alarm status associated with the highest alarm severity as
     *            specified by <code>alarmSeverity</code>.
     * @param coveredPeriodFraction
     *            fraction of the total interval that is covered by the
     *            aggregated sample. For example, if an aggregated sample is
     *            generated for a period of 30 seconds, but the samples used to
     *            build this aggregated sample only cover 27 seconds of this
     *            period, the fraction is 0.9.
     * @param metaDataSample
     *            sample which is used as the source for the created sample's
     *            meta-data. The meta-data includes information not directly
     *            associated with the sample's value such as limits and display
     *            information. This sample must be of type
     *            <code>SCALAR_FLOAT</code>, <code>ARRAY_FLOAT</code> or
     *            <code>AGGREGATED_SCALAR_FLOAT</code>.
     * @return new aggregated sample created from the specified parameters.
     */
    public ChannelAccessAggregatedSample createAggregatedScalarFloatSample(
            long sampleTimeStamp, double mean, double std, float minimum,
            float maximum, ChannelAccessAlarmSeverity alarmSeverity,
            ChannelAccessAlarmStatus alarmStatus, double coveredPeriodFraction,
            ChannelAccessUdtSample metaDataSample) {
        // This method is only used internally, so we use assertions instead of
        // preconditions.
        assert (metaDataSample != null);
        assert (metaDataSample.getType().equals(
                ChannelAccessSampleType.SCALAR_FLOAT)
                || metaDataSample.getType().equals(
                        ChannelAccessSampleType.ARRAY_FLOAT) || metaDataSample
                .getType().equals(
                        ChannelAccessSampleType.AGGREGATED_SCALAR_FLOAT));
        UDTValue oldUdtValue = metaDataSample.getValue();
        UDTValue newUdtValue = aggregatedScalarFloat.newValue();
        newUdtValue.setDouble(UDT_COLUMN_MEAN, mean);
        newUdtValue.setDouble(UDT_COLUMN_STD, std);
        newUdtValue.setFloat(UDT_COLUMN_MIN, minimum);
        newUdtValue.setFloat(UDT_COLUMN_MAX, maximum);
        newUdtValue.setShort(UDT_COLUMN_ALARM_SEVERITY,
                alarmSeverity.toSeverityCode());
        newUdtValue.setShort(UDT_COLUMN_ALARM_STATUS,
                alarmStatus.toAlarmStatusCode());
        newUdtValue.setDouble(UDT_COLUMN_COVERED_PERIOD_FRACTION,
                coveredPeriodFraction);
        for (String columnName : UDT_AGGREGATED_SCALAR_FLOATING_POINT_META_DATA_COLUMNS) {
            newUdtValue.setBytesUnsafe(columnName,
                    oldUdtValue.getBytesUnsafe(columnName));
        }
        return new ChannelAccessAggregatedSample(sampleTimeStamp,
                ChannelAccessSampleType.AGGREGATED_SCALAR_FLOAT, newUdtValue);
    }

    /**
     * Creates an aggregated scalar long sample. The sample is created by taking
     * the meta-data (limits, units, etc.) from the specified
     * <code>metaDataSample</code> and adding the specified values to create a
     * new aggregated sample.
     * 
     * @param sampleTimeStamp
     *            time stamp of the newly created sample. The time is specified
     *            as the number of nanoseconds since epoch (January 1st, 1970,
     *            00:00:00 UTC).
     * @param mean
     *            mean of the aggregated samples. In mathematical terms this is
     *            ∑ p<sub>i</sub> x<sub>i</sub> where x<sub>i</sub> are the
     *            individual samples and p<sub>i</sub> = t<sub>sample</sub> /
     *            t<sub>total</sub>.
     * @param std
     *            standard deviation of the aggregated samples. In mathematical
     *            terms this is √(&lt;x<sup>2</sup>&gt; - &lt;x&gt;<sup>2</sup>)
     *            where &lt;x&gt; is the <code>mean</code> and
     *            &lt;x<sup>2</sup>&gt; = ∑ p<sub>i</sub>
     *            x<sub>i</sub><sup>2</sup> with x<sub>i</sub> being the
     *            individual samples and p<sub>i</sub> = t<sub>sample</sub> /
     *            t<sub>total</sub>.
     * @param minimum
     *            least value of any sample that participated in building the
     *            aggregated sample.
     * @param maximum
     *            greatest value of any sample that participated in building the
     *            aggregated sample.
     * @param alarmSeverity
     *            highest alarm severity encountered among all samples.
     * @param alarmStatus
     *            alarm status associated with the highest alarm severity as
     *            specified by <code>alarmSeverity</code>.
     * @param coveredPeriodFraction
     *            fraction of the total interval that is covered by the
     *            aggregated sample. For example, if an aggregated sample is
     *            generated for a period of 30 seconds, but the samples used to
     *            build this aggregated sample only cover 27 seconds of this
     *            period, the fraction is 0.9.
     * @param metaDataSample
     *            sample which is used as the source for the created sample's
     *            meta-data. The meta-data includes information not directly
     *            associated with the sample's value such as limits and display
     *            information. This sample must be of type
     *            <code>SCALAR_LONG</code>, <code>ARRAY_LONG</code> or
     *            <code>AGGREGATED_SCALAR_LONG</code>.
     * @return new aggregated sample created from the specified parameters.
     */
    public ChannelAccessAggregatedSample createAggregatedScalarLongSample(
            long sampleTimeStamp, double mean, double std, int minimum,
            int maximum, ChannelAccessAlarmSeverity alarmSeverity,
            ChannelAccessAlarmStatus alarmStatus, double coveredPeriodFraction,
            ChannelAccessUdtSample metaDataSample) {
        // This method is only used internally, so we use assertions instead of
        // preconditions.
        assert (metaDataSample != null);
        assert (metaDataSample.getType().equals(
                ChannelAccessSampleType.SCALAR_LONG)
                || metaDataSample.getType().equals(
                        ChannelAccessSampleType.ARRAY_LONG) || metaDataSample
                .getType().equals(
                        ChannelAccessSampleType.AGGREGATED_SCALAR_LONG));
        UDTValue oldUdtValue = metaDataSample.getValue();
        UDTValue newUdtValue = aggregatedScalarLong.newValue();
        newUdtValue.setDouble(UDT_COLUMN_MEAN, mean);
        newUdtValue.setDouble(UDT_COLUMN_STD, std);
        newUdtValue.setInt(UDT_COLUMN_MIN, minimum);
        newUdtValue.setInt(UDT_COLUMN_MAX, maximum);
        newUdtValue.setShort(UDT_COLUMN_ALARM_SEVERITY,
                alarmSeverity.toSeverityCode());
        newUdtValue.setShort(UDT_COLUMN_ALARM_STATUS,
                alarmStatus.toAlarmStatusCode());
        newUdtValue.setDouble(UDT_COLUMN_COVERED_PERIOD_FRACTION,
                coveredPeriodFraction);
        for (String columnName : UDT_AGGREGATED_SCALAR_INTEGER_META_DATA_COLUMNS) {
            newUdtValue.setBytesUnsafe(columnName,
                    oldUdtValue.getBytesUnsafe(columnName));
        }
        return new ChannelAccessAggregatedSample(sampleTimeStamp,
                ChannelAccessSampleType.AGGREGATED_SCALAR_LONG, newUdtValue);
    }

    /**
     * Creates an aggregated scalar short sample. The sample is created by
     * taking the meta-data (limits, units, etc.) from the specified
     * <code>metaDataSample</code> and adding the specified values to create a
     * new aggregated sample.
     * 
     * @param sampleTimeStamp
     *            time stamp of the newly created sample. The time is specified
     *            as the number of nanoseconds since epoch (January 1st, 1970,
     *            00:00:00 UTC).
     * @param mean
     *            mean of the aggregated samples. In mathematical terms this is
     *            ∑ p<sub>i</sub> x<sub>i</sub> where x<sub>i</sub> are the
     *            individual samples and p<sub>i</sub> = t<sub>sample</sub> /
     *            t<sub>total</sub>.
     * @param std
     *            standard deviation of the aggregated samples. In mathematical
     *            terms this is √(&lt;x<sup>2</sup>&gt; - &lt;x&gt;<sup>2</sup>)
     *            where &lt;x&gt; is the <code>mean</code> and
     *            &lt;x<sup>2</sup>&gt; = ∑ p<sub>i</sub>
     *            x<sub>i</sub><sup>2</sup> with x<sub>i</sub> being the
     *            individual samples and p<sub>i</sub> = t<sub>sample</sub> /
     *            t<sub>total</sub>.
     * @param minimum
     *            least value of any sample that participated in building the
     *            aggregated sample.
     * @param maximum
     *            greatest value of any sample that participated in building the
     *            aggregated sample.
     * @param alarmSeverity
     *            highest alarm severity encountered among all samples.
     * @param alarmStatus
     *            alarm status associated with the highest alarm severity as
     *            specified by <code>alarmSeverity</code>.
     * @param coveredPeriodFraction
     *            fraction of the total interval that is covered by the
     *            aggregated sample. For example, if an aggregated sample is
     *            generated for a period of 30 seconds, but the samples used to
     *            build this aggregated sample only cover 27 seconds of this
     *            period, the fraction is 0.9.
     * @param metaDataSample
     *            sample which is used as the source for the created sample's
     *            meta-data. The meta-data includes information not directly
     *            associated with the sample's value such as limits and display
     *            information. This sample must be of type
     *            <code>SCALAR_SHORT</code>, <code>ARRAY_SHORT</code> or
     *            <code>AGGREGATED_SCALAR_SHORT</code>.
     * @return new aggregated sample created from the specified parameters.
     */
    public ChannelAccessAggregatedSample createAggregatedScalarShortSample(
            long sampleTimeStamp, double mean, double std, short minimum,
            short maximum, ChannelAccessAlarmSeverity alarmSeverity,
            ChannelAccessAlarmStatus alarmStatus, double coveredPeriodFraction,
            ChannelAccessUdtSample metaDataSample) {
        // This method is only used internally, so we use assertions instead of
        // preconditions.
        assert (metaDataSample != null);
        assert (metaDataSample.getType().equals(
                ChannelAccessSampleType.SCALAR_SHORT)
                || metaDataSample.getType().equals(
                        ChannelAccessSampleType.ARRAY_SHORT) || metaDataSample
                .getType().equals(
                        ChannelAccessSampleType.AGGREGATED_SCALAR_SHORT));
        UDTValue oldUdtValue = metaDataSample.getValue();
        UDTValue newUdtValue = aggregatedScalarShort.newValue();
        newUdtValue.setDouble(UDT_COLUMN_MEAN, mean);
        newUdtValue.setDouble(UDT_COLUMN_STD, std);
        newUdtValue.setShort(UDT_COLUMN_MIN, minimum);
        newUdtValue.setShort(UDT_COLUMN_MAX, maximum);
        newUdtValue.setShort(UDT_COLUMN_ALARM_SEVERITY,
                alarmSeverity.toSeverityCode());
        newUdtValue.setShort(UDT_COLUMN_ALARM_STATUS,
                alarmStatus.toAlarmStatusCode());
        newUdtValue.setDouble(UDT_COLUMN_COVERED_PERIOD_FRACTION,
                coveredPeriodFraction);
        for (String columnName : UDT_AGGREGATED_SCALAR_INTEGER_META_DATA_COLUMNS) {
            newUdtValue.setBytesUnsafe(columnName,
                    oldUdtValue.getBytesUnsafe(columnName));
        }
        return new ChannelAccessAggregatedSample(sampleTimeStamp,
                ChannelAccessSampleType.AGGREGATED_SCALAR_SHORT, newUdtValue);
    }

    /**
     * Creates a raw sample from the specified controls and time values, using
     * the time stamp from the time value. It is expected that the types of the
     * controls and time values match. For example, if one is
     * <code>DBR_CTRL_DOUBLE</code>, the other one must be
     * <code>DBR_TIME_DOUBLE</code>.
     * 
     * @param controlsValue
     *            value used for the sample's meta-data.
     * @param timeValue
     *            value used for the sample's time-stamp, alarm state, and
     *            value.
     */
    public ChannelAccessRawSample createRawSample(
            ChannelAccessTimeValue<?> timeValue,
            ChannelAccessControlsValue<?> controlsValue) {
        // This method is only used internally, so we use assertions instead of
        // preconditions.
        assert (timeValue != null);
        assert (controlsValue != null);
        assert (controlsValue.getType().toSimpleType().equals(timeValue
                .getType().toSimpleType()));
        int seconds = timeValue.getTimeSeconds();
        long timeStamp = (OFFSET_EPICS_EPOCH_TO_UNIX_EPOCH_SECONDS + (seconds < 0 ? (seconds + 4294967296L)
                : seconds))
                * 1000000000L + timeValue.getTimeNanoseconds();
        return createRawSample(timeValue, controlsValue, timeStamp);
    }

    /**
     * Creates a raw sample from the specified controls and time values, using
     * the specified time stamp instead of using the time stamp from the time
     * value. It is expected that the types of the controls and time values
     * match. For example, if one is <code>DBR_CTRL_DOUBLE</code>, the other one
     * must be <code>DBR_TIME_DOUBLE</code>.
     * 
     * @param controlsValue
     *            value used for the sample's meta-data.
     * @param timeValue
     *            value used for the sample's time-stamp, alarm state, and
     *            value.
     * @param timeStamp
     *            time stamp of the newly created sample. The time stamp is
     *            specified as the number of nanoseconds since epoch (January
     *            1st, 1970, 00:00:00 UTC).
     */
    public ChannelAccessRawSample createRawSample(
            ChannelAccessTimeValue<?> timeValue,
            ChannelAccessControlsValue<?> controlsValue, long timeStamp) {
        // This method is only used internally, so we use assertions instead of
        // preconditions.
        assert (timeValue != null);
        assert (controlsValue != null);
        assert (controlsValue.getType().toSimpleType().equals(timeValue
                .getType().toSimpleType()));
        assert (timeStamp >= 0L);
        Pair<ChannelAccessSampleType, UDTValue> sampleTypeAndValue = convertValue(
                timeValue, controlsValue);
        return new ChannelAccessRawSample(timeStamp,
                sampleTypeAndValue.getLeft(), sampleTypeAndValue.getRight(),
                true);
    }

    /**
     * <p>
     * Deserializes the meta-data of an aggregated sample. Depending on the type
     * of the aggregated sample, the meta-data may include engineering units,
     * precision, and various limits. The alarm severity and status is also
     * included in the returned value.
     * </p>
     * 
     * <p>
     * The returned value does not contain any value elements. For an aggregated
     * sample, the mean value stored in the sample might be of a type that is
     * not compatible with the native element type of the returned value. For
     * this reason, the
     * {@link #deserializeMeanColumn(ChannelAccessAggregatedSample)} method
     * should be used for deserializing the mean value. In the same way, the
     * {@link #deserializeStdColumn(ChannelAccessAggregatedSample)} method can
     * be used for deserializing the standard deviation.
     * </p>
     * 
     * <p>
     * The minimum and maximum values included in the aggregated sample cannot
     * be returned as part of the returned value either. This information can be
     * deserialized by using one of the type-specific
     * <code>deserialize…MinColumn</code> and <code>deserialize…MaxColumn</code>
     * methods.
     * </p>
     * 
     * @param sample
     *            sample to be deserialized. Only the meta-data stored in the
     *            sample is deserialized. The actual value has to be
     *            deserialized separately.
     * @return Channel Access value containing the meta-data from the sample.
     *         The exact type of the returned value depends on the type of the
     *         sample and thus the meta-data being available.
     */
    public ChannelAccessControlsValue<?> deserializeAggregatedSampleMetaData(
            ChannelAccessAggregatedSample sample) {
        UDTValue value = sample.getValue();
        switch (sample.getType()) {
        case AGGREGATED_SCALAR_CHAR:
            return ChannelAccessValueFactory.createControlsChar(
                    ArrayUtils.EMPTY_BYTE_ARRAY, deserializeAlarmStatus(value),
                    deserializeAlarmSeverity(value),
                    value.getString(UDT_COLUMN_UNITS),
                    value.getByte(UDT_COLUMN_UPPER_DISPLAY_LIMIT),
                    value.getByte(UDT_COLUMN_LOWER_DISPLAY_LIMIT),
                    value.getByte(UDT_COLUMN_UPPER_ALARM_LIMIT),
                    value.getByte(UDT_COLUMN_UPPER_WARNING_LIMIT),
                    value.getByte(UDT_COLUMN_LOWER_WARNING_LIMIT),
                    value.getByte(UDT_COLUMN_LOWER_ALARM_LIMIT),
                    value.getByte(UDT_COLUMN_UPPER_CONTROL_LIMIT),
                    value.getByte(UDT_COLUMN_LOWER_CONTROL_LIMIT),
                    Charsets.UTF_8);
        case AGGREGATED_SCALAR_DOUBLE:
            return ChannelAccessValueFactory.createControlsDouble(
                    ArrayUtils.EMPTY_DOUBLE_ARRAY,
                    deserializeAlarmStatus(value),
                    deserializeAlarmSeverity(value),
                    value.getShort(UDT_COLUMN_PRECISION),
                    value.getString(UDT_COLUMN_UNITS),
                    value.getDouble(UDT_COLUMN_UPPER_DISPLAY_LIMIT),
                    value.getDouble(UDT_COLUMN_LOWER_DISPLAY_LIMIT),
                    value.getDouble(UDT_COLUMN_UPPER_ALARM_LIMIT),
                    value.getDouble(UDT_COLUMN_UPPER_WARNING_LIMIT),
                    value.getDouble(UDT_COLUMN_LOWER_WARNING_LIMIT),
                    value.getDouble(UDT_COLUMN_LOWER_ALARM_LIMIT),
                    value.getDouble(UDT_COLUMN_UPPER_CONTROL_LIMIT),
                    value.getDouble(UDT_COLUMN_LOWER_CONTROL_LIMIT),
                    Charsets.UTF_8);
        case AGGREGATED_SCALAR_FLOAT:
            return ChannelAccessValueFactory.createControlsFloat(
                    ArrayUtils.EMPTY_FLOAT_ARRAY,
                    deserializeAlarmStatus(value),
                    deserializeAlarmSeverity(value),
                    value.getShort(UDT_COLUMN_PRECISION),
                    value.getString(UDT_COLUMN_UNITS),
                    value.getFloat(UDT_COLUMN_UPPER_DISPLAY_LIMIT),
                    value.getFloat(UDT_COLUMN_LOWER_DISPLAY_LIMIT),
                    value.getFloat(UDT_COLUMN_UPPER_ALARM_LIMIT),
                    value.getFloat(UDT_COLUMN_UPPER_WARNING_LIMIT),
                    value.getFloat(UDT_COLUMN_LOWER_WARNING_LIMIT),
                    value.getFloat(UDT_COLUMN_LOWER_ALARM_LIMIT),
                    value.getFloat(UDT_COLUMN_UPPER_CONTROL_LIMIT),
                    value.getFloat(UDT_COLUMN_LOWER_CONTROL_LIMIT),
                    Charsets.UTF_8);
        case AGGREGATED_SCALAR_LONG:
            return ChannelAccessValueFactory.createControlsLong(
                    ArrayUtils.EMPTY_INT_ARRAY, deserializeAlarmStatus(value),
                    deserializeAlarmSeverity(value),
                    value.getString(UDT_COLUMN_UNITS),
                    value.getInt(UDT_COLUMN_UPPER_DISPLAY_LIMIT),
                    value.getInt(UDT_COLUMN_LOWER_DISPLAY_LIMIT),
                    value.getInt(UDT_COLUMN_UPPER_ALARM_LIMIT),
                    value.getInt(UDT_COLUMN_UPPER_WARNING_LIMIT),
                    value.getInt(UDT_COLUMN_LOWER_WARNING_LIMIT),
                    value.getInt(UDT_COLUMN_LOWER_ALARM_LIMIT),
                    value.getInt(UDT_COLUMN_UPPER_CONTROL_LIMIT),
                    value.getInt(UDT_COLUMN_LOWER_CONTROL_LIMIT),
                    Charsets.UTF_8);
        case AGGREGATED_SCALAR_SHORT:
            return ChannelAccessValueFactory.createControlsShort(
                    ArrayUtils.EMPTY_SHORT_ARRAY,
                    deserializeAlarmStatus(value),
                    deserializeAlarmSeverity(value),
                    value.getString(UDT_COLUMN_UNITS),
                    value.getShort(UDT_COLUMN_UPPER_DISPLAY_LIMIT),
                    value.getShort(UDT_COLUMN_LOWER_DISPLAY_LIMIT),
                    value.getShort(UDT_COLUMN_UPPER_ALARM_LIMIT),
                    value.getShort(UDT_COLUMN_UPPER_WARNING_LIMIT),
                    value.getShort(UDT_COLUMN_LOWER_WARNING_LIMIT),
                    value.getShort(UDT_COLUMN_LOWER_ALARM_LIMIT),
                    value.getShort(UDT_COLUMN_UPPER_CONTROL_LIMIT),
                    value.getShort(UDT_COLUMN_LOWER_CONTROL_LIMIT),
                    Charsets.UTF_8);
        default:
            throw new RuntimeException("Unhandled sample type: "
                    + sample.getType());
        }
    }

    /**
     * Deserializes and returns the alarm severity stored in the specified
     * sample.
     * 
     * @param sample
     *            sample that contains the alarm severity to be returned.
     * @return alarm severity stored in the sample.
     */
    public ChannelAccessAlarmSeverity deserializeAlarmSeverityColumn(
            ChannelAccessUdtSample sample) {
        return deserializeAlarmSeverity(sample.getValue());
    }

    /**
     * Deserializes and returns the alarm status stored in the specified sample.
     * 
     * @param sample
     *            sample that contains the alarm status to be returned.
     * @return alarm status stored in the sample.
     */
    public ChannelAccessAlarmStatus deserializeAlarmStatusColumn(
            ChannelAccessUdtSample sample) {
        return deserializeAlarmStatus(sample.getValue());
    }

    /**
     * Deserializes and returns the aggregated sample's maximum. The maximum is
     * the greatest value that any of the values used when building the
     * aggregated samples had. It is an error to use this method on a sample
     * that does not have a "max" column of type tinyint.
     * 
     * @param sample
     *            sample that contains the maximum value to be returned.
     * @return maximum value stored in the aggregated sample.
     * @throws RuntimeException
     *             if the "max" column is not of type tinyint.
     */
    public byte deserializeByteMaxColumn(ChannelAccessAggregatedSample sample) {
        return sample.getValue().getByte(UDT_COLUMN_MAX);
    }

    /**
     * Deserializes and returns the aggregated sample's minimum. The minimum is
     * the least value that any of the values used when building the aggregated
     * samples had. It is an error to use this method on a sample that does not
     * have a "min" column of type tinyint.
     * 
     * @param sample
     *            sample that contains the minimum value to be returned.
     * @return minimum value stored in the aggregated sample.
     * @throws RuntimeException
     *             if the "min" column is not of type tinyint.
     */
    public byte deserializeByteMinColumn(ChannelAccessAggregatedSample sample) {
        return sample.getValue().getByte(UDT_COLUMN_MIN);
    }

    /**
     * Deserializes the sample's "value" column as a byte. It is an error to use
     * this method on a sample that does not have a "value" column or that has a
     * "value" column that is not of type tinyint.
     * 
     * @param sample
     *            sample that contains an integer "value" column.
     * @return value of the sample's "value" column.
     * @throws RuntimeException
     *             if the sample does not have a "value" column or the "value"
     *             column is not of type tinyint.
     */
    public byte deserializeByteValueColumn(ChannelAccessRawSample sample) {
        return sample.getValue().getByte(UDT_COLUMN_VALUE);
    }

    /**
     * Deserializes and returns the aggregated sample's covered period fraction.
     * This fraction specified how much of the period that is supposed to be
     * covered by the aggregated sample was actually covered by the samples that
     * were used to calculate the aggregated sample. When the source samples
     * covered the complete interval, the fraction is 1.0. However, if the
     * sample only covered 70 percent of the interval (and the other 30 percent
     * of the interval were covered by samples of a different type or the
     * channel was disconnected), the fraction is only 0.7.
     * 
     * @param sample
     *            sample that contains the covered-period fraction to be
     *            returned.
     * @return covered-period fraction stored in the aggregated sample.
     */
    public double deserializeCoveredPeriodFractionColumn(
            ChannelAccessAggregatedSample sample) {
        return sample.getValue().getDouble(UDT_COLUMN_COVERED_PERIOD_FRACTION);
    }

    /**
     * Deserializes and returns the aggregated sample's maximum. The maximum is
     * the greatest value that any of the values used when building the
     * aggregated samples had. It is an error to use this method on a sample
     * that does not have a "max" column of type double.
     * 
     * @param sample
     *            sample that contains the maximum value to be returned.
     * @return maximum value stored in the aggregated sample.
     * @throws RuntimeException
     *             if the "max" column is not of type double.
     */
    public double deserializeDoubleMaxColumn(
            ChannelAccessAggregatedSample sample) {
        return sample.getValue().getDouble(UDT_COLUMN_MAX);
    }

    /**
     * Deserializes and returns the aggregated sample's minimum. The minimum is
     * the least value that any of the values used when building the aggregated
     * samples had. It is an error to use this method on a sample that does not
     * have a "min" column of type double.
     * 
     * @param sample
     *            sample that contains the minimum value to be returned.
     * @return minimum value stored in the aggregated sample.
     * @throws RuntimeException
     *             if the "min" column is not of type double.
     */
    public double deserializeDoubleMinColumn(
            ChannelAccessAggregatedSample sample) {
        return sample.getValue().getDouble(UDT_COLUMN_MIN);
    }

    /**
     * Deserializes the sample's value column as a double. It is an error to use
     * this method on a sample that does not have a "value" column or that has a
     * "value" column of a different type.
     * 
     * @param sample
     *            sample that contains a double "value" column.
     * @return value of the sample's "value" column.
     * @throws RuntimeException
     *             if the sample does not have a "value" column or the "value"
     *             column is not of type double.
     */
    public double deserializeDoubleValueColumn(ChannelAccessRawSample sample) {
        return sample.getValue().getDouble(UDT_COLUMN_VALUE);
    }

    /**
     * Deserializes and returns the aggregated sample's maximum. The maximum is
     * the greatest value that any of the values used when building the
     * aggregated samples had. It is an error to use this method on a sample
     * that does not have a "max" column of type float.
     * 
     * @param sample
     *            sample that contains the maximum value to be returned.
     * @return maximum value stored in the aggregated sample.
     * @throws RuntimeException
     *             if the "max" column is not of type float.
     */
    public float deserializeFloatMaxColumn(ChannelAccessAggregatedSample sample) {
        return sample.getValue().getFloat(UDT_COLUMN_MAX);
    }

    /**
     * Deserializes and returns the aggregated sample's minimum. The minimum is
     * the least value that any of the values used when building the aggregated
     * samples had. It is an error to use this method on a sample that does not
     * have a "min" column of type float.
     * 
     * @param sample
     *            sample that contains the minimum value to be returned.
     * @return minimum value stored in the aggregated sample.
     * @throws RuntimeException
     *             if the "min" column is not of type float.
     */
    public float deserializeFloatMinColumn(ChannelAccessAggregatedSample sample) {
        return sample.getValue().getFloat(UDT_COLUMN_MIN);
    }

    /**
     * Deserializes the sample's "value" column as a float. It is an error to
     * use this method on a sample that does not have a "value" column or that
     * has a "value" column of a different type.
     * 
     * @param sample
     *            sample that contains a float "value" column.
     * @return value of the sample's value column.
     * @throws RuntimeException
     *             if the sample does not have a "value" column or the "value"
     *             column is not of type float.
     */
    public float deserializeFloatValueColumn(ChannelAccessRawSample sample) {
        return sample.getValue().getFloat(UDT_COLUMN_VALUE);
    }

    /**
     * Deserializes and returns the aggregated sample's maximum. The maximum is
     * the greatest value that any of the values used when building the
     * aggregated samples had. It is an error to use this method on a sample
     * that does not have a "max" column of type int.
     * 
     * @param sample
     *            sample that contains the maximum value to be returned.
     * @return maximum value stored in the aggregated sample.
     * @throws RuntimeException
     *             if the "max" column is not of type int.
     */
    public int deserializeLongMaxColumn(ChannelAccessAggregatedSample sample) {
        return sample.getValue().getInt(UDT_COLUMN_MAX);
    }

    /**
     * Deserializes and returns the aggregated sample's minimum. The minimum is
     * the least value that any of the values used when building the aggregated
     * samples had. It is an error to use this method on a sample that does not
     * have a "min" column of type int.
     * 
     * @param sample
     *            sample that contains the minimum value to be returned.
     * @return minimum value stored in the aggregated sample.
     * @throws RuntimeException
     *             if the "min" column is not of type int.
     */
    public int deserializeLongMinColumn(ChannelAccessAggregatedSample sample) {
        return sample.getValue().getInt(UDT_COLUMN_MIN);
    }

    /**
     * Deserializes the sample's "value" column as an int. It is an error to use
     * this method on a sample that does not have a "value" column or that has a
     * "value" column that is not of type int.
     * 
     * @param sample
     *            sample that contains an integer "value" column.
     * @return value of the sample's "value" column.
     * @throws RuntimeException
     *             if the sample does not have a "value" column or the "value"
     *             column is not of type int.
     */
    public int deserializeLongValueColumn(ChannelAccessRawSample sample) {
        return sample.getValue().getInt(UDT_COLUMN_VALUE);
    }

    /**
     * Deserializes and returns the aggregated sample's mean. The mean is the
     * (weighted) average of all samples that were used to build the aggregated
     * sample.
     * 
     * @param sample
     *            sample that contains the mean value to be returned.
     * @return mean value stored in the aggregated sample.
     */
    public double deserializeMeanColumn(ChannelAccessAggregatedSample sample) {
        return sample.getValue().getDouble(UDT_COLUMN_MEAN);
    }

    /**
     * Deserializes a raw sample. The returned value is generated from the
     * various columns that make up the value in the underlying data store.
     * 
     * @param sample
     *            sample to be deserialized.
     * @return value of the sample. This value includes the meta-data associated
     *         with the sample as well as the actual value.
     */
    public ChannelAccessControlsValue<?> deserializeSample(
            ChannelAccessRawSample sample) {
        UDTValue value = sample.getValue();
        switch (sample.getType()) {
        case ARRAY_CHAR:
            return deserializeCharValue(value, true);
        case ARRAY_DOUBLE:
            return deserializeDoubleValue(value, true);
        case ARRAY_ENUM:
            return deserializeEnumValue(value, true);
        case ARRAY_FLOAT:
            return deserializeFloatValue(value, true);
        case ARRAY_LONG:
            return deserializeLongValue(value, true);
        case ARRAY_SHORT:
            return deserializeShortValue(value, true);
        case ARRAY_STRING:
            return deserializeStringValue(value, true);
        case SCALAR_CHAR:
            return deserializeCharValue(value, false);
        case SCALAR_DOUBLE:
            return deserializeDoubleValue(value, false);
        case SCALAR_ENUM:
            return deserializeEnumValue(value, false);
        case SCALAR_FLOAT:
            return deserializeFloatValue(value, false);
        case SCALAR_LONG:
            return deserializeLongValue(value, false);
        case SCALAR_SHORT:
            return deserializeShortValue(value, false);
        case SCALAR_STRING:
            return deserializeStringValue(value, false);
        default:
            throw new RuntimeException("Raw sample has unexpected type "
                    + sample.getType() + ".");
        }
    }

    /**
     * Deserializes and returns the aggregated sample's maximum. The maximum is
     * the greatest value that any of the values used when building the
     * aggregated samples had. It is an error to use this method on a sample
     * that does not have a "max" column of type smallint.
     * 
     * @param sample
     *            sample that contains the maximum value to be returned.
     * @return maximum value stored in the aggregated sample.
     * @throws RuntimeException
     *             if the "max" column is not of type smallint.
     */
    public short deserializeShortMaxColumn(ChannelAccessAggregatedSample sample) {
        return sample.getValue().getShort(UDT_COLUMN_MAX);
    }

    /**
     * Deserializes and returns the aggregated sample's minimum. The minimum is
     * the least value that any of the values used when building the aggregated
     * samples had. It is an error to use this method on a sample that does not
     * have a "min" column of type smallint.
     * 
     * @param sample
     *            sample that contains the minimum value to be returned.
     * @return minimum value stored in the aggregated sample.
     * @throws RuntimeException
     *             if the "min" column is not of type smallint.
     */
    public short deserializeShortMinColumn(ChannelAccessAggregatedSample sample) {
        return sample.getValue().getShort(UDT_COLUMN_MIN);
    }

    /**
     * Deserializes the sample's "value" column as a short. It is an error to
     * use this method on a sample that does not have a "value" column or that
     * has a "value" column that is not of type smallint.
     * 
     * @param sample
     *            sample that contains an integer "value" column.
     * @return value of the sample's "value" column.
     * @throws RuntimeException
     *             if the sample does not have a "value" column or the "value"
     *             column is not of type smallint.
     */
    public short deserializeShortValueColumn(ChannelAccessRawSample sample) {
        return sample.getValue().getShort(UDT_COLUMN_VALUE);
    }

    /**
     * Deserializes and returns the aggregated sample's standard deviation. The
     * aggregated deviation is a measure of how much the samples that were used
     * when building the aggregated sample are scattered.
     * 
     * @param sample
     *            sample that contains the standard deviation to be returned.
     * @return standard deviation stored in the aggregated sample.
     */
    public double deserializeStdColumn(ChannelAccessAggregatedSample sample) {
        return sample.getValue().getDouble(UDT_COLUMN_STD);
    }

    /**
     * Serializes a sample by converting it to the JSON format version 1. This
     * method delegates to {@link JsonV1SampleSerializer} for performing the
     * actual serialization.
     * 
     * @param jsonGenerator
     *            JSON generator to which the serialized data is written.
     * @param sample
     *            sample to be serialized.
     * @throws IOException
     *             if the <code>jsonGenerator</code> throws such an exception.
     */
    public void serializeSampleToJsonV1(JsonGenerator jsonGenerator,
            ChannelAccessSample sample) throws IOException {
        if (sample instanceof ChannelAccessRawSample) {
            serializeRawSampleToJsonV1(jsonGenerator,
                    (ChannelAccessRawSample) sample);
        } else if (sample instanceof ChannelAccessAggregatedSample) {
            serializeAggregatedSampleToJsonV1(jsonGenerator,
                    (ChannelAccessAggregatedSample) sample);
        } else if (sample instanceof ChannelAccessDisabledSample) {
            JsonV1SampleSerializer
                    .serializeStringSample(
                            jsonGenerator,
                            sample.getTimeStamp(),
                            "Archive_Disabled",
                            JsonV1SampleSerializer.Severity.INVALID,
                            false,
                            "Archive_Disabled",
                            sample.isOriginalSample() ? JsonV1SampleSerializer.Quality.ORIGINAL
                                    : JsonV1SampleSerializer.Quality.INTERPOLATED);
        } else if (sample instanceof ChannelAccessDisconnectedSample) {
            JsonV1SampleSerializer
                    .serializeStringSample(
                            jsonGenerator,
                            sample.getTimeStamp(),
                            "Disconnected",
                            JsonV1SampleSerializer.Severity.INVALID,
                            false,
                            "Disconnected",
                            sample.isOriginalSample() ? JsonV1SampleSerializer.Quality.ORIGINAL
                                    : JsonV1SampleSerializer.Quality.INTERPOLATED);
        } else {
            throw new RuntimeException("Unsupported sample class: "
                    + sample.getClass().getName());
        }
    }

    /**
     * Returns the user-defined type for the CQL column storing a sample of the
     * specified type. This method is intended for use by
     * {@link ChannelAccessDatabaseAccess} when creating the tables for storing
     * samples.
     * 
     * @param sampleType
     *            type of the sample for which the corresponding user-defined
     *            type for the CQL storage shall be returned.
     * @return user-defined CQL type for storing samples of the specified type.
     */
    UDTType getColumnType(ChannelAccessSampleType sampleType) {
        switch (sampleType) {
        case AGGREGATED_SCALAR_CHAR:
            return SchemaBuilder.frozen(UDT_AGGREGATED_SCALAR_CHAR);
        case AGGREGATED_SCALAR_DOUBLE:
            return SchemaBuilder.frozen(UDT_AGGREGATED_SCALAR_DOUBLE);
        case AGGREGATED_SCALAR_FLOAT:
            return SchemaBuilder.frozen(UDT_AGGREGATED_SCALAR_FLOAT);
        case AGGREGATED_SCALAR_LONG:
            return SchemaBuilder.frozen(UDT_AGGREGATED_SCALAR_LONG);
        case AGGREGATED_SCALAR_SHORT:
            return SchemaBuilder.frozen(UDT_AGGREGATED_SCALAR_SHORT);
        case ARRAY_CHAR:
            return SchemaBuilder.frozen(UDT_ARRAY_CHAR);
        case ARRAY_DOUBLE:
            return SchemaBuilder.frozen(UDT_ARRAY_DOUBLE);
        case ARRAY_ENUM:
            return SchemaBuilder.frozen(UDT_ARRAY_ENUM);
        case ARRAY_FLOAT:
            return SchemaBuilder.frozen(UDT_ARRAY_FLOAT);
        case ARRAY_LONG:
            return SchemaBuilder.frozen(UDT_ARRAY_LONG);
        case ARRAY_SHORT:
            return SchemaBuilder.frozen(UDT_ARRAY_SHORT);
        case ARRAY_STRING:
            return SchemaBuilder.frozen(UDT_ARRAY_STRING);
        case SCALAR_CHAR:
            return SchemaBuilder.frozen(UDT_SCALAR_CHAR);
        case SCALAR_DOUBLE:
            return SchemaBuilder.frozen(UDT_SCALAR_DOUBLE);
        case SCALAR_ENUM:
            return SchemaBuilder.frozen(UDT_SCALAR_ENUM);
        case SCALAR_FLOAT:
            return SchemaBuilder.frozen(UDT_SCALAR_FLOAT);
        case SCALAR_LONG:
            return SchemaBuilder.frozen(UDT_SCALAR_LONG);
        case SCALAR_SHORT:
            return SchemaBuilder.frozen(UDT_SCALAR_SHORT);
        case SCALAR_STRING:
            return SchemaBuilder.frozen(UDT_SCALAR_STRING);
        default:
            throw new RuntimeException("Unhandled sample type: " + sampleType);
        }
    }

    private Pair<ChannelAccessSampleType, UDTValue> convertCharValue(
            ChannelAccessTimeChar timeValue,
            ChannelAccessControlsChar controlsValue) {
        ChannelAccessSampleType sampleType;
        UDTValue sampleValue;
        if (timeValue.getValueSize() == 1) {
            sampleType = ChannelAccessSampleType.SCALAR_CHAR;
            sampleValue = scalarChar.newValue();
            sampleValue.setByte(UDT_COLUMN_VALUE, timeValue.getValue().get());
        } else {
            sampleType = ChannelAccessSampleType.ARRAY_CHAR;
            sampleValue = arrayChar.newValue();
            sampleValue.setBytes(UDT_COLUMN_VALUE, timeValue.getValue());
        }
        setAlarmColumns(sampleValue, timeValue);
        setUnitsColumn(sampleValue, controlsValue);
        sampleValue.setByte(UDT_COLUMN_LOWER_WARNING_LIMIT,
                controlsValue.getLowerWarningLimit());
        sampleValue.setByte(UDT_COLUMN_UPPER_WARNING_LIMIT,
                controlsValue.getUpperWarningLimit());
        sampleValue.setByte(UDT_COLUMN_LOWER_ALARM_LIMIT,
                controlsValue.getLowerAlarmLimit());
        sampleValue.setByte(UDT_COLUMN_UPPER_ALARM_LIMIT,
                controlsValue.getUpperAlarmLimit());
        sampleValue.setByte(UDT_COLUMN_LOWER_DISPLAY_LIMIT,
                controlsValue.getLowerDisplayLimit());
        sampleValue.setByte(UDT_COLUMN_UPPER_DISPLAY_LIMIT,
                controlsValue.getUpperDisplayLimit());
        sampleValue.setByte(UDT_COLUMN_LOWER_CONTROL_LIMIT,
                controlsValue.getLowerControlLimit());
        sampleValue.setByte(UDT_COLUMN_UPPER_CONTROL_LIMIT,
                controlsValue.getUpperControlLimit());
        return Pair.of(sampleType, sampleValue);
    }

    private Pair<ChannelAccessSampleType, UDTValue> convertDoubleValue(
            ChannelAccessTimeDouble timeValue,
            ChannelAccessControlsDouble controlsValue) {
        ChannelAccessSampleType sampleType;
        UDTValue sampleValue;
        if (timeValue.getValueSize() == 1) {
            sampleType = ChannelAccessSampleType.SCALAR_DOUBLE;
            sampleValue = scalarDouble.newValue();
            sampleValue.setDouble(UDT_COLUMN_VALUE, timeValue.getValue().get());
        } else {
            sampleType = ChannelAccessSampleType.ARRAY_DOUBLE;
            sampleValue = arrayDouble.newValue();
            DoubleBuffer doubleBuffer = timeValue.getValue();
            ByteBuffer byteBuffer = ByteBuffer.allocate(doubleBuffer
                    .remaining() * 8);
            byteBuffer.asDoubleBuffer().put(doubleBuffer);
            sampleValue.setBytes(UDT_COLUMN_VALUE, byteBuffer);
        }
        setAlarmColumns(sampleValue, timeValue);
        setPrecisionAndUnitsColumns(sampleValue, controlsValue);
        sampleValue.setDouble(UDT_COLUMN_LOWER_WARNING_LIMIT,
                controlsValue.getLowerWarningLimit());
        sampleValue.setDouble(UDT_COLUMN_UPPER_WARNING_LIMIT,
                controlsValue.getUpperWarningLimit());
        sampleValue.setDouble(UDT_COLUMN_LOWER_ALARM_LIMIT,
                controlsValue.getLowerAlarmLimit());
        sampleValue.setDouble(UDT_COLUMN_UPPER_ALARM_LIMIT,
                controlsValue.getUpperAlarmLimit());
        sampleValue.setDouble(UDT_COLUMN_LOWER_DISPLAY_LIMIT,
                controlsValue.getLowerDisplayLimit());
        sampleValue.setDouble(UDT_COLUMN_UPPER_DISPLAY_LIMIT,
                controlsValue.getUpperDisplayLimit());
        sampleValue.setDouble(UDT_COLUMN_LOWER_CONTROL_LIMIT,
                controlsValue.getLowerControlLimit());
        sampleValue.setDouble(UDT_COLUMN_UPPER_CONTROL_LIMIT,
                controlsValue.getUpperControlLimit());
        return Pair.of(sampleType, sampleValue);
    }

    private Pair<ChannelAccessSampleType, UDTValue> convertEnumValue(
            ChannelAccessTimeEnum timeValue,
            ChannelAccessGraphicsEnum controlsValue) {
        ChannelAccessSampleType sampleType;
        UDTValue sampleValue;
        if (timeValue.getValueSize() == 1) {
            sampleType = ChannelAccessSampleType.SCALAR_ENUM;
            sampleValue = scalarEnum.newValue();
            sampleValue.setShort(UDT_COLUMN_VALUE, timeValue.getValue().get());
        } else {
            sampleType = ChannelAccessSampleType.ARRAY_ENUM;
            sampleValue = arrayEnum.newValue();
            ShortBuffer shortBuffer = timeValue.getValue();
            ByteBuffer byteBuffer = ByteBuffer
                    .allocate(shortBuffer.remaining() * 2);
            byteBuffer.asShortBuffer().put(shortBuffer);
            sampleValue.setBytes(UDT_COLUMN_VALUE, byteBuffer);
        }
        setAlarmColumns(sampleValue, timeValue);
        sampleValue.setList(UDT_COLUMN_LABELS, controlsValue.getLabels());
        return Pair.of(sampleType, sampleValue);
    }

    private Pair<ChannelAccessSampleType, UDTValue> convertFloatValue(
            ChannelAccessTimeFloat timeValue,
            ChannelAccessControlsFloat controlsValue) {
        ChannelAccessSampleType sampleType;
        UDTValue sampleValue;
        if (timeValue.getValueSize() == 1) {
            sampleType = ChannelAccessSampleType.SCALAR_FLOAT;
            sampleValue = scalarFloat.newValue();
            sampleValue.setFloat(UDT_COLUMN_VALUE, timeValue.getValue().get());
        } else {
            sampleType = ChannelAccessSampleType.ARRAY_FLOAT;
            sampleValue = arrayFloat.newValue();
            FloatBuffer floatBuffer = timeValue.getValue();
            ByteBuffer byteBuffer = ByteBuffer
                    .allocate(floatBuffer.remaining() * 4);
            byteBuffer.asFloatBuffer().put(floatBuffer);
            sampleValue.setBytes(UDT_COLUMN_VALUE, byteBuffer);
        }
        setAlarmColumns(sampleValue, timeValue);
        setPrecisionAndUnitsColumns(sampleValue, controlsValue);
        sampleValue.setFloat(UDT_COLUMN_LOWER_WARNING_LIMIT,
                controlsValue.getLowerWarningLimit());
        sampleValue.setFloat(UDT_COLUMN_UPPER_WARNING_LIMIT,
                controlsValue.getUpperWarningLimit());
        sampleValue.setFloat(UDT_COLUMN_LOWER_ALARM_LIMIT,
                controlsValue.getLowerAlarmLimit());
        sampleValue.setFloat(UDT_COLUMN_UPPER_ALARM_LIMIT,
                controlsValue.getUpperAlarmLimit());
        sampleValue.setFloat(UDT_COLUMN_LOWER_DISPLAY_LIMIT,
                controlsValue.getLowerDisplayLimit());
        sampleValue.setFloat(UDT_COLUMN_UPPER_DISPLAY_LIMIT,
                controlsValue.getUpperDisplayLimit());
        sampleValue.setFloat(UDT_COLUMN_LOWER_CONTROL_LIMIT,
                controlsValue.getLowerControlLimit());
        sampleValue.setFloat(UDT_COLUMN_UPPER_CONTROL_LIMIT,
                controlsValue.getUpperControlLimit());
        return Pair.of(sampleType, sampleValue);
    }

    private Pair<ChannelAccessSampleType, UDTValue> convertLongValue(
            ChannelAccessTimeLong timeValue,
            ChannelAccessControlsLong controlsValue) {
        ChannelAccessSampleType sampleType;
        UDTValue sampleValue;
        if (timeValue.getValueSize() == 1) {
            sampleType = ChannelAccessSampleType.SCALAR_LONG;
            sampleValue = scalarLong.newValue();
            sampleValue.setInt(UDT_COLUMN_VALUE, timeValue.getValue().get());
        } else {
            sampleType = ChannelAccessSampleType.ARRAY_LONG;
            sampleValue = arrayLong.newValue();
            IntBuffer intBuffer = timeValue.getValue();
            ByteBuffer byteBuffer = ByteBuffer
                    .allocate(intBuffer.remaining() * 4);
            byteBuffer.asIntBuffer().put(intBuffer);
            sampleValue.setBytes(UDT_COLUMN_VALUE, byteBuffer);
        }
        setAlarmColumns(sampleValue, timeValue);
        setUnitsColumn(sampleValue, controlsValue);
        sampleValue.setInt(UDT_COLUMN_LOWER_WARNING_LIMIT,
                controlsValue.getLowerWarningLimit());
        sampleValue.setInt(UDT_COLUMN_UPPER_WARNING_LIMIT,
                controlsValue.getUpperWarningLimit());
        sampleValue.setInt(UDT_COLUMN_LOWER_ALARM_LIMIT,
                controlsValue.getLowerAlarmLimit());
        sampleValue.setInt(UDT_COLUMN_UPPER_ALARM_LIMIT,
                controlsValue.getUpperAlarmLimit());
        sampleValue.setInt(UDT_COLUMN_LOWER_DISPLAY_LIMIT,
                controlsValue.getLowerDisplayLimit());
        sampleValue.setInt(UDT_COLUMN_UPPER_DISPLAY_LIMIT,
                controlsValue.getUpperDisplayLimit());
        sampleValue.setInt(UDT_COLUMN_LOWER_CONTROL_LIMIT,
                controlsValue.getLowerControlLimit());
        sampleValue.setInt(UDT_COLUMN_UPPER_CONTROL_LIMIT,
                controlsValue.getUpperControlLimit());
        return Pair.of(sampleType, sampleValue);
    }

    private Pair<ChannelAccessSampleType, UDTValue> convertShortValue(
            ChannelAccessTimeShort timeValue,
            ChannelAccessControlsShort controlsValue) {
        ChannelAccessSampleType sampleType;
        UDTValue sampleValue;
        if (timeValue.getValueSize() == 1) {
            sampleType = ChannelAccessSampleType.SCALAR_SHORT;
            sampleValue = scalarShort.newValue();
            sampleValue.setShort(UDT_COLUMN_VALUE, timeValue.getValue().get());
        } else {
            sampleType = ChannelAccessSampleType.ARRAY_SHORT;
            sampleValue = arrayShort.newValue();
            ShortBuffer shortBuffer = timeValue.getValue();
            ByteBuffer byteBuffer = ByteBuffer
                    .allocate(shortBuffer.remaining() * 2);
            byteBuffer.asShortBuffer().put(shortBuffer);
            sampleValue.setBytes(UDT_COLUMN_VALUE, byteBuffer);
        }
        setAlarmColumns(sampleValue, timeValue);
        setUnitsColumn(sampleValue, controlsValue);
        sampleValue.setShort(UDT_COLUMN_LOWER_WARNING_LIMIT,
                controlsValue.getLowerWarningLimit());
        sampleValue.setShort(UDT_COLUMN_UPPER_WARNING_LIMIT,
                controlsValue.getUpperWarningLimit());
        sampleValue.setShort(UDT_COLUMN_LOWER_ALARM_LIMIT,
                controlsValue.getLowerAlarmLimit());
        sampleValue.setShort(UDT_COLUMN_UPPER_ALARM_LIMIT,
                controlsValue.getUpperAlarmLimit());
        sampleValue.setShort(UDT_COLUMN_LOWER_DISPLAY_LIMIT,
                controlsValue.getLowerDisplayLimit());
        sampleValue.setShort(UDT_COLUMN_UPPER_DISPLAY_LIMIT,
                controlsValue.getUpperDisplayLimit());
        sampleValue.setShort(UDT_COLUMN_LOWER_CONTROL_LIMIT,
                controlsValue.getLowerControlLimit());
        sampleValue.setShort(UDT_COLUMN_UPPER_CONTROL_LIMIT,
                controlsValue.getUpperControlLimit());
        return Pair.of(sampleType, sampleValue);
    }

    private Pair<ChannelAccessSampleType, UDTValue> convertStringValue(
            ChannelAccessTimeString timeValue) {
        ChannelAccessSampleType sampleType;
        UDTValue sampleValue;
        if (timeValue.getValueSize() == 1) {
            sampleType = ChannelAccessSampleType.SCALAR_STRING;
            sampleValue = scalarString.newValue();
            sampleValue
                    .setString(UDT_COLUMN_VALUE, timeValue.getValue().get(0));
        } else {
            sampleType = ChannelAccessSampleType.ARRAY_STRING;
            sampleValue = arrayString.newValue();
            sampleValue.setBytes(UDT_COLUMN_VALUE,
                    ByteBuffer.wrap(timeValue.getRawValue()));
        }
        setAlarmColumns(sampleValue, timeValue);
        return Pair.of(sampleType, sampleValue);
    }

    private Pair<ChannelAccessSampleType, UDTValue> convertValue(
            ChannelAccessTimeValue<?> timeValue,
            ChannelAccessControlsValue<?> controlsValue) {
        // We switch based on the type of the value with time information and
        // simply expect the other value to be of the corresponding graphics
        // type. The code that creates the samples should ensure that this
        // expectation is never violated.
        switch (timeValue.getType()) {
        case DBR_TIME_CHAR:
            return convertCharValue((ChannelAccessTimeChar) timeValue,
                    (ChannelAccessControlsChar) controlsValue);
        case DBR_TIME_DOUBLE:
            return convertDoubleValue((ChannelAccessTimeDouble) timeValue,
                    (ChannelAccessControlsDouble) controlsValue);
        case DBR_TIME_ENUM:
            return convertEnumValue((ChannelAccessTimeEnum) timeValue,
                    (ChannelAccessGraphicsEnum) controlsValue);
        case DBR_TIME_FLOAT:
            return convertFloatValue((ChannelAccessTimeFloat) timeValue,
                    (ChannelAccessControlsFloat) controlsValue);
        case DBR_TIME_LONG:
            return convertLongValue((ChannelAccessTimeLong) timeValue,
                    (ChannelAccessControlsLong) controlsValue);
        case DBR_TIME_SHORT:
            return convertShortValue((ChannelAccessTimeShort) timeValue,
                    (ChannelAccessControlsShort) controlsValue);
        case DBR_TIME_STRING:
            return convertStringValue((ChannelAccessTimeString) timeValue);
        default:
            throw new RuntimeException("Unexpected type for time value: "
                    + timeValue.getType());
        }
    }

    private void createUserDefinedAggregatedScalarFloatingPointType(
            String udtName, DataType dataType) {
        createUserDefinedType(addLimitColumns(
                addPrecisionAndUnitsColumns(addAlarmColumns(addAggregatedScalarValueColumns(
                        SchemaBuilder.createType(udtName), dataType))),
                dataType));
    }

    private void createUserDefinedAggregatedScalarIntegerType(String udtName,
            DataType dataType) {
        createUserDefinedType(addLimitColumns(
                addUnitsColumn(addAlarmColumns(addAggregatedScalarValueColumns(
                        SchemaBuilder.createType(udtName), dataType))),
                dataType));
    }

    private void createUserDefinedArrayFloatingPointType(String udtName,
            DataType dataType) {
        createUserDefinedType(addLimitColumns(
                addPrecisionAndUnitsColumns(addAlarmColumns(addArrayValueColumn(SchemaBuilder
                        .createType(udtName)))), dataType));
    }

    private void createUserDefinedArrayIntegerType(String udtName,
            DataType dataType) {
        createUserDefinedType(addLimitColumns(
                addUnitsColumn(addAlarmColumns(addArrayValueColumn(SchemaBuilder
                        .createType(udtName)))), dataType));
    }

    private void createUserDefinedScalarFloatingPointType(String udtName,
            DataType dataType) {
        createUserDefinedType(addLimitColumns(
                addPrecisionAndUnitsColumns(addAlarmColumns(addScalarValueColumn(
                        SchemaBuilder.createType(udtName), dataType))),
                dataType));
    }

    private void createUserDefinedScalarIntegerType(String udtName,
            DataType dataType) {
        createUserDefinedType(addLimitColumns(
                addUnitsColumn(addAlarmColumns(addScalarValueColumn(
                        SchemaBuilder.createType(udtName), dataType))),
                dataType));
    }

    private void createUserDefinedType(CreateType createType) {
        session.execute(createType.ifNotExists());
    }

    private void createUserDefinedTypes() {
        // Char
        createUserDefinedScalarIntegerType(UDT_SCALAR_CHAR, DataType.tinyint());
        scalarChar = getUserType(UDT_SCALAR_CHAR);
        createUserDefinedArrayIntegerType(UDT_ARRAY_CHAR, DataType.tinyint());
        arrayChar = getUserType(UDT_ARRAY_CHAR);
        // Double
        createUserDefinedScalarFloatingPointType(UDT_SCALAR_DOUBLE,
                DataType.cdouble());
        scalarDouble = getUserType(UDT_SCALAR_DOUBLE);
        createUserDefinedArrayFloatingPointType(UDT_ARRAY_DOUBLE,
                DataType.cdouble());
        arrayDouble = getUserType(UDT_ARRAY_DOUBLE);
        // Enum
        createUserDefinedType(addLabelsColumn(addAlarmColumns(addScalarValueColumn(
                SchemaBuilder.createType(UDT_SCALAR_ENUM), DataType.smallint()))));
        scalarEnum = getUserType(UDT_SCALAR_ENUM);
        createUserDefinedType(addLabelsColumn(addAlarmColumns(addArrayValueColumn(SchemaBuilder
                .createType(UDT_ARRAY_ENUM)))));
        arrayEnum = getUserType(UDT_ARRAY_ENUM);
        // Float
        createUserDefinedScalarFloatingPointType(UDT_SCALAR_FLOAT,
                DataType.cfloat());
        scalarFloat = getUserType(UDT_SCALAR_FLOAT);
        createUserDefinedArrayFloatingPointType(UDT_ARRAY_FLOAT,
                DataType.cfloat());
        arrayFloat = getUserType(UDT_ARRAY_FLOAT);
        // Long
        createUserDefinedScalarIntegerType(UDT_SCALAR_LONG, DataType.cint());
        scalarLong = getUserType(UDT_SCALAR_LONG);
        createUserDefinedArrayIntegerType(UDT_ARRAY_LONG, DataType.cint());
        arrayLong = getUserType(UDT_ARRAY_LONG);
        // Short
        createUserDefinedScalarIntegerType(UDT_SCALAR_SHORT,
                DataType.smallint());
        scalarShort = getUserType(UDT_SCALAR_SHORT);
        createUserDefinedArrayIntegerType(UDT_ARRAY_SHORT, DataType.smallint());
        arrayShort = getUserType(UDT_ARRAY_SHORT);
        // String
        createUserDefinedType(addAlarmColumns(addScalarValueColumn(
                SchemaBuilder.createType(UDT_SCALAR_STRING), DataType.text())));
        scalarString = getUserType(UDT_SCALAR_STRING);
        createUserDefinedType(addAlarmColumns(addArrayValueColumn(SchemaBuilder
                .createType(UDT_ARRAY_STRING))));
        arrayString = getUserType(UDT_ARRAY_STRING);

        // Aggregated types. We only support scalar, numeric types.
        createUserDefinedAggregatedScalarIntegerType(
                UDT_AGGREGATED_SCALAR_CHAR, DataType.tinyint());
        aggregatedScalarChar = getUserType(UDT_AGGREGATED_SCALAR_CHAR);
        createUserDefinedAggregatedScalarFloatingPointType(
                UDT_AGGREGATED_SCALAR_DOUBLE, DataType.cdouble());
        aggregatedScalarDouble = getUserType(UDT_AGGREGATED_SCALAR_DOUBLE);
        createUserDefinedAggregatedScalarFloatingPointType(
                UDT_AGGREGATED_SCALAR_FLOAT, DataType.cfloat());
        aggregatedScalarFloat = getUserType(UDT_AGGREGATED_SCALAR_FLOAT);
        createUserDefinedAggregatedScalarIntegerType(
                UDT_AGGREGATED_SCALAR_LONG, DataType.cint());
        aggregatedScalarLong = getUserType(UDT_AGGREGATED_SCALAR_LONG);
        createUserDefinedAggregatedScalarIntegerType(
                UDT_AGGREGATED_SCALAR_SHORT, DataType.smallint());
        aggregatedScalarShort = getUserType(UDT_AGGREGATED_SCALAR_SHORT);
    }

    private ChannelAccessAlarmSeverity deserializeAlarmSeverity(UDTValue value) {
        return ChannelAccessAlarmSeverity.forSeverityCode(value
                .getShort(UDT_COLUMN_ALARM_SEVERITY));
    }

    private ChannelAccessAlarmStatus deserializeAlarmStatus(UDTValue value) {
        return ChannelAccessAlarmStatus.forAlarmStatusCode(value
                .getShort(UDT_COLUMN_ALARM_STATUS));
    }

    private ChannelAccessControlsChar deserializeCharValue(UDTValue value,
            boolean array) {
        byte[] valueArray;
        if (array) {
            valueArray = toByteArray(value.getBytes(UDT_COLUMN_VALUE));
        } else {
            valueArray = new byte[] { value.getByte(UDT_COLUMN_VALUE) };
        }
        return ChannelAccessValueFactory.createControlsChar(valueArray,
                deserializeAlarmStatus(value), deserializeAlarmSeverity(value),
                value.getString(UDT_COLUMN_UNITS),
                value.getByte(UDT_COLUMN_UPPER_DISPLAY_LIMIT),
                value.getByte(UDT_COLUMN_LOWER_DISPLAY_LIMIT),
                value.getByte(UDT_COLUMN_UPPER_ALARM_LIMIT),
                value.getByte(UDT_COLUMN_UPPER_WARNING_LIMIT),
                value.getByte(UDT_COLUMN_LOWER_WARNING_LIMIT),
                value.getByte(UDT_COLUMN_LOWER_ALARM_LIMIT),
                value.getByte(UDT_COLUMN_UPPER_CONTROL_LIMIT),
                value.getByte(UDT_COLUMN_LOWER_CONTROL_LIMIT), Charsets.UTF_8);
    }

    private ChannelAccessControlsDouble deserializeDoubleValue(UDTValue value,
            boolean array) {
        double[] valueArray;
        if (array) {
            valueArray = toDoubleArray(value.getBytes(UDT_COLUMN_VALUE)
                    .asDoubleBuffer());
        } else {
            valueArray = new double[] { value.getDouble(UDT_COLUMN_VALUE) };
        }
        return ChannelAccessValueFactory
                .createControlsDouble(valueArray,
                        deserializeAlarmStatus(value),
                        deserializeAlarmSeverity(value),
                        value.getShort(UDT_COLUMN_PRECISION),
                        value.getString(UDT_COLUMN_UNITS),
                        value.getDouble(UDT_COLUMN_UPPER_DISPLAY_LIMIT),
                        value.getDouble(UDT_COLUMN_LOWER_DISPLAY_LIMIT),
                        value.getDouble(UDT_COLUMN_UPPER_ALARM_LIMIT),
                        value.getDouble(UDT_COLUMN_UPPER_WARNING_LIMIT),
                        value.getDouble(UDT_COLUMN_LOWER_WARNING_LIMIT),
                        value.getDouble(UDT_COLUMN_LOWER_ALARM_LIMIT),
                        value.getDouble(UDT_COLUMN_UPPER_CONTROL_LIMIT),
                        value.getDouble(UDT_COLUMN_LOWER_CONTROL_LIMIT),
                        Charsets.UTF_8);
    }

    private ChannelAccessGraphicsEnum deserializeEnumValue(UDTValue value,
            boolean array) {
        short[] valueArray;
        if (array) {
            valueArray = toShortArray(value.getBytes(UDT_COLUMN_VALUE)
                    .asShortBuffer());
        } else {
            valueArray = new short[] { value.getShort(UDT_COLUMN_VALUE) };
        }
        return ChannelAccessValueFactory.createControlsEnum(valueArray,
                deserializeAlarmStatus(value), deserializeAlarmSeverity(value),
                value.getList(UDT_COLUMN_LABELS, String.class), Charsets.UTF_8);
    }

    private ChannelAccessControlsFloat deserializeFloatValue(UDTValue value,
            boolean array) {
        float[] valueArray;
        if (array) {
            valueArray = toFloatArray(value.getBytes(UDT_COLUMN_VALUE)
                    .asFloatBuffer());
        } else {
            valueArray = new float[] { value.getFloat(UDT_COLUMN_VALUE) };
        }
        return ChannelAccessValueFactory.createControlsFloat(valueArray,
                deserializeAlarmStatus(value), deserializeAlarmSeverity(value),
                value.getShort(UDT_COLUMN_PRECISION),
                value.getString(UDT_COLUMN_UNITS),
                value.getFloat(UDT_COLUMN_UPPER_DISPLAY_LIMIT),
                value.getFloat(UDT_COLUMN_LOWER_DISPLAY_LIMIT),
                value.getFloat(UDT_COLUMN_UPPER_ALARM_LIMIT),
                value.getFloat(UDT_COLUMN_UPPER_WARNING_LIMIT),
                value.getFloat(UDT_COLUMN_LOWER_WARNING_LIMIT),
                value.getFloat(UDT_COLUMN_LOWER_ALARM_LIMIT),
                value.getFloat(UDT_COLUMN_UPPER_CONTROL_LIMIT),
                value.getFloat(UDT_COLUMN_LOWER_CONTROL_LIMIT), Charsets.UTF_8);
    }

    private ChannelAccessControlsLong deserializeLongValue(UDTValue value,
            boolean array) {
        int[] valueArray;
        if (array) {
            valueArray = toIntArray(value.getBytes(UDT_COLUMN_VALUE)
                    .asIntBuffer());
        } else {
            valueArray = new int[] { value.getInt(UDT_COLUMN_VALUE) };
        }
        return ChannelAccessValueFactory.createControlsLong(valueArray,
                deserializeAlarmStatus(value), deserializeAlarmSeverity(value),
                value.getString(UDT_COLUMN_UNITS),
                value.getInt(UDT_COLUMN_UPPER_DISPLAY_LIMIT),
                value.getInt(UDT_COLUMN_LOWER_DISPLAY_LIMIT),
                value.getInt(UDT_COLUMN_UPPER_ALARM_LIMIT),
                value.getInt(UDT_COLUMN_UPPER_WARNING_LIMIT),
                value.getInt(UDT_COLUMN_LOWER_WARNING_LIMIT),
                value.getInt(UDT_COLUMN_LOWER_ALARM_LIMIT),
                value.getInt(UDT_COLUMN_UPPER_CONTROL_LIMIT),
                value.getInt(UDT_COLUMN_LOWER_CONTROL_LIMIT), Charsets.UTF_8);
    }

    private ChannelAccessControlsShort deserializeShortValue(UDTValue value,
            boolean array) {
        short[] valueArray;
        if (array) {
            valueArray = toShortArray(value.getBytes(UDT_COLUMN_VALUE)
                    .asShortBuffer());
        } else {
            valueArray = new short[] { value.getShort(UDT_COLUMN_VALUE) };
        }
        return ChannelAccessValueFactory.createControlsShort(valueArray,
                deserializeAlarmStatus(value), deserializeAlarmSeverity(value),
                value.getString(UDT_COLUMN_UNITS),
                value.getShort(UDT_COLUMN_UPPER_DISPLAY_LIMIT),
                value.getShort(UDT_COLUMN_LOWER_DISPLAY_LIMIT),
                value.getShort(UDT_COLUMN_UPPER_ALARM_LIMIT),
                value.getShort(UDT_COLUMN_UPPER_WARNING_LIMIT),
                value.getShort(UDT_COLUMN_LOWER_WARNING_LIMIT),
                value.getShort(UDT_COLUMN_LOWER_ALARM_LIMIT),
                value.getShort(UDT_COLUMN_UPPER_CONTROL_LIMIT),
                value.getShort(UDT_COLUMN_LOWER_CONTROL_LIMIT), Charsets.UTF_8);
    }

    private ChannelAccessAlarmOnlyString deserializeStringValue(UDTValue value,
            boolean array) {
        String[] valueArray;
        if (array) {
            valueArray = toStringArray(value.getBytes(UDT_COLUMN_VALUE));
        } else {
            valueArray = new String[] { value.getString(UDT_COLUMN_VALUE) };
        }
        return ChannelAccessValueFactory.createControlsString(
                Arrays.asList(valueArray), deserializeAlarmStatus(value),
                deserializeAlarmSeverity(value), Charsets.UTF_8);
    }

    private UserType getUserType(String udtName) {
        return session.getCluster().getMetadata()
                .getKeyspace(session.getLoggedKeyspace()).getUserType(udtName);
    }

    private void setAlarmColumns(UDTValue sampleValue,
            ChannelAccessAlarmValue<?> alarmValue) {
        sampleValue.setShort(UDT_COLUMN_ALARM_SEVERITY, alarmValue
                .getAlarmSeverity().toSeverityCode());
        sampleValue.setShort(UDT_COLUMN_ALARM_STATUS, alarmValue
                .getAlarmStatus().toAlarmStatusCode());
    }

    private void setUnitsColumn(UDTValue sampleValue,
            ChannelAccessNumericGraphicsValue<?> graphicsValue) {
        sampleValue.setString(UDT_COLUMN_UNITS, graphicsValue.getUnits());
    }

    private void setPrecisionAndUnitsColumns(UDTValue sampleValue,
            ChannelAccessFloatingPointGraphicsValue<?> graphicsValue) {
        sampleValue
                .setShort(UDT_COLUMN_PRECISION, graphicsValue.getPrecision());
        setUnitsColumn(sampleValue, graphicsValue);
    }

}
