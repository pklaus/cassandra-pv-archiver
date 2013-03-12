/*
 * Copyright 2013 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.csstudio.archive.cassandra.internal;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import org.csstudio.data.values.IDoubleValue;
import org.csstudio.data.values.IEnumeratedMetaData;
import org.csstudio.data.values.IEnumeratedValue;
import org.csstudio.data.values.ILongValue;
import org.csstudio.data.values.IMetaData;
import org.csstudio.data.values.IMinMaxDoubleValue;
import org.csstudio.data.values.INumericMetaData;
import org.csstudio.data.values.ISeverity;
import org.csstudio.data.values.IStringValue;
import org.csstudio.data.values.ITimestamp;
import org.csstudio.data.values.IValue;
import org.csstudio.data.values.ValueFactory;

/**
 * Serializer and deserializer for {@link IValue} objects. This serializer
 * cannot be directly used with the Astyanax library, because it requires
 * information that is stored in the column name and not duplicated in the
 * column value.
 * 
 * @author Sebastian Marsching
 */
public abstract class ValueSerializer {

    private final static Charset CHARSET_UTF8 = Charset.forName("UTF-8");

    private final static byte VERSION_NUMBER = 0x02;

    private final static byte TYPE_DOUBLE = 0x01;
    private final static byte TYPE_MIN_MAX_DOUBLE = 0x02;
    private final static byte TYPE_ENUM = 0x03;
    private final static byte TYPE_LONG = 0x04;
    private final static byte TYPE_STRING = 0x05;
    private final static byte VALUE_TYPE_BYTE = 0x00;
    private final static byte VALUE_TYPE_SHORT = 0x01;
    private final static byte VALUE_TYPE_INT = 0x02;
    private final static byte VALUE_TYPE_LONG = 0x03;

    public static ByteBuffer toByteBuffer(IValue value) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
        DataOutputStream dos = new DataOutputStream(baos);
        try {
            // First we write the version number. We do this so that we can
            // change the format later and still be compatible with existing
            // data.
            dos.writeByte(VERSION_NUMBER);
            // Next we write the info byte. The info byte stores the type in
            // bits 0 to 2 (least significant bits) and the severity in bits 4
            // to 6.
            byte infoByte = 0x00;
            byte severity = SeverityMapper
                    .severityToNumber(value.getSeverity());
            infoByte |= severity << 4;
            if (value instanceof IDoubleValue) {
                if (value instanceof IMinMaxDoubleValue) {
                    infoByte |= TYPE_MIN_MAX_DOUBLE;
                } else {
                    infoByte |= TYPE_DOUBLE;
                }
            } else if (value instanceof IEnumeratedValue) {
                infoByte |= TYPE_ENUM;
            } else if (value instanceof ILongValue) {
                infoByte |= TYPE_LONG;
            } else if (value instanceof IStringValue) {
                infoByte |= TYPE_STRING;
            } else {
                throw new IllegalArgumentException("Value of unknown type "
                        + value.getClass().getName() + " is not supported.");
            }
            dos.writeByte(infoByte);
            // Now we write a second info-byte. This byte stores a flag in bit
            // 0, indicating whether meta-data is present. Bit 1 stores the type
            // of meta-data. Bit 2 stores a flag indicating whether a status
            // string is present.
            byte infoByte2 = 0x00;
            IMetaData metaData = value.getMetaData();
            if (metaData != null) {
                infoByte2 |= 0x01;
                if (metaData instanceof IEnumeratedMetaData) {
                    // We encode this as zero.
                } else if (metaData instanceof INumericMetaData) {
                    infoByte2 |= 0x02;
                } else {
                    throw new IllegalArgumentException(
                            "Unsupported meta-data type: "
                                    + metaData.getClass().getName());
                }
            }
            String status = value.getStatus();
            if (status != null) {
                infoByte2 |= 0x04;
            }
            dos.writeByte(infoByte2);
            // If we have meta-data, we write it now.
            if (metaData != null) {
                if (metaData instanceof IEnumeratedMetaData) {
                    IEnumeratedMetaData enumMetaData = (IEnumeratedMetaData) metaData;
                    String[] states = enumMetaData.getStates();
                    writeStringArray(dos, states);
                } else if (metaData instanceof INumericMetaData) {
                    INumericMetaData numMetaData = (INumericMetaData) metaData;
                    dos.writeDouble(numMetaData.getAlarmHigh());
                    dos.writeDouble(numMetaData.getAlarmLow());
                    dos.writeDouble(numMetaData.getDisplayHigh());
                    dos.writeDouble(numMetaData.getDisplayLow());
                    dos.writeDouble(numMetaData.getWarnHigh());
                    dos.writeDouble(numMetaData.getWarnLow());
                    dos.writeInt(numMetaData.getPrecision());
                    writeString(dos, numMetaData.getUnits());
                } else {
                    throw new IllegalArgumentException(
                            "Unsupported meta-data type: "
                                    + metaData.getClass().getName());
                }
            }
            // If we have a status string, we write it now.
            if (status != null) {
                writeString(dos, status);
            }
            // Now we store the part of the value which depends on the type.
            if (value instanceof IDoubleValue) {
                if (value instanceof IMinMaxDoubleValue) {
                    IMinMaxDoubleValue specificValue = (IMinMaxDoubleValue) value;
                    double[] values = specificValue.getValues();
                    writeDoubleArray(dos, values);
                    dos.writeDouble(specificValue.getMinimum());
                    dos.writeDouble(specificValue.getMaximum());
                } else {
                    IDoubleValue specificValue = (IDoubleValue) value;
                    double[] values = specificValue.getValues();
                    writeDoubleArray(dos, values);
                }
            } else if (value instanceof IEnumeratedValue) {
                IEnumeratedValue specificValue = (IEnumeratedValue) value;
                int[] values = specificValue.getValues();
                writeIntArray(dos, values);
            } else if (value instanceof ILongValue) {
                ILongValue specificValue = (ILongValue) value;
                long[] values = specificValue.getValues();
                writeLongArray(dos, values);
            } else if (value instanceof IStringValue) {
                IStringValue specificValue = (IStringValue) value;
                String[] values = specificValue.getValues();
                writeStringArray(dos, values);
            } else {
                throw new IllegalArgumentException("Value of unknown type "
                        + value.getClass().getName() + " is not supported.");
            }
            dos.flush();
        } catch (IOException e) {
            throw new RuntimeException(
                    "Unexpected IOException while trying to serialize value.",
                    e);
        }
        return ByteBuffer.wrap(baos.toByteArray());
    }

    public static IValue fromByteBuffer(ByteBuffer byteBuffer,
            ITimestamp timestamp, IValue.Quality quality) {
        ByteArrayInputStream bais = new ByteArrayInputStream(
                byteArrayForBuffer(byteBuffer));
        DataInputStream dis = new DataInputStream(bais);
        try {
            // First we check that the data format has the expected version.
            byte versionByte = dis.readByte();
            if (versionByte != VERSION_NUMBER && versionByte != 0x01) {
                throw new IOException("Expected version number "
                        + VERSION_NUMBER + " but got version number "
                        + versionByte + ".");
            }
            // Next we decode the first info byte which contains the value type
            // and severity.
            byte infoByte = dis.readByte();
            byte valueType = (byte) (infoByte & 0x07);
            byte severityByte = (byte) ((infoByte & 0x70) >> 4);
            ISeverity severity = SeverityMapper.numberToSeverity(severityByte);
            // Now we read the second info byte which stores information about
            // the meta-data and status.
            byte infoByte2 = dis.readByte();
            // Read the meta-data if available.
            INumericMetaData numericMetaData = null;
            IEnumeratedMetaData enumeratedMetaData = null;
            if ((infoByte2 & 0x01) != 0) {
                if ((infoByte2 & 0x02) != 0) {
                    double alarmHigh = dis.readDouble();
                    double alarmLow = dis.readDouble();
                    double displayHigh = dis.readDouble();
                    double displayLow = dis.readDouble();
                    double warnHigh = dis.readDouble();
                    double warnLow = dis.readDouble();
                    int precision = dis.readInt();
                    String units = readString(dis);
                    numericMetaData = ValueFactory.createNumericMetaData(
                            displayLow, displayHigh, warnLow, warnHigh,
                            alarmLow, alarmHigh, precision, units);
                } else {
                    String[] states = readStringArray(dis);
                    enumeratedMetaData = ValueFactory
                            .createEnumeratedMetaData(states);
                }
            }
            // Read status if available.
            String status = null;
            if ((infoByte2 & 0x04) != 0) {
                status = readString(dis);
            }

            switch (valueType) {
            case TYPE_DOUBLE: {
                double[] values = readDoubleArray(dis);
                return ValueFactory.createDoubleValue(timestamp, severity,
                        status, numericMetaData, quality, values);
            }
            case TYPE_MIN_MAX_DOUBLE: {
                double[] values = readDoubleArray(dis);
                double minValue = dis.readDouble();
                double maxValue = dis.readDouble();
                return ValueFactory.createMinMaxDoubleValue(timestamp,
                        severity, status, numericMetaData, quality, values,
                        minValue, maxValue);
            }
            case TYPE_ENUM: {
                int[] values = readIntArray(dis, versionByte);
                return ValueFactory.createEnumeratedValue(timestamp, severity,
                        status, enumeratedMetaData, quality, values);
            }
            case TYPE_LONG: {
                long[] values = readLongArray(dis, versionByte);
                return ValueFactory.createLongValue(timestamp, severity,
                        status, numericMetaData, quality, values);
            }
            case TYPE_STRING: {
                String[] values = readStringArray(dis);
                return ValueFactory.createStringValue(timestamp, severity,
                        status, quality, values);
            }
            default:
                throw new IOException("Read unexpected value type " + valueType
                        + ".");
            }
        } catch (IOException e) {
            throw new RuntimeException(
                    "Error while trying to deserialize value: "
                            + e.getMessage(), e);
        }
    }

    private static byte[] byteArrayForBuffer(ByteBuffer byteBuffer) {
        byte[] bytes = new byte[byteBuffer.remaining()];
        byteBuffer.get(bytes);
        return bytes;
    }

    private static void writeStringArray(DataOutputStream dos, String[] strings)
            throws IOException {
        writeSmallPositiveInt(dos, strings.length);
        for (String s : strings) {
            writeString(dos, s);
        }
    }

    private static String[] readStringArray(DataInputStream dis)
            throws IOException {
        int length = readSmallPositiveInt(dis);
        String[] strings = new String[length];
        for (int i = 0; i < length; i++) {
            strings[i] = readString(dis);
        }
        return strings;
    }

    private static void writeString(DataOutputStream dos, String string)
            throws IOException {
        byte[] stringBytes = string.getBytes(CHARSET_UTF8);
        writeSmallPositiveInt(dos, stringBytes.length);
        dos.write(stringBytes);
    }

    private static String readString(DataInputStream dis) throws IOException {
        int numberOfBytes = readSmallPositiveInt(dis);
        byte[] stringBytes = new byte[numberOfBytes];
        dis.readFully(stringBytes);
        return new String(stringBytes, CHARSET_UTF8);
    }

    private static void writeDoubleArray(DataOutputStream dos, double[] doubles)
            throws IOException {
        writeSmallPositiveInt(dos, doubles.length);
        for (double d : doubles) {
            dos.writeDouble(d);
        }
    }

    private static double[] readDoubleArray(DataInputStream dis)
            throws IOException {
        int numberOfElements = readSmallPositiveInt(dis);
        double[] values = new double[numberOfElements];
        for (int i = 0; i < numberOfElements; i++) {
            values[i] = dis.readDouble();
        }
        return values;
    }

    private static void writeLongArray(DataOutputStream dos, long[] longs)
            throws IOException {
        writeSmallPositiveInt(dos, longs.length);
        long min = 0;
        long max = 0;
        for (long l : longs) {
            min = Math.min(l, min);
            max = Math.max(l, max);
        }
        if (min >= Byte.MIN_VALUE && max <= Byte.MAX_VALUE) {
            dos.writeByte(VALUE_TYPE_BYTE);
            for (long l : longs) {
                dos.writeByte((byte) l);
            }
        } else if (min >= Short.MIN_VALUE && max <= Short.MAX_VALUE) {
            dos.writeByte(VALUE_TYPE_SHORT);
            for (long l : longs) {
                dos.writeShort((short) l);
            }
        } else if (min <= Integer.MIN_VALUE && max <= Integer.MAX_VALUE) {
            dos.writeByte(VALUE_TYPE_INT);
            for (long l : longs) {
                dos.writeInt((int) l);
            }
        } else {
            dos.writeByte(VALUE_TYPE_LONG);
            for (long l : longs) {
                dos.writeLong(l);
            }
        }
    }

    private static long[] readLongArray(DataInputStream dis, byte version)
            throws IOException {
        int numberOfElements = readSmallPositiveInt(dis);
        long[] values = new long[numberOfElements];
        byte valueType;
        if (version == 0x01) {
            valueType = VALUE_TYPE_LONG;
        } else {
            valueType = dis.readByte();
        }
        switch (valueType) {
        case VALUE_TYPE_BYTE:
            for (int i = 0; i < numberOfElements; i++) {
                values[i] = dis.readByte();
            }
            break;
        case VALUE_TYPE_SHORT:
            for (int i = 0; i < numberOfElements; i++) {
                values[i] = dis.readShort();
            }
            break;
        case VALUE_TYPE_INT:
            for (int i = 0; i < numberOfElements; i++) {
                values[i] = dis.readInt();
            }
            break;
        case VALUE_TYPE_LONG:
            for (int i = 0; i < numberOfElements; i++) {
                values[i] = dis.readLong();
            }
            break;
        default:
            throw new IOException("Found unsupported value type " + valueType
                    + ".");
        }
        return values;
    }

    private static void writeIntArray(DataOutputStream dos, int[] ints)
            throws IOException {
        writeSmallPositiveInt(dos, ints.length);
        int min = 0;
        int max = 0;
        for (int i : ints) {
            min = Math.min(i, min);
            max = Math.max(i, max);
        }
        if (min >= Byte.MIN_VALUE && max <= Byte.MAX_VALUE) {
            dos.writeByte(VALUE_TYPE_BYTE);
            for (int i : ints) {
                dos.writeByte((byte) i);
            }
        } else if (min >= Short.MIN_VALUE && max <= Short.MAX_VALUE) {
            dos.writeByte(VALUE_TYPE_SHORT);
            for (int i : ints) {
                dos.writeShort((short) i);
            }
        } else {
            dos.writeByte(VALUE_TYPE_INT);
            for (int i : ints) {
                dos.writeInt(i);
            }
        }
    }

    private static int[] readIntArray(DataInputStream dis, byte version)
            throws IOException {
        int numberOfElements = readSmallPositiveInt(dis);
        int[] values = new int[numberOfElements];
        byte valueType;
        if (version == 0x01) {
            valueType = VALUE_TYPE_INT;
        } else {
            valueType = dis.readByte();
        }
        switch (valueType) {
        case VALUE_TYPE_BYTE:
            for (int i = 0; i < numberOfElements; i++) {
                values[i] = dis.readByte();
            }
            break;
        case VALUE_TYPE_SHORT:
            for (int i = 0; i < numberOfElements; i++) {
                values[i] = dis.readShort();
            }
            break;
        case VALUE_TYPE_INT:
            for (int i = 0; i < numberOfElements; i++) {
                values[i] = dis.readInt();
            }
            break;
        default:
            throw new IOException("Found unsupported value type " + valueType
                    + ".");
        }
        return values;
    }

    private static void writeSmallPositiveInt(DataOutputStream dos, int number)
            throws IOException {
        // For an integer that is usually small but can be big, it is very
        // space inefficient to write all four bytes. Therefore we use the
        // most significant bit of the first to indicate whether the number is
        // greater than 127. If the number is actually greater, we use three
        // additional bytes for the rest of the number. Negative numbers are
        // not supported because we use the most significant bit for the
        // above-mentioned flag.
        if (number < 0) {
            throw new IllegalArgumentException(
                    "Negative numbers are not supported.");
        }
        if (number < 128) {
            dos.writeByte(number);
        } else {
            dos.writeInt(number |= 0x80000000);
        }
    }

    private static int readSmallPositiveInt(DataInputStream dis)
            throws IOException {
        // Read a positive integer that has been written by
        // writeSmallPositiveInt.
        byte firstByte = dis.readByte();
        if ((firstByte & 0x80) != 0) {
            firstByte &= 0x7F;
            byte secondByte = dis.readByte();
            byte thirdByte = dis.readByte();
            byte fourthByte = dis.readByte();
            return ((firstByte & 0xFF) << 24) + ((secondByte & 0xFF) << 16)
                    + ((thirdByte & 0xFF) << 8) + (fourthByte & 0xFF);
        } else {
            return firstByte;
        }
    }

}
