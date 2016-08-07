/*
 * Copyright 2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.controlsystem.channelaccess.internal;

import java.nio.charset.Charset;

import com.aquenos.cassandra.pvarchiver.controlsystem.channelaccess.ChannelAccessControlSystemSupport;
import com.aquenos.cassandra.pvarchiver.controlsystem.channelaccess.ChannelAccessSampleType;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType.Field;

/**
 * <p>
 * Size estimator for stored samples. This estimator calculates the size that a
 * sample will take in the SSTable when being stored (before applying SSTable
 * compression). This number is important, because the column-family row that
 * stores the samples for a sample bucket (the partition in CQL terms) should
 * neither be too wide nor too narrow. This means that we need a good estimation
 * of the size that a column-family row currently has, so that we can decide
 * when to start a new sample bucket.
 * </p>
 * 
 * <p>
 * This class is intended for use by {@link ChannelAccessControlSystemSupport}
 * and its associated classes only.
 * </p>
 * 
 * @author Sebastian Marsching
 */
public final class SampleSizeEstimator {

    private static final Charset UTF8 = Charset.forName("UTF-8");

    private static final int BIGINT_COLUMN = 8;
    private static final int BOOL_COLUMN = 1;
    private static final int CLUSTERING_COLUMN_OVERHEAD = 3;
    private static final int COLUMN_OVERHEAD = 18;
    private static final int CQL_ROW_OVERHEAD = 18;
    private static final int DOUBLE_COLUMN = 8;
    private static final int FLOAT_COLUMN = 4;
    private static final int INT_COLUMN = 4;
    private static final int SMALLINT_COLUMN = 2;
    private static final int TINYINT_COLUMN = 1;
    private static final int DISABLED_COLUMN = COLUMN_OVERHEAD
            + ChannelAccessDatabaseAccess.COLUMN_DISABLED.getBytes(UTF8).length
            + BOOL_COLUMN;
    private static final int DISCONNECTED_COLUMN = COLUMN_OVERHEAD
            + ChannelAccessDatabaseAccess.COLUMN_DISCONNECTED.getBytes(UTF8).length
            + BOOL_COLUMN;
    private static final int SAMPLE_TIME_COLUMN = CLUSTERING_COLUMN_OVERHEAD
            + BIGINT_COLUMN;
    private static final int TUPLE_FIELD_OVERHEAD = 4;
    private static final int SAMPLE_ROW_BASE = CQL_ROW_OVERHEAD
            + SAMPLE_TIME_COLUMN;
    private static final int DISABLED_SAMPLE_ROW = SAMPLE_ROW_BASE
            + DISABLED_COLUMN;
    private static final int DISCONNECTED_SAMPLE_ROW = SAMPLE_ROW_BASE
            + DISCONNECTED_COLUMN;

    private static int estimateUdtValueSize(UDTValue value) {
        int i = 0;
        int size = 0;
        for (Field field : value.getType()) {
            size += TUPLE_FIELD_OVERHEAD;
            // We first check whether the field is one of the well-known,
            // fix-sized types. In this case, we can simply use the known size.
            // We only access the byte buffer for getting the size when we
            // cannot deduce the value size from the type because this
            // internally involves duplicating the byte buffer which is
            // something we want to avoid.
            switch (field.getType().getName()) {
            case BIGINT:
                size += BIGINT_COLUMN;
                break;
            case BOOLEAN:
                size += BOOL_COLUMN;
                break;
            case DOUBLE:
                size += DOUBLE_COLUMN;
                break;
            case FLOAT:
                size += FLOAT_COLUMN;
                break;
            case INT:
                size += INT_COLUMN;
                break;
            case SMALLINT:
                size += SMALLINT_COLUMN;
                break;
            case TINYINT:
                size += TINYINT_COLUMN;
                break;
            default:
                size += value.getBytesUnsafe(i).remaining();
                break;
            }
            ++i;
        }
        return size;
    }

    private static String typeToColumnName(ChannelAccessSampleType type) {
        switch (type) {
        case AGGREGATED_SCALAR_CHAR:
            return ChannelAccessDatabaseAccess.COLUMN_AGGREGATED_SCALAR_CHAR;
        case AGGREGATED_SCALAR_DOUBLE:
            return ChannelAccessDatabaseAccess.COLUMN_AGGREGATED_SCALAR_DOUBLE;
        case AGGREGATED_SCALAR_FLOAT:
            return ChannelAccessDatabaseAccess.COLUMN_AGGREGATED_SCALAR_FLOAT;
        case AGGREGATED_SCALAR_LONG:
            return ChannelAccessDatabaseAccess.COLUMN_AGGREGATED_SCALAR_LONG;
        case AGGREGATED_SCALAR_SHORT:
            return ChannelAccessDatabaseAccess.COLUMN_AGGREGATED_SCALAR_SHORT;
        case ARRAY_CHAR:
            return ChannelAccessDatabaseAccess.COLUMN_ARRAY_CHAR;
        case ARRAY_DOUBLE:
            return ChannelAccessDatabaseAccess.COLUMN_ARRAY_DOUBLE;
        case ARRAY_ENUM:
            return ChannelAccessDatabaseAccess.COLUMN_ARRAY_ENUM;
        case ARRAY_FLOAT:
            return ChannelAccessDatabaseAccess.COLUMN_ARRAY_FLOAT;
        case ARRAY_LONG:
            return ChannelAccessDatabaseAccess.COLUMN_ARRAY_LONG;
        case ARRAY_SHORT:
            return ChannelAccessDatabaseAccess.COLUMN_ARRAY_SHORT;
        case ARRAY_STRING:
            return ChannelAccessDatabaseAccess.COLUMN_ARRAY_STRING;
        case SCALAR_CHAR:
            return ChannelAccessDatabaseAccess.COLUMN_SCALAR_CHAR;
        case SCALAR_DOUBLE:
            return ChannelAccessDatabaseAccess.COLUMN_SCALAR_DOUBLE;
        case SCALAR_ENUM:
            return ChannelAccessDatabaseAccess.COLUMN_SCALAR_ENUM;
        case SCALAR_FLOAT:
            return ChannelAccessDatabaseAccess.COLUMN_SCALAR_FLOAT;
        case SCALAR_LONG:
            return ChannelAccessDatabaseAccess.COLUMN_SCALAR_LONG;
        case SCALAR_SHORT:
            return ChannelAccessDatabaseAccess.COLUMN_SCALAR_SHORT;
        case SCALAR_STRING:
            return ChannelAccessDatabaseAccess.COLUMN_SCALAR_STRING;
        default:
            throw new RuntimeException("Unhandled sample type: " + type);
        }
    }

    private SampleSizeEstimator() {
    }

    /**
     * Estimates the size of the specified aggregated sample. The returned
     * number is the number of bytes that the sample will occupy when being
     * stored in the Cassandra database. This size includes the overhead that is
     * needed for the columns (e.g. data-structures storing the column names,
     * etc.) and the row key (time stamp).
     * 
     * @param sample
     *            aggregated sample for which the storage size shall be
     *            estimated.
     * @return estimated size of the serialized sample in the Cassandra data
     *         store (in bytes).
     * @throws NullPointerException
     *             if <code>sample</code> is <code>null</code>.
     */
    public static int estimateAggregatedSampleSize(
            ChannelAccessAggregatedSample sample) {
        return SAMPLE_ROW_BASE + COLUMN_OVERHEAD
                + typeToColumnName(sample.getType()).length()
                + estimateUdtValueSize(sample.getValue());
    }

    /**
     * Estimates the size of a "disabled" sample. The size includes the overhead
     * that is needed for the columns (e.g. data-structures storing the column
     * name, etc.). As the size of a disabled sample is constant (such a sample
     * does not have any variably sized data-structures), this method returns a
     * constant.
     * 
     * @return estimated size of the serialized sample in the Cassandra data
     *         store (in bytes).
     */
    public static int estimateDisabledSampleSize() {
        return DISABLED_SAMPLE_ROW;
    }

    /**
     * Estimates the size of a "disconnected" sample. The size includes the
     * overhead that is needed for the columns (e.g. data-structures storing the
     * column name, etc.). As the size of a disconnected sample is constant
     * (such a sample does not have any variably sized data-structures), this
     * method returns a constant.
     * 
     * @return estimated size of the serialized sample in the Cassandra data
     *         store (in bytes).
     */
    public static int estimateDisconnectedSampleSize() {
        return DISCONNECTED_SAMPLE_ROW;
    }

    /**
     * Estimates the size of the specified raw sample. The returned number is
     * the number of bytes that the sample will occupy when being stored in the
     * Cassandra database. This size includes the overhead that is needed for
     * the columns (e.g. data-structures storing the column names, etc.) and the
     * row key (time stamp).
     * 
     * @param sample
     *            raw sample for which the storage size shall be estimated.
     * @return estimated size of the serialized sample in the Cassandra data
     *         store (in bytes).
     * @throws NullPointerException
     *             if <code>sample</code> is <code>null</code>.
     */
    public static int estimateRawSampleSize(ChannelAccessRawSample sample) {
        return SAMPLE_ROW_BASE + COLUMN_OVERHEAD
                + typeToColumnName(sample.getType()).length()
                + estimateUdtValueSize(sample.getValue());
    }

}
