/*
 * Copyright 2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.controlsystem.channelaccess;

/**
 * Channel Access sample type. Each sample stored in the database is of one of
 * these types.
 * 
 * @author Sebastian Marsching
 */
public enum ChannelAccessSampleType {

    /**
     * Aggregated scalar character. Such an aggregated sample is created when
     * aggregating multiple {@link #SCALAR_CHAR} samples.
     */
    AGGREGATED_SCALAR_CHAR,

    /**
     * Aggregated scalar double. Such an aggregated sample is created when
     * aggregating multiple {@link #SCALAR_DOUBLE} samples.
     */
    AGGREGATED_SCALAR_DOUBLE,

    /**
     * Aggregated scalar float. Such an aggregated sample is created when
     * aggregating multiple {@link #SCALAR_FLOAT} samples.
     */
    AGGREGATED_SCALAR_FLOAT,

    /**
     * Aggregated scalar long. Such an aggregated sample is created when
     * aggregating multiple {@link #SCALAR_LONG} samples.
     */
    AGGREGATED_SCALAR_LONG,

    /**
     * Aggregated scalar short. Such an aggregated sample is created when
     * aggregating multiple {@link #SCALAR_SHORT} samples.
     */
    AGGREGATED_SCALAR_SHORT,

    /**
     * Character (8-bit integer) array.
     */
    ARRAY_CHAR,

    /**
     * Double (64-bit floating-point number) array.
     */
    ARRAY_DOUBLE,

    /**
     * Enum array.
     */
    ARRAY_ENUM,

    /**
     * Float (32-bit floating-point number) array.
     */
    ARRAY_FLOAT,

    /**
     * Long (32-bit integer) array.
     */
    ARRAY_LONG,

    /**
     * Short (16-bit integer) array.
     */
    ARRAY_SHORT,

    /**
     * String (each 40 bytes at maximum) array.
     */
    ARRAY_STRING,

    /**
     * Indicates that the channel was disabled at the time.
     */
    DISABLED,

    /**
     * Indicates that the channel was disconnected at the time.
     */
    DISCONNECTED,

    /**
     * Scalar char (8-bit integer).
     */
    SCALAR_CHAR,

    /**
     * Scalar double (64-bit floating-point number).
     */
    SCALAR_DOUBLE,

    /**
     * Scalar enum.
     */
    SCALAR_ENUM,

    /**
     * Scalar float (32-bit floating-point number).
     */
    SCALAR_FLOAT,

    /**
     * Scalar long (32-bit integer).
     */
    SCALAR_LONG,

    /**
     * Scalar short (16-bit integer).
     */
    SCALAR_SHORT,

    /**
     * Scalar (single-element) string (40 bytes at maximum).
     */
    SCALAR_STRING;

}
