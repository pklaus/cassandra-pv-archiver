/*
 * Copyright 2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.controlsystem.channelaccess.internal;

import com.aquenos.cassandra.pvarchiver.controlsystem.channelaccess.ChannelAccessControlSystemSupport;
import com.aquenos.cassandra.pvarchiver.controlsystem.channelaccess.ChannelAccessSampleType;
import com.datastax.driver.core.UDTValue;

/**
 * <p>
 * Raw Channel Access sample. Such a sample is built from the data that has been
 * received over the network.
 * </p>
 * 
 * <p>
 * This class is only intended for use by
 * {@link ChannelAccessControlSystemSupport} and its associated classes.
 * </p>
 * 
 * @author Sebastian Marsching
 */
public class ChannelAccessRawSample extends ChannelAccessUdtSample {

    private static boolean isSupportedType(ChannelAccessSampleType type) {
        switch (type) {
        case ARRAY_CHAR:
        case ARRAY_DOUBLE:
        case ARRAY_ENUM:
        case ARRAY_FLOAT:
        case ARRAY_LONG:
        case ARRAY_SHORT:
        case ARRAY_STRING:
        case SCALAR_CHAR:
        case SCALAR_DOUBLE:
        case SCALAR_ENUM:
        case SCALAR_FLOAT:
        case SCALAR_LONG:
        case SCALAR_SHORT:
        case SCALAR_STRING:
            return true;
        default:
            return false;
        }
    }

    /**
     * Creates a raw sample. This constructor is intended for
     * {@link ChannelAccessDatabaseAccess} and
     * {@link ChannelAccessSampleValueAccess}. Other classes should use these
     * utility classes for creating instances of this class.
     * 
     * @param timeStamp
     *            sample's time-stamp. The time-stamp is specified as the number
     *            of nanoseconds since epoch (January 1st, 1970, 00:00:00 UTC).
     * @param type
     *            sample's type. This must be one of the <code>SCALAR_</code> or
     *            <code>ARRAY_</code> types. The <code>AGGREGATED_</code> types
     *            are not supported by this class.
     * @param value
     *            sample's value. The value's type must match the specified
     *            <code>type</code>.
     * @param originalSample
     *            <code>true</code> if this sample is an original sample that
     *            has been received over the network, <code>false</code> if it
     *            the result of a decimation process.
     */
    ChannelAccessRawSample(long timeStamp, ChannelAccessSampleType type,
            UDTValue value, boolean originalSample) {
        super(timeStamp, type, value, originalSample);
        // This constructor is only used internally, so we use assertions
        // instead of preconditions.
        assert (isSupportedType(type));
    }

}
