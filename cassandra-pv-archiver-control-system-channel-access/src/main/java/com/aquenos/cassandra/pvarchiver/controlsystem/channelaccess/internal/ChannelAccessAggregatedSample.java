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
 * Aggregated Channel Access sample. Such a sample is built by aggregating raw
 * or aggregated samples through the {@link ChannelAccessSampleDecimator}.
 * </p>
 * 
 * <p>
 * This class is only intended for use by
 * {@link ChannelAccessControlSystemSupport} and its associated classes.
 * </p>
 * 
 * @author Sebastian Marsching
 */
public class ChannelAccessAggregatedSample extends ChannelAccessUdtSample {

    private static boolean isSupportedType(ChannelAccessSampleType type) {
        switch (type) {
        case AGGREGATED_SCALAR_CHAR:
        case AGGREGATED_SCALAR_DOUBLE:
        case AGGREGATED_SCALAR_FLOAT:
        case AGGREGATED_SCALAR_LONG:
        case AGGREGATED_SCALAR_SHORT:
            return true;
        default:
            return false;
        }
    }

    /**
     * Creates an aggregated sample. This constructor is intended for
     * {@link ChannelAccessDatabaseAccess} and
     * {@link ChannelAccessSampleValueAccess}. Other classes should use these
     * utility classes for creating instances of this class.
     * 
     * @param timeStamp
     *            sample's time-stamp. The time-stamp is specified as the number
     *            of nanoseconds since epoch (January 1st, 1970, 00:00:00 UTC).
     * @param type
     *            sample's type. This must be one of the
     *            <code>AGGREGATED_</code> types. The <code>SCALAR_</code> and
     *            <code>ARRAY_</code> types are not supported by this class.
     * @param value
     *            sample's value. The value's type must match the specified
     *            <code>type</code>.
     */
    ChannelAccessAggregatedSample(long timeStamp, ChannelAccessSampleType type,
            UDTValue value) {
        super(timeStamp, type, value, false);
        // This constructor is only used internally, so we use assertions
        // instead of preconditions.
        assert (isSupportedType(type));
    }

}
