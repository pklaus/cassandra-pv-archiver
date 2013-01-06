/*
 * Copyright 2012-2013 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.csstudio.archive.cassandra;

import org.csstudio.data.values.IValue;

/**
 * Represents a sample stored in the archive.
 * 
 * @author Sebastian Marsching
 * @see SampleStore
 */
public class Sample {
    private long compressionPeriod;
    private String channelName;
    private IValue value;

    /**
     * Constructor. Creates a sample using the specified parameters.
     * 
     * @param compressionPeriod
     *            compression period of the compression level the sample is
     *            stored for (in seconds).
     * @param channelName
     *            name of the channel the sample originates from.
     * @param value
     *            the value of the sample (including meta-data).
     */
    public Sample(long compressionPeriod, String channelName, IValue value) {
        this.compressionPeriod = compressionPeriod;
        this.channelName = channelName;
        this.value = value;
    }

    /**
     * Returns the compression period this sample is stored for.
     * 
     * @return compression period of the compression level this sample is stored
     *         for (in seconds).
     * @see com.aquenos.csstudio.archive.config.cassandra.CompressionLevelConfig
     */
    public long getCompressionPeriod() {
        return compressionPeriod;
    }

    /**
     * Returns the name of the channel this sample originates from.
     * 
     * @return name of the channel this sample originates from.
     */
    public String getChannelName() {
        return channelName;
    }

    /**
     * Returns the actual value of the sample. The value also contains meta-data
     * like the alarm severity and timestamp.
     * 
     * @return actual value of the sample.
     */
    public IValue getValue() {
        return value;
    }

    @Override
    public int hashCode() {
        int hashCode = 17;
        hashCode += (int) (compressionPeriod ^ (compressionPeriod >>> 32));
        if (channelName != null) {
            hashCode *= 17;
            hashCode += channelName.hashCode();
        }
        if (value != null) {
            hashCode *= 17;
            hashCode += value.hashCode();
        }
        return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof Sample)) {
            return false;
        }
        Sample other = (Sample) obj;
        if (this.compressionPeriod != other.compressionPeriod) {
            return false;
        }
        if (this.channelName == null && other.channelName != null) {
            return false;
        }
        if (this.value == null && other.value != null) {
            return false;
        }
        if (this.channelName != null
                && !this.channelName.equals(other.channelName)) {
            return false;
        }
        if (this.value != null && !this.value.equals(other.value)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "Sample [Channel = "
                + ((channelName != null) ? channelName : "null") + ", Value = "
                + ((value != null) ? value : "null") + "]";
    }
}
