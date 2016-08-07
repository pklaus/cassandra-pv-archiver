package com.aquenos.cassandra.pvarchiver.controlsystem.channelaccess.internal;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

import com.aquenos.cassandra.pvarchiver.controlsystem.channelaccess.ChannelAccessSample;
import com.aquenos.cassandra.pvarchiver.controlsystem.channelaccess.ChannelAccessSampleType;
import com.datastax.driver.core.UDTValue;

/**
 * <p>
 * Base class for all Channel Access samples that internally store their value
 * as a {@link UDTValue}.
 * </p>
 * 
 * <p>
 * This class is intended as the parent class of {@link ChannelAccessRawSample}
 * and {@link ChannelAccessAggregatedSample} and should not be used directly by
 * any other classes.
 * </p>
 * 
 * @author Sebastian Marsching
 */
public abstract class ChannelAccessUdtSample implements ChannelAccessSample {

    private boolean originalSample;
    private long timeStamp;
    private ChannelAccessSampleType type;
    private UDTValue value;

    /**
     * Creates a UDT sample. This constructor is intended for
     * {@link ChannelAccessAggregatedSample} and {@link ChannelAccessRawSample}.
     * Only these classes should be derived from this class.
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
    ChannelAccessUdtSample(long timeStamp, ChannelAccessSampleType type,
            UDTValue value, boolean originalSample) {
        // This constructor is only used internally, so we use assertions
        // instead of preconditions.
        assert (timeStamp >= 0);
        assert (type != null);
        assert (value != null);
        this.timeStamp = timeStamp;
        this.type = type;
        this.value = value;
        this.originalSample = originalSample;
    }

    @Override
    public boolean isOriginalSample() {
        return originalSample;
    }

    @Override
    public long getTimeStamp() {
        return timeStamp;
    }

    public ChannelAccessSampleType getType() {
        return type;
    }

    /**
     * Returns the sample's underlying value (as stored in the database). This
     * method is only intended for use by {@link ChannelAccessDatabaseAccess}
     * and {@link ChannelAccessSampleValueAccess}.
     * 
     * @return this sample's value like it is stored in the database.
     */
    UDTValue getValue() {
        return value;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(originalSample).append(timeStamp)
                .append(type).append(value).toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || !this.getClass().equals(obj.getClass())) {
            return false;
        }
        ChannelAccessUdtSample other = (ChannelAccessUdtSample) obj;
        return new EqualsBuilder()
                .append(this.originalSample, other.originalSample)
                .append(this.timeStamp, other.timeStamp)
                .append(this.type, other.type).append(this.value, other.value)
                .isEquals();
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.reflectionToString(this);
    }

}
