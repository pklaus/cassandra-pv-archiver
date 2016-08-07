/*
 * Copyright 2015-2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.controlsystem;

import java.util.UUID;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

import com.google.common.base.Preconditions;

/**
 * Identifier for a sample bucket. Each sample bucket has a unique identifier
 * consisting of the channel's data ID, the decimation level, and the start time
 * of the bucket.
 * 
 * @author Sebastian Marsching
 */
public final class SampleBucketId {

    private long bucketStartTime;
    private UUID channelDataId;
    private int decimationLevel;

    /**
     * Create an identifier for a sample bucket. The sample bucket is identified
     * by the channel's data ID, the decimation level, and the sample bucket's
     * start time.
     * 
     * @param channelDataId
     *            unique identifier associated with the data (samples) for the
     *            channel.
     * @param decimationLevel
     *            decimation level for which the sample bucket stores data. The
     *            decimation level is identified by the number of seconds
     *            between two samples. A decimation level of zero specifies that
     *            this decimation level stores (undecimated) raw samples, which
     *            typically do not have a fixed period.
     * @param bucketStartTime
     *            start time of the bucket. A bucket only stores samples that
     *            have a time stamp that is greater than or equal to the
     *            bucket's start-time. The start time is specified as the number
     *            of nanoseconds since epoch (January 1st, 1970, 00:00:00 UTC).
     */
    public SampleBucketId(UUID channelDataId, int decimationLevel,
            long bucketStartTime) {
        Preconditions.checkNotNull(channelDataId,
                "The channelDataId must not be null.");
        Preconditions.checkArgument(decimationLevel >= 0,
                "The decimationLevel must not be negative.");
        Preconditions.checkArgument(bucketStartTime >= 0L,
                "The bucketStartTime must not be negative.");
        this.channelDataId = channelDataId;
        this.decimationLevel = decimationLevel;
        this.bucketStartTime = bucketStartTime;
    }

    /**
     * Returns the start time of the sample bucket as the number of nanoseconds
     * since epoch (January 1st, 1970, 00:00:00 UTC). A bucket only stores
     * sample with time stamps equal to or greater than the bucket's start time
     * and strictly less than the following bucket's start time.
     * 
     * @return start time of the bucket as the number of nanoseconds since
     *         epoch.
     */
    public long getBucketStartTime() {
        return bucketStartTime;
    }

    /**
     * Returns the unique identifier that is associated with the data (samples)
     * for the channel. While a channel's name might change due to renaming, the
     * data ID will be permanent (until the channel is deleted), so even after
     * renaming a channel, its associated data can still be found.
     * 
     * @return unique identifier associated with the channel's data.
     */
    public UUID getChannelDataId() {
        return channelDataId;
    }

    /**
     * Returns the decimation level for which the sample bucket stores data. The
     * decimation level is identified by the number of seconds between two
     * samples. A decimation level of zero specifies that this decimation level
     * stores (undecimated) raw samples, which typically do not have a fixed
     * period.
     * 
     * 
     * @return decimation level of the bucket.
     */
    public int getDecimationLevel() {
        return decimationLevel;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(bucketStartTime)
                .append(channelDataId).append(decimationLevel).toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof SampleBucketId)) {
            return false;
        }
        SampleBucketId other = (SampleBucketId) obj;
        return new EqualsBuilder()
                .append(this.bucketStartTime, other.bucketStartTime)
                .append(this.channelDataId, other.channelDataId)
                .append(this.decimationLevel, other.decimationLevel).isEquals();
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this);
    }

}
