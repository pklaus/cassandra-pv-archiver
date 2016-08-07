/*
 * Copyright 2015-2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.controlsystem;

import com.google.common.base.Preconditions;

/**
 * State information about a sample bucket. This object aggregates the state
 * information of a sample bucket that is needed by the archiving code. When
 * needed, the archiving code asks the {@link ControlSystemSupport} for the
 * state associated with a specific sample bucket using the
 * {@link ControlSystemSupport#getSampleBucketState(SampleBucketId)} method.
 * 
 * @author Sebastian Marsching
 *
 */
public final class SampleBucketState {

    private int currentBucketSize;
    private long latestSampleTimeStamp;

    /**
     * Creates an object representing the specified sample-bucket state.
     * 
     * @param currentBucketSize
     *            current size of the sample bucket (in bytes). This is the size
     *            that was stored when the last sample was written. If no sample
     *            has been written yet, this should be zero.
     * @param latestSampleTimeStamp
     *            time stamp of the newest sample in the bucket. If no sample
     *            has been written yet, this should be zero. The time stamp is
     *            specified as the number of nanoseconds since epoch (January
     *            1st, 1970, 00:00:00 UTC).
     * @throws IllegalArgumentException
     *             if <code>currentBucketSize</code> or
     *             <code>latestSampleTimeStamp</code> is negative.
     */
    public SampleBucketState(int currentBucketSize, long latestSampleTimeStamp) {
        Preconditions.checkArgument(currentBucketSize >= 0,
                "The currentBucketSize must not be negative.");
        Preconditions.checkArgument(latestSampleTimeStamp >= 0L,
                "The latestSampleTimeStap must not be negative.");
        this.currentBucketSize = currentBucketSize;
        this.latestSampleTimeStamp = latestSampleTimeStamp;
    }

    /**
     * Returns the current size of the sample bucket (in bytes). This is the
     * size that was stored when the last sample was written. If no sample has
     * been written yet, this should be zero.
     * 
     * @return current bucket size in bytes.
     */
    public int getCurrentBucketSize() {
        return currentBucketSize;
    }

    /**
     * Returns the time stamp of the newest sample in the bucket. If no sample
     * has been written yet, this should be zero. The time stamp is returned as
     * the number of nanoseconds since epoch (January 1st, 1970, 00:00:00 UTC).
     * 
     * @return time stamp of the newest sample in the bucket.
     */
    public long getLatestSampleTimeStamp() {
        return latestSampleTimeStamp;
    }

    @Override
    public int hashCode() {
        int hashCode = 19;
        hashCode = hashCode * 53 + currentBucketSize;
        hashCode = hashCode
                * 53
                + ((int) (latestSampleTimeStamp ^ (latestSampleTimeStamp >> 32)));
        return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !obj.getClass().equals(this.getClass())) {
            return false;
        }
        SampleBucketState other = (SampleBucketState) obj;
        return (this.currentBucketSize == other.currentBucketSize)
                && (this.latestSampleTimeStamp == other.latestSampleTimeStamp);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("SampleBucketState@");
        sb.append(Integer.toHexString(System.identityHashCode(this)));
        sb.append("[currentBucketSize=");
        sb.append(currentBucketSize);
        sb.append(",latestSampleTimeStamp=");
        sb.append(latestSampleTimeStamp);
        sb.append("]");
        return sb.toString();
    }

}
