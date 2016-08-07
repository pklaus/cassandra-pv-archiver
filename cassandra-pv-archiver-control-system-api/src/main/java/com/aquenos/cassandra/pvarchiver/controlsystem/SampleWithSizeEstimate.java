/*
 * Copyright 2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.controlsystem;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

import com.google.common.base.Preconditions;

/**
 * Pair of a sample and its estimated size. This class is used when a sample is
 * supposed to be written and thus not only the sample but also its estimated
 * size is needed.
 * 
 * @author Sebastian Marsching
 *
 * @param <SampleType>
 *            type of the sample that is stored in this object.
 */
public final class SampleWithSizeEstimate<SampleType extends Sample> {

    private final int estimatedSampleSize;
    private final SampleType sample;

    /**
     * Creates a pair that stored the specified sample and estimated size.
     * 
     * @param sample
     *            sample that is supposed to be written.
     * @param estimatedSampleSize
     *            estimated size of the specified sample when serialized into
     *            the database (in bytes). This information is used by the
     *            archiving code to keep track of the total size of the samples
     *            that are stored inside a bucket. As storing too much (or too
     *            little) data in a single bucket has an impact on performance,
     *            the archiving code will decide to start a new bucket when a
     *            certain size has been reached. Therefore, this estimate should
     *            be as accurate as possible. Must not be negative.
     * @throws IllegalArgumentException
     *             if <code>estimatedSampleSize</code> is negative.
     * @throws NullPointerException
     *             if <code>sample</code> is <code>null</code>.
     */
    public SampleWithSizeEstimate(SampleType sample, int estimatedSampleSize) {
        Preconditions.checkNotNull(sample);
        Preconditions.checkArgument(estimatedSampleSize >= 0);
        this.sample = sample;
        this.estimatedSampleSize = estimatedSampleSize;
    }

    /**
     * Returns the estimated size of the sample that is returned by
     * {@link #getSample()}. The size is specified in bytes. This information is
     * used by the archiving code to keep track of the total size of the samples
     * that are stored inside a bucket.
     * 
     * @return estimated size of the sample (always greater than or equal to
     *         zero).
     */
    public int getEstimatedSampleSize() {
        return estimatedSampleSize;
    }

    /**
     * Returns the sample that is supposed to be written.
     * 
     * @return sample stored in this pair.
     */
    public SampleType getSample() {
        return sample;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(estimatedSampleSize).append(sample)
                .toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof SampleWithSizeEstimate)) {
            return false;
        }
        SampleWithSizeEstimate<?> other = (SampleWithSizeEstimate<?>) obj;
        return new EqualsBuilder().append(this.sample, other.sample)
                .append(this.estimatedSampleSize, other.estimatedSampleSize)
                .isEquals();
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this);
    }

}
