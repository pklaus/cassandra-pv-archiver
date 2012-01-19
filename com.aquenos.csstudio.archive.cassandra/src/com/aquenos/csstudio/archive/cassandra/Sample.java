/*
 * Copyright 2012 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.csstudio.archive.cassandra;

import org.csstudio.data.values.ITimestamp;
import org.csstudio.data.values.IValue;

/**
 * Represents a sample stored in the archive.
 * 
 * @author Sebastian Marsching
 * @see SampleStore
 */
public class Sample {
	private String compressionLevelName;
	private String channelName;
	private IValue value;
	private ITimestamp precedingSampleTimestamp;

	/**
	 * Constructor. Creates a sample using the specified parameters.
	 * 
	 * @param compressionLevelName
	 *            name of the compression level the sample is stored for.
	 * @param channelName
	 *            name of the channel the sample originates from.
	 * @param value
	 *            the value of the sample (including meta-data).
	 * @param precedingSampleTimestamp
	 *            the timestamp of the sample preceding this sample at the the
	 *            same compression-level or <code>null</code> if none such
	 *            sample exists or it is unknown.
	 */
	public Sample(String compressionLevelName, String channelName,
			IValue value, ITimestamp precedingSampleTimestamp) {
		this.compressionLevelName = compressionLevelName;
		this.channelName = channelName;
		this.value = value;
		this.precedingSampleTimestamp = precedingSampleTimestamp;
	}

	/**
	 * Returns the compression level this sample is stored for.
	 * 
	 * @return compression level this sample is stored for.
	 * @see com.aquenos.csstudio.archive.config.cassandra.CompressionLevelConfig
	 */
	public String getCompressionLevelName() {
		return compressionLevelName;
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

	/**
	 * Returns the timestamp of the sample preceding this sample. This might be
	 * <code>null</code> if this is the first sample for the given channel and
	 * compression level or if the is an preceding sample but it was unknown
	 * when this sample was stored. Even if the preceding timestamp is not
	 * <code>null</code>, there is no guarantee that a sample with the specified
	 * timestamp does actually exist. This method mainly exists to provide a
	 * faster way of going backwards in archived data.
	 * 
	 * @return timestamp of the sample preceding this sample or
	 *         <code>null</code>.
	 * @see SampleStore#findPrecedingSample(me.prettyprint.hector.api.mutation.Mutator,
	 *      Sample)
	 */
	public ITimestamp getPrecedingSampleTimestamp() {
		return precedingSampleTimestamp;
	}

	/**
	 * Sets the timestamp of the preceding sample. This method is only intended
	 * to be used by
	 * {@link SampleStore#findPrecedingSample(me.prettyprint.hector.api.mutation.Mutator, Sample)}
	 * .
	 * 
	 * @param precedingSampleTimestamp
	 *            timestamp of the preceding sample or <code>null</code> if this
	 *            is the first sample.
	 */
	protected void setPrecedingSampleTimestamp(
			ITimestamp precedingSampleTimestamp) {
		this.precedingSampleTimestamp = precedingSampleTimestamp;
	}

	@Override
	public int hashCode() {
		int hashCode = 1;
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
