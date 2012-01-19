/*
 * Copyright 2012 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.csstudio.archive.config.cassandra;

import org.csstudio.data.values.ITimestamp;

/**
 * State for a specific compression level and channel.
 * 
 * @author Sebastian Marsching
 * @see CassandraChannelConfig
 * @see CompressionLevelConfig
 */
public class CompressionLevelState {
	private String compressionLevelName;
	private String channelName;
	private ITimestamp lastSavedSampleTime;
	private ITimestamp nextSampleTime;

	/**
	 * Creates a compression-level state object.
	 * 
	 * @param compressionLevelName
	 *            name of the compression level.
	 * @param channelName
	 *            name of the channel.
	 * @param lastSavedSampleTime
	 *            timestamp of the latest sample stored for this compression
	 *            level.
	 * @param nextSampleTime
	 *            timestamp for the next sample that should be processed for
	 *            this compression-level.
	 */
	public CompressionLevelState(String compressionLevelName,
			String channelName, ITimestamp lastSavedSampleTime,
			ITimestamp nextSampleTime) {
		this.compressionLevelName = compressionLevelName;
		this.channelName = channelName;
		this.lastSavedSampleTime = lastSavedSampleTime;
		this.nextSampleTime = nextSampleTime;
	}

	/**
	 * Returns the name of this compression level.
	 * 
	 * @return compression-level name.
	 */
	public String getCompressionLevelName() {
		return compressionLevelName;
	}

	/**
	 * Returns the name of the channel.
	 * 
	 * @return channel name.
	 */
	public String getChannelName() {
		return channelName;
	}

	/**
	 * Returns the timestamp of the latest sample stored for this compression
	 * level and channel.
	 * 
	 * @return timestamp of the latest sample or <code>null</code> if no sample
	 *         exists or the timestamp is unknown.
	 */
	public ITimestamp getLastSavedSampleTime() {
		return lastSavedSampleTime;
	}

	/**
	 * Returns the timestamp for the next sample that should be processed. This
	 * time is meaningless for the special "raw" level. For other compression
	 * levels, once enough data is available from the raw level, the compressed
	 * sample is calculated for this time and stored, if it differs from the
	 * last sample saved.
	 * 
	 * @return timestamp for the next sample to be processed.
	 */
	public ITimestamp getNextSampleTime() {
		return nextSampleTime;
	}
}
