/*
 * Copyright 2012 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.csstudio.archive.config.cassandra;

import org.csstudio.archive.config.ChannelConfig;
import org.csstudio.archive.config.SampleMode;
import org.csstudio.data.values.ITimestamp;

/**
 * Channel configuration for the Cassandra archiver.
 * 
 * @author Sebastian Marsching
 * @see CassandraArchiveConfig
 */
public class CassandraChannelConfig extends ChannelConfig {

	private String engineName;
	private String groupName;

	/**
	 * Creates a channel configuration object.
	 * 
	 * @param engineName
	 *            name of the engine for this channel.
	 * @param groupName
	 *            name of the group this channel is part of.
	 * @param channelName
	 *            name of this channel.
	 * @param sampleMode
	 *            sample mode to use (scan or monitor).
	 * @param lastSampleTime
	 *            timestamp of the latest raw sample for this channel.
	 */
	public CassandraChannelConfig(String engineName, String groupName,
			String channelName, SampleMode sampleMode, ITimestamp lastSampleTime) {
		super(channelName, sampleMode, lastSampleTime);
		this.engineName = engineName;
		this.groupName = groupName;

	}

	/**
	 * Returns the name of the engine for this channel.
	 * 
	 * @return engine name for this channel.
	 */
	public String getEngineName() {
		return engineName;
	}

	/**
	 * Returns the name of the group this channel is part of.
	 * 
	 * @return group name for this channel.
	 */
	public String getGroupName() {
		return groupName;
	}

}
