/*
 * Copyright 2012-2013 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.csstudio.archive.config.cassandra;

/**
 * Configuration for a channel's compression level.
 * 
 * @author Sebastian Marsching
 * @see CassandraChannelConfig
 * @see CompressionLevelState
 */
public class CompressionLevelConfig {

    private String channelName;
    private long compressionPeriod;
    private long retentionPeriod;

    /**
     * Creates a compression level configuration object.
     * 
     * @param channelName
     *            name of the channel.
     * @param compressionPeriod
     *            period between compressed samples (in seconds).
     * @param retentionPeriod
     *            period after which samples are deleted (in seconds).
     */
    public CompressionLevelConfig(String channelName, long compressionPeriod,
            long retentionPeriod) {
        this.channelName = channelName;
        this.compressionPeriod = compressionPeriod;
        this.retentionPeriod = retentionPeriod;
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
     * Returns the time between compressed samples (in seconds). This defines
     * the time resolution of this compression level. This option does not have
     * a meaning for the special "raw" compression level, because the time
     * between raw samples is defined by the channel configuration.
     * 
     * @return compression period in seconds.
     */
    public long getCompressionPeriod() {
        return compressionPeriod;
    }

    /**
     * Returns the retention period (in seconds). This is the time after which
     * samples are deleted. However, not the local clock is used as the
     * reference but the newest sample stored for the same channel and
     * compression level.
     * 
     * @return retention period in seconds.
     */
    public long getRetentionPeriod() {
        return retentionPeriod;
    }
}
