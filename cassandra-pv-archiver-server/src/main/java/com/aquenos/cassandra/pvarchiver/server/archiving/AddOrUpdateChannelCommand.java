/*
 * Copyright 2015-2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.archiving;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

import com.aquenos.cassandra.pvarchiver.server.archiving.internal.ArchiveConfigurationUtils;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import com.google.common.base.Preconditions;

/**
 * Command for adding or updating a channel. This command is very similar to the
 * {@link AddChannelCommand}, but it will work even if the channel already
 * exists. In this case, the channel's configuration is updated to match the
 * specified configuration. This command object contains all data that is needed
 * for the add-or-update operation. Typically, it is passed to the
 * {@link ArchiveConfigurationService}, but it might also be sent over the
 * network before actually being processed.
 * 
 * @author Sebastian Marsching
 */
public final class AddOrUpdateChannelCommand extends
        ArchiveConfigurationCommand {

    private String channelName;
    private String controlSystemType;
    private Set<Integer> decimationLevels;
    private Map<Integer, Integer> decimationLevelToRetentionPeriod;
    private boolean enabled;
    private Map<String, String> options;
    private UUID serverId;

    /**
     * Creates an "add or update channel" command. If the channel does not exist
     * yet, it is added. If it already exists, it is updated with the specified
     * configuration. However, if it already exists and the control-system type
     * or the server ID are different from the ones specified in this command,
     * the operation will fail.
     * 
     * @param channelName
     *            name of the channel to be added or updated.
     * @param controlSystemType
     *            string identifying the control-system support that is used for
     *            the channel. If the channel already exists and the specified
     *            control-system type does not match the one of the existing
     *            channel, the operation will fail.
     * @param decimationLevels
     *            set of decimation levels that shall be created for the
     *            channel. If the channel already exists, decimation levels that
     *            currently exist but are not listed in this set are deleted.
     *            The number identifying a decimation level represents the
     *            period between two samples (in seconds). The decimation level
     *            zero (for raw samples) is always created, even if it is not
     *            contained in the specified set. If <code>null</code>, only the
     *            decimation level zero is created.
     * @param decimationLevelToRetentionPeriod
     *            map containing the mapping of decimation levels to the
     *            corresponding retention period (both in seconds). If an entry
     *            for a decimation level is missing, a retention period of zero
     *            (keep samples indefinitely) is assumed. Negative retention
     *            periods are silently converted to zero. A <code>null</code>
     *            reference has the same effect as an empty map (use a retention
     *            period of zero for all decimation levels). This map must not
     *            contain negative keys. It must also not contain a mapping that
     *            violates the constraint that each decimation period must have
     *            a retention period equal to or greater than the one of the
     *            preceding (next smaller decimation period) decimation level.
     * @param enabled
     *            <code>true</code> if archiving for the channel shall be
     *            enabled, <code>false</code> if archiving shall be disabled.
     * @param options
     *            map storing the control-system specific options for the
     *            channel. A <code>null</code> reference has the same effect as
     *            an empty map.
     * @param serverId
     *            ID of the server to which the channel shall be added. If the
     *            channel already exists and the server ID does not match the
     *            one of existing channel, the operation will fail.
     * @throws IllegalArgumentException
     *             if <code>channelName</code> is empty,
     *             <code>decimationLevels</code> contains negative elements,
     *             <code>decimationLevelToRetentionPeriod</code> contains
     *             negative keys, or
     *             <code>decimationLevelToRetentionPeriod</code> contains a
     *             mapping that violates the constraint that each decimation
     *             period must have a retention period equal to or greater than
     *             the one of the preceding (next smaller decimation period)
     *             decimation level.
     * @throws NullPointerException
     *             if <code>channelName</code>, <code>controlSystemType</code>,
     *             or <code>serverId</code> is <code>null</code> or
     *             <code>decimationLevels</code>,
     *             <code>decimationLevelToRetentionPeriod</code>, or
     *             <code>options</code> contains <code>null</code> keys or
     *             values.
     */
    @JsonCreator
    public AddOrUpdateChannelCommand(
            @JsonProperty(value = "channelName", required = true) String channelName,
            @JsonProperty(value = "controlSystemType", required = true) String controlSystemType,
            @JsonProperty("decimationLevels") Set<Integer> decimationLevels,
            @JsonProperty("decimationLevelToRetentionPeriod") Map<Integer, Integer> decimationLevelToRetentionPeriod,
            @JsonProperty(value = "enabled", required = true) boolean enabled,
            @JsonProperty("options") Map<String, String> options,
            @JsonProperty(value = "serverId", required = true) UUID serverId) {
        Preconditions.checkNotNull(channelName,
                "The channelName must not be null.");
        Preconditions.checkArgument(!channelName.isEmpty(),
                "The channelName must not be empty.");
        Preconditions.checkNotNull(controlSystemType,
                "The controlSystemType must not be null.");
        Preconditions.checkNotNull(serverId, "The serverId must not be null.");
        decimationLevels = ArchiveConfigurationUtils
                .normalizeAndVerifyDecimationLevelsSet(decimationLevels);
        decimationLevelToRetentionPeriod = ArchiveConfigurationUtils
                .normalizeAndVerifyDecimationLevelToRetentionPeriodMap(
                        decimationLevelToRetentionPeriod, decimationLevels);
        options = ArchiveConfigurationUtils.copyAndVerifyOptionsMap(options,
                "options");
        this.channelName = channelName;
        this.controlSystemType = controlSystemType;
        this.decimationLevels = decimationLevels;
        this.decimationLevelToRetentionPeriod = decimationLevelToRetentionPeriod;
        this.enabled = enabled;
        this.options = options;
        this.serverId = serverId;
    }

    @Override
    public Type getCommandType() {
        return Type.ADD_OR_UPDATE_CHANNEL;
    }

    /**
     * Returns the name of the channel that shall be added or updated. The value
     * returned is never <code>null</code> or the empty string.
     * 
     * @return name of the channel to be added.
     */
    public String getChannelName() {
        return channelName;
    }

    /**
     * Returns a string identifying the control-system support for the channel
     * to be added or updated. If the channel already exists and the specified
     * control-system type does not match the one of the existing channel, the
     * operation will fail. The value returned is never <code>null</code>.
     * 
     * @return string identifying the control-system support for the channel.
     */
    public String getControlSystemType() {
        return controlSystemType;
    }

    /**
     * Returns the set containing the decimation levels that shall be created
     * for the channel. If the channel already exists, decimation levels that
     * currently exist but are not listed in this set are deleted. Each
     * decimation level is identified by the period between two samples in the
     * respective decimation level (in seconds). THe decimation level zero is
     * used for raw samples. The set is never <code>null</code>, never empty,
     * and does not contain <code>null</code> elements. It always contains the
     * element zero.
     * 
     * @return decimation levels to be created for the channel.
     */
    @JsonSerialize(contentUsing = ToStringSerializer.class)
    public Set<Integer> getDecimationLevels() {
        return decimationLevels;
    }

    /**
     * <p>
     * Returns the map storing the mapping of decimation levels to their
     * respective retention period. The retention period of a decimation level
     * specifies how long (in seconds) samples in this decimation level are
     * supposed to be kept. A retention period of zero specifies that samples
     * are supposed to be stored indefinitely.
     * </p>
     * 
     * <p>
     * The map returned is never <code>null</code>, does not contain
     * <code>null</code> keys or values and contains a mapping for each
     * decimation level (as returned by {@link #getDecimationLevels()}). It is
     * also guaranteed that the retention period of each decimation level is
     * greater than or equal to the retention period of the preceding decimation
     * level (that is the decimation level with the next smaller decimation
     * period).
     * </p>
     * 
     * @return map mapping decimation levels to their respective retention
     *         period.
     */
    @JsonSerialize(contentUsing = ToStringSerializer.class)
    public Map<Integer, Integer> getDecimationLevelToRetentionPeriod() {
        return decimationLevelToRetentionPeriod;
    }

    /**
     * Tells whether archiving shall be enabled for the channel to be added or
     * updated.
     * 
     * @return <code>true</code> if archiving shall be enabled,
     *         <code>false</code> if it shall be disabled.
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Returns control-system-specific configuration-options for the channel.
     * The meaning of the options depends entirely on the control-system support
     * used for the channel. The map returned is never <code>null</code> and
     * does not contain <code>null</code> keys or values.
     * 
     * @return control-system specific options.
     */
    public Map<String, String> getOptions() {
        return options;
    }

    /**
     * Returns the ID of the server to which the channel shall be added. If the
     * channel already exists and the server ID does not match the one of
     * existing channel, the operation will fail. The value returned is never
     * <code>null</code>.
     * 
     * @return ID of the server to which the channel shall be added.
     */
    public UUID getServerId() {
        return serverId;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(channelName)
                .append(controlSystemType).append(decimationLevels)
                .append(decimationLevelToRetentionPeriod).append(enabled)
                .append(options).append(serverId).toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || !obj.getClass().equals(this.getClass())) {
            return false;
        }
        AddOrUpdateChannelCommand other = (AddOrUpdateChannelCommand) obj;
        return new EqualsBuilder()
                .append(this.channelName, other.channelName)
                .append(this.controlSystemType, other.controlSystemType)
                .append(this.decimationLevels, other.decimationLevels)
                .append(this.decimationLevelToRetentionPeriod,
                        other.decimationLevelToRetentionPeriod)
                .append(this.enabled, other.enabled)
                .append(this.options, other.options)
                .append(this.serverId, other.serverId).isEquals();
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this);
    }

}
