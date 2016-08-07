/*
 * Copyright 2015-2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.archiving;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

import com.aquenos.cassandra.pvarchiver.server.archiving.internal.ArchiveConfigurationUtils;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

/**
 * Command for updating a channel. Updating a channel results in the channel's
 * configuration to be changed according to the requested update and the channel
 * being refreshed with the new configuration. This command object contains all
 * data that is needed for the remove operation. Typically, it is passed to the
 * {@link ArchiveConfigurationService}, but it might also be sent over the
 * network before actually being processed.
 * 
 * @author Sebastian Marsching
 *
 */
public final class UpdateChannelCommand extends ArchiveConfigurationCommand {

    private String channelName;
    private String expectedControlSystemType;
    private UUID expectedServerId;
    private Set<Integer> decimationLevels;
    private Set<Integer> addDecimationLevels;
    private Set<Integer> removeDecimationLevels;
    private Map<Integer, Integer> decimationLevelToRetentionPeriod;
    private Boolean enabled;
    private Map<String, String> options;
    private Map<String, String> addOptions;
    private Set<String> removeOptions;

    /**
     * <p>
     * Creates an "update channel" command. If the specified
     * <code>expectedServerId</code> does not match the actual ID of the server
     * that currently owns the channel or if
     * <code>expectedControlSystemType</code> does not match the actual
     * control-system type of the channel, the operation will fail.
     * </p>
     * 
     * <p>
     * The changes that should be made by the update can be described in a
     * differential fashions: If a part of the configuration is not supposed to
     * be modified, <code>null</code> can be passed for the respective argument
     * to this constructor.
     * </p>
     * 
     * <p>
     * The set if decimation levels that shall be added and removed can be
     * described in an explicit or in a differential fashion. For an explicit
     * description, use the <code>decimationLevels</code> argument. For a
     * differential description, use the <code>addDecimationLevels</code> and
     * <code>removeDecimationLevels</code> arguments. Only one of the two ways
     * to describe the change can be used at the same time. Setting both
     * <code>decimationLevels</code> and either or both of
     * <code>addDecimationLevels</code> and <code>removeDecimationLevels</code>
     * results in an {@link IllegalArgumentException}.
     * </p>
     * 
     * <p>
     * If decimation levels are listed in <code>decimationLevels</code> or
     * <code>addDecimationLevels</code>, but do not have a corresponding entry
     * in <code>decimationLevelToRetentionPeriod</code>, a retention period of
     * zero is assumed for these decimation levels, even if they already exist
     * with a different retention period.
     * </p>
     * 
     * <p>
     * Like the decimation levels, the control-system-specific configuration
     * options can also be described in an explicit or in a differential
     * fashion. For an explicit description, use the <code>options</code>
     * argument. For a differential description, use the <code>addOptions</code>
     * and <code>removeOptions</code> arguments. Only one of the two ways to
     * describe the change can be used at the same time. Setting both
     * <code>options</code> and either or both of <code>addOptions</code> and
     * <code>removeOptions</code> results in an {@link IllegalArgumentException}
     * .
     * </p>
     * 
     * <p>
     * As a general rule, none of the maps and sets may contain
     * <code>null</code> keys or values. Such an entry results in a
     * {@link NullPointerException}.
     * </p>
     * 
     * <p>
     * Decimation levels may not be negative. Negative decimation levels result
     * in an {@link IllegalArgumentException}. Negative retention periods are
     * allowed but are silently converted to a retention period of zero.
     * </p>
     * 
     * <p>
     * A decimation level or control-system-specific configuration option cannot
     * be added and removed at the same time. Any attempt to do so results in an
     * {@link IllegalArgumentException}.
     * </p>
     * 
     * <p>
     * The entries in the <code>decimationLevelToRetentionPeriod</code> map must
     * match the constraint that the retention period of each decimation level
     * is greater than or equal to the retention period of the preceding
     * decimation level (that is the decimation level with the next smaller
     * decimation period). A violation of this constraint (for a decimation
     * level that is not going to be removed anyway) will result in an
     * {@link IllegalArgumentException} being thrown. However, this check cannot
     * strictly guarantee that the whole retention-period configuration is valid
     * unless <code>decimationLevels</code> is not <code>null</code>. If it is
     * <code>null</code> existing retention period configurations that might be
     * merged with the specified configuration could lead to a violation of the
     * constraint. Such a violation will only be detected later when the command
     * is actually executed, leading to an {@link IllegalArgumentException}
     * being thrown then.
     * </p>
     * 
     * @param channelName
     *            name of the channel to be updated.
     * @param expectedControlSystemType
     *            if not <code>null</code>, the update operation will fail if
     *            the channel's actual control-system type is different.
     * @param expectedServerId
     *            if not <code>null</code>, the update operation will fail if
     *            the channel is currently registered with a different server
     *            than the specified one.
     * @param decimationLevels
     *            set of decimation levels that shall exist for the channel
     *            after the update. May only be specified if both
     *            <code>addDecimationLevels</code> and
     *            <code>removeDecimationLevels</code> are <code>null</code>.
     * @param addDecimationLevels
     *            set of decimation levels to be added. May only be specified if
     *            <code>decimationLevels</code> is <code>null</code>.
     * @param removeDecimationLevels
     *            set of decimation levels to be removed. May only be specified
     *            if <code>decimationLevels</code> is <code>null</code>.
     * @param decimationLevelToRetentionPeriod
     *            map mapping decimation levels to their respective retention
     *            period. Decimation levels specified in this map will have
     *            their retention periods updated with the specified values.
     *            However, decimation levels not explicitly specified in this
     *            map might also have their retention period reset to zero.
     *            Refer to the description of
     *            {@link #getDecimationLevelToRetentionPeriod()} for details.
     *            Negative retention periods are silently converted to zero.
     *            This map must not contain negative keys. It must also not
     *            contain a mapping that violates the constraint that each
     *            decimation period must have a retention period equal to or
     *            greater than the one of the preceding (next smaller decimation
     *            period) decimation level.
     * @param enabled
     *            whether archiving for the channel shall be enabled or
     *            disabled.
     * @param options
     *            control-system-specific configuration options. May only be
     *            specified if both <code>addOptions</code> and
     *            <code>removeOptions</code> are <code>null</code>.
     * @param addOptions
     *            control-system-specific configuration options to be added. May
     *            only be specified if <code>options</code> is <code>null</code>
     *            .
     * @param removeOptions
     *            control-system-specific configuration options to be removed.
     *            May only be specified if <code>options</code> is
     *            <code>null</code>.
     * @throws IllegalArgumentException
     *             if <code>channelName</code> is the empty string or any of the
     *             constraints described earlier are violated.
     * @throws NullPointerException
     *             if <code>channelName</code> is null or any of the argument
     *             maps and sets contains <code>null</code> keys or values.
     */
    @JsonCreator
    public UpdateChannelCommand(
            @JsonProperty(value = "channelName", required = true) String channelName,
            @JsonProperty("expectedControlSystemType") String expectedControlSystemType,
            @JsonProperty("expectedServerId") UUID expectedServerId,
            @JsonProperty("decimationLevels") Set<Integer> decimationLevels,
            @JsonProperty("addDecimationLevels") Set<Integer> addDecimationLevels,
            @JsonProperty("removeDecimationLevels") Set<Integer> removeDecimationLevels,
            @JsonProperty("decimationLevelToRetentionPeriod") Map<Integer, Integer> decimationLevelToRetentionPeriod,
            @JsonProperty("enabled") Boolean enabled,
            @JsonProperty("options") Map<String, String> options,
            @JsonProperty("addOptions") Map<String, String> addOptions,
            @JsonProperty("removeOptions") Set<String> removeOptions) {
        Preconditions.checkNotNull(channelName,
                "The channelName must not be null.");
        Preconditions.checkArgument(!channelName.isEmpty(),
                "The channelName must not be empty.");
        Preconditions
                .checkArgument(
                        decimationLevels == null
                                || (addDecimationLevels == null && removeDecimationLevels == null),
                        "If decimationLevels is specified, addDecimationLevels and removeDecimationLevels must be null.");
        Preconditions
                .checkArgument(options == null
                        || (addOptions == null && removeOptions == null),
                        "If options is specified, addOptions and removeOptions must be null.");
        if (decimationLevels != null) {
            decimationLevels = ArchiveConfigurationUtils
                    .normalizeAndVerifyDecimationLevelsSet(decimationLevels);
            decimationLevelToRetentionPeriod = ArchiveConfigurationUtils
                    .normalizeAndVerifyDecimationLevelToRetentionPeriodMap(
                            decimationLevelToRetentionPeriod, decimationLevels);
        } else if (addDecimationLevels != null
                || removeDecimationLevels != null) {
            // The addDecimationLevels and removeDecimationLevels set are
            // special because we do not want to include the raw
            // decimation-level in either of them.
            addDecimationLevels = ArchiveConfigurationUtils
                    .normalizeAndVerifyDecimationLevelsSetWithoutRawLevel(
                            addDecimationLevels, "addDecimationLevels");
            removeDecimationLevels = ArchiveConfigurationUtils
                    .normalizeAndVerifyDecimationLevelsSetWithoutRawLevel(
                            removeDecimationLevels, "removeDecimationLevels");
            Preconditions
                    .checkArgument(
                            Sets.intersection(addDecimationLevels,
                                    removeDecimationLevels).isEmpty(),
                            "A decimation level cannot be added and removed at the same time.");
            decimationLevelToRetentionPeriod = ArchiveConfigurationUtils
                    .normalizeAndVerifyDecimationLevelToRetentionPeriodMap(
                            decimationLevelToRetentionPeriod,
                            addDecimationLevels, removeDecimationLevels);
        } else {
            // If the decimation levels themselves are not modified, the
            // retention periods might still be updated. We treat this like
            // having empty sets for adding and removing decimation levels.
            decimationLevelToRetentionPeriod = ArchiveConfigurationUtils
                    .normalizeAndVerifyDecimationLevelToRetentionPeriodMap(
                            decimationLevelToRetentionPeriod,
                            Collections.<Integer> emptySet(),
                            Collections.<Integer> emptySet());
        }
        if (options != null) {
            options = ArchiveConfigurationUtils.copyAndVerifyOptionsMap(
                    options, "options");
        }
        if (addOptions != null || removeOptions != null) {
            addOptions = ArchiveConfigurationUtils.copyAndVerifyOptionsMap(
                    addOptions, "addOptions");
            removeOptions = ArchiveConfigurationUtils.copyAndVerifyOptionsSet(
                    removeOptions, "removeOptions");
            // There is no sense in adding and removing the same decimation
            // level or option in a single operation.
            Preconditions.checkArgument(
                    Sets.intersection(addOptions.keySet(), removeOptions)
                            .isEmpty(),
                    "An option cannot be added and removed at the same time.");
        }
        this.channelName = channelName;
        this.expectedControlSystemType = expectedControlSystemType;
        this.expectedServerId = expectedServerId;
        this.decimationLevels = decimationLevels;
        this.addDecimationLevels = addDecimationLevels;
        this.removeDecimationLevels = removeDecimationLevels;
        this.decimationLevelToRetentionPeriod = decimationLevelToRetentionPeriod;
        this.enabled = enabled;
        this.options = options;
        this.addOptions = addOptions;
        this.removeOptions = removeOptions;
    }

    @Override
    public Type getCommandType() {
        return Type.UPDATE_CHANNEL;
    }

    /**
     * Returns the name of the channel that shall be updated. The value returned
     * is never <code>null</code> or the empty string.
     * 
     * @return name of the channel to be updated.
     */
    public String getChannelName() {
        return channelName;
    }

    /**
     * Returns the expected control-system type of the channel. If
     * <code>null</code>, the channel will be updated regardless of its current
     * control-system type. If not <code>null</code>, the update operation will
     * fail if the channel's actual control-system type is different.
     * 
     * @return expected control-system type of the channel to be updated.
     */
    @JsonInclude(Include.NON_NULL)
    public String getExpectedControlSystemType() {
        return expectedControlSystemType;
    }

    /**
     * Returns the ID of the server which is expected to currently own the
     * channel. If <code>null</code> the channel will be updated regardless of
     * which server currently owns the channel. If not <code>null</code>, the
     * update operation will fail if the channel is currently registered with a
     * different server than the specified one.
     * 
     * @return ID of the server that is expected to currently own the channel or
     *         <code>null</code> if the channel should be updated regardless of
     *         the server that currently owns the channel.
     */
    @JsonInclude(Include.NON_NULL)
    public UUID getExpectedServerId() {
        return expectedServerId;
    }

    /**
     * <p>
     * Returns the set of decimation levels that shall exist for the channel
     * after the update. Each decimation level is identified by the period
     * between two consecutive samples (in seconds). The decimation level
     * storing raw samples is represented by a period of zero. As the decimation
     * level for raw samples must always exist, the returned set always contains
     * the zero element.
     * </p>
     * 
     * <p>
     * Decimation levels in the returned set that currently do not exist will be
     * added when the update operation is performed. Decimation levels that
     * currently exist but are not in the set are removed respectively.
     * </p>
     * 
     * <p>
     * If the returned set is <code>null</code>, the set of decimation levels is
     * either not supposed to be changed or the changes are described in a
     * differential fashion through the sets returned by
     * {@link #getAddDecimationLevels()} and
     * {@link #getRemoveDecimationLevels()}. If this method does not return
     * <code>null</code>, {@link #getAddDecimationLevels()} and
     * {@link #getRemoveDecimationLevels()} both return <code>null</code>.
     * </p>
     * 
     * <p>
     * The returned set does never contain <code>null</code> or negative
     * elements. It is never empty, but the return value itself might be
     * <code>null</code>.
     * </p>
     * 
     * @return set describing the decimation levels that shall exist for the
     *         channel after the update or <code>null</code> if the set of
     *         decimation levels is not supposed to be changed or the change is
     *         describe in a differential fashion.
     * @see #getAddDecimationLevels()
     * @see #getRemoveDecimationLevels()
     */
    @JsonInclude(Include.NON_NULL)
    @JsonSerialize(contentUsing = ToStringSerializer.class)
    public Set<Integer> getDecimationLevels() {
        return decimationLevels;
    }

    /**
     * <p>
     * Returns the set of decimation levels to be added. Each decimation level
     * is identified by the period between two consecutive samples (in seconds).
     * The decimation level storing raw samples is represented by a period of
     * zero. As the decimation level for raw samples must always exist, the
     * returned set never contains the zero element.
     * </p>
     * 
     * <p>
     * If a decimation level specified in this set already exists before the
     * update operation, the update operation will only change the decimation
     * level's retention period according to the value in the map returned by
     * {@link #getDecimationLevelToRetentionPeriod()}. That map will always
     * contain an entry for each decimation level that is contained in the set
     * returned by this method. If not specified explicitly, the retention
     * period will be zero.
     * </p>
     * 
     * <p>
     * If the returned set is <code>null</code>, the set of decimation levels is
     * either not supposed to be changed or the changes are described in an
     * explicit fashion through the set returned by
     * {@link #getDecimationLevels()}. If this method does not return
     * <code>null</code>, {@link #getRemoveDecimationLevels()} does not return
     * <code>null</code> either and {@link #getDecimationLevels()} returns
     * <code>null</code>. If this method returns <code>null</code>,
     * {@link #getRemoveDecimationLevels()} also returns <code>null</code>.
     * </p>
     * 
     * <p>
     * The returned set does never contain <code>null</code> or negative
     * elements. However, the set may be empty and the return value itself might
     * be <code>null</code>. The set never contains a decimation level that is
     * also contained in the set returned by
     * {@link #getRemoveDecimationLevels()}.
     * </p>
     * 
     * @return set of decimation levels that shall be added for the channel or
     *         <code>null</code> if the set of decimation levels is not supposed
     *         to be changed or the change is described in an explicit fashion.
     * @see #getDecimationLevels()
     * @see #getRemoveDecimationLevels()
     */
    @JsonInclude(Include.NON_NULL)
    @JsonSerialize(contentUsing = ToStringSerializer.class)
    public Set<Integer> getAddDecimationLevels() {
        return addDecimationLevels;
    }

    /**
     * <p>
     * Returns the set of decimation levels to be removed. Each decimation level
     * is identified by the period between two consecutive samples (in seconds).
     * The decimation level storing raw samples is represented by a period of
     * zero. As the decimation level for raw samples must always exist, the
     * returned set never contains the zero element.
     * </p>
     * 
     * <p>
     * If a decimation level specified in this set does not exist before the
     * update operation, the update operation will simply skip the element.
     * </p>
     * 
     * <p>
     * If the returned set is <code>null</code>, the set of decimation levels is
     * either not supposed to be changed or the changes are described in an
     * explicit fashion through the set returned by
     * {@link #getDecimationLevels()}. If this method does not return
     * <code>null</code>, {@link #getAddDecimationLevels()} does not return
     * <code>null</code> either and {@link #getDecimationLevels()} returns
     * <code>null</code>. If this method returns <code>null</code>,
     * {@link #getAddDecimationLevels()} also returns <code>null</code>.
     * </p>
     * 
     * <p>
     * The returned set does never contain <code>null</code> or negative
     * elements. However, the set may be empty and the return value itself might
     * be <code>null</code>. The set never contains a decimation level that is
     * also contained in the set returned by {@link #getAddDecimationLevels()}.
     * </p>
     * 
     * @return set of decimation levels that shall be removed from the channel
     *         or <code>null</code> if the set of decimation levels is not
     *         supposed to be changed or the change is described in an explicit
     *         fashion.
     * @see #getAddDecimationLevels()
     * @see #getDecimationLevels()
     */
    @JsonInclude(Include.NON_NULL)
    @JsonSerialize(contentUsing = ToStringSerializer.class)
    public Set<Integer> getRemoveDecimationLevels() {
        return removeDecimationLevels;
    }

    /**
     * <p>
     * Returns the map mapping decimation levels to their respective retention
     * period. Each map entry has a retention level as its key and the
     * respective retention period (in seconds) as its value. Each decimation
     * level is identified by the period between two consecutive samples (in
     * seconds). The decimation level storing raw samples is represented by a
     * period of zero. A retention period of zero indicates that samples for the
     * respective decimation level shall be kept indefinitely.
     * </p>
     * 
     * <p>
     * If either of the sets returned by {@link #getDecimationLevels()} and
     * {@link #getAddDecimationLevels()} is not <code>null</code> and not empty,
     * this map is guaranteed to contain an entry for each decimation level in
     * the returned set. If no retention period for the respective decimation
     * level was specified when creating this command, this map will contain a
     * retention period of zero for the respective decimation level. This map
     * may also contain entries for decimation levels that are not to be added
     * but already exist. In this case, the decimation levels' retention periods
     * will be updated according to their entries in this map. If this map
     * contains an entry for a decimation level that does not exist and is not
     * to be added, the entry is simply skipped.
     * </p>
     * 
     * <p>
     * If this method returns <code>null</code>, the retention periods are not
     * supposed to be changed. However, this method will never return
     * <code>null</code>, if {@link #getDecimationLevels()} or
     * {@link #getAddDecimationLevels()} do not return <code>null</code>.
     * </p>
     * 
     * <p>
     * The returned map does never contain <code>null</code> keys or values and
     * the values are never negative. However, the map may be empty and the
     * return value itself might be <code>null</code>.
     * </p>
     * 
     * <p>
     * It is also guaranteed that the retention period of each decimation level
     * is greater than or equal to the retention period of the preceding
     * decimation level (that is the decimation level with the next smaller
     * decimation period). However, this only guarantees that the whole
     * retention-period configuration is valid when
     * {@link #getDecimationLevels()} does not return <code>null</code> and thus
     * all retention periods will be updated with the values from this map.
     * Otherwise, the entries from this map might get merged with existing
     * entries, leading to invalid retention periods. Such a situation can only
     * be detected when the update is actually applied, then leading to an
     * exception.
     * </p>
     * 
     * @return map mapping decimation levels to their respective retention
     *         periods.
     * @see #getAddDecimationLevels()
     * @see #getDecimationLevels()
     */
    @JsonInclude(Include.NON_NULL)
    @JsonSerialize(contentUsing = ToStringSerializer.class)
    public Map<Integer, Integer> getDecimationLevelToRetentionPeriod() {
        return decimationLevelToRetentionPeriod;
    }

    /**
     * Tells whether archiving for the channel shall be enabled or disabled. If
     * <code>true</code>, archiving is supposed to be enabled. If
     * <code>false</code>, archiving is supposed to be disabled. If
     * <code>null</code>, the channel's current enabled state will not be
     * changed.
     * 
     * @return flag indicating the channel's future enabled state or
     *         <code>null</code> if the enabled state is not supposed to be
     *         changed.
     */
    @JsonInclude(Include.NON_NULL)
    public Boolean getEnabled() {
        return enabled;
    }

    /**
     * <p>
     * Returns the control-system-specific configuration options for the
     * channel. If not <code>null</code>, the channel's options are replaced
     * with the options in this map. Options that currently exist for the
     * channel but that do not exist in the returned map are removed when the
     * channel is updated.
     * </p>
     * 
     * <p>
     * If the returned map is <code>null</code>, the control-system-specific
     * configuration options for the channel are either not supposed to be
     * changed or the changes are described in a differential fashion through
     * the map returned by {@link #getAddOptions()} and the set returned by
     * {@link #getRemoveOptions()}. If this method does not return
     * <code>null</code>, {@link #getAddOptions()} and
     * {@link #getRemoveOptions()} both return <code>null</code>.
     * </p>
     * 
     * <p>
     * The returned map does never contain <code>null</code> keys or values.
     * However, the map may be empty and the return value itself might be
     * <code>null</code>.
     * </p>
     * 
     * @return map with control-system-specific configuration options that shall
     *         replace the channel's current configuration options or
     *         <code>null</code> if the configuration options are not supposed
     *         to be changed or the change is described in a differential
     *         fashion.
     * @see #getAddOptions()
     * @see #getRemoveOptions()
     */
    @JsonInclude(Include.NON_NULL)
    public Map<String, String> getOptions() {
        return options;
    }

    /**
     * <p>
     * Returns the map of control-system-specific configuration options to be
     * added. If a configuration option specified in this map already exists
     * before the update operation, the update operation will simply update the
     * option's value to match the value in this map.
     * </p>
     * 
     * <p>
     * If the returned map is <code>null</code>, the channel's
     * control-system-specific configuration options are either not supposed to
     * be changed or the changes are described in an explicit fashion through
     * the map returned by {@link #getOptions()}. If this method does not return
     * <code>null</code>, {@link #getRemoveOptions()} does not return
     * <code>null</code> either and {@link #getOptions()} returns
     * <code>null</code>. If this method returns <code>null</code>,
     * {@link #getRemoveOptions()} also returns <code>null</code>.
     * </p>
     * 
     * <p>
     * The returned map does never contain <code>null</code> keys or values.
     * However, the map may be empty and the return value itself might be
     * <code>null</code>. The map never contains a configuration option that is
     * also contained in the set returned by {@link #getRemoveOptions()}.
     * </p>
     * 
     * @return map of control-system-specific configuration options to be added
     *         or <code>null</code> if the configurations options are not
     *         supposed to be changed or the change is described explicitly.
     * @see #getOptions()
     * @see #getRemoveOptions()
     */
    @JsonInclude(Include.NON_NULL)
    public Map<String, String> getAddOptions() {
        return addOptions;
    }

    /**
     * <p>
     * Returns the set of control-system-specific configuration options to be
     * removed. If an option specified in this set does not exist before the
     * update operation, the update operation will simply skip the element.
     * </p>
     * 
     * <p>
     * If the returned set is <code>null</code>, the channel's
     * control-system-specific configuration options are either not supposed to
     * be changed or the changes are described in an explicit fashion through
     * the map returned by {@link #getOptions()}. If this method does not return
     * <code>null</code>, {@link #getAddOptions()} does not return
     * <code>null</code> either and {@link #getOptions()} returns
     * <code>null</code>. If this method returns <code>null</code>,
     * {@link #getAddOptions()} also returns <code>null</code>.
     * </p>
     * 
     * <p>
     * The returned set does never contain <code>null</code> elements. However,
     * the set may be empty and the return value itself might be
     * <code>null</code>. The set never contains a configuration option that is
     * also contained in the map returned by {@link #getAddOptions()}.
     * </p>
     * 
     * @return set of control-system-specific configuration options to be
     *         removed or <code>null</code> if the configurations options are
     *         not supposed to be changed or the change is described explicitly.
     * @see #getAddOptions()
     * @see #getOptions()
     */
    @JsonInclude(Include.NON_NULL)
    public Set<String> getRemoveOptions() {
        return removeOptions;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(channelName)
                .append(expectedControlSystemType).append(expectedServerId)
                .append(decimationLevels).append(addDecimationLevels)
                .append(removeDecimationLevels)
                .append(decimationLevelToRetentionPeriod).append(enabled)
                .append(options).append(addOptions).append(removeOptions)
                .toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || !obj.getClass().equals(this.getClass())) {
            return false;
        }
        UpdateChannelCommand other = (UpdateChannelCommand) obj;
        return new EqualsBuilder()
                .append(this.channelName, other.channelName)
                .append(this.expectedControlSystemType,
                        other.expectedControlSystemType)
                .append(this.expectedServerId, other.expectedServerId)
                .append(this.decimationLevels, other.decimationLevels)
                .append(this.addDecimationLevels, other.addDecimationLevels)
                .append(this.removeDecimationLevels,
                        other.removeDecimationLevels)
                .append(this.decimationLevelToRetentionPeriod,
                        other.decimationLevelToRetentionPeriod)
                .append(this.enabled, other.enabled)
                .append(this.options, other.options)
                .append(this.addOptions, other.addOptions)
                .append(this.removeOptions, other.removeOptions).isEquals();
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this);
    }

}
