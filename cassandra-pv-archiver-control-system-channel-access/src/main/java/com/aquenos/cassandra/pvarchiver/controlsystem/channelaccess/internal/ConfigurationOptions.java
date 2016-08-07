/*
 * Copyright 2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.controlsystem.channelaccess.internal;

import java.util.Locale;
import java.util.Map;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;

import com.aquenos.cassandra.pvarchiver.controlsystem.channelaccess.ChannelAccessControlSystemSupport;
import com.aquenos.epics.jackie.common.protocol.ChannelAccessEventMask;

/**
 * <p>
 * Configuration options for the {@link ChannelAccessControlSystemSupport}. The
 * options can be specified on a per-server or a per-channel level. If an option
 * is not defined for a specific channel, the value defined for the respective
 * server is used. If a configuration option is not defined for a server, a
 * default value is used.
 * </p>
 * 
 * <p>
 * This class is only intended for use by
 * {@link ChannelAccessControlSystemSupport} and its associated classes.
 * </p>
 * 
 * @author Sebastian Marsching
 */
public class ConfigurationOptions {

    /**
     * Clock source to be used when archiving a sample. The {@link #ORIGIN}
     * clock typically has the advantage that it is closer to the value's real
     * time-stamp because the delay between reading the value from hardware and
     * generating the time stamp is smaller. On the other hand, the
     * {@link #LOCAL} clock might be preferred if the origin clock is not
     * reliable. As a compromise, the {@link #PREFER_ORIGIN} option allows for
     * using the origin clock unless it is significantly skewed compared to the
     * local clock.
     * 
     * @author Sebastian Marsching
     */
    public static enum ClockSource {

        /**
         * Use the local clock (the one of the archive server) for determining
         * the sample's time stamp.
         */
        LOCAL,

        /**
         * Use the time-stamp that was provided with the Channel Access value
         * (typically by the device server) as the sample's time stamp.
         */
        ORIGIN,

        /**
         * Prefer the original time-stamp of the Channel Access value (the same
         * one that is used when specifying the {@link #ORIGIN} clock), but fall
         * back to using the {@link #LOCAL} clock when there is a large skew
         * between the two clocks.
         */
        PREFER_ORIGIN;

    }

    private static final String OPTION_CLOCK_SOURCE = "clockSource";
    private static final String OPTION_ENABLING_CHANNEL = "enablingChannel";
    private static final String OPTION_MAX_CLOCK_SKEW = "maxClockSkew";
    private static final String OPTION_MAX_UPDATE_PERIOD = "maxUpdatePeriod";
    private static final String OPTION_META_DATA_MONITOR_MASK = "metaDataMonitorMask";
    private static final String OPTION_MIN_UPDATE_PERIOD = "minUpdatePeriod";
    private static final String OPTION_MONITOR_MASK = "monitorMask";
    private static final String OPTION_WRITE_SAMPLE_WHEN_DISABLED = "writeSampleWhenDisabled";
    private static final String OPTION_WRITE_SAMPLE_WHEN_DISCONNECTED = "writeSampleWhenDisconnected";

    private ClockSource clockSource;
    private String enablingChannel;
    private long maxClockSkew;
    private long maxUpdatePeriod;
    private ChannelAccessEventMask metaDataMonitorMask;
    private long minUpdatePeriod;
    private ChannelAccessEventMask monitorMask;
    private boolean writeSampleWhenDisabled;
    private boolean writeSampleWhenDisconnected;

    private static boolean parseBoolean(String optionName, String optionValue) {
        if (optionValue.equalsIgnoreCase("TRUE")) {
            return true;
        } else if (optionValue.equalsIgnoreCase("FALSE")) {
            return false;
        } else {
            throwInvalidConfigurationOption(optionName, optionValue,
                    "Invalid value for boolean flag. The value must be \"true\" or \"false\".");
            // This return statement is never used because the method call
            // results in an exception being thrown, but the compiler will
            // complain if we do not have a return statement in this branch.
            return false;
        }
    }

    private static <T extends Enum<T>> T parseEnum(String optionName,
            String optionValue, Class<T> enumType) {
        try {
            // By convention, enum constants should have an upper-case name, so
            // we can simply convert the option value to upper case.
            return Enum.valueOf(enumType,
                    optionValue.toUpperCase(Locale.ENGLISH));
        } catch (IllegalArgumentException e) {
            throwInvalidConfigurationOption(optionName, optionValue,
                    "There is no such enum value for this type.");
            // This return statement is never used because the method call
            // results in an exception being thrown, but the compiler will
            // complain if we do not have a return statement in this branch.
            return null;
        }
    }

    private static ChannelAccessEventMask parseMonitorMask(String optionName,
            String optionValue) {
        String[] tokens = optionValue.split("[\\t |,]");
        ChannelAccessEventMask mask = ChannelAccessEventMask.DBE_NONE;
        for (String token : tokens) {
            if (token.isEmpty()) {
                continue;
            }
            if (token.equalsIgnoreCase("VALUE")) {
                mask = mask.or(ChannelAccessEventMask.DBE_VALUE);
            } else if (token.equalsIgnoreCase("ARCHIVE")) {
                mask = mask.or(ChannelAccessEventMask.DBE_ARCHIVE);
            } else if (token.equalsIgnoreCase("ALARM")) {
                mask = mask.or(ChannelAccessEventMask.DBE_ALARM);
            } else if (token.equalsIgnoreCase("PROPERTY")) {
                mask = mask.or(ChannelAccessEventMask.DBE_PROPERTY);
            } else {
                throwInvalidConfigurationOption(optionName, optionValue,
                        "Invalid token in Channel Access event mask: \""
                                + StringEscapeUtils.escapeJava(token) + "\".");
            }
        }
        if (!mask.isAny()) {
            throwInvalidConfigurationOption(optionName, optionValue,
                    "The Channel Access event mask must not be empty.");
        }
        return mask;
    }

    private static long parseNonNegativeTime(String optionName,
            String optionValue) {
        long nanoseconds = parseTime(optionName, optionValue);
        if (nanoseconds < 0L) {
            throwInvalidConfigurationOption(optionName, optionValue,
                    "The value must not be negative.");
        }
        return nanoseconds;
    }

    private static long parseTime(String optionName, String optionValue) {
        double doubleValue;
        try {
            doubleValue = Double.parseDouble(optionValue);
        } catch (NumberFormatException e) {
            throwInvalidConfigurationOption(optionName, optionValue,
                    "This value does not represent a valid double-precision floating-point number.");
            // This return statement is never used because the method call
            // results in an exception being thrown, but the compiler will
            // complain if we do not have a return statement in this branch.
            return -1L;
        }
        if (Double.isInfinite(doubleValue) || Double.isNaN(doubleValue)) {
            throwInvalidConfigurationOption(optionName, optionValue,
                    "The value must represent a finite number.");
        }
        long nanoseconds = Math.round(doubleValue * 1000000000.0);
        // There is no double number that exactly matches Long.MAX_VALUE or
        // Long.MIN_VALUE. Doubles (like longs) are 64 bit, but some of these
        // 64 bits are used for the exponent. If the rounded number is
        // Long.MAX_VALUE or Long.MIN_VALUE, the original number must already
        // have been outside the range supported by us.
        if (nanoseconds == Long.MAX_VALUE) {
            throwInvalidConfigurationOption(optionName, optionValue,
                    "The specified value is too large.");
        }
        if (nanoseconds == Long.MIN_VALUE) {
            throwInvalidConfigurationOption(optionName, optionValue,
                    "The specified value is too small.");
        }
        // If the long value is zero, but the double value is not, this must be
        // because the specified double value is too close to zero. We throw an
        // exception rather than using zero because a value of zero typically
        // has a special meaning and it might thus be irritating if the user
        // specified a non-zero value but zero is actually used.
        // Math.round also returns zero for NaN, but we handled this case
        // earlier.
        if (nanoseconds == 0L && doubleValue != 0.0) {
            throwInvalidConfigurationOption(optionName, optionValue,
                    "The specified value is too close to zero (its absolute value is too small).");
        }
        return nanoseconds;
    }

    private static void throwInvalidConfigurationOption(String optionName,
            String optionValue, String message) {
        throw new IllegalArgumentException("The control-system option \""
                + StringEscapeUtils.escapeJava(optionName)
                + "\" has the invalid value \""
                + StringEscapeUtils.escapeJava(optionValue) + "\": " + message);
    }

    private static void throwUnknownConfigurationOption(String optionName) {
        throw new IllegalArgumentException("The control-system option \""
                + StringEscapeUtils.escapeJava(optionName)
                + "\" is not supported.");
    }

    /**
     * Creates a configuration for the specified server-wide options. If an
     * option is not specified in <code>serverOptions</code>, a default value is
     * used. An option with a value that is <code>null</code> or the empty
     * string is treated as missing, unless the option name does not refer to a
     * support option at all.
     * 
     * @param serverOptions
     *            map storing configuration options, using their names as keys
     *            and string representations of their values as values.
     * @throws IllegalArgumentException
     *             if the <code>serverOptions</code> map contains a
     *             configuration option that is not supported or a configuration
     *             option that has a value that is not valid for that option.
     * @throws NullPointerException
     *             if <code>serverOptions</code> is null or contains
     *             <code>null</code> keys.
     */
    public ConfigurationOptions(Map<String, String> serverOptions) {
        // First, we initialize the configuration with the default values.
        this.clockSource = ClockSource.PREFER_ORIGIN;
        this.enablingChannel = null;
        this.maxClockSkew = 30000000000L;
        this.maxUpdatePeriod = 0L;
        this.metaDataMonitorMask = ChannelAccessEventMask.DBE_PROPERTY;
        this.minUpdatePeriod = 0L;
        this.monitorMask = ChannelAccessEventMask.DBE_ARCHIVE
                .or(ChannelAccessEventMask.DBE_ALARM);
        this.writeSampleWhenDisabled = false;
        this.writeSampleWhenDisconnected = false;
        // Second, we iterate over the configuration options, explicitly setting
        // the value if a non-null, non-empty string is provided for the option.
        initializeConfigurationFromMap(serverOptions);
    }

    /**
     * Create a configuration for the specified channel-specific options. If an
     * option is not specified in the <code>channelOptions</code> the
     * corresponding value from the <code>serverOptions</code> is used. An
     * option with a value that is <code>null</code> or the empty string is
     * treated as missing, unless the option name does not refer to a support
     * option at all.
     * 
     * @param channelOptions
     *            map storing configuration options, using their names as keys
     *            and string representations of their values as values.
     * @param serverOptions
     *            server-wide options that shall be used as defaults when an
     *            option is not specified in the <code>channelOptions</code>.
     * @throws IllegalArgumentException
     *             if the <code>channelOptions</code> map contains a
     *             configuration option that is not supported or a configuration
     *             option that has a value that is not valid for that option.
     * @throws NullPointerException
     *             if <code>channelOptions</code> or <code>serverOptions</code>
     *             is null or <code>channelOption</code> contains
     *             <code>null</code> keys.
     */
    public ConfigurationOptions(Map<String, String> channelOptions,
            ConfigurationOptions serverOptions) {
        // First, we initialize the configuration with the values from the
        // server configuration.
        this.clockSource = serverOptions.getClockSource();
        this.enablingChannel = serverOptions.getEnablingChannel();
        this.maxClockSkew = serverOptions.getMaxClockSkew();
        this.maxUpdatePeriod = serverOptions.getMaxUpdatePeriod();
        this.metaDataMonitorMask = serverOptions.getMetaDataMonitorMask();
        this.minUpdatePeriod = serverOptions.getMinUpdatePeriod();
        this.monitorMask = serverOptions.getMonitorMask();
        this.writeSampleWhenDisabled = serverOptions
                .isWriteSampleWhenDisabled();
        this.writeSampleWhenDisconnected = serverOptions
                .isWriteSampleWhenDisconnected();
        // Second, we iterate over the configuration options, explicitly setting
        // the value if a non-null, non-empty string is provided for the option.
        initializeConfigurationFromMap(channelOptions);
    }

    /**
     * <p>
     * Returns the clock source that is used when archiving samples. Each time,
     * a sample is archive, the time stamp that shall be used for the sample has
     * to be determined. When using the {@linkplain ClockSource#ORIGIN origin}
     * clock source, the time stamp provided by the Channel Access server when
     * sending the value that is going to be written with the sample is used.
     * When using the {@linkplain ClockSource#LOCAL local} clock source, the
     * time of the local system-clock (of the archive server) is used.
     * </p>
     * 
     * <p>
     * The {@linkplain ClockSource#PREFER_ORIGIN prefer-origin} option only
     * makes sense with a {@linkplain #getMaxClockSkew() maximum clock skew}
     * that is greater than zero: In this case, the time stamp that was sent by
     * the Channel Access server is used unless the difference between this time
     * stamp and the locally generated time-stamp is too large. If the
     * difference is greater than the specified maximum clock skew, the local
     * time-stamp is used. When the maximum clock skew is zero, using this
     * option has the same effect as using the <code>ORIGIN</code> option.
     * </p>
     * 
     * <p>
     * The default value is <code>PREFER_ORIGIN</code>.
     * <p>
     * 
     * @return clock source used when archiving samples.
     */
    public ClockSource getClockSource() {
        return clockSource;
    }

    /**
     * <p>
     * Returns the name of the enabling channel for this channel. If this
     * channel does not have an enabling channel (which means that it is always
     * enabled when it is enabled in the archive configuration), this method
     * returns <code>null</code>.
     * </p>
     * 
     * <p>
     * An enabling channel can be used in order to conditionally enable or
     * disable archiving for a channel at runtime. For example, archiving for a
     * channel might only be enabled if the device providing the archived
     * channel is in a certain state of operation.
     * </p>
     * 
     * <p>
     * When this configuration option is set (not <code>null</code>) the channel
     * might be disabled even when it is statically enabled in the archive
     * configuration. It is only enabled when it is both statically enabled in
     * the archive configuration and when the enabled channel can be connected
     * and has a value that indicates that the channel shall be enabled.
     * </p>
     * 
     * <p>
     * When the enabling channel's value is of an integer type, this channel is
     * enabled if the enabling channel's value is non-zero. If the enabling
     * channel's value is of a floating-point type, this channel is enabled when
     * the enabling channel's value is neither zero nor not-a-number. When the
     * enabling channel's value is of a string type, this channel is enabled
     * when the enabling channel's value is neither the empty string, nor "0",
     * "false", "no", or "off". Leading and trailing white-space is ignored for
     * this comparison and the comparison is not case sensitive.
     * </p>
     * 
     * <p>
     * The default value is <code>null</code>, which means that the channel is
     * always enabled when it is statically enabled in the archive
     * configuration.
     * </p>
     * 
     * @return name of the enabling channel for this channel or
     *         <code>null</code> if this channel does not have an enabling
     *         channel.
     */
    public String getEnablingChannel() {
        return enablingChannel;
    }

    /**
     * <p>
     * Returns maximum clock skew that is allowed (in nanoseconds). The maximum
     * clock skew defines how much the time stamp provided by the Channel Access
     * server with a value may differ from the local system-time.
     * </p>
     * 
     * <p>
     * If the difference between the original value time and the local
     * system-time is greater than the limit specified by this option, the
     * behavior depends on the {@linkplain ClockSource clock-source option}. If
     * <code>ORIGIN</code>, the received sample is discarded. If
     * <code>PREFER_ORIGIN</code>, the time stamp from the local system-clock is
     * used instead of the original time stamp.
     * </p>
     * 
     * <p>
     * If this option is zero, the clock skew check is disabled, meaning that
     * the original time stamp is always used when the clock source is set to
     * <code>ORIGIN</code> or <code>PREFER_ORIGIN</code>.
     * </p>
     * 
     * <p>
     * If the {@linkplain #getClockSource() clock source} is set to
     * <code>LOCAL</code> this option does not have any effect.
     * </p>
     * 
     * <p>
     * The default value is 30 seconds (30,000,000,000 nanoseconds).
     * </p>
     * 
     * @return maximum clock skew (in nanoseconds). The returned value always
     *         positive or zero. A value of zero indicates that the clock skew
     *         check should be disabled.
     */
    public long getMaxClockSkew() {
        return maxClockSkew;
    }

    /**
     * <p>
     * Returns the maximum update period (in nanoseconds). The maximum update
     * period is the maximum time that passes between writing two samples
     * (measured by comparing the local system clock, not the sample
     * time-stamps).
     * </p>
     * 
     * <p>
     * When a sample is written and no other sample arrives within the specified
     * period, the sample that has been written before is written again using an
     * updated time stamp. Due to processing delays, the actual period between
     * writing the two samples might be slightly greater than the specified
     * period. For obvious reasons, this time stamp is always generated using
     * the local system clock, regardless of the {@linkplain #getClockSource()
     * clock-source} setting. For this reason, the same considerations regarding
     * time-stamps that apply when writing
     * {@linkplain #isWriteSampleWhenDisabled() disabled} or
     * {@linkplain #isWriteSampleWhenDisconnected() disconnected} samples also
     * apply to to this option.
     * </p>
     * 
     * <p>
     * When this configuration option is zero, the mechanism is disabled,
     * meaning that each sample is only written once (unless the Channel Access
     * server sends it more than once).
     * </p>
     * 
     * <p>
     * Setting this option to a non-zero value can be useful to distinguish
     * situations where the archive server is not running from situations where
     * a channel's value simply does not change for a prolonged period of time.
     * When using decimated samples, however, this is of limited use because the
     * decimation algorithm is currently not able to detect such a situation,
     * even when this option is used. This means that the period where samples
     * are missing will be visible in the raw samples, but decimated samples are
     * still going to be generated for that period.
     * </p>
     * 
     * <p>
     * When both the maximum update period and the
     * {@linkplain #getMinUpdatePeriod() minimum update period} are specified
     * (non-zero), the maximum update period must be equal to or greater than
     * the minimum update period.
     * </p>
     * 
     * <p>
     * The default value is zero.
     * </p>
     * 
     * @return maximum update period (in nanoseconds) after which a sample is
     *         repeated when no other sample has been received. A value of zero
     *         indicates that there is no upper limit on the time period between
     *         two samples and samples are not repeated with an updated
     *         time-stamp.
     */
    public long getMaxUpdatePeriod() {
        return maxUpdatePeriod;
    }

    /**
     * <p>
     * Returns the minimum update period (in nanoseconds). The minimum update
     * period is the minimum time that must pass between writing two samples
     * (measured by comparing the local system clock when the samples are
     * written, not by comparing their time stamps).
     * </p>
     * 
     * <p>
     * When a sample arrives less than the specified period after writing the
     * last sample, that new sample is not written. However, it is going to be
     * written later when the remaining period passes without another sample
     * arriving. When another sample arrives while waiting for the period to
     * pass, the sample received earlier is discarded.
     * </p>
     * 
     * <p>
     * When this configuration option is zero, there is no lower limit on the
     * time period between two samples. This means that new samples are written
     * as they arrive. Samples might still be lost when they arrive at a higher
     * rate than they can be written to the database, but unlike the limit
     * imposed by this configuration option, that limit is only due to
     * performance limitations and depends on the environment and system load.
     * </p>
     * 
     * <p>
     * When both the {@linkplain #getMaxUpdatePeriod() maximum update period}
     * and the minimum update period are specified (non-zero), the maximum
     * update period must be equal to or greater than the minimum update period.
     * </p>
     * 
     * <p>
     * The default value is zero.
     * </p>
     * 
     * @return minimum time period between writing two samples (in nanoseconds).
     *         A value of zero indicates that there is no lower limit on the
     *         time period and thus samples are written at an unbounded rate.
     * @see #getMaxUpdatePeriod()
     */
    public long getMinUpdatePeriod() {
        return minUpdatePeriod;
    }

    /**
     * <p>
     * Returns the event mask that is used for monitoring meta-data changes.
     * Meta-data is additional information for a channel that has to be
     * monitored separately from its alarm state, its value, and its time-stamp.
     * The meta-data can include information like engineering units, display
     * limits, alarm limits, etc.
     * </p>
     * 
     * <p>
     * Monitor events for the meta-data monitor do not directly result in
     * samples being written to the archive. Instead, the meta-data is saved in
     * memory and used the next time a value or alarm-state update is received
     * through the regular monitor.
     * </p>
     * 
     * <p>
     * This option should usually not be changed, unless one has to deal with a
     * server that does not support the <code>DBE_PROPERTY</code> mask.
     * </p>
     * 
     * <p>
     * The default value is <code>DBE_PROPERTY</code>.
     * <p>
     * 
     * @return event mask used for the meta-data monitor.
     * @see #getMonitorMask()
     */
    public ChannelAccessEventMask getMetaDataMonitorMask() {
        return metaDataMonitorMask;
    }

    /**
     * <p>
     * Returns the event mask that is used for monitoring alarm-state and value
     * changes.
     * </p>
     * 
     * <p>
     * Typically, each event on the corresponding monitor results in a sample
     * being written in the archive. Therefore, this event mask controls the
     * amount of data written into the archive.
     * </p>
     * 
     * <p>
     * The default value is <code>DBE_ARCHIVE | DBE_ALARM</code> (send a monitor
     * event on value changes marked for archiving and on alarm-state changes).
     * <p>
     * 
     * @return event mask used for the alarm-state and value monitor.
     * @see #getMetaDataMonitorMask()
     */
    public ChannelAccessEventMask getMonitorMask() {
        return monitorMask;
    }

    /**
     * <p>
     * Tells whether a sample should be written when a channel is disabled. If
     * <code>true</code>, a special sample acting as a marker for the disabled
     * state is written to the archive when a channel is disabled in the archive
     * configuration or is temporarily disabled because a condition for enabling
     * archiving is not met. This way, one can tell from the archived data
     * whether a value simply did not change for an extended period of time or
     * no updates could be received because the channel was not enabled at all.
     * </p>
     * 
     * <p>
     * For obvious reasons, there is no time from the origin when writing such a
     * sample. Therefore, the local system-time on the archiving server is used
     * for the sample's time-stamp. This has two possible effects: If the clock
     * on the origin server is skewed into the future, the disabled marker
     * sample might be discarded because it will get a time stamp that is less
     * than the time stamp of the most recent sample that has already been
     * written. If the clock on the origin server is skewed into the past, the
     * sample received when the channel is enabled might have a time stamp that
     * is less than the time stamp of the disabled marker sample and might thus
     * be discarded, giving the impression that the channel was disabled longer
     * than it actually was and losing samples.
     * </p>
     * 
     * <p>
     * A similar problem might arise if the clocks are well-synchronized but the
     * channel's time-stamp is only updated rarely (e.g. only when the channel's
     * value changes in the backing hardware) and thus the sample sent when the
     * channel is enabled has still the old time-stamp. In this case, no sample
     * are lost, but from the archived data of the channel will look like the
     * channel has been disabled for a longer time (until a sample with a new
     * time-stamp is sent).
     * </p>
     * 
     * <p>
     * For these reasons, this configuration option should only be enabled if
     * the channel either uses the archive server's local system-clock as a
     * time-stamp source or if the clocks are well synchronized and the
     * channel's time-stamp is updated frequently.
     * </p>
     * 
     * <p>
     * The default value is <code>false</code> (do not write a sample when a
     * channel is disabled).
     * </p>
     * 
     * @return <code>true</code> if a marker sample should be written when a
     *         channel is disabled, <code>false</code> if simply no samples
     *         should be written for a disabled channel.
     */
    public boolean isWriteSampleWhenDisabled() {
        return writeSampleWhenDisabled;
    }

    /**
     * <p>
     * Tells whether a sample should be written when a channel is disconnected.
     * If <code>true</code>, a special sample acting as a marker for the
     * disconnected state is written to the archive when a channel disconnects
     * (or cannot be connected when the channel is initialized). This way, one
     * can tell from the archived data whether a value simply did not change for
     * an extended period of time or no updates could be received because the
     * channel was not connected at all.
     * </p>
     * 
     * <p>
     * For obvious reasons, there is no time from the origin when writing such a
     * sample. Therefore, the local system-time on the archiving server is used
     * for the sample's time-stamp. This has two possible effects: If the clock
     * on the origin server is skewed into the future, the disconnect marker
     * sample might be discarded because it will get a time stamp that is less
     * than the time stamp of the most recent sample that has already been
     * written. If the clock on the origin server is skewed into the past, the
     * sample received when the channel reconnects might have a time stamp that
     * is less than the time stamp of the disconnect marker sample and might
     * thus be discarded, giving the impression that the channel was
     * disconnected longer than it actually was and losing samples.
     * </p>
     * 
     * <p>
     * A similar problem might arise if the clocks are well-synchronized but the
     * channel's time-stamp is only updated rarely (e.g. only when the channel's
     * value changes in the backing hardware) and thus the sample sent when the
     * channel reconnects has still the old time-stamp. In this case, no sample
     * are lost, but from the archived data of the channel will look like the
     * channel has been disconnected for a longer time (until a sample with a
     * new time-stamp is sent).
     * </p>
     * 
     * <p>
     * For these reasons, this configuration option should only be enabled if
     * the channel either uses the archive server's local system-clock as a
     * time-stamp source or if the clocks are well synchronized and the
     * channel's time-stamp is updated frequently.
     * </p>
     * 
     * <p>
     * The default value is <code>false</code> (do not write a sample when a
     * channel is disconnected).
     * </p>
     * 
     * @return <code>true</code> if a marker sample should be written when a
     *         channel is disconnected, <code>false</code> if simply no samples
     *         should be written for a disconnected channel.
     */
    public boolean isWriteSampleWhenDisconnected() {
        return writeSampleWhenDisconnected;
    }

    private void initializeConfigurationFromMap(Map<String, String> options) {
        for (Map.Entry<String, String> configurationOption : options.entrySet()) {
            String optionName = configurationOption.getKey();
            String optionValue = configurationOption.getValue();
            switch (optionName) {
            case OPTION_CLOCK_SOURCE:
                if (!StringUtils.isEmpty(optionValue)) {
                    this.clockSource = parseEnum(optionName, optionValue,
                            ClockSource.class);
                }
                break;
            case OPTION_ENABLING_CHANNEL:
                if (!StringUtils.isEmpty(optionValue)) {
                    this.enablingChannel = optionValue;
                }
                break;
            case OPTION_MAX_CLOCK_SKEW:
                if (!StringUtils.isEmpty(optionValue)) {
                    this.maxClockSkew = parseNonNegativeTime(optionName,
                            optionValue);
                }
                break;
            case OPTION_MAX_UPDATE_PERIOD:
                if (!StringUtils.isEmpty(optionValue)) {
                    this.maxUpdatePeriod = parseNonNegativeTime(optionName,
                            optionValue);
                }
                break;
            case OPTION_META_DATA_MONITOR_MASK:
                if (!StringUtils.isEmpty(optionValue)) {
                    this.metaDataMonitorMask = parseMonitorMask(optionName,
                            optionValue);
                }
                break;
            case OPTION_MIN_UPDATE_PERIOD:
                if (!StringUtils.isEmpty(optionValue)) {
                    this.minUpdatePeriod = parseNonNegativeTime(optionName,
                            optionValue);
                }
                break;
            case OPTION_MONITOR_MASK:
                if (!StringUtils.isEmpty(optionValue)) {
                    this.metaDataMonitorMask = parseMonitorMask(optionName,
                            optionValue);
                }
                break;
            case OPTION_WRITE_SAMPLE_WHEN_DISABLED:
                if (!StringUtils.isEmpty(optionValue)) {
                    this.writeSampleWhenDisabled = parseBoolean(optionName,
                            optionValue);
                }
                break;
            case OPTION_WRITE_SAMPLE_WHEN_DISCONNECTED:
                if (!StringUtils.isEmpty(optionValue)) {
                    this.writeSampleWhenDisconnected = parseBoolean(optionName,
                            optionValue);
                }
                break;
            default:
                throwUnknownConfigurationOption(optionName);
            }
        }
        if (maxUpdatePeriod != 0L && minUpdatePeriod != 0L
                && maxUpdatePeriod < minUpdatePeriod) {
            // One of the two options might have been inherited from the server
            // configuration, so we have to throw the exception for the option
            // that has been specified. One of the two options has to be present
            // because otherwise both options from the server configuration are
            // used and those have been verified earlier.
            if (options.containsKey(OPTION_MAX_UPDATE_PERIOD)) {
                throwInvalidConfigurationOption("maxUpdatePeriod",
                        options.get(OPTION_MAX_UPDATE_PERIOD),
                        "The maxUpdatePeriod cannot be less than the minUpdatePeriod.");
            } else if (options.containsKey(OPTION_MIN_UPDATE_PERIOD)) {
                throwInvalidConfigurationOption("minUpdatePeriod",
                        options.get(OPTION_MAX_UPDATE_PERIOD),
                        "The minUpdatePeriod cannot be greater than the maxUpdatePeriod.");
            } else {
                throw new RuntimeException(
                        "Invalid values for maxUpdatePeriod / minUpdatePeriod in parent configuration object. This indicates a bug in the software.");
            }
        }
    }

}
