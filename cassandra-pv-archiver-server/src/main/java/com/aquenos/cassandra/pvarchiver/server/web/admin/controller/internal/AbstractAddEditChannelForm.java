/*
 * Copyright 2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.web.admin.controller.internal;

import java.util.NavigableMap;

import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import com.aquenos.cassandra.pvarchiver.server.web.admin.controller.UiController;

/**
 * <p>
 * Abstract base class for form-backing beans that are used for adding and
 * editing channels.
 * </p>
 * 
 * <p>
 * This class is only intended for use by the {@link UiController} and its
 * associated views.
 * </p>
 * 
 * @author Sebastian Marsching
 */
public abstract class AbstractAddEditChannelForm {

    /**
     * Bean storing a control-system-specific configuration option associated
     * with a channel.
     * 
     * @author Sebastian Marsching
     *
     */
    public static final class ControlSystemOption {

        @NotNull
        private String name;
        @NotNull
        private String value;

        /**
         * Returns the option name.
         * 
         * @return option name or <code>null</code> if this bean has not been
         *         properly initialized yet.
         */
        public String getName() {
            return name;
        }

        /**
         * Sets the option name. A <code>null</code> value is acceptable,
         * however this will later result in a validation error.
         * 
         * @param name
         *            option name (may be <code>null</code>).
         */
        public void setName(String name) {
            this.name = name;
        }

        /**
         * Returns the option value.
         * 
         * @return option value or <code>null</code> if this bean has not been
         *         properly initialized yet.
         */
        public String getValue() {
            return value;
        }

        /**
         * Returns the option value. A <code>null</code> value is acceptable,
         * however this will later result in a validation error.
         * 
         * @param value
         *            option value (may be <code>null</code>).
         */
        public void setValue(String value) {
            this.value = value;
        }

    }

    /**
     * Bean storing the configuration for a decimation level.
     * 
     * @author Sebastian Marsching
     */
    public final static class DecimationLevel {

        @NotNull
        @Valid
        private TimePeriod decimationPeriod;
        @NotNull
        @Valid
        private TimePeriod retentionPeriod;

        /**
         * Returns the decimation period.
         * 
         * @return decimation period or <code>null</code> if this bean has not
         *         been properly initialized yet.
         */
        public TimePeriod getDecimationPeriod() {
            return decimationPeriod;
        }

        /**
         * Sets the decimation period. A <code>null</code> value is acceptable,
         * however this will later result in a validation error.
         * 
         * @param decimationPeriod
         *            decimation period (may be <code>null</code>).
         */
        public void setDecimationPeriod(TimePeriod decimationPeriod) {
            this.decimationPeriod = decimationPeriod;
        }

        /**
         * Returns the decimation period.
         * 
         * @return retention period or <code>null</code> if this bean has not
         *         been properly initialized yet.
         */
        public TimePeriod getRetentionPeriod() {
            return retentionPeriod;
        }

        /**
         * Sets the retention period. A <code>null</code> value is acceptable,
         * however this will later result in a validation error.
         * 
         * @param retentionPeriod
         *            retention period (may be <code>null</code>).
         */
        public void setRetentionPeriod(TimePeriod retentionPeriod) {
            this.retentionPeriod = retentionPeriod;
        }

    }

    /**
     * Bean storing a time period. A time period can be specified in different
     * units and must be non-negative to pass validation.
     * 
     * @author Sebastian Marsching
     */
    public static final class TimePeriod {

        @Min(0)
        @NotNull
        private Integer counts;
        @NotNull
        private TimePeriodUnit unit;

        /**
         * Returns the counts of this period. The counts combined with the unit
         * of this period define the resulting time period.
         * 
         * @return count of time units or <code>null</code> if this bean has not
         *         been properly initialized yet.
         */
        public Integer getCounts() {
            return counts;
        }

        /**
         * Sets the counts of this period. The counts combined with the unit of
         * this period define the resulting time period. A <code>null</code>
         * value or negtive value is acceptable, however this will later result
         * in a validation error.
         * 
         * @param counts
         *            count of time units (may be <code>null</code>).
         */
        public void setCounts(Integer counts) {
            this.counts = counts;
        }

        /**
         * Returns the unit used for this period. The unit combined with the
         * counts of this period defines the resulting time period.
         * 
         * @return time unit or <code>null</code> if this bean has not been
         *         properly initialized yet.
         */
        public TimePeriodUnit getUnit() {
            return unit;
        }

        /**
         * Sets the unit used for this period. The unit combined with the counts
         * of this period defines the resulting time period. A <code>null</code>
         * value is acceptable, however this will later result in a validation
         * error.
         * 
         * @param unit
         *            time unit (may be <code>null</code>).
         */
        public void setUnit(TimePeriodUnit unit) {
            this.unit = unit;
        }

        /**
         * Returns the time period represented by this object in seconds,
         * regardless of the unit that is used to define this time period. If
         * the time unit is not {@link TimePeriodUnit#SECONDS} or
         * {@link TimePeriodUnit#ZERO} and multiplying the counts with the
         * number of seconds per unit would result in a number exceeding
         * {@link Integer#MAX_VALUE}, an {@link IllegalStateException} is
         * thrown. Such an exception is also thrown if the number of counts is
         * negative.
         * 
         * @return this time period in seconds or <code>null</code> if the
         *         number of counts or the time unit of this period is
         *         <code>null</code>.
         * @throws IllegalArgumentException
         *             if the number of counts is negative or too big so that it
         *             would result in a number greter than
         *             {@link Integer#MAX_VALUE} after being multiplied with the
         *             number of seconds per time unit.
         */
        public Integer getSeconds() {
            if (counts == null || unit == null) {
                return null;
            }
            switch (unit) {
            case ZERO:
                return 0;
            case SECONDS:
                return counts;
            case MINUTES:
                if (counts > 35791394) {
                    throw new IllegalStateException("The number of minutes ("
                            + counts
                            + ") is too large to be represented in seconds.");
                } else {
                    return counts * 60;
                }
            case HOURS:
                if (counts > 596523) {
                    throw new IllegalStateException("The number of hours ("
                            + counts
                            + ") is too large to be represented in seconds.");
                } else {
                    return counts * 3600;
                }
            case DAYS:
                if (counts > 24855) {
                    throw new IllegalStateException("The number of days ("
                            + counts
                            + ") is too large to be represented in seconds.");
                } else {
                    return counts * 86400;
                }
            default:
                throw new IllegalStateException(
                        "The current time-period unit (" + unit
                                + ") is not supported.");
            }
        }
    }

    /**
     * Time units supported by the {@link TimePeriod}.
     * 
     * @author Sebastian Marsching
     *
     */
    public static enum TimePeriodUnit {

        /**
         * Special unit for zero periods. If this unit is used, the period is
         * zero regardless of the number of counts.
         */
        ZERO,

        /**
         * Seconds.
         */
        SECONDS,

        /**
         * Minutes.
         */
        MINUTES,

        /**
         * Hours.
         */
        HOURS,

        /**
         * Days.
         */
        DAYS

    }

    @Valid
    private NavigableMap<Integer, DecimationLevel> decimationLevels;
    @NotNull
    private Boolean enabled;
    @Valid
    private NavigableMap<Integer, ControlSystemOption> options;

    /**
     * Default constructor. The constructor is private so that only classes in
     * this package can extends this class.
     */
    AbstractAddEditChannelForm() {
    }

    /**
     * Returns the decimation levels that are stored in this bean. The keys used
     * for this map form an artificial index. Only the zero key has a special
     * meaning because it should always represent the raw samples decimation
     * period.
     * 
     * @return decimation levels stored in this bean or <code>null</code> if the
     *         decimation levels have not been set.
     */
    public NavigableMap<Integer, DecimationLevel> getDecimationLevels() {
        return decimationLevels;
    }

    /**
     * Sets the decimation levels that are stored in this bean. The keys used
     * for this map form an artificial index. Only the zero key has a special
     * meaning because it should always represent the raw samples decimation
     * period.
     * 
     * @param decimationLevels
     *            decimation levels to be stored in this bean (may be
     *            <code>null</code>). The map is used as-is without copying it.
     */
    public void setDecimationLevels(
            NavigableMap<Integer, DecimationLevel> decimationLevels) {
        this.decimationLevels = decimationLevels;
    }

    /**
     * Returns the state of the archiving enabled flag. <code>true</code> means
     * that archiving for the channel shall be enabled, <code>false</code> means
     * that it shall be disabled.
     * 
     * @return archiving enabled flag or <code>null</code> if the flag has not
     *         been initialized yet.
     */
    public Boolean getEnabled() {
        return enabled;
    }

    /**
     * Sets the archiving enabled flag. <code>true</code> means that archiving
     * for the channel shall be enabled, <code>false</code> means that it shall
     * be disabled. A <code>null</code> value is acceptable, however this will
     * later result in a validation error.
     * 
     * @param enabled
     *            archiving enabled flag (may be <code>null</code>).
     */
    public void setEnabled(Boolean enabled) {
        this.enabled = enabled;
    }

    /**
     * Returns the map of control-system-specific configuration options. The
     * keys used for this map form an artificial index.
     * 
     * @return control-system options stored in this bean or <code>null</code>
     *         if the control-system options have not been set.
     */
    public NavigableMap<Integer, ControlSystemOption> getOptions() {
        return options;
    }

    /**
     * Sets the control-system-specific configuration options that are stored in
     * this bean. The keys used for this map form an artificial index.
     * 
     * @param options
     *            control-system options to be stored in this bean (may be
     *            <code>null</code>). The map is used as-is without copying it.
     */
    public void setOptions(NavigableMap<Integer, ControlSystemOption> options) {
        this.options = options;
    }

}
