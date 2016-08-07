/*
 * Copyright 2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.archiving.internal;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;

/**
 * Utility methods dealing with the archive configuration. The methods in this
 * class are only intended for use by the classes in the same package and the
 * <code>com.aquenos.cassandra.pvarchiver.server.archiving</code> package. For
 * performance reasons, some of the methods do not sanitize their inputs,
 * leading to undefined behavior if inputs are invalid. Please refer to the
 * method descriptions for details.
 * 
 * @author Sebastian Marsching
 */
public final class ArchiveConfigurationUtils {

    private ArchiveConfigurationUtils() {
    }

    // The compiler might be clever enough to automatically detect when we are
    // using primitive constants when an object is needed. However, if it is not
    // clever, we might end up with a lot of autoboxed integers. This here
    // should help avoid the problem completely.
    private static final Integer ZERO = Integer.valueOf(0);

    /**
     * <p>
     * Copies a map of control-system specific options and verifies that it is
     * well-formed.
     * </p>
     * 
     * <p>
     * This method creates an independent, unmodifiable copy of the map. This
     * method also verifies that the specified map contains no <code>null</code>
     * keys or values. It throws an exception if it finds such keys or values.
     * </p>
     * 
     * @param options
     *            map using option names for the keys and the corresponding
     *            option values for the values. If <code>null</code>, an empty
     *            map is used as the input for this method.
     * @param mapName
     *            name of the variable that holds the map in the calling
     *            function. This information is used when constructing exception
     *            messages.
     * @return sorted, unmodifiable copy of the specified map.
     * @throws NullPointerException
     *             if the specified map contains <code>null</code> keys or
     *             values.
     */
    public static SortedMap<String, String> copyAndVerifyOptionsMap(
            Map<String, String> options, String mapName) {
        // We want to treat a null value like an empty map. As we do not have
        // to run any verification code for an empty map, we can simply return
        // the empty map directly.
        if (options == null) {
            return ImmutableSortedMap.of();
        }
        // We want to make an independent copy of the map. So we create a new
        // map and copy the entries while checking them.
        TreeMap<String, String> newOptions = new TreeMap<String, String>();
        for (Map.Entry<String, String> option : options.entrySet()) {
            String name = option.getKey();
            String value = option.getValue();
            // We do not allow null keys or values.
            if (name == null || value == null) {
                throw new NullPointerException("The " + mapName
                        + " map must not contain null keys or values.");
            }
            newOptions.put(name, value);
        }
        // Before returning the map, we make sure that it cannot be modified any
        // longer.
        return Collections.unmodifiableSortedMap(newOptions);
    }

    /**
     * <p>
     * Copies a set of control-system-specific option names.
     * </p>
     * 
     * <p>
     * This method creates an independent, unmodifiable copy of the set. This
     * method also verifies that the specified set contains no <code>null</code>
     * elements. It throws an exception if it finds such elements.
     * </p>
     * 
     * @param options
     *            set of option names. If <code>null</code>, an empty set is
     *            used as the input for this method.
     * @param setName
     *            name of the variable that holds the set in the calling
     *            function. This information is used when constructing exception
     *            messages.
     * @return sorted, unmodifiable copy of the specified set.
     * @throws NullPointerException
     *             if the specified map contains <code>null</code> elements.
     */
    public static SortedSet<String> copyAndVerifyOptionsSet(
            Set<String> options, String setName) {
        // We want to treat a null value like an empty set. As we do not have
        // to run any verification code for an empty set, we can simply return
        // the empty set directly.
        if (options == null) {
            return ImmutableSortedSet.of();
        }
        // We want to make an independent copy of the set. So we create a new
        // map and copy the entries while checking them.
        TreeSet<String> newOptions = new TreeSet<String>();
        for (String optionName : options) {
            // We do not allow null elements.
            if (optionName == null) {
                throw new NullPointerException("The " + setName
                        + " set must not contain null elements.");
            }
            newOptions.add(optionName);
        }
        // Before returning the set, we make sure that it cannot be modified any
        // longer.
        return Collections.unmodifiableSortedSet(newOptions);
    }

    /**
     * <p>
     * Normalizes a set of decimation levels and verifies that it is
     * well-formed. This method is designed to be used when a set of decimation
     * levels is supposed to <em>not</em> contain the raw (zero period)
     * decimation level. If the zero element is contained in the specified set,
     * it is silently removed in the returned set.
     * </p>
     * 
     * <p>
     * This method creates an independent, unmodifiable copy of the set. This
     * method also verifies that the specified set contains no <code>null</code>
     * or negative elements. It throws an exception if it finds such elements.
     * </p>
     * 
     * @param decimationLevels
     *            set of decimation levels to be copied, normalized, and
     *            verified. If <code>null</code>, an empty set is used as the
     *            input for this method.
     * @param setName
     *            name of the variable that holds the set in the calling
     *            function. This information is used when constructing exception
     *            messages.
     * @return sorted, unmodifiable copy of the specified set that does not
     *         contain the zero element.
     * @throws IllegalArgumentException
     *             if the specified set contains negative elements.
     * @throws NullPointerException
     *             if the specified set contains <code>null</code> elements.
     * @see #normalizeAndVerifyDecimationLevelsSet(Set)
     */
    public static SortedSet<Integer> normalizeAndVerifyDecimationLevelsSetWithoutRawLevel(
            Set<Integer> decimationLevels, String setName) {
        // If the decimationLevels set is null, we want to treat it like an
        // empty on.
        if (decimationLevels == null) {
            return ImmutableSortedSet.of();
        }
        SortedSet<Integer> newDecimationLevels = copyAndVerifyDecimationLevelsSet(
                decimationLevels, setName);
        // We do not want to include the raw decimation level, so we remove it,
        // if it is present. Calling remove for an element that is not present
        // is safe, so we do not need an if clause here.
        newDecimationLevels.remove(ZERO);
        // Before returning the set, we make sure that it cannot be modified any
        // longer.
        return Collections.unmodifiableSortedSet(newDecimationLevels);
    }

    /**
     * <p>
     * Normalizes a set of decimation levels and verifies that it is
     * well-formed. This method is designed to be used when a set of decimation
     * levels is supposed to contain the raw (zero period) decimation level. If
     * the zero element is not contained in the specified set, it is silently
     * added in the returned set.
     * </p>
     * 
     * <p>
     * This method creates an independent, unmodifiable copy of the set. This
     * method also verifies that the specified set contains no <code>null</code>
     * or negative elements. It throws an exception if it finds such elements.
     * </p>
     * 
     * @param decimationLevels
     *            set of decimation levels to be copied, normalized, and
     *            verified. If <code>null</code>, an empty set is used as the
     *            input for this method.
     * @return sorted, unmodifiable copy of the specified set that always
     *         contains the zero element.
     * @throws IllegalArgumentException
     *             if the specified set contains negative elements.
     * @throws NullPointerException
     *             if the specified set contains <code>null</code> elements.
     */
    public static SortedSet<Integer> normalizeAndVerifyDecimationLevelsSet(
            Set<Integer> decimationLevels) {
        // If the decimationLevels set is null, we want to treat it like an
        // empty on. As the decimation levels set should always contain the raw
        // (zero) decimation level, we take a shortcut and simply return a
        // singleton set with the zero element.
        if (decimationLevels == null) {
            return ImmutableSortedSet.of(ZERO);
        }
        SortedSet<Integer> newDecimationLevels = copyAndVerifyDecimationLevelsSet(
                decimationLevels, "decimationLevels");
        // A channel always has a decimation level of zero (for raw samples). If
        // this decimation level is not listed explicitly, we add it. Actually,
        // we can simply add it always because this is a no-op if it is already
        // present.
        newDecimationLevels.add(ZERO);
        // Before returning the set, we make sure that it cannot be modified any
        // longer.
        return Collections.unmodifiableSortedSet(newDecimationLevels);
    }

    /**
     * <p>
     * Normalizes a map of decimation-level to retention-period mappings and
     * verifies that it is well-formed. This method is designed to be used when
     * a map of decimation-level to retention-period mappings is supposed to be
     * used with a known set of decimation levels.
     * </p>
     * 
     * <p>
     * This method creates an independent, unmodifiable copy of the map. This
     * method also verifies that the specified map contains no <code>null</code>
     * or negative keys or values. It throws an exception if it finds such keys
     * or values.
     * </p>
     * 
     * <p>
     * This method removes all mappings that are for decimation levels that are
     * not also contained in the specified set of decimation levels. It adds a
     * mapping for all decimation levels that are contained in the specified set
     * of decimation levels but do not have a corresponding mapping in the
     * specified map. When such a mapping is added, a retention period of zero
     * is used.
     * </p>
     * 
     * <p>
     * After making the previously described modifications, it is verified that
     * the retention periods in the map match the constraint that the retention
     * period of each decimation level must be greater than or equal to the
     * decimation period of the preceding decimation level. The preceding
     * decimation level is the decimation level with the closest decimation
     * period less than the checked level's decimation period. A retention
     * period of zero is considered greater than all other retention periods
     * because zero is used as a marker for an unbounded retention period. If
     * any of the retention periods violates this constraint, an exception is
     * thrown. The actual verification is done by calling the
     * {@link #verifyRetentionPeriodsAscending(SortedMap)} method.
     * </p>
     * 
     * <p>
     * This method assumes that the specified set of decimation levels has
     * already been normalized and verified, for example using the
     * {@link #normalizeAndVerifyDecimationLevelsSet(Set)} method. If the
     * specified set is <code>null</code> or contains <code>null</code> or
     * negative elements, the behavior of this method is undefined.
     * </p>
     * 
     * @param decimationLevelToRetentionPeriod
     *            map using decimation levels (or rather their decimation
     *            periods in seconds) for the keys and the corresponding
     *            retention periods (in seconds) for the values. If
     *            <code>null</code>, an empty map is used as the input for this
     *            method.
     * @param decimationLevels
     *            set of decimation levels that are used for filtering the
     *            specified map. Map entries that do not have a corresponding
     *            entry in this set are removed from the map. Set entries that
     *            do not have a corresponding entry in the map are added to the
     *            map. The set must be well-formed (see the method description)
     *            or the behavior of this method is undefined.
     * @return sorted, unmodifiable copy of the specified map that contains
     *         exactly one entry for each element of the specified set so that
     *         the map's key-set is equal to the specified set.
     * @throws IllegalArgumentException
     *             if the specified map contains negative keys or values or if
     *             the ordering constraint for the retention periods is violated
     *             (see method description).
     * @throws NullPointerException
     *             if the specified map contains <code>null</code> keys or
     *             values.
     */
    public static SortedMap<Integer, Integer> normalizeAndVerifyDecimationLevelToRetentionPeriodMap(
            Map<Integer, Integer> decimationLevelToRetentionPeriod,
            Set<Integer> decimationLevels) {
        // The decimationLevels set should have been checked before calling this
        // method, so we only use an assertion here.
        assert (decimationLevels != null);
        // When the map is null, we treat it as an empty map. We still want to
        // run the rest of the logic because it might actually add entries to
        // the map.
        if (decimationLevelToRetentionPeriod == null) {
            decimationLevelToRetentionPeriod = Collections.emptyMap();
        }
        // We might want to add entries to the map for decimation levels that
        // are missing from the map but present in the set. For this reason, we
        // create a new map and copy the entries from the old one. We also use
        // this chance to check the individual entries.
        TreeMap<Integer, Integer> newDecimationLevelToRetentionPeriod = new TreeMap<Integer, Integer>();
        for (Map.Entry<Integer, Integer> decimationLevelAndRetentionPeriod : decimationLevelToRetentionPeriod
                .entrySet()) {
            Integer decimationLevel = decimationLevelAndRetentionPeriod
                    .getKey();
            Integer retentionPeriod = decimationLevelAndRetentionPeriod
                    .getValue();
            // We do not allow null keys or values.
            if (decimationLevel == null || retentionPeriod == null) {
                throw new NullPointerException(
                        "The decimationLevelToRetentionPeriod map must not contain null keys or values.");
            }
            // Decimation levels must not be negative.
            if (decimationLevel < 0) {
                throw new IllegalArgumentException(
                        "A decimation level cannot have a negative decimation period.");
            }
            // We allow negative values for the retention period but convert
            // them to zero.
            retentionPeriod = retentionPeriod < 0 ? ZERO : retentionPeriod;
            // If the decimation level is also in the decimationLevels set, we
            // want to keep the entry.
            if (decimationLevels.contains(decimationLevel)) {
                newDecimationLevelToRetentionPeriod.put(decimationLevel,
                        retentionPeriod);
            }
        }
        // We expect to have a retention period for every decimation level that
        // is defined. If it is not defined explicitly we use a value of zero.
        for (Integer decimationLevel : decimationLevels) {
            // The decimationLevels set should have been checked before calling
            // this method, so we only use an assertion here.
            assert (decimationLevel != null);
            assert (decimationLevel >= 0);
            if (!newDecimationLevelToRetentionPeriod
                    .containsKey(decimationLevel)) {
                newDecimationLevelToRetentionPeriod.put(decimationLevel, ZERO);
            }
        }
        // Retention periods for decimation levels with greater decimation
        // periods must also be greater.
        ArchiveConfigurationUtils
                .verifyRetentionPeriodsAscending(newDecimationLevelToRetentionPeriod);
        // Before returning the map, we make sure that it cannot be modified any
        // longer.
        return Collections
                .unmodifiableSortedMap(newDecimationLevelToRetentionPeriod);
    }

    /**
     * <p>
     * Normalizes a map of decimation-level to retention-period mappings and
     * verifies that it is well-formed. This method is designed to be used when
     * a map of decimation-level to retention-period mappings is supposed to be
     * used without knowing the exact set of decimation levels. However, the
     * information gathered from the specified sets of decimation levels to be
     * added and to be removed is used to at least confirm that the map is
     * well-formed with respect to this information.
     * </p>
     * 
     * <p>
     * This method creates an independent, unmodifiable copy of the map. This
     * method also verifies that the specified map contains no <code>null</code>
     * or negative keys or values. It throws an exception if it finds such keys
     * or values.
     * </p>
     * 
     * <p>
     * This method removes all mappings that are for decimation levels that are
     * contained in the set of decimation levels to be removed. It also adds a
     * mapping for all decimation levels that are contained in the specified set
     * of decimation levels to be added, but do not have a corresponding mapping
     * in the specified map. When such a mapping is added, a retention period of
     * zero is used.
     * </p>
     * 
     * <p>
     * After making the previously described modifications, it is verified that
     * the retention periods in the map match the constraint that the retention
     * period of each decimation level must be greater than or equal to the
     * decimation period of the preceding decimation level. The preceding
     * decimation level is the decimation level with the closest decimation
     * period less than the checked level's decimation period. A retention
     * period of zero is considered greater than all other retention periods
     * because zero is used as a marker for an unbounded retention period. If
     * any of the retention periods violates this constraint, an exception is
     * thrown. The actual verification is done by calling the
     * {@link #verifyRetentionPeriodsAscending(SortedMap)} method.
     * </p>
     * 
     * <p>
     * This method assumes that the specified sets of decimation levels to be
     * added and to be removed have already been normalized and verified, for
     * example using the
     * {@link #normalizeAndVerifyDecimationLevelsSetWithoutRawLevel(Set, String)}
     * method. If one of the specified set is <code>null</code> or contains
     * <code>null</code> or negative elements, the behavior of this method is
     * undefined.
     * </p>
     * 
     * @param decimationLevelToRetentionPeriod
     *            map using decimation levels (or rather their decimation
     *            periods in seconds) for the keys and the corresponding
     *            retention periods (in seconds) for the values. If
     *            <code>null</code>, an empty map is used as the input for this
     *            method.
     * @param addDecimationLevels
     *            set of decimation levels that shall exist in the map. Set
     *            entries that do not have a corresponding entry in the map are
     *            added to the map. The set must be well-formed (see the method
     *            description) or the behavior of this method is undefined.
     * @param removeDecimationLevels
     *            set of decimation levels that shall not exist in the map. Map
     *            entries that do not have a corresponding entry in this set are
     *            removed from the map. The set must be well-formed (see the
     *            method description) or the behavior of this method is
     *            undefined.
     * @return sorted, unmodifiable copy of the specified map that (in addition
     *         to other entries copied from the specified map) contains an entry
     *         for each element of the specified set of decimation levels to be
     *         added and does not contain any entries with a key contained in
     *         the specified set of decimation levels to be removed.
     * @throws IllegalArgumentException
     *             if the specified map contains negative keys or values or if
     *             the ordering constraint for the retention periods is violated
     *             (see method description).
     * @throws NullPointerException
     *             if the specified map contains <code>null</code> keys or
     *             values.
     */
    public static SortedMap<Integer, Integer> normalizeAndVerifyDecimationLevelToRetentionPeriodMap(
            Map<Integer, Integer> decimationLevelToRetentionPeriod,
            Set<Integer> addDecimationLevels,
            Set<Integer> removeDecimationLevels) {
        // The addDecimationLevels and removeDecimationLevels sets should have
        // been checked before calling this method, so we only use an assertion
        // here.
        assert (addDecimationLevels != null);
        assert (removeDecimationLevels != null);
        // When the map is null, we treat it as an empty map. We still want to
        // run the rest of the logic because it might actually add entries to
        // the map.
        if (decimationLevelToRetentionPeriod == null) {
            decimationLevelToRetentionPeriod = Collections.emptyMap();
        }
        // We might want to add entries to the map for decimation levels that
        // are missing from the map but present in the set. For this reason, we
        // create a new map and copy the entries from the old one. We also use
        // this chance to check the individual entries.
        TreeMap<Integer, Integer> newDecimationLevelToRetentionPeriod = new TreeMap<Integer, Integer>();
        for (Map.Entry<Integer, Integer> decimationLevelAndRetentionPeriod : decimationLevelToRetentionPeriod
                .entrySet()) {
            Integer decimationLevel = decimationLevelAndRetentionPeriod
                    .getKey();
            Integer retentionPeriod = decimationLevelAndRetentionPeriod
                    .getValue();
            // We do not allow null keys or values.
            if (decimationLevel == null || retentionPeriod == null) {
                throw new NullPointerException(
                        "The decimationLevelToRetentionPeriod map must not contain null keys or values.");
            }
            // Decimation levels must not be negative.
            if (decimationLevel < 0) {
                throw new IllegalArgumentException(
                        "A decimation level cannot have a negative decimation period.");
            }
            // We allow negative values for the retention period but convert
            // them to zero.
            retentionPeriod = retentionPeriod < 0 ? ZERO : retentionPeriod;
            // If the decimation level is in the removeDecimationLevels set, we
            // want to discard it. Otherwise, we add it to the new map.
            if (!removeDecimationLevels.contains(decimationLevel)) {
                newDecimationLevelToRetentionPeriod.put(decimationLevel,
                        retentionPeriod);
            }
        }
        // We expect to have a retention period for every decimation level that
        // is is also part of the addDecimationLevels set. If it is not defined
        // explicitly we use a retention period of zero.
        for (Integer decimationLevel : addDecimationLevels) {
            // The addDecimationLevels set should have been checked before
            // calling this method, so we only use an assertion here.
            assert (decimationLevel != null);
            assert (decimationLevel >= 0);
            if (!newDecimationLevelToRetentionPeriod
                    .containsKey(decimationLevel)) {
                newDecimationLevelToRetentionPeriod.put(decimationLevel, ZERO);
            }
        }
        // Retention periods for decimation levels with greater decimation
        // periods must also be greater. This verification cannot ensure that
        // the new retention periods are valid because they might be merged with
        // existing ones. However, this verification will fail early if the
        // problem can already be detected here. The other situations are
        // detected in UpdateChannelOperation.
        ArchiveConfigurationUtils
                .verifyRetentionPeriodsAscending(newDecimationLevelToRetentionPeriod);
        // Before returning the map, we make sure that it cannot be modified any
        // longer.
        return Collections
                .unmodifiableSortedMap(newDecimationLevelToRetentionPeriod);
    }

    /**
     * <p>
     * Verifies that the retention periods in a map of decimation-level to
     * retention-period mappings are ascending.
     * </p>
     * 
     * <p>
     * The verification is done by iterating over the values (retention periods)
     * in the map in ascending order of their keys (decimation levels). If a
     * value is found that is less than the preceding value, an exception is
     * thrown. For the purpose of comparing to retention periods, a value of
     * zero is considered greater than all other values because it is used as a
     * marker for an unbounded retention period.
     * </p>
     * 
     * <p>
     * This method assumes that the specified map of decimation-level to
     * retention-period mappings has already been normalized and verified, for
     * example using the
     * {@link #normalizeAndVerifyDecimationLevelToRetentionPeriodMap(Map, Set)}
     * method. If the specified map is <code>null</code> or contains
     * <code>null</code> or negative keys or values, the behavior of this method
     * is undefined.
     * </p>
     * 
     * @param decimationLevelToRetentionPeriod
     *            map using decimation levels (or rather their decimation
     *            periods in seconds) for the keys and the corresponding
     *            retention periods (in seconds) for the values. The map is
     *            assumed to be well-formed (see the description of this
     *            method). If it is not, the behavior of this method is
     *            undefined.
     * @throws IllegalArgumentException
     *             if the specified map contains a mapping that has a lesser
     *             retention period than the one of the preceding (lesser
     *             decimation period) mapping.
     */
    public static void verifyRetentionPeriodsAscending(
            SortedMap<Integer, Integer> decimationLevelToRetentionPeriod) {
        // This method is only used internally. When it is called, we can assume
        // that the map has already been verified regarding null elements,
        // negative elements, etc. For this reason, we can expect that all
        // elements are non-null, non-negative numbers and we only use
        // assertions to verify this.
        assert (decimationLevelToRetentionPeriod != null);
        int lastDecimationPeriod = 0;
        int lastRetentionPeriod = 0;
        boolean firstEntry = true;
        for (Map.Entry<Integer, Integer> decimationLevelAndRetentionPeriod : decimationLevelToRetentionPeriod
                .entrySet()) {
            int decimationPeriod = decimationLevelAndRetentionPeriod.getKey();
            int retentionPeriod = decimationLevelAndRetentionPeriod.getValue();
            if (firstEntry) {
                firstEntry = false;
                lastDecimationPeriod = decimationPeriod;
                lastRetentionPeriod = retentionPeriod;
                continue;
            }
            // The map is sorted by decimation periods, so we only have to check
            // whether the retention period is greater than or equal to the last
            // one.
            if (retentionPeriod != 0
                    && (lastRetentionPeriod == 0 || retentionPeriod < lastRetentionPeriod)) {
                throw new IllegalArgumentException(
                        "Decimation level "
                                + decimationPeriod
                                + " has a retention period of "
                                + retentionPeriodToString(retentionPeriod)
                                + " which is less than the retention period of decimation level "
                                + lastDecimationPeriod + " which is "
                                + retentionPeriodToString(lastRetentionPeriod)
                                + ".");
            }
            lastDecimationPeriod = decimationPeriod;
            lastRetentionPeriod = retentionPeriod;
        }
    }

    private static SortedSet<Integer> copyAndVerifyDecimationLevelsSet(
            Set<Integer> decimationLevels, String setName) {
        // If decimationLevels is null, this should already have been handled in
        // the calling method.
        assert (decimationLevels != null);
        // We want an independent copy of the set, so we create a new one.
        TreeSet<Integer> newDecimationLevels = new TreeSet<Integer>();
        // We validate the entries while copying.
        for (Integer decimationLevel : decimationLevels) {
            if (decimationLevel == null) {
                throw new NullPointerException("The " + setName
                        + " set must not contain null elements.");
            }
            if (decimationLevel < 0) {
                throw new IllegalArgumentException(
                        "A decimation level cannot have a negative decimation period.");
            }
            newDecimationLevels.add(decimationLevel);
        }
        return newDecimationLevels;
    }

    private static String retentionPeriodToString(int retentionPeriod) {
        if (retentionPeriod == 0) {
            return "unbounded";
        } else {
            return Integer.toString(retentionPeriod);
        }
    }

}
