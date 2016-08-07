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

import com.aquenos.cassandra.pvarchiver.server.web.admin.controller.UiController;

/**
 * <p>
 * Form-backing bean for editing channel configurations.
 * </p>
 * 
 * <p>
 * This class is only intended for use by the {@link UiController} and its
 * associated views.
 * </p>
 * 
 * @author Sebastian Marsching
 */
public class EditChannelForm extends AbstractAddEditChannelForm {

    private NavigableMap<Integer, DecimationLevel> originalDecimationLevels;
    private NavigableMap<Integer, ControlSystemOption> originalOptions;
    private NavigableMap<Integer, Boolean> removeDecimationLevels;
    private NavigableMap<Integer, Boolean> removeOptions;

    /**
     * Returns the original decimation levels that are stored in this bean. The
     * original decimation levels are the ones that where defined when the edit
     * operation was started. The keys in this map must be the same ones as in
     * the map returned by {@link #getDecimationLevels()}.
     * 
     * @return original decimation levels stored in this bean or
     *         <code>null</code> if the origina decimation levels have not been
     *         set.
     */
    public NavigableMap<Integer, DecimationLevel> getOriginalDecimationLevels() {
        return originalDecimationLevels;
    }

    /**
     * Sets the original decimation levels that are stored in this bean. The
     * original decimation levels are the ones that where defined when the edit
     * operation was started. The keys in this map must be the same ones as in
     * the map set through {@link #setDecimationLevels(NavigableMap)}.
     * 
     * @param originalDecimationLevels
     *            original decimation levels to be stored in this bean (may be
     *            <code>null</code>). The map is used as-is without copying it.
     */
    public void setOriginalDecimationLevels(
            NavigableMap<Integer, DecimationLevel> originalDecimationLevels) {
        this.originalDecimationLevels = originalDecimationLevels;
    }

    /**
     * Returns the map of original control-system-specific configuration
     * options. The original options are the ones that where defined when the
     * edit operation was started. The keys in this map must be the same ones as
     * in the map returned by {@link #getOptions()}.
     * 
     * @return original control-system options stored in this bean or
     *         <code>null</code> if the original control-system options have not
     *         been set.
     */
    public NavigableMap<Integer, ControlSystemOption> getOriginalOptions() {
        return originalOptions;
    }

    /**
     * Sets the original control-system-specific configuration options that are
     * stored in this bean. The original options are the ones that where defined
     * when the edit operation was started. The keys in this map must be the
     * same ones as in the map set through {@link #setOptions(NavigableMap)}.
     * 
     * @param originalOptions
     *            original control-system options to be stored in this bean (may
     *            be <code>null</code>). The map is used as-is without copying
     *            it.
     */
    public void setOriginalOptions(
            NavigableMap<Integer, ControlSystemOption> originalOptions) {
        this.originalOptions = originalOptions;
    }

    /**
     * Returns the map of decimation levels to be removed. The keys in this map
     * must be the same ones as in the map returned by
     * {@link #getDecimationLevels()}. Each decimation level whose index in that
     * map is mapped to a boolean value of <code>true</code> in this map, is
     * removed instead of being added.
     * 
     * @return indices of decimation levels that shall be removed or
     *         <code>null</code> if the decimation level to be removed have not
     *         been set.
     */
    public NavigableMap<Integer, Boolean> getRemoveDecimationLevels() {
        return removeDecimationLevels;
    }

    /**
     * Sets the map of decimation levels to be removed. The keys in this map
     * must be the same ones as in the map set through
     * {@link #setDecimationLevels(NavigableMap)}. Each decimation level whose
     * index in that map is mapped to a boolean value of <code>true</code> in
     * this map, is removed instead of being added.
     * 
     * @param removeDecimationLevels
     *            indices of decimation levels that shall be removed if they are
     *            mapped to <code>true</code> (may be <code>null</code>). The
     *            map is used as-is without copying it.
     */
    public void setRemoveDecimationLevels(
            NavigableMap<Integer, Boolean> removeDecimationLevels) {
        this.removeDecimationLevels = removeDecimationLevels;
    }

    /**
     * Returns the map of control-system-specific configuration options to be
     * removed. The keys in this map must be the same ones as in the map
     * returned by {@link #getOptions()}. Each option whose index in that map is
     * mapped to a boolean value of <code>true</code> in this map, is removed
     * instead of being added.
     * 
     * @return indices of control-system options that shall be removed or
     *         <code>null</code> if the options to be removed have not been set.
     */
    public NavigableMap<Integer, Boolean> getRemoveOptions() {
        return removeOptions;
    }

    /**
     * Sets the map of control-system-specific configuration options to be
     * removed. The keys in this map must be the same ones as in the map set
     * through {@link #setOptions(NavigableMap)}. Each option whose index in
     * that map is mapped to a boolean value of <code>true</code> in this map,
     * is removed instead of being added.
     * 
     * @param removeOptions
     *            indices of control-system options that shall be removed if
     *            they are mapped to <code>true</code> (may be <code>null</code>
     *            ). The map is used as-is without copying it.
     */
    public void setRemoveOptions(NavigableMap<Integer, Boolean> removeOptions) {
        this.removeOptions = removeOptions;
    }

}
