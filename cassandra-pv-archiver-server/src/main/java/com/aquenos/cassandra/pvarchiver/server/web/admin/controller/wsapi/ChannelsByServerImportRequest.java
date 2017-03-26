/*
 * Copyright 2017 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.web.admin.controller.wsapi;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

/**
 * <p>
 * Request object for the "import channel configuration" function of the
 * web-service API.
 * </p>
 * 
 * <p>
 * This class is primarily intended to facilitate JSON deserialization of the
 * data provided by an API client. The input data is validated to ensure that it
 * conforms to the API specification.
 * </p>
 * 
 * @author Sebastian Marsching
 */
public class ChannelsByServerImportRequest {

    private final boolean addChannels;
    private final byte[] configurationFile;
    private final boolean removeChannels;
    private final boolean simulate;
    private final boolean updateChannels;

    /**
     * Creates a new request object.
     * 
     * @param addChannels
     *            tells whether channels should be added to the server
     *            configuration. If <code>true</code> channels that exist in the
     *            configuration file but not on the server are added. If
     *            <code>false</code> channels that exist in the configuration
     *            file but not on the server are not added. If
     *            <code>null</code>, a value of <code>false</code> is assumed.
     * @param configurationFile
     *            configuration file contents. These contents must represent a
     *            valid XML configuration file. Contents that do not constitute
     *            a valid XML configuration file will not lead to this
     *            constructor throwing an exception, but will later result in an
     *            exception when the file's contents are processed.
     * @param removeChannels
     *            tells whether channels that only exist on the server should be
     *            removed. If <code>true</code>, channels that exist on the
     *            server but not in the configuration file are removed. If
     *            <code>false</code>, channels that exist on the server but not
     *            in the configuration file are simply ignored. If
     *            <code>null</code>, a value of <code>false</code> is assumed.
     * @param simulate
     *            tells whether changes should actually be applied
     *            (<code>false</code> or should only be simulated
     *            (<code>true</code>). If <code>null</code>, a value of
     *            <code>false</code> is assumed.
     * @param updateChannels
     *            tells whether channels on the server should be updated. If
     *            <code>true</code>, channels that exist in the configuration
     *            file and on the server are updated with the configuration
     *            specified in the configuration file. If <code>false</code>,
     *            channels in the configuration file that also exist on the
     *            server are not touched. If <code>null</code>, a value of
     *            <code>false</code> is assumed.
     * @throws NullPointerException
     *             if <code>configurationFile</code> is <code>null</code>.
     */
    public ChannelsByServerImportRequest(
            @JsonProperty("addChannels") Boolean addChannels,
            @JsonProperty(value = "configurationFile", required = true) byte[] configurationFile,
            @JsonProperty("removeChannels") Boolean removeChannels,
            @JsonProperty("simulate") Boolean simulate,
            @JsonProperty("updateChannels") Boolean updateChannels) {
        Preconditions.checkNotNull(configurationFile);
        this.addChannels = (addChannels == null) ? false : addChannels;
        this.configurationFile = configurationFile;
        this.removeChannels = (removeChannels == null) ? false : removeChannels;
        this.simulate = (simulate == null) ? false : simulate;
        this.updateChannels = (updateChannels == null) ? false : updateChannels;
    }

    /**
     * Tells whether channels should be added to the server configuration. If
     * <code>true</code> channels that exist in the configuration file but not
     * on the server are added. If <code>false</code> channels that exist in the
     * configuration file but not on the server are not added.
     * 
     * @return <code>true</code> if missing channels shall be added to the
     *         server, <code>false</code> if no new channels shall be added to
     *         the server.
     */
    public boolean isAddChannels() {
        return addChannels;
    }

    /**
     * Returns the contents of the configuration file that shall be imported.
     * 
     * @return configuration file contents.
     */
    public byte[] getConfigurationFile() {
        return configurationFile;
    }

    /**
     * Tells whether channels that only exist on the server should be removed.
     * If <code>true</code>, channels that exist on the server but not in the
     * configuration file are removed. If <code>false</code>, channels that
     * exist on the server but not in the configuration file are simply ignored.
     * 
     * @return <code>true</code> if channels that are missing in the
     *         configuration file shall also be removed from the server,
     *         <code>false</code> if no channels shall be removed.
     */
    public boolean isRemoveChannels() {
        return removeChannels;
    }

    /**
     * Tells whether changes should actually be applied (<code>false</code> or
     * should only be simulated (<code>true</code>).
     * 
     * @return <code>true</code> if the import process should run in simulation
     *         mode, <code>false</code> if changes to the server configuration
     *         shall actually be made.
     */
    public boolean isSimulate() {
        return simulate;
    }

    /**
     * Tells whether channels on the server should be updated. If
     * <code>true</code>, channels that exist in the configuration file and on
     * the server are updated with the configuration specified in the
     * configuration file. If <code>false</code>, channels in the configuration
     * file that also exist on the server are not touched.
     * 
     * @return <code>true</code> if the configuration for existing channels
     *         shall be updated, <code>false</code> if existing channels shall
     *         not be touched.
     */
    public boolean isUpdateChannels() {
        return updateChannels;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(addChannels)
                .append(configurationFile).append(removeChannels)
                .append(simulate).append(updateChannels).toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || !obj.getClass().equals(this.getClass())) {
            return false;
        }
        ChannelsByServerImportRequest other = (ChannelsByServerImportRequest) obj;
        return new EqualsBuilder().append(this.addChannels, other.addChannels)
                .append(this.configurationFile, other.configurationFile)
                .append(this.removeChannels, other.removeChannels)
                .append(this.simulate, other.simulate)
                .append(this.updateChannels, other.updateChannels).isEquals();
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this);
    }

}
