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

/**
 * <p>
 * Response object for the "export channel configuration" function of the
 * web-service API.
 * </p>
 * 
 * <p>
 * This object is primarily intended to facilitate the JSON serialization of the
 * data provided by the API controller. For this reason, it does not perform any
 * checks on the parameters used to construct the object.
 * </p>
 * 
 * @author Sebastian Marsching
 */
public class ChannelsByServerExportResponse {

    private final byte[] configurationFile;

    /**
     * Creates a new response. This constructor does not verify the validity of
     * the arguments and simply uses them as-is.
     * 
     * @param configurationFile
     *            raw contents of the exported configuration file.
     */
    public ChannelsByServerExportResponse(
            @JsonProperty("configurationFile") byte[] configurationFile) {
        this.configurationFile = configurationFile;
    }

    /**
     * Returns the contents of the exported configuration file. The contents are
     * provided as binary data as information about the character set is
     * explicitly included in the XML data.
     * 
     * @return configuration file contents.
     */
    public byte[] getConfigurationFile() {
        return configurationFile;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(configurationFile).toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || !obj.getClass().equals(this.getClass())) {
            return false;
        }
        ChannelsByServerExportResponse other = (ChannelsByServerExportResponse) obj;
        return new EqualsBuilder()
                .append(this.configurationFile, other.configurationFile)
                .isEquals();
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this);
    }

}
