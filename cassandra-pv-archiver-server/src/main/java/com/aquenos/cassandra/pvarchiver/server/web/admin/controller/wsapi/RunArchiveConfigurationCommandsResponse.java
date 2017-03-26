/*
 * Copyright 2016-2017 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.web.admin.controller.wsapi;

import java.util.List;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

import com.aquenos.cassandra.pvarchiver.server.archiving.ArchiveConfigurationCommandResult;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * <p>
 * Response object for the "run archive configuration commands" function of the
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
 * @see RunArchiveConfigurationCommandsRequest
 */
public class RunArchiveConfigurationCommandsResponse {

    private final String errorMessage;
    private final List<ArchiveConfigurationCommandResult> results;

    /**
     * Creates a response. This constructor does not verify the validity of the
     * arguments and simply uses them as-is.
     * 
     * @param errorMessage
     *            error message indicating a global error (an error that
     *            happened before changes where made to the configuration).
     *            <code>null</code> if there is no global error (there might
     *            still be errors for individual channels).
     * @param results
     *            results of the commands that were specified in the request.
     *            The results must be in the same order as the commands in the
     *            request.
     */
    @JsonCreator
    public RunArchiveConfigurationCommandsResponse(
            @JsonProperty("errorMessage") String errorMessage,
            @JsonProperty("results") List<ArchiveConfigurationCommandResult> results) {
        this.errorMessage = errorMessage;
        this.results = results;
    }

    /**
     * Returns the error message indicating a global error (an error that
     * happened before changed where made to the configuration).
     * <code>null</code> if there is no global error (there might still be
     * errors for individual channels).
     * 
     * @return global error message.
     */
    public String getErrorMessage() {
        return errorMessage;
    }

    /**
     * Returns the results to the commands that were specified with the request.
     * The results are in the same order as the corresponding commands in the
     * request.
     * 
     * @return results to the commands that were executed.
     */
    public List<ArchiveConfigurationCommandResult> getResults() {
        return this.results;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(errorMessage).append(results)
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
        RunArchiveConfigurationCommandsResponse other = (RunArchiveConfigurationCommandsResponse) obj;
        return new EqualsBuilder().append(this.errorMessage, other.errorMessage)
                .append(this.results, other.results).isEquals();
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this);
    }

}
