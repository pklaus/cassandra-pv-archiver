/*
 * Copyright 2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.internode;

import java.util.List;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

import com.aquenos.cassandra.pvarchiver.server.archiving.ArchiveConfigurationCommandResult;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * <p>
 * Response to a {@link RunArchiveConfigurationCommandsRequest}.
 * </p>
 * 
 * <p>
 * This response object is a simple wrapper around a list of
 * {@link ArchiveConfigurationCommandResult}s. However, it is needed because
 * serialization and deserialization of a generic list to and from JSON does not
 * always work correctly unless complete type information is available.
 * </p>
 * 
 * @author Sebastian Marsching
 * @see RunArchiveConfigurationCommandsRequest
 */
public class RunArchiveConfigurationCommandsResponse {

    private List<ArchiveConfigurationCommandResult> results;

    /**
     * Creates a response.
     * 
     * @param results
     *            results of the commands that were specified in the request.
     *            The results must be in the same order as the commands in the
     *            request.
     * @throws NullPointerException
     *             if <code>results</code> is <code>null</code> or contains
     *             <code>null</code> elements.
     */
    @JsonCreator
    public RunArchiveConfigurationCommandsResponse(
            @JsonProperty(value = "results", required = true) List<ArchiveConfigurationCommandResult> results) {
        Preconditions.checkNotNull(results,
                "The results list must not be null.");
        // We do not have to test the list for null elements, because copyOf(..)
        // will throw a NullPointerException if there are null elements.
        this.results = ImmutableList.copyOf(results);
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
        return new HashCodeBuilder().append(results).toHashCode();
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
        return new EqualsBuilder().append(this.results, other.results)
                .isEquals();
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this);
    }

}
