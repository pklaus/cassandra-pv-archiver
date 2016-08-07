/*
 * Copyright 2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.web.common.spring;

import org.springframework.boot.context.embedded.MimeMappings;

/**
 * Utility methods for extending a {@link MimeMappings} object with additional
 * MIME types.
 * 
 * @author Sebastian Marsching
 */
public abstract class MimeMappingsExtensions {

    /**
     * <p>
     * Extends the specified MIME mappings with mappings for font-related MIME
     * types. The returned MIME mappings represent the combination of the
     * specified mappings and the additional mappings.
     * </p>
     * 
     * <p>
     * This method registers the following file extensions with their respective
     * MIME types:
     * </p>
     * 
     * <table summary="file-extension to mime-type mappings registered by this method">
     * <thead>
     * <tr>
     * <th>File extension</th>
     * <th>MIME type</th>
     * </tr>
     * </thead> <tbody>
     * <tr>
     * <td>eot</td>
     * <td>application/vnd.ms-fontobject</td>
     * </tr>
     * <tr>
     * <td>otf</td>
     * <td>application/font-sfnt</td>
     * </tr>
     * <tr>
     * <td>ttf</td>
     * <td>application/font-sfnt</td>
     * </tr>
     * <tr>
     * <td>woff</td>
     * <td>application/font-woff</td>
     * </tr>
     * </tbody>
     * </table>
     * 
     * @param baseMimeMappings
     *            MIME mappings that are used as the base if the returned MIME
     *            mappings. These mappings are not modified. A new mappings
     *            object initialized from these mappings is created instead. If
     *            <code>null</code>, {@link MimeMappings#DEFAULT} is used.
     * @return new MIME mappings representing the specified MIME mappings
     *         extended with mappings for font-specific file extensions.
     */
    public static MimeMappings extendWithFontTypes(MimeMappings baseMimeMappings) {
        MimeMappings mimeMappings;
        if (baseMimeMappings == null) {
            mimeMappings = new MimeMappings(MimeMappings.DEFAULT);
        } else {
            mimeMappings = new MimeMappings(baseMimeMappings);
        }
        mimeMappings.add("eot", "application/vnd.ms-fontobject");
        mimeMappings.add("otf", "application/font-sfnt");
        mimeMappings.add("ttf", "application/font-sfnt");
        mimeMappings.add("woff", "application/font-woff");
        return mimeMappings;
    }

}
