/*
 * Copyright 2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.web.common.spring;

import org.apache.catalina.connector.Connector;
import org.apache.commons.lang3.StringUtils;
import org.springframework.boot.context.embedded.tomcat.TomcatConnectorCustomizer;

/**
 * <p>
 * Customizer for Tomcat {@link Connector}s that configures compression.
 * </p>
 * 
 * <p>
 * This customizer sets the <code>compressableMimeType</code>,
 * <code>compression</code>, <code>compressionMinSize</code>, and
 * <code>noCompressionUserAgents</code> property on each connector it is invoked
 * for.
 * </p>
 * 
 * <p>
 * Only those properties that have been configured are actually set. All other
 * properties are not set, leaving Tomcat's defaults untouched.
 * </p>
 * 
 * @author Sebastian Marsching
 */
public class ConfigureCompressionTomcatConnectorCustomizer implements
        TomcatConnectorCustomizer {

    /**
     * MIME types that are configured as compressable when using
     * {@link #enableCompression()}.
     */
    public static final String[] DEFAULT_COMPRESSABLE_MIME_TYPES = new String[] {
            "application/font-sfnt", "application/javascript",
            "application/json", "application/vnd.ms-fontobject",
            "application/xml", "image/svg+xml", "image/vnd.microsoft.icon",
            "text/css", "text/html", "text/javascript", "text/plain",
            "text/xml" };

    private final String compressableMimeType;
    private final String compression;
    private final Integer compressionMinSize;
    private final String noCompressionUserAgents;

    /**
     * Returns a customizer that disables compression. Effectively, this
     * customizer sets the <code>compression</code> property to <code>off</code>
     * .
     * 
     * @return customizer that disables compression.
     */
    public static ConfigureCompressionTomcatConnectorCustomizer disableCompression() {
        return new ConfigureCompressionTomcatConnectorCustomizer(null, "off",
                null, null);
    }

    /**
     * Returns a customizer that enabled compression using a default set of MIME
     * types. Effectively, this customizer sets the <code>compression</code>
     * property to <code>on</code> and the <code>compressableMimeType</code>
     * property to a string generated from the MIME types listed in
     * {@link #DEFAULT_COMPRESSABLE_MIME_TYPES}.
     * 
     * @return customizer that enables compression for a set of default MIME
     *         types.
     */
    public static ConfigureCompressionTomcatConnectorCustomizer enableCompression() {
        return enableCompression(DEFAULT_COMPRESSABLE_MIME_TYPES, null, null);
    }

    /**
     * Returns a customizer that enables compression. Effectively, this
     * customizer sets <code>compression</code> to <code>on</code>. If the
     * respective parameters are not <code>null</code>, this customizer also
     * sets the <code>compressableMediaType</code>,
     * <code>compressionMinSize</code>, and <code>noCompressionUserAgents</code>
     * properties.
     * 
     * @param compressableMimeTypes
     *            MIME types for which compression shall be enabled. The
     *            <code>compressableMediaType</code> property is set to a string
     *            representing a comma-separated list generated from the
     *            specified strings. If <code>null</code> , the
     *            <code>compressableMediaType</code> property is not set,
     *            keeping Tomcat's default value (
     *            <code>text/html,text/xml,text/plain,text/css,text/javascript,application/javascript</code>
     *            as of Tomcat 8.0.30).
     * @param compressionMinSize
     *            minimum size of a response body in bytes for which compression
     *            shall be enabled. The <code>compressionMinSize</code> property
     *            is set to a string generated from this number. If
     *            <code>null</code>, the <code>compressionMinSize</code>
     *            property is not set, keeping Tomcat's default value (
     *            <code>2048</code> as of Tomcat 8.0.30).
     * @param noCompressionUserAgents
     *            regular expression matching the <code>User-Agent</code> header
     *            of clients for which compression shall be disabled, even if
     *            they signal that they support compression. If
     *            <code>null</code>, the <code>noCompressionUserAgents</code>
     *            property is not set, keeping Tomcat's default value (
     *            <code>null</code> as of Tomcat 8.0.30).
     * @return customizer that enables compression and sets the specified
     *         compression-related properties.
     */
    public static ConfigureCompressionTomcatConnectorCustomizer enableCompression(
            String[] compressableMimeTypes, Integer compressionMinSize,
            String noCompressionUserAgents) {
        String compressableMimeTypesAsSingleString;
        if (compressableMimeTypes == null) {
            compressableMimeTypesAsSingleString = null;
        } else {
            compressableMimeTypesAsSingleString = StringUtils.join(
                    compressableMimeTypes, ',');
        }
        return enableCompression(compressableMimeTypesAsSingleString,
                compressionMinSize, noCompressionUserAgents);
    }

    /**
     * Returns a customizer that enables compression. Effectively, this
     * customizer sets <code>compression</code> to <code>on</code>. If the
     * respective parameters are not <code>null</code>, this customizer also
     * sets the <code>compressableMediaType</code>,
     * <code>compressionMinSize</code>, and <code>noCompressionUserAgents</code>
     * properties.
     * 
     * @param compressableMimeTypes
     *            comma-separated list of MIME types for which compression shall
     *            be enabled. If <code>null</code>, the
     *            <code>compressableMediaType</code> property is not set,
     *            keeping Tomcat's default value (
     *            <code>text/html,text/xml,text/plain,text/css,text/javascript,application/javascript</code>
     *            as of Tomcat 8.0.30).
     * @param compressionMinSize
     *            minimum size of a response body in bytes for which compression
     *            shall be enabled. The <code>compressionMinSize</code> property
     *            is set to a string generated from this number. If
     *            <code>null</code>, the <code>compressionMinSize</code>
     *            property is not set, keeping Tomcat's default value (
     *            <code>2048</code> as of Tomcat 8.0.30).
     * @param noCompressionUserAgents
     *            regular expression matching the <code>User-Agent</code> header
     *            of clients for which compression shall be disabled, even if
     *            they signal that they support compression. If
     *            <code>null</code>, the <code>noCompressionUserAgents</code>
     *            property is not set, keeping Tomcat's default value (
     *            <code>null</code> as of Tomcat 8.0.30).
     * @return customizer that enables compression and sets the specified
     *         compression-related properties.
     */
    public static ConfigureCompressionTomcatConnectorCustomizer enableCompression(
            String compressableMimeTypes, Integer compressionMinSize,
            String noCompressionUserAgents) {
        return new ConfigureCompressionTomcatConnectorCustomizer(
                compressableMimeTypes, "on", compressionMinSize,
                noCompressionUserAgents);
    }

    /**
     * Returns a customizer that enforces compression. Effectively, this
     * customizer sets <code>compression</code> to <code>force</code>.
     * 
     * @return customizer set enforces compression.
     */
    public static ConfigureCompressionTomcatConnectorCustomizer forceCompression() {
        return new ConfigureCompressionTomcatConnectorCustomizer(null, "force",
                null, null);
    }

    private ConfigureCompressionTomcatConnectorCustomizer(
            String compressableMimeType, String compression,
            Integer compressionMinSize, String noCompressionUserAgents) {
        this.compressableMimeType = compressableMimeType;
        this.compression = compression;
        this.compressionMinSize = compressionMinSize;
        this.noCompressionUserAgents = noCompressionUserAgents;
    }

    @Override
    public void customize(Connector connector) {
        if (compressableMimeType != null) {
            connector.setProperty("compressableMimeType", compressableMimeType);
        }
        if (compression != null) {
            connector.setProperty("compression", compression);
        }
        if (compressionMinSize != null) {
            connector.setProperty("compressionMinSize",
                    compressionMinSize.toString());
        }
        if (noCompressionUserAgents != null) {
            connector.setProperty("noCompressionUserAgents",
                    noCompressionUserAgents);
        }
    }

}
