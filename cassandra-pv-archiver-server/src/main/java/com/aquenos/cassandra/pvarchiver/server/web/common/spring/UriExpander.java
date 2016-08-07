/*
 * Copyright 2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.web.common.spring;

import java.net.URI;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.web.util.DefaultUriTemplateHandler;
import org.springframework.web.util.UriComponentsBuilder;

import com.aquenos.cassandra.pvarchiver.common.CustomUrlCodec;
import com.google.common.base.Preconditions;

/**
 * <p>
 * Expands URI templates that may contain URI variables and encodes them for
 * being used in a view.
 * </p>
 * 
 * <p>
 * If the URI template starts with a forward slash ("/"), it is prepended with
 * the context path of the web application. If it starts with a forward slash
 * followed by a tilde ("~/"), the tilde is removed and the rest of the URI
 * template is kept as is. In all other cases, the URI template is kept as is.
 * </p>
 * 
 * <p>
 * After transforming the URI template as described earlier, the URI template
 * and the URI variables are passed to an internal instance of
 * {@link DefaultUriTemplateHandler}. This instance has its "parse path"
 * property set to <code>true</code>, so that URI variables used as path
 * components can safely contain characters, in particular forward slashes, that
 * would otherwise cause problems.
 * </p>
 * 
 * <p>
 * Finally, the resulting URI is converted to a {@link String} using
 * {@link URI#toASCIIString()} and encoded using
 * {@link HttpServletResponse#encodeURL(String)}.
 * </p>
 * 
 * <p>
 * This class is mainly intended for use by the {@link UriExpanderDialect}.
 * </p>
 * 
 * @author Sebastian Marsching
 */
public class UriExpander {

    private static final DefaultUriTemplateHandler URI_TEMPLATE_HANDLER;

    static {
        URI_TEMPLATE_HANDLER = new DefaultUriTemplateHandler();
        URI_TEMPLATE_HANDLER.setStrictEncoding(true);
    }

    private HttpServletRequest request;
    private HttpServletResponse response;

    /**
     * Creates a URI expander for the specified request and response. The HTTP
     * servlet request is needed to determine the context path when the
     * specified URI template is context-relative. The HTTP servlet response is
     * needed to encode the resulting URI (so that session parameters get
     * appended, if needed).
     * 
     * @param request
     *            request to be used for determining the context path.
     * @param response
     *            response to be used for encoding the resulting URI.
     */
    public UriExpander(HttpServletRequest request, HttpServletResponse response) {
        Preconditions.checkNotNull(request, "The request must not be null.");
        Preconditions.checkNotNull(response, "The response must not be null.");
        this.request = request;
        this.response = response;
    }

    /**
     * <p>
     * Encodes a string so that it can safely be passed as a URI variable to one
     * of the <code>expand</code> methods.
     * </p>
     * 
     * <p>
     * In general, this method does not have to be used in order to produce a
     * correct URI. The <code>expand</code> methods will always encode the URI
     * in a way that it is correct according to the specification. However, such
     * an URI may produce undesired results when being passed to a servlet
     * container, because the container may decode it and interpret parts of it.
     * </p>
     * 
     * <p>
     * This method will call {@link CustomUrlCodec#encode(String)} and thus
     * encode the string in a way that makes it opaque to any processing
     * performed by a web server, servlet container, or other component that
     * might intercept URIs. In particular, an encoded string can safely be used
     * as a path segment or a matrix variable without effecting the path
     * hierarchy, as characters like "/", "=", ".", and ";" are encoded in such
     * a way that they do not get decoded when applying regular URL decoding.
     * </p>
     * 
     * <p>
     * On the other hand, an application receiving a string that has been
     * encoded through this method has to decode it explicitly calling the
     * {@link CustomUrlCodec#decode(String)} method.
     * </p>
     * 
     * @param string
     *            string to be encoded.
     * @return encoded string that is safe use anywhere in a URI path or query
     *         string.
     */
    public String encode(String string) {
        if (string == null) {
            return null;
        } else {
            return CustomUrlCodec.encode(string);
        }
    }

    /**
     * <p>
     * Expand the give URI template with a map of URI variables.
     * </p>
     * 
     * <p>
     * If the URI template starts with a forward slash ("/"), it is prepended
     * with the context path of the web application. If it starts with a forward
     * slash followed by a tilde ("~/"), the tilde is removed and the rest of
     * the URI template is kept as is. In all other cases, the URI template is
     * kept as is.
     * </p>
     * 
     * <p>
     * After transforming the URI template as described earlier, the URI
     * template and the URI variables are passed to an internal instance of
     * {@link DefaultUriTemplateHandler}. This instance has its "parse path"
     * property set to <code>true</code>, so that URI variables used as path
     * components can safely contain characters, in particular forward slashes,
     * that would otherwise cause problems.
     * </p>
     * 
     * <p>
     * Finally, the resulting URI is converted to a {@link String} using
     * {@link URI#toASCIIString()} and encoded using
     * {@link HttpServletResponse#encodeURL(String)}.
     * </p>
     * 
     * @param uriTemplate
     *            the URI template string.
     * @param uriVariables
     *            the URI variables.
     * @return the resulting URI.
     * @throws IllegalArgumentException
     *             if the <code>uriTemplate</code> cannot be handled by the
     *             underlying {@link UriComponentsBuilder}.
     * @throws NullPointerException
     *             if <code>uriTemplate</code> is <code>null</code>.
     */
    public String expand(String uriTemplate, Map<String, ?> uriVariables) {
        return response.encodeURL(URI_TEMPLATE_HANDLER.expand(
                transformUriTemplate(uriTemplate), uriVariables)
                .toASCIIString());
    }

    /**
     * <p>
     * Expand the give URI template with an array of URI variable values.
     * </p>
     * 
     * <p>
     * If the URI template starts with a forward slash ("/"), it is prepended
     * with the context path of the web application. If it starts with a forward
     * slash followed by a tilde ("~/"), the tilde is removed and the rest of
     * the URI template is kept as is. In all other cases, the URI template is
     * kept as is.
     * </p>
     * 
     * <p>
     * After transforming the URI template as described earlier, the URI
     * template and the URI variables are passed to an internal instance of
     * {@link DefaultUriTemplateHandler}. This instance has its "parse path"
     * property set to <code>true</code>, so that URI variables used as path
     * components can safely contain characters, in particular forward slashes,
     * that would otherwise cause problems.
     * </p>
     * 
     * <p>
     * Finally, the resulting URI is converted to a {@link String} using
     * {@link URI#toASCIIString()} and encoded using
     * {@link HttpServletResponse#encodeURL(String)}.
     * </p>
     * 
     * @param uriTemplate
     *            the URI template string.
     * @param uriVariableValues
     *            the URI variable values.
     * @return the resulting URI
     * @throws IllegalArgumentException
     *             if the <code>uriTemplate</code> cannot be handled by the
     *             underlying {@link UriComponentsBuilder}.
     * @throws NullPointerException
     *             if <code>uriTemplate</code> is <code>null</code>.
     */
    public String expand(String uriTemplate, Object... uriVariableValues) {
        return response.encodeURL(URI_TEMPLATE_HANDLER.expand(
                transformUriTemplate(uriTemplate), uriVariableValues)
                .toASCIIString());
    }

    private String transformUriTemplate(String uriTemplate) {
        if (uriTemplate.startsWith("/") && !uriTemplate.startsWith("//")) {
            return request.getContextPath() + uriTemplate;
        } else if (uriTemplate.startsWith("~/")) {
            return uriTemplate.substring(1);
        } else {
            return uriTemplate;
        }
    }

}
