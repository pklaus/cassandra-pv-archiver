/*
 * Copyright 2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.web.common.spring;

import java.util.Collections;
import java.util.Set;

import org.thymeleaf.context.IExpressionContext;
import org.thymeleaf.context.IWebContext;
import org.thymeleaf.dialect.IExpressionObjectDialect;
import org.thymeleaf.expression.IExpressionObjectFactory;

/**
 * Dialect that extends the expression evaluation context with a
 * <code>uriExpander</code> object. This <code>uriExpander</code> is an instance
 * of {@link UriExpander}. It can be used to expand URIs that would not be
 * handled correctly by Thymeleaf's built-in link expressions. In particular, it
 * handles path variables with reserved characters (like the forward slash)
 * correctly.
 * 
 * @author Sebastian Marsching
 */
public class UriExpanderDialect implements IExpressionObjectDialect {

    /**
     * Expression object factory for the {@link UriExpanderDialect}.
     * 
     * @author Sebastian Marsching
     */
    private static class UriExpanderExpressionObjectFactory implements
            IExpressionObjectFactory {

        private static final String URI_EXPANDER_EXPRESSION_OBJECT_NAME = "uriExpander";
        private static final Set<String> ALL_EXPRESSION_OBJECT_NAMES = Collections
                .singleton(URI_EXPANDER_EXPRESSION_OBJECT_NAME);

        @Override
        public Set<String> getAllExpressionObjectNames() {
            return ALL_EXPRESSION_OBJECT_NAMES;
        }

        @Override
        public Object buildObject(IExpressionContext context,
                String expressionObjectName) {
            switch (expressionObjectName) {
            case URI_EXPANDER_EXPRESSION_OBJECT_NAME:
                if (context instanceof IWebContext) {
                    IWebContext webContext = (IWebContext) context;
                    UriExpander uriExpander = new UriExpander(
                            webContext.getRequest(), webContext.getResponse());
                    return uriExpander;
                }
                break;
            }
            return null;
        }

        @Override
        public boolean isCacheable(String expressionObjectName) {
            switch (expressionObjectName) {
            case URI_EXPANDER_EXPRESSION_OBJECT_NAME:
                return true;
            default:
                return false;
            }
        }

    }

    private static final IExpressionObjectFactory EXPRESSION_OBJECT_FACTORY = new UriExpanderExpressionObjectFactory();

    @Override
    public IExpressionObjectFactory getExpressionObjectFactory() {
        return EXPRESSION_OBJECT_FACTORY;
    }

    @Override
    public String getName() {
        return "UriExpander";
    }

}
