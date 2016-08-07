/*
 * Copyright 2015 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.web.common.spring;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.mvc.method.RequestMappingInfo;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping;

/**
 * Modified {@link RequestMappingHandlerMapping} that ignores non-public
 * methods. By default, the {@link RequestMapping} annotation works on all
 * methods. This mapping implementation ensures that the annotation is only
 * considered when used on public methods. If the annotation is present on a
 * non-public method, a warning is logged and the annotation is ignored.
 * 
 * @author Sebastian Marsching
 */
public class PublicOnlyRequestMappingHandlerMapping extends
        RequestMappingHandlerMapping {

    @Override
    protected RequestMappingInfo getMappingForMethod(Method method,
            Class<?> handlerType) {
        RequestMappingInfo info = super
                .getMappingForMethod(method, handlerType);
        if (info != null && !Modifier.isPublic(method.getModifiers())) {
            logger.warn("Ignoring non-public method with @RequestMapping annotation: "
                    + method);
            return null;
        } else {
            return info;
        }
    }

}
