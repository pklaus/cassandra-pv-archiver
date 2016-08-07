/*
 * Copyright 2015 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.web.admin.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.ModelAndView;

/**
 * Controller handling redirects for paths that do not have a view associated
 * but are used as part of a path that has a view associated. This controller
 * redirects to the closest URI to the one requested.
 * 
 * @author Sebastian Marsching
 */
@Controller
public class IndexRedirectController {

    /**
     * Redirects requests to <code>/</code> and <code>/admin</code> to
     * <code>/admin/ui</code>.
     * 
     * @return redirect view.
     */
    @RequestMapping({ "/", "/admin" })
    public ModelAndView redirectIndex() {
        return new ModelAndView("redirect:/admin/ui/");
    }

}
