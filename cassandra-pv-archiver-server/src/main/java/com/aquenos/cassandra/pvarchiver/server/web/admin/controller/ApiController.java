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

/**
 * Controller handling requests to the administrative API. This controller
 * implements a REST-style API using the JSON protocol and exposes it at the
 * <code>/admin/api</code> URI.
 * 
 * @author Sebastian Marsching
 */
@Controller
@RequestMapping("/admin/api")
public class ApiController {

}
