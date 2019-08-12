/*
 * Copyright (C) 2013 Brockmann Consult GmbH (info@brockmann-consult.de)
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, see http://www.gnu.org/licenses/
 */

package com.bc.calvalus.portal.server;

import static com.bc.calvalus.portal.server.BackendServiceImpl.getUserName;

import com.bc.calvalus.production.ProcessingLogHandler;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ServiceContainer;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Servlet to handle log file viewing for productions
 *
 * @author MarcoZ
 * @author Hans
 */
public class HadoopLogServlet extends HttpServlet {

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException, ServletException {
        doGet(req, resp);
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException, ServletException {
        ServletOutputStream outputStream = resp.getOutputStream();
        String productionId = req.getParameter("productionId");
        ServiceContainer serviceContainer = (ServiceContainer) getServletContext().getAttribute("serviceContainer");
        BackendConfig backendConfig = new BackendConfig(getServletContext());
        boolean withExternalAccessControl = serviceContainer.getHadoopConfiguration().getBoolean(
                    "calvalus.accesscontrol.external", false);
        ProcessingLogHandler logHandler = new ProcessingLogHandler(backendConfig.getConfigMap(),
                                                                   withExternalAccessControl);
        int responseStatus;
        if (productionId == null) {
            responseStatus = logHandler.showErrorPage("Missing query parameter 'productionId'", outputStream);
        } else {
            try {
                Production production = serviceContainer.getProductionService().getProduction(productionId);
                if (production == null) {
                    throw new ProductionException("Failed to get production for id: " + productionId);
                }
                final String userName = getUserName(req);
                responseStatus = logHandler.handleProduction(production, outputStream, userName);
            } catch (ProductionException e) {
                responseStatus = logHandler.showErrorPage("Failed to get production for id: " + productionId,
                                                          outputStream);
            }
        }
        if (responseStatus != 0) {
            resp.setContentType("text/html");
        }
    }
}
