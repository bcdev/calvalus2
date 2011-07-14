/*
 * Copyright (C) 2011 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.production;

import com.bc.calvalus.commons.WorkflowException;
import com.bc.calvalus.commons.WorkflowItem;
import com.bc.ceres.binding.dom.DomElement;
import com.bc.ceres.binding.dom.Xpp3DomElement;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.esa.beam.framework.datamodel.ProductData;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

/**
 * Write a production as XML or HTML for later reference.
 */
public class ProductionWriter {
    private static final String HTML_TEMPLATE = "com/bc/calvalus/production/request.html.vm";
    private static final DateFormat dateFormat = ProductData.UTC.createDateFormat("yyyy-MM-dd HH:mm:ss");

    public static void writeProductionAsXML(Production production, File stagingDir) throws IOException {
        FileWriter fileWriter = new FileWriter(new File(stagingDir, "request.xml"));
        try {
            fileWriter.write(ProductionWriter.asXML(production));
        } finally {
            fileWriter.close();
        }
    }

    public static void writeProductionAsHTML(Production production, File stagingDir) throws IOException, ProductionException {
        writeProductionAsHTML(production, new String[0], stagingDir);
    }

    public static void writeProductionAsHTML(Production production, String[] imgUrls, File stagingDir) throws IOException, ProductionException {
        FileWriter fileWriter = new FileWriter(new File(stagingDir, "request.html"));
        Map<String, Object> map = createTemplateMap(production, imgUrls);
        try {
            fileWriter.write(ProductionWriter.asHTML(map));
        } finally {
            fileWriter.close();
        }
    }

    private static Map<String, Object> createTemplateMap(Production production, String[] imgUrls) {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("production", production);
        Map<String, String> productionParameters = production.getProductionRequest().getParameters();
        ArrayList<String> keys = new ArrayList<String>(productionParameters.keySet());
        Collections.sort(keys);

        Map<String, String> parameters = new TreeMap<String, String>();
        for (String key : keys) {
            String value = productionParameters.get(key);
            if (value.startsWith("<")) {
                value = value.replaceAll("<", "&lt;").replaceAll(">", "&gt;");
            }
            parameters.put(key, value);
        }
        map.put("parameters", parameters);
        map.put("urls", imgUrls);
        return map;
    }

    public static String asHTML(Map<String, Object> templateMap) throws ProductionException {
        VelocityEngine velocityEngine = new VelocityEngine();
        Properties properties = new Properties();
        properties.setProperty("resource.loader", "class");
        properties.setProperty("class.resource.loader.class", "org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader");
        try {
            velocityEngine.init(properties);
        } catch (Exception e) {
            throw new ProductionException(String.format("Failed to initialise Velocity engine: %s", e.getMessage()), e);
        }
        VelocityContext context = new VelocityContext(templateMap);
        try {
            Template htmlTemplate = velocityEngine.getTemplate(HTML_TEMPLATE);
            StringWriter writer = new StringWriter();
            htmlTemplate.merge(context, writer);
            return writer.toString();
        } catch (Exception e) {
            throw new ProductionException("Failed to generate HTML page", e);
        }
    }

    public static String asXML(Production production) {
        DomElement productionDom = new Xpp3DomElement("production");
        addNode(productionDom, "id", production.getId());
        addNode(productionDom, "name", production.getName());

        WorkflowItem workflow = production.getWorkflow();
        addNode(productionDom, "submitTime", workflow.getSubmitTime());
        addNode(productionDom, "startTime", workflow.getStartTime());
        addNode(productionDom, "stopTime", workflow.getStopTime());

        DomElement requestDom = productionDom.createChild("request");
        ProductionRequest productionRequest = production.getProductionRequest();
        addNode(requestDom, "productionType", productionRequest.getProductionType());
        addNode(requestDom, "userName", productionRequest.getUserName());

        Map<String, String> parameters = productionRequest.getParameters();
        for (Map.Entry<String, String> entry : parameters.entrySet()) {
            addNode(requestDom, entry.getKey(), entry.getValue());
        }
        return productionDom.toXml();
    }

    private static void addNode(DomElement dom, String name, Date date) {
        addNode(dom, name, asString(date));
    }

    private static void addNode(DomElement dom, String name, String value) {
        DomElement domChild = dom.createChild(name);
        domChild.setValue(value);
    }

    private static String asString(Date date) {
        return date != null ? dateFormat.format(date) : "";
    }
}
