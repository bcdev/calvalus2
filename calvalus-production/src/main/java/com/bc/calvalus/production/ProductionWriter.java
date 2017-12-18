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

import com.bc.calvalus.commons.DateUtils;
import com.bc.calvalus.commons.WorkflowItem;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;

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
    private static final String XML_TEMPLATE = "com/bc/calvalus/production/request.xml.vm";
    private static final DateFormat dateFormat = DateUtils.createDateFormat("yyyy-MM-dd HH:mm:ss");

    private final Map<String, Object> templateMap;

    public ProductionWriter(Production production) {
        this(production, new String[0]);
    }

    public ProductionWriter(Production production, String[] imgUrls) {
        this.templateMap = createTemplateMap(production, imgUrls);
    }

    public void write(File stagingDir) throws IOException, ProductionException {
        writeAsXML(stagingDir);
        writeAsHTML(stagingDir);
    }

    public void writeAsXML(File stagingDir) throws IOException, ProductionException {
        FileWriter fileWriter = new FileWriter(new File(stagingDir, "request.xml"));
        try {
            fileWriter.write(asXML());
        } finally {
            fileWriter.close();
        }
    }

    public void writeAsHTML(File stagingDir) throws IOException, ProductionException {
        FileWriter fileWriter = new FileWriter(new File(stagingDir, "request.html"));
        try {
            fileWriter.write(asHTML());
        } finally {
            fileWriter.close();
        }
    }

    String asHTML() throws ProductionException {
        return mergeTemplate(HTML_TEMPLATE);
    }

    String asXML() throws ProductionException {
        return mergeTemplate(XML_TEMPLATE);
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

        WorkflowItem workflow = production.getWorkflow();
        map.put("submitTime", asString(workflow.getSubmitTime()));
        map.put("startTime", asString(workflow.getStartTime()));
        map.put("stopTime", asString(workflow.getStopTime()));

        return map;
    }

    private String mergeTemplate(String templateName) throws ProductionException {
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
            Template htmlTemplate = velocityEngine.getTemplate(templateName);
            StringWriter writer = new StringWriter();
            htmlTemplate.merge(context, writer);
            return writer.toString();
        } catch (Exception e) {
            throw new ProductionException("Failed to generate page from template: " + templateName, e);
        }
    }

    private static String asString(Date date) {
        return date != null ? dateFormat.format(date) : "";
    }
}
