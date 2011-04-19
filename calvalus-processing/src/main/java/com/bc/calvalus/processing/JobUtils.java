/*
 * Copyright (C) 2010 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.processing;


import com.bc.ceres.binding.PropertyContainer;
import com.bc.ceres.binding.PropertySet;
import com.bc.ceres.binding.dom.DefaultDomConverter;
import com.bc.ceres.binding.dom.DomElement;
import com.bc.ceres.binding.dom.Xpp3DomElement;
import com.thoughtworks.xstream.io.copy.HierarchicalStreamCopier;
import com.thoughtworks.xstream.io.xml.XppDomWriter;
import com.thoughtworks.xstream.io.xml.XppReader;
import com.thoughtworks.xstream.io.xml.xppdom.Xpp3Dom;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKTReader;
import org.apache.hadoop.conf.Configuration;
import org.esa.beam.framework.gpf.annotations.ParameterDescriptorFactory;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Diverse utilities.
 *
 * @author MarcoZ
 */
public class JobUtils {

    public static void initSystemProperties(Configuration configuration) {
        Map<String, String> properties = convertProperties(configuration.get(JobConfNames.CALVALUS_SYSTEM_PROPERTIES));
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            System.setProperty(entry.getKey(), entry.getValue());
        }
    }

    public static String convertProperties(Properties properties) {
        if (properties.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        List<String> nameList = new ArrayList<String>(properties.stringPropertyNames());
        Collections.sort(nameList);
        for (String name : nameList) {
            sb.append(name);
            sb.append("=");
            sb.append(properties.getProperty(name));
            sb.append(",");
        }
        int length = sb.length();
        return sb.substring(0, length-1);
    }

    public static Map<String, String> convertProperties(String propertiesString) {
        Map<String, String> map = new HashMap<String, String>();
        if (propertiesString != null) {
            String[] properties = propertiesString.split(",");
            for (String property : properties) {
                String[] keyValue = property.split("=");
                if (keyValue.length == 2) {
                    map.put(keyValue[0].trim(), keyValue[1].trim());
                }
            }
        }
        return  map;
    }

    public static Geometry createGeometry(String geometryWkt) {
        if (geometryWkt == null || geometryWkt.isEmpty()) {
            return null;
        }
        final WKTReader wktReader = new WKTReader();
        try {
            return wktReader.read(geometryWkt);
        } catch (com.vividsolutions.jts.io.ParseException e) {
            throw new IllegalArgumentException("Illegal region geometry: " + geometryWkt, e);
        }
    }

}
