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

package com.bc.calvalus.production.cli;

import com.bc.calvalus.production.ProductionRequest;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.Namespace;
import org.jdom.input.SAXBuilder;

import java.io.IOException;
import java.io.Reader;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.List;

/**
 * Converts a given WPS request into a ProductionRequest.
 *
 * @author MarcoZ
 * @author Norman
 */
class WpsProductionRequestConverter {
    private final Document document;

    WpsProductionRequestConverter(Reader reader) throws JDOMException, IOException {
        SAXBuilder saxBuilder = new SAXBuilder();
        document = saxBuilder.build(reader);
    }

    ProductionRequest loadProductionRequest(String userName) throws IOException {

        Element executeElement = document.getRootElement();

        Namespace wps = executeElement.getNamespace("wps");
        Namespace ows = executeElement.getNamespace("ows");
        Namespace xlink = executeElement.getNamespace("xlink");

        String processIdentifier = executeElement.getChildText("Identifier", ows);

        Element dataInputs = executeElement.getChild("DataInputs", wps);
        @SuppressWarnings({"unchecked"})
        List<Element> inputElements = (List<Element>) dataInputs.getChildren("Input", wps);

        HashMap<String, String> parameterMap = new HashMap<String, String>();

        for (Element inputElement : inputElements) {
            String parameterName = inputElement.getChildText("Identifier", ows);

            Element dataElement = inputElement.getChild("Data", wps);
            String parameterValue = dataElement.getChildText("LiteralData", wps);
            if (parameterValue == null) {
                Element complexDataElement = dataElement.getChild("ComplexData", wps);
                if (complexDataElement != null) {
                    StringWriter out = new StringWriter();
                    Element complexContent = (Element) complexDataElement.getChildren().get(0);
                    new org.jdom.output.XMLOutputter().output(complexContent, out);
                    parameterValue = out.toString();
                } else {
                    Element referenceElement = dataElement.getChild("Reference", wps);
                    if (referenceElement != null) {
                        parameterValue = referenceElement.getAttributeValue("href", xlink);
                    }
                }
            }

            if (parameterValue != null) {
                if (parameterMap.containsKey(parameterName)) {
                    parameterValue = String.format("%s,%s", parameterMap.get(parameterName), parameterValue);
                }
                parameterMap.put(parameterName, parameterValue);
            }
        }
        return new ProductionRequest(processIdentifier, userName, parameterMap);
    }
}
