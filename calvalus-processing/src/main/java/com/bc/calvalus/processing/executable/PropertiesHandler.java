package com.bc.calvalus.processing.executable;

import com.bc.ceres.binding.dom.DomElement;
import com.bc.ceres.binding.dom.XppDomElement;
import com.thoughtworks.xstream.io.copy.HierarchicalStreamCopier;
import com.thoughtworks.xstream.io.xml.XmlFriendlyNameCoder;
import com.thoughtworks.xstream.io.xml.XppDomWriter;
import com.thoughtworks.xstream.io.xml.XppReader;
import com.thoughtworks.xstream.io.xml.xppdom.XppDom;
import org.esa.snap.core.util.StringUtils;
import org.xmlpull.mxp1.MXParser;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Properties;

/**
 * Converts the processing parameters,
 * from either key, value pairs or XML into java properties.
 * <p/>
 * TODO Maybe this could be integrated into {@link com.bc.ceres.resource.Resource}
 */
public class PropertiesHandler {

    private static XmlFriendlyNameCoder nameCoder = new XmlFriendlyNameCoder();

    public static Properties asProperties(String processorParameters) throws IOException {
        Properties properties = new Properties();
        if (StringUtils.isNotNullAndNotEmpty(processorParameters)) {
            Reader reader = new StringReader(processorParameters);
            try {
                if (isXml(processorParameters)) {
                    handleChildElements(properties, createDomElement(reader), "");
                } else {
                    properties.load(reader);
                }
            } finally {
                reader.close();
            }
        }
        return properties;
    }

    private static void handleChildElements(Properties properties, DomElement parentDomElements, String prefix) {
        DomElement[] childElements = parentDomElements.getChildren();
        if (childElements.length > 0) {
            if (childElements.length > 1 && parentDomElements.getChildren(childElements[0].getName()).length == childElements.length) {
                // all children have the same name and there is more than one child
                if (!prefix.isEmpty()) {
                    properties.setProperty(prefix + "length", Integer.toString(childElements.length));
                }
                int index = 0;
                for (DomElement element : childElements) {
                    String value = element.getValue();
                    if (value != null && element.getChildCount() == 0) {
                        properties.setProperty(prefix + Integer.toString(index), value);
                    } else {
                        handleChildElements(properties, element, prefix + Integer.toString(index) + ".");
                    }
                    index++;
                }
            } else {
                for (DomElement element : childElements) {
                    String name = nameCoder.decodeNode(element.getName());
                    String elementValue = element.getValue();
                    String value = elementValue != null ? nameCoder.decodeNode(elementValue) : null;
                    if (value != null && element.getChildCount() == 0) {
                        properties.setProperty(prefix + name, value);
                    } else {
                        handleChildElements(properties, element, prefix + name + ".");
                    }
                }
            }
        }
    }

    private static DomElement createDomElement(Reader reader) {
        XppDomWriter domWriter = new XppDomWriter();
        new HierarchicalStreamCopier().copy(new XppReader(reader, new MXParser()), domWriter);
        XppDom xppDom = domWriter.getConfiguration();
        return new XppDomElement(xppDom);
    }

    static boolean isXml(String textContent) {
        String t = textContent.trim();
        return t.startsWith("<?xml ") || t.startsWith("<?XML ") || (t.startsWith("<") && t.endsWith(">"));
    }
}
