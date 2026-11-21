package com.bc.calvalus.processing.hadoop;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import org.apache.commons.io.input.XmlStreamReader;
import org.jdom.Document;
import org.jdom.JDOMException;
import org.junit.Test;
import org.jdom.Element;
import org.jdom.input.SAXBuilder;

import javax.xml.namespace.NamespaceContext;
import javax.xml.namespace.QName;
import javax.xml.stream.Location;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.List;
import java.util.Map;

/**
 * TODO add API doc
 *
 * @author Martin Boettcher
 */
public class TableInputFormatTest {


    private static final TypeReference<Map<String, Object>> VALUE_TYPE_REF = new TypeReference<Map<String, Object>>() {
    };

    private static String INPUT_TABLE = "{ \"inputTableJson\": { \"entry\": [ "
       + "  { \"input\": \"/calvalus/projects/okosat/l3-msi-0.7/32V/2025/02/01/20250201-P1D-L3-32V-60m-v0.7.nc\", "
       + "\"timeIndex\": 0, \"writeChunksOnly\": \"false\" },"
       + "  { \"input\": \"/calvalus/projects/okosat/l3-msi-0.7/32V/2025/02/02/20250202-P1D-L3-32V-60m-v0.7.nc\", "
       + "\"timeIndex\": 1, \"writeChunksOnly\": \"true\" },"
       + "  { \"input\": \"/calvalus/projects/okosat/l3-msi-0.7/32V/2025/02/03/20250203-P1D-L3-32V-60m-v0.7.nc\", "
       + "\"timeIndex\": 2, \"writeChunksOnly\": \"true\" }"
       + "]}}";

    @Test
    public void testConvertInputTableContent() throws IOException, JDOMException {
        final ObjectMapper jsonParser = new ObjectMapper();
        jsonParser.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
        Map<String, Object> structuredParameter = jsonParser.readValue(INPUT_TABLE, VALUE_TYPE_REF);

        XmlMapper xmlMapper = new XmlMapper();
        final String xml = xmlMapper.writeValueAsString(structuredParameter);
        String xmlValue = xml.substring("<LinkedHashMap>".length(), xml.length() - "</LinkedHashMap>".length());
        
        Reader reader = new BufferedReader(new StringReader(xmlValue));
        SAXBuilder saxBuilder = new SAXBuilder();
        Document document = saxBuilder.build(reader);
        Element root = document.getRootElement();
        final List<Element> children = root.getChildren();
        for (Element e : children) {
            System.out.println(e.getChild("input").getText());
            System.out.println(e.getChild("timeIndex").getText());
            System.out.println(e.getChild("writeChunksOnly").getText());
        }
    }
}