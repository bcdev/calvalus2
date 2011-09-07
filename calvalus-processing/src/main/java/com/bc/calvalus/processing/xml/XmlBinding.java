package com.bc.calvalus.processing.xml;

import com.bc.ceres.binding.BindingException;
import com.bc.ceres.binding.ConversionException;
import com.bc.ceres.binding.PropertyContainer;
import com.bc.ceres.binding.PropertySet;
import com.bc.ceres.binding.ValidationException;
import com.bc.ceres.binding.dom.DefaultDomConverter;
import com.bc.ceres.binding.dom.DomElement;
import com.bc.ceres.binding.dom.Xpp3DomElement;
import com.thoughtworks.xstream.io.copy.HierarchicalStreamCopier;
import com.thoughtworks.xstream.io.xml.XppDomWriter;
import com.thoughtworks.xstream.io.xml.XppReader;
import com.thoughtworks.xstream.io.xml.xppdom.Xpp3Dom;
import org.esa.beam.framework.gpf.annotations.ParameterDescriptorFactory;

import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

// todo - move to commons (nf)

/**
 * A utility class that converts Java objects into XML and vice versa.
 *
 * @author MarcoZ
 * @author Norman
 */
public class XmlBinding {

    private final ParameterDescriptorFactory parameterDescriptorFactory;

    public XmlBinding() {
        this(new ParameterDescriptorFactory());
    }

    public XmlBinding(ParameterDescriptorFactory parameterDescriptorFactory) {
        this.parameterDescriptorFactory = parameterDescriptorFactory;
    }

    public DomElement convertXmlToDomElement(String xml) {
        XppDomWriter domWriter = new XppDomWriter();
        new HierarchicalStreamCopier().copy(new XppReader(new StringReader(xml)), domWriter);
        Xpp3Dom xpp3Dom = domWriter.getConfiguration();
        return new Xpp3DomElement(xpp3Dom);
    }

    public <T> T convertXmlToObject(String xml, T object) throws BindingException {
        convertXmlToPropertySet(xml, object.getClass(),
                                PropertyContainer.createObjectBacked(object, parameterDescriptorFactory));
        return object;
    }

    public Map<String, Object> convertXmlToMap(String xml, Class<?> schema) throws ValidationException, ConversionException {
        Map<String, Object> map = new HashMap<String, Object>();
        convertXmlToPropertySet(xml, schema,
                                PropertyContainer.createMapBacked(map, schema, parameterDescriptorFactory));
        return map;
    }

    public String convertObjectToXml(Object object) throws ConversionException {
        DefaultDomConverter domConverter = new DefaultDomConverter(object.getClass(), parameterDescriptorFactory);
        DomElement parametersDom = new Xpp3DomElement("parameters");
        domConverter.convertValueToDom(object, parametersDom);
        return parametersDom.toXml();
    }

    private void convertXmlToPropertySet(String xml, Class<? extends Object> schema, PropertySet propertySet) throws ValidationException, ConversionException {
        propertySet.setDefaultValues();
        DefaultDomConverter domConverter = new DefaultDomConverter(schema, parameterDescriptorFactory);
        DomElement domElement = convertXmlToDomElement(xml);
        domConverter.convertDomToValue(domElement, propertySet);
    }
}
