package com.bc.calvalus.processing.hadoop;

import com.thoughtworks.xstream.XStream;
import org.esa.snap.core.datamodel.MetadataElement;
import org.esa.snap.core.util.StringUtils;

public class MetadataSerializer {

    private final XStream xStream;

    public MetadataSerializer() {
        xStream = new XStream();
    }

    public String toXml(MetadataElement metadataElement) {
        if (metadataElement == null) {
            return "";
        }
        return xStream.toXML(metadataElement);
    }

    public MetadataElement fromXml(String xml) {
        if (StringUtils.isNullOrEmpty(xml)) {
            return null;
        }
        return (MetadataElement) xStream.fromXML(xml);
    }
}
