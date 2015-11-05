package com.bc.calvalus.processing.l3.multiregion;

import com.bc.calvalus.processing.xml.XmlConvertible;
import com.bc.ceres.binding.BindingException;
import com.bc.ceres.binding.ConversionException;
import org.apache.hadoop.conf.Configuration;
import org.esa.snap.core.gpf.annotations.Parameter;
import org.esa.snap.core.gpf.annotations.ParameterBlockConverter;

/**
 *  The config for for formatting
 *  multiple regions of a Binning product at once.
 */
public class L3MultiRegionFormatConfig implements XmlConvertible {

    public static class Region {
        @Parameter
        private String name;
        @Parameter
        private String regionWKT;

        public String getName() {
            return name;
        }

        public String getRegionWKT() {
            return regionWKT;
        }

        @Override
        public String toString() {
            return "Region{" +
                    "name='" + name + '\'' +
                    ", regionWKT='" + regionWKT + '\'' +
                    '}';
        }
    }

    @Parameter(itemAlias = "region")
    private Region[] regions;

    public Region[] getRegions() {
        return regions;
    }

    public L3MultiRegionFormatConfig(Region... regions) {
        this.regions = regions;
    }

    public static L3MultiRegionFormatConfig get(Configuration jobConfig) {
        String xml = jobConfig.get("calvalus.l3.multiformat");
        if (xml == null) {
            throw new IllegalArgumentException("Missing multiformat configuration '" + "calvalus.l3.multiformat" + "'");
        }
        try {
            return fromXml(xml);
        } catch (BindingException e) {
            throw new IllegalArgumentException("Invalid multiformat configuration: " + e.getMessage(), e);
        }
    }

    public static L3MultiRegionFormatConfig fromXml(String xml) throws BindingException {
        return new ParameterBlockConverter().convertXmlToObject(xml, new L3MultiRegionFormatConfig());
    }

    @Override
    public String toXml() {
        try {
            return new ParameterBlockConverter().convertObjectToXml(this);
        } catch (ConversionException e) {
            throw new RuntimeException(e);
        }
    }

}
