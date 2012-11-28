package com.bc.calvalus.processing.analysis;

import com.bc.calvalus.processing.JobConfigNames;
import com.bc.ceres.binding.BindingException;
import org.apache.hadoop.conf.Configuration;
import org.esa.beam.framework.gpf.annotations.Parameter;
import org.esa.beam.framework.gpf.annotations.ParameterBlockConverter;

/**
 * @author Marco Peters
 */
public class Quicklooks {

    @Parameter(alias = "quicklooks", itemAlias = "config", itemsInlined = true)
    private QLConfig[] configs;

    public QLConfig[] getConfigs() {
        return configs;
    }

    public Quicklooks() {
    }

    public static QLConfig[] get(Configuration jobConfig) {
        String xml = jobConfig.get(JobConfigNames.CALVALUS_QUICKLOOK_PARAMETERS);
        if (xml == null) {
            throw new IllegalArgumentException(
                    "Missing configuration '" + JobConfigNames.CALVALUS_QUICKLOOK_PARAMETERS + "'");
        }
        return fromXml(xml).getConfigs();
    }

    static Quicklooks fromXml(String xml) {
        try {
            return new ParameterBlockConverter().convertXmlToObject(xml, new Quicklooks());
        } catch (BindingException e) {
            throw new IllegalArgumentException("Invalid configuration: " + e.getMessage(), e);
        }
    }

    /**
     * Configuration for quick look generation
     */
    public static class QLConfig {

        @Parameter
        private String imageType;

        @Parameter
        private int subSamplingX;
        @Parameter
        private int subSamplingY;

        @Parameter
        private String[] RGBAExpressions;
        @Parameter
        private double[] RGBAMinSamples;
        @Parameter
        private double[] RGBAMaxSamples;

        @Parameter
        private String bandName;
        @Parameter(defaultValue = "false")
        private boolean appendBandName;
        @Parameter
        private String cpdURL;

        @Parameter
        private String overlayURL;

        public int getSubSamplingX() {
            return subSamplingX;
        }

        public int getSubSamplingY() {
            return subSamplingY;
        }

        public String[] getRGBAExpressions() {
            return RGBAExpressions;
        }

        public double[] getRGBAMinSamples() {
            return RGBAMinSamples;
        }

        public double[] getRGBAMaxSamples() {
            return RGBAMaxSamples;
        }

        public String getBandName() {
            return bandName;
        }

        public String getCpdURL() {
            return cpdURL;
        }

        public String getImageType() {
            return imageType;
        }

        public String getOverlayURL() {
            return overlayURL;
        }

        public boolean isAppendBandName() {
            return appendBandName;
        }
    }
}
