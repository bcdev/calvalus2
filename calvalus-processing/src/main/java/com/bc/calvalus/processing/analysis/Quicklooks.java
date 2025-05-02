package com.bc.calvalus.processing.analysis;

import com.bc.calvalus.processing.JobConfigNames;
import com.bc.ceres.binding.BindingException;
import org.apache.hadoop.conf.Configuration;
import org.esa.snap.core.gpf.annotations.Parameter;
import org.esa.snap.core.gpf.annotations.ParameterBlockConverter;

import java.awt.Color;

/**
 * @author Marco Peters
 */
public class Quicklooks {

    @Parameter(alias = "quicklooks", itemAlias = "config")
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

    public static Quicklooks fromXml(String xml) {
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
        @Parameter(defaultValue = "0,0,0")
        private Color backgroundColor;

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

        @Parameter(defaultValue = "RGB")
        private String bandName;
        @Parameter
        private String cpdURL;

        @Parameter
        private String overlayURL;
        @Parameter
        private String[] maskOverlays;
        @Parameter(defaultValue = "false")
        private boolean legendEnabled;
        @Parameter
        private String shapefileURL;

        @Parameter(defaultValue = "false")
        private boolean wmsEnabled;

        public String getImageType() {
            return imageType;
        }

        public Color getBackgroundColor() {
            return backgroundColor;
        }

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

        public String getOverlayURL() {
            return overlayURL;
        }

        public String[] getMaskOverlays() {
            return maskOverlays;
        }

        public boolean isLegendEnabled() {
            return legendEnabled;
        }

        public String getShapefileURL() {
            return shapefileURL;
        }

        public void setBandName(String bandName) {
            this.bandName = bandName;
        }

        public void setImageType(String imageType) {
            this.imageType = imageType;
        }

        public void setBackgroundColor(Color backgroundColor) {
            this.backgroundColor = backgroundColor;
        }

        public void setCpdURL(String cpdURL) {
            this.cpdURL = cpdURL;
        }

        public void setMaskOverlays(String[] maskOverlays) {
            this.maskOverlays = maskOverlays;
        }

        public void setLegendEnabled(boolean legendEnabled) {
            this.legendEnabled = legendEnabled;
        }

        public void setOverlayURL(String overlayURL) {
            this.overlayURL = overlayURL;
        }

        public boolean isWmsEnabled() {
            return wmsEnabled;
        }

    }
}
