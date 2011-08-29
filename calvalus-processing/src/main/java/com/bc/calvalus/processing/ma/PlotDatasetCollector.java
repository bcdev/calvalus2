package com.bc.calvalus.processing.ma;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Norman
 */
public class PlotDatasetCollector {

    private final String groupAttributeName;
    private Map<String, PlotDataset> plotMap;
    private List<PlotDataset> plotDatasets;
    private Object[] headerValues;
    private int groupAttributeIndex;
    private List<VariablePair> variablePairs;

    public PlotDatasetCollector(String groupAttributeName) {
        this.groupAttributeName = groupAttributeName;
    }

    public int getGroupAttributeIndex() {
        return groupAttributeIndex;
    }

    public VariablePair[] getVariablePairs() {
        if (variablePairs != null) {
            return variablePairs.toArray(new VariablePair[variablePairs.size()]);
        }
        return new VariablePair[0];
    }

    public PlotDataset[] getPlotDatasets() {
        if (plotMap != null) {
            return plotDatasets.toArray(new PlotDataset[plotDatasets.size()]);
        }
        return new PlotDataset[0];
    }


    public void collectRecord(String key, RecordWritable record) {
        if (key.equals(MAMapper.HEADER_KEY)) {
            if (headerValues != null) {
                throw new IllegalStateException("Header record seen twice.");
            }
            this.headerValues = record.getValues();
            this.groupAttributeIndex = findIndex(record.getValues(), groupAttributeName);
            this.variablePairs = findVariablePairs(record.getValues());
            this.plotMap = new HashMap<String, PlotDataset>();
            this.plotDatasets = new ArrayList<PlotDataset>();
        } else {
            if (headerValues == null) {
                throw new IllegalStateException("Data record seen before header record.");
            }
            Object[] values = record.getValues();
            String groupName = "";
            if (groupAttributeIndex >= 0) {
                groupName = String.valueOf(values[groupAttributeIndex]);
            }
            for (VariablePair variablePair : variablePairs) {
                String plotKey = String.format("%s.%s.%s",
                                               groupName,
                                               variablePair.referenceAttributeName,
                                               variablePair.satelliteAttributeName);
                PlotDataset plotDataset = plotMap.get(plotKey);
                if (plotDataset == null) {
                    plotDataset = new PlotDataset(groupName, variablePair);
                    plotMap.put(plotKey, plotDataset);
                    plotDatasets.add(plotDataset);
                }
                Number referenceValue = (Number) values[variablePair.referenceAttributeIndex];
                Number satelliteValue = (Number) values[variablePair.satelliteAttributeIndex];
                if (!Double.isNaN(referenceValue.doubleValue())
                        && !Double.isNaN(satelliteValue.doubleValue())) {
                    if (satelliteValue instanceof AggregatedNumber) {
                        AggregatedNumber aggregatedNumber = (AggregatedNumber) satelliteValue;
                        plotDataset.points.add(new Point(referenceValue.doubleValue(),
                                                  aggregatedNumber.mean,
                                                  aggregatedNumber.sigma,
                                                  aggregatedNumber.n));
                    } else {
                        plotDataset.points.add(new Point(referenceValue.doubleValue(),
                                                  satelliteValue.doubleValue(),
                                                  0.0, 1));
                    }
                }
            }
        }
    }

    private static int findIndex(Object[] headerValues, String attributeName) {
        int attributeIndex = -1;
        for (int i = 0; i < headerValues.length; i++) {
            String headerName = headerValues[i].toString();
            if (headerName.equalsIgnoreCase(attributeName)) {
                attributeIndex = i;
                break;
            }
        }
        return attributeIndex;
    }

    private static List<VariablePair> findVariablePairs(Object[] headerValues) {
        ArrayList<VariablePair> variablePairs = new ArrayList<VariablePair>();
        for (int i = 0; i < headerValues.length - 1; i++) {
            String headerName1 = headerValues[i].toString();
            for (int j = i + 1; j < headerValues.length; j++) {
                String headerName2 = headerValues[j].toString();
                if (headerName1.equalsIgnoreCase(headerName2)) {
                    variablePairs.add(new VariablePair(headerName1, i, headerName2, j));
                }
            }
        }
        return variablePairs;
    }

    public static class PlotDataset {
        private final String groupName;
        private final VariablePair variablePair;
        private final List<Point> points;

        public PlotDataset(String groupName, VariablePair variablePair) {
            this.groupName = groupName;
            this.variablePair = variablePair;
            points = new ArrayList<Point>(32);
        }

        public String getGroupName() {
            return groupName;
        }

        public VariablePair getVariablePair() {
            return variablePair;
        }

        public Point[] getPoints() {
            return points.toArray(new Point[points.size()]);
        }
    }

    public static class Point {
        public final double referenceValue;
        public final double satelliteMean;
        public final double satelliteSigma;
        public final int satelliteN;

        public Point(double referenceValue, double satelliteMean, double satelliteSigma, int satelliteN) {
            this.referenceValue = referenceValue;
            this.satelliteMean = satelliteMean;
            this.satelliteSigma = satelliteSigma;
            this.satelliteN = satelliteN;
        }
    }

    public static class VariablePair {
        public final String referenceAttributeName;
        public final int referenceAttributeIndex;
        public final String satelliteAttributeName;
        public final int satelliteAttributeIndex;

        public VariablePair(String referenceAttributeName, int referenceAttributeIndex,
                            String satelliteAttributeName, int satelliteAttributeIndex) {
            this.referenceAttributeName = referenceAttributeName;
            this.referenceAttributeIndex = referenceAttributeIndex;
            this.satelliteAttributeName = satelliteAttributeName;
            this.satelliteAttributeIndex = satelliteAttributeIndex;
        }

    }
}
