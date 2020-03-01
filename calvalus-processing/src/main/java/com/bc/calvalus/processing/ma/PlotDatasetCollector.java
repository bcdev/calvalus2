package com.bc.calvalus.processing.ma;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Builds a list of {@code PlotDataset}s from given record data.
 *
 * @author Norman
 */
public class PlotDatasetCollector implements RecordProcessor {

    private final String groupAttributeName;
    private final MAConfig.VariableMapping[] variableMappings;
    private Map<String, PlotDataset> plotDatasetMap;
    private List<PlotDataset> plotDatasets;
    private int groupAttributeIndex;
    private List<VariablePair> variablePairs;
    private int exclusionIndex;

    public PlotDatasetCollector(String groupAttributeName, MAConfig.VariableMapping... variableMappings) {
        this.groupAttributeName = groupAttributeName;
        this.variableMappings = variableMappings;
    }

    public int getGroupAttributeIndex() {
        return groupAttributeIndex;
    }

    public VariablePair[] getVariablePairs() {
        if (hasHeaderBeenSeen()) {
            return variablePairs.toArray(new VariablePair[variablePairs.size()]);
        }
        return new VariablePair[0];
    }

    public PlotDataset[] getPlotDatasets() {
        if (hasHeaderBeenSeen()) {
            return plotDatasets.toArray(new PlotDataset[plotDatasets.size()]);
        }
        return new PlotDataset[0];
    }

    @Override
    public void processHeaderRecord(Object[] attributeNames, Object[] annotationNames) throws IOException {
        if (hasHeaderBeenSeen()) {
            //throw new IllegalStateException("Header record seen twice.");
            return;  // TODO: preliminary reaction to changed table columns
        }
        this.groupAttributeIndex = findIndex(attributeNames, groupAttributeName);
        this.variablePairs = findVariablePairs(attributeNames, variableMappings);
        this.plotDatasetMap = new HashMap<String, PlotDataset>();
        this.plotDatasets = new ArrayList<PlotDataset>();
        this.exclusionIndex = Arrays.asList(annotationNames).indexOf(DefaultHeader.ANNOTATION_EXCLUSION_REASON);
    }

    @Override
    public void processDataRecord(String key, Object[] recordValues, Object[] annotationValues) throws IOException {
        if (!hasHeaderBeenSeen()) {
            throw new IllegalStateException("Data record seen before header record.");
        }
        String reason = "";
        if (exclusionIndex >= 0) {
            reason = (String) annotationValues[exclusionIndex];
        }
        if (!reason.isEmpty()) {
            return;
        }
        final String groupName = getGroupName(recordValues);
        for (VariablePair variablePair : variablePairs) {
            PlotDataset plotDataset = getPlotDataset(groupName, variablePair);
            Object referenceValue = recordValues[variablePair.referenceAttributeIndex];
            Object satelliteValue = recordValues[variablePair.satelliteAttributeIndex];
            Number referenceNumber = toNumber(referenceValue);
            Number satelliteNumber = toNumber(satelliteValue);
            if (isValidDataPoint(referenceNumber, satelliteNumber)) {
                collectDataPoint(plotDataset, referenceNumber, satelliteNumber);
            } else {
                // Uncomment the following if you want to find out why your plots are empty

//                LOG.warning(String.format("Match-up data point rejected: " +
//                                                  "referenceName=[%s], " +
//                                                  "referenceIndex=[%s], " +
//                                                  "referenceValue=[%s], " +
//                                                  "satelliteName=[%s], " +
//                                                  "satelliteIndex=[%s], " +
//                                                  "satelliteValue=[%s]",
//                                          variablePair.referenceAttributeName,
//                                          variablePair.referenceAttributeIndex,
//                                          referenceValue,
//                                          variablePair.satelliteAttributeName,
//                                          variablePair.satelliteAttributeIndex,
//                                          satelliteValue));

            }
        }
    }

    @Override
    public void finalizeRecordProcessing() throws IOException {
    }

    private boolean isValidDataPoint(Number referenceValue, Number satelliteValue) {
        return isValidNumber(referenceValue) && isValidNumber(satelliteValue);
    }

    private Number toNumber(Object value) {
        return value instanceof Number ? (Number) value : null;
    }

    private boolean isValidNumber(Number number) {
        return number != null
               && !Double.isNaN(number.doubleValue())
               && !Double.isInfinite(number.doubleValue());
    }

    private static void collectDataPoint(PlotDataset plotDataset, Number referenceValue, Number satelliteValue) {
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

    private boolean hasHeaderBeenSeen() {
        return variablePairs != null;
    }

    private String getGroupName(Object[] values) {
        String groupName = "";
        if (groupAttributeIndex >= 0) {
            groupName = String.valueOf(values[groupAttributeIndex]);
        }
        return groupName;
    }

    private PlotDataset getPlotDataset(String groupName, VariablePair variablePair) {
        String plotKey = String.format("%s.%s.%s",
                                       groupName,
                                       variablePair.referenceAttributeName,
                                       variablePair.satelliteAttributeName);
        PlotDataset plotDataset = plotDatasetMap.get(plotKey);
        if (plotDataset == null) {
            plotDataset = new PlotDataset(groupName, variablePair);
            plotDatasetMap.put(plotKey, plotDataset);
            plotDatasets.add(plotDataset);
        }
        return plotDataset;
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

    private static List<VariablePair> findVariablePairs(Object[] headerValues, MAConfig.VariableMapping[] variableMappings) {
        ArrayList<VariablePair> variablePairs = new ArrayList<VariablePair>();
        for (int i = 0; i < headerValues.length - 1; i++) {
            VariablePair pair = findPair(headerValues, i);
            if (pair != null) {
                variablePairs.add(pair);
            } else {
                pair = findPairWithMapping(headerValues, i, variableMappings);
                if (pair != null) {
                    variablePairs.add(pair);
                }
            }
        }
        return variablePairs;
    }

    private static VariablePair findPair(Object[] headerValues, int i) {
        String headerName1 = headerValues[i].toString();
        for (int j = i + 1; j < headerValues.length; j++) {
            String headerName2 = headerValues[j].toString();
            if (headerName1.equalsIgnoreCase(headerName2)) {
                return new VariablePair(headerName1, i, headerName2, j);
            }
        }

        String aggHeaderName1 = PixelExtractor.ATTRIB_NAME_AGGREG_PREFIX + headerName1;
        for (int j = i + 1; j < headerValues.length; j++) {
            String headerName2 = headerValues[j].toString();
            if (aggHeaderName1.equalsIgnoreCase(headerName2)) {
                return new VariablePair(headerName1, i, headerName2.substring(PixelExtractor.ATTRIB_NAME_AGGREG_PREFIX.length()), j);
            }
        }
        return null;
    }

    private static VariablePair findPairWithMapping(Object[] headerValues, int i, MAConfig.VariableMapping[] variableMappings) {
        String refHeaderName1 = headerValues[i].toString();
        String satHeaderName = null;
        for (MAConfig.VariableMapping variableMapping : variableMappings) {
            if (refHeaderName1.equals(variableMapping.getReference())) {
                satHeaderName = variableMapping.getSatellite();
            }
        }
        if (satHeaderName != null) {
            String aggSatHeaderName1 = PixelExtractor.ATTRIB_NAME_AGGREG_PREFIX + satHeaderName;
            for (int j = i + 1; j < headerValues.length; j++) {
                String headerName2 = headerValues[j].toString();
                if (satHeaderName.equals(headerName2)) {
                    return new VariablePair(refHeaderName1, i, headerName2, j);
                }
                if (aggSatHeaderName1.equalsIgnoreCase(headerName2)) {
                    return new VariablePair(refHeaderName1, i, headerName2.substring(PixelExtractor.ATTRIB_NAME_AGGREG_PREFIX.length()), j);
                }
            }
        }

        return null;
    }

    public static class PlotDataset {

        private final String groupName;
        private final VariablePair variablePair;
        private final List<Point> points;

        public PlotDataset(String groupName, VariablePair variablePair) {
            this(groupName, variablePair, new ArrayList<Point>(32));
        }

        public PlotDataset(String groupName, VariablePair variablePair, List<Point> points) {
            this.groupName = groupName;
            this.variablePair = variablePair;
            this.points = points;
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
