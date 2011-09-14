package com.bc.calvalus.processing.ma;

import com.bc.calvalus.commons.CalvalusLogger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Builds a list of {@code PlotDataset}s from given record data.
 *
 * @author Norman
 */
public class PlotDatasetCollector implements RecordProcessor {

    private static final Logger LOG = CalvalusLogger.getLogger();

    private final String groupAttributeName;
    private Map<String, PlotDataset> plotDatasetMap;
    private List<PlotDataset> plotDatasets;
    private int groupAttributeIndex;
    private List<VariablePair> variablePairs;

    public PlotDatasetCollector(String groupAttributeName) {
        this.groupAttributeName = groupAttributeName;
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
    public void processHeaderRecord(Object[] headerValues) {
        if (hasHeaderBeenSeen()) {
            throw new IllegalStateException("Header record seen twice.");
        }
        this.groupAttributeIndex = findIndex(headerValues, groupAttributeName);
        this.variablePairs = findVariablePairs(headerValues);
        this.plotDatasetMap = new HashMap<String, PlotDataset>();
        this.plotDatasets = new ArrayList<PlotDataset>();
    }

    @Override
    public void processDataRecord(int recordIndex, Object[] recordValues) {
        if (!hasHeaderBeenSeen()) {
            throw new IllegalStateException("Data record seen before header record.");
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
    public void finalizeRecordProcessing(int numRecords) throws IOException {
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

    private static List<VariablePair> findVariablePairs(Object[] headerValues) {
        ArrayList<VariablePair> variablePairs = new ArrayList<VariablePair>();
        for (int i = 0; i < headerValues.length - 1; i++) {
            VariablePair pair = findPair(headerValues, i);
            if (pair != null) {
                variablePairs.add(pair);
            }
        }
        return variablePairs;
    }

    private static VariablePair findPair(Object[] headerValues, int i) {
        VariablePair pair = null;
        String headerName1 = headerValues[i].toString();
        for (int j = i + 1; j < headerValues.length; j++) {
            String headerName2 = headerValues[j].toString();
            if (headerName1.equalsIgnoreCase(headerName2)) {
                return new VariablePair(headerName1, i, headerName2, j);
            }
        }

        String aggHeaderName1 = PixelExtractor.ATTRIB_NAME_AGGREG_PREFIX +headerName1;
        for (int j = i + 1; j < headerValues.length; j++) {
            String headerName2 = headerValues[j].toString();
            if (aggHeaderName1.equalsIgnoreCase(headerName2)) {
                return new VariablePair(headerName1, i, headerName2.substring(PixelExtractor.ATTRIB_NAME_AGGREG_PREFIX.length()), j);
            }
        }

        return pair;
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
