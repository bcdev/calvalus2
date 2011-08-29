package com.bc.calvalus.processing.ma;

import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;

/**
 * @author Norman
 */
public class PlotDatasetCollectorTest {
    @Test
    public void testThatGroupIndexIsIdentifiedIfAvailable() throws Exception {
        PlotDatasetCollector collector = new PlotDatasetCollector("site");
        collector.collectRecord(MAMapper.HEADER_KEY, new RecordWritable(new Object[]{"ID", "LAT", "LON", "SITE", "CHL"}));
        assertEquals(3, collector.getGroupAttributeIndex());
    }

    @Test
    public void testThatGroupIndexIsNotIdentifiedIfNotFound() throws Exception {
        PlotDatasetCollector collector = new PlotDatasetCollector("city");
        collector.collectRecord(MAMapper.HEADER_KEY, new RecordWritable(new Object[]{"ID", "LAT", "LON", "SITE", "CHL"}));
        assertEquals(-1, collector.getGroupAttributeIndex());
    }

    @Test
    public void testThatVariablePairsAreFound() throws Exception {
        PlotDatasetCollector collector = new PlotDatasetCollector("SITE");
        collector.collectRecord(MAMapper.HEADER_KEY, new RecordWritable(new Object[]{
                "ID", "LAT", "LON", "DATE", "SITE",
                "CHL", "WIND", "TSM", "TEMP", "pixel_x", "pixel_y", "chl", "algal", "tsm"}));
        PlotDatasetCollector.VariablePair[] variablePairs = collector.getVariablePairs();

        assertNotNull(variablePairs);
        assertEquals(2, variablePairs.length);

        assertEquals("CHL", variablePairs[0].referenceAttributeName);
        assertEquals(5, variablePairs[0].referenceAttributeIndex);
        assertEquals("chl", variablePairs[0].satelliteAttributeName);
        assertEquals(11, variablePairs[0].satelliteAttributeIndex);

        assertEquals("TSM", variablePairs[1].referenceAttributeName);
        assertEquals(7, variablePairs[1].referenceAttributeIndex);
        assertEquals("tsm", variablePairs[1].satelliteAttributeName);
        assertEquals(13, variablePairs[1].satelliteAttributeIndex);
    }

    @Test
    public void testThatUngroupedPlotsAreGenerated() throws Exception {

        PlotDatasetCollector collector = new PlotDatasetCollector(null);
        collector.collectRecord(MAMapper.HEADER_KEY, new RecordWritable(new Object[]{"SITE", "CHL", "chl"}));
        collector.collectRecord("R1", new RecordWritable(new Object[]{"Benguela", 0.4, 0.41}));
        collector.collectRecord("R2", new RecordWritable(new Object[]{"Benguela", 0.5, 0.49}));
        collector.collectRecord("R3", new RecordWritable(new Object[]{"Benguela", 0.2, 0.27}));

        PlotDatasetCollector.PlotDataset[] plotDatasets = collector.getPlotDatasets();
        assertNotNull(plotDatasets);
        assertEquals(1, plotDatasets.length);

        assertNotNull(plotDatasets[0]);
        assertEquals("", plotDatasets[0].getGroupName());
        assertEquals("CHL", plotDatasets[0].getVariablePair().referenceAttributeName);
        assertEquals("chl", plotDatasets[0].getVariablePair().satelliteAttributeName);
        assertNotNull(plotDatasets[0].getPoints());
        assertEquals(3, plotDatasets[0].getPoints().length);
        assertEquals(0.4, plotDatasets[0].getPoints()[0].referenceValue, 1E-10);
        assertEquals(0.5, plotDatasets[0].getPoints()[1].referenceValue, 1E-10);
        assertEquals(0.2, plotDatasets[0].getPoints()[2].referenceValue, 1E-10);
        assertEquals(0.41, plotDatasets[0].getPoints()[0].satelliteMean, 1E-10);
        assertEquals(0.49, plotDatasets[0].getPoints()[1].satelliteMean, 1E-10);
        assertEquals(0.27, plotDatasets[0].getPoints()[2].satelliteMean, 1E-10);
    }

    @Test
    public void testThatGroupedPlotsAreGenerated() throws Exception {

        PlotDatasetCollector collector = new PlotDatasetCollector("SITE");
        collector.collectRecord(MAMapper.HEADER_KEY, new RecordWritable(new Object[]{"SITE", "CHL", "chl", "ALGAL", "algal"}));
        collector.collectRecord("R1", new RecordWritable(new Object[]{"Benguela", 0.4, 0.41, 0.01, 0.02}));
        collector.collectRecord("R2", new RecordWritable(new Object[]{"Benguela", 0.5, 0.49, 0.02, 0.01}));
        collector.collectRecord("R3", new RecordWritable(new Object[]{"Benguela", 0.2, 0.27, 0.04, 0.03}));
        collector.collectRecord("R4", new RecordWritable(new Object[]{"Boussole", 0.4, 0.41, 0.01, 0.02}));
        collector.collectRecord("R5", new RecordWritable(new Object[]{"Boussole", 0.5, 0.49, 0.02, 0.01}));
        collector.collectRecord("R6", new RecordWritable(new Object[]{"Boussole", 0.2, 0.27, 0.04, 0.03}));

        PlotDatasetCollector.PlotDataset[] plotDatasets = collector.getPlotDatasets();
        assertNotNull(plotDatasets);
        assertEquals(4, plotDatasets.length);

        assertEquals("Benguela", plotDatasets[0].getGroupName());
        assertEquals("CHL", plotDatasets[0].getVariablePair().referenceAttributeName);
        assertEquals("chl", plotDatasets[0].getVariablePair().satelliteAttributeName);

        assertEquals("Benguela", plotDatasets[1].getGroupName());
        assertEquals("ALGAL", plotDatasets[1].getVariablePair().referenceAttributeName);
        assertEquals("algal", plotDatasets[1].getVariablePair().satelliteAttributeName);

        assertEquals("Boussole", plotDatasets[2].getGroupName());
        assertEquals("CHL", plotDatasets[2].getVariablePair().referenceAttributeName);
        assertEquals("chl", plotDatasets[2].getVariablePair().satelliteAttributeName);

        assertEquals("Boussole", plotDatasets[3].getGroupName());
        assertEquals("ALGAL", plotDatasets[3].getVariablePair().referenceAttributeName);
        assertEquals("algal", plotDatasets[3].getVariablePair().satelliteAttributeName);
    }
}
