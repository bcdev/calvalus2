package com.bc.calvalus.processing.ma;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author Norman
 */
public class PlotDatasetCollectorTest {

    @Test
    public void testThatGroupIndexIsIdentifiedIfAvailable() throws Exception {
        PlotDatasetCollector collector = new PlotDatasetCollector("site");
        collector.processHeaderRecord(new Object[]{"ID", "LAT", "LON", "SITE", "CHL"}, new Object[]{""});
        assertEquals(3, collector.getGroupAttributeIndex());
    }

    @Test
    public void testThatGroupIndexIsNotIdentifiedIfNotFound() throws Exception {
        PlotDatasetCollector collector = new PlotDatasetCollector("city");
        collector.processHeaderRecord(new Object[]{"ID", "LAT", "LON", "SITE", "CHL"}, new Object[]{""});
        assertEquals(-1, collector.getGroupAttributeIndex());
    }

    @Test
    public void testThatVariablePairsAreFound() throws Exception {
        PlotDatasetCollector collector = new PlotDatasetCollector("SITE");
        collector.processHeaderRecord(new Object[]{
                "ID", "LAT", "LON", "DATE", "SITE",
                "CHL", "WIND", "TSM", "TEMP", "*pixel_x", "*pixel_y", "*chl", "*algal", "*tsm"
        }, new Object[]{""});
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
        collector.processHeaderRecord(new Object[]{"SITE", "CHL", "chl"}, new Object[]{"ExclusionReason"});
        collector.processDataRecord(new Object[]{"Benguela", 0.4, 0.41}, new Object[]{""});
        collector.processDataRecord(new Object[]{"Benguela", 0.5, 0.49}, new Object[]{""});
        collector.processDataRecord(new Object[]{"Benguela", 0.1, 0.11}, new Object[]{OverlappingRecordSelector.EXCLUSION_REASON_OVERLAPPING});
        collector.processDataRecord(new Object[]{"Benguela", 0.2, 0.27}, new Object[]{""});

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
        collector.processHeaderRecord(new Object[]{"SITE", "CHL", "chl", "ALGAL", "algal"}, new Object[]{""});
        collector.processDataRecord(new Object[]{"Benguela", 0.4, 0.41, 0.01, 0.02}, new Object[]{""});
        collector.processDataRecord(new Object[]{"Benguela", 0.5, 0.49, 0.02, 0.01}, new Object[]{""});
        collector.processDataRecord(new Object[]{"Benguela", 0.2, 0.27, 0.04, 0.03}, new Object[]{""});
        collector.processDataRecord(new Object[]{"Boussole", 0.4, 0.41, 0.01, 0.02}, new Object[]{""});
        collector.processDataRecord(new Object[]{"Boussole", 0.5, 0.49, 0.02, 0.01}, new Object[]{""});
        collector.processDataRecord(new Object[]{"Boussole", 0.2, 0.27, 0.04, 0.03}, new Object[]{""});
        collector.processDataRecord(new Object[]{"Boussole", 0.2, 0.27, "invalid", 0.03}, new Object[]{""});  // will be rejected, warning logged

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
