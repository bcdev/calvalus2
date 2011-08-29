package com.bc.calvalus.processing.ma;

import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;

/**
 * @author Norman
 */
public class PlotDataCollectorTest {
    @Test
    public void testThatGroupIndexIsIdentifiedIfAvailable() throws Exception {
        PlotDataCollector collector = new PlotDataCollector("site");
        collector.put(MAMapper.HEADER_KEY, new RecordWritable(new Object[]{"ID", "LAT", "LON", "SITE", "CHL"}));
        assertEquals(3, collector.getGroupAttributeIndex());
    }

    @Test
    public void testThatGroupIndexIsNotIdentifiedIfNotFound() throws Exception {
        PlotDataCollector collector = new PlotDataCollector("city");
        collector.put(MAMapper.HEADER_KEY, new RecordWritable(new Object[]{"ID", "LAT", "LON", "SITE", "CHL"}));
        assertEquals(-1, collector.getGroupAttributeIndex());
    }

    @Test
    public void testThatVariablePairsAreFound() throws Exception {
        PlotDataCollector collector = new PlotDataCollector("SITE");
        collector.put(MAMapper.HEADER_KEY, new RecordWritable(new Object[]{
                "ID", "LAT", "LON", "DATE", "SITE",
                "CHL", "WIND","TSM", "TEMP", "pixel_x", "pixel_y", "chl", "algal", "tsm"}));
        PlotDataCollector.VariablePair[] variablePairs = collector.getVariablePairs();

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

        PlotDataCollector collector = new PlotDataCollector(null);
        collector.put(MAMapper.HEADER_KEY, new RecordWritable(new Object[]{ "SITE", "CHL", "chl"}));
        collector.put("R1", new RecordWritable(new Object[]{"Benguela", 0.4, 0.41}));
        collector.put("R2", new RecordWritable(new Object[]{"Benguela", 0.5, 0.49}));
        collector.put("R3", new RecordWritable(new Object[]{"Benguela", 0.2, 0.27}));

        PlotDataCollector.Plot[] plots = collector.getPlots();
        assertNotNull(plots);
        assertEquals(1, plots.length);

        assertNotNull(plots[0]);
        assertEquals("", plots[0].getGroupName());
        assertEquals("CHL", plots[0].getVariablePair().referenceAttributeName);
        assertEquals("chl", plots[0].getVariablePair().satelliteAttributeName);
        assertNotNull(plots[0].getPoints());
        assertEquals(3, plots[0].getPoints().length);
        assertEquals(0.4, plots[0].getPoints()[0].referenceValue, 1E-10);
        assertEquals(0.5, plots[0].getPoints()[1].referenceValue, 1E-10);
        assertEquals(0.2, plots[0].getPoints()[2].referenceValue, 1E-10);
        assertEquals(0.41, plots[0].getPoints()[0].satelliteMean, 1E-10);
        assertEquals(0.49, plots[0].getPoints()[1].satelliteMean, 1E-10);
        assertEquals(0.27, plots[0].getPoints()[2].satelliteMean, 1E-10);
    }

    @Test
    public void testThatGroupedPlotsAreGenerated() throws Exception {

        PlotDataCollector collector = new PlotDataCollector("SITE");
        collector.put(MAMapper.HEADER_KEY, new RecordWritable(new Object[]{"SITE", "CHL", "chl", "ALGAL", "algal"}));
        collector.put("R1", new RecordWritable(new Object[]{"Benguela", 0.4, 0.41, 0.01, 0.02}));
        collector.put("R2", new RecordWritable(new Object[]{"Benguela", 0.5, 0.49, 0.02, 0.01}));
        collector.put("R3", new RecordWritable(new Object[]{"Benguela", 0.2, 0.27, 0.04, 0.03}));
        collector.put("R4", new RecordWritable(new Object[]{"Boussole", 0.4, 0.41, 0.01, 0.02}));
        collector.put("R5", new RecordWritable(new Object[]{"Boussole", 0.5, 0.49, 0.02, 0.01}));
        collector.put("R6", new RecordWritable(new Object[]{"Boussole", 0.2, 0.27, 0.04, 0.03}));

        PlotDataCollector.Plot[] plots = collector.getPlots();
        assertNotNull(plots);
        assertEquals(4, plots.length);

        assertEquals("Benguela", plots[0].getGroupName());
        assertEquals("CHL", plots[0].getVariablePair().referenceAttributeName);
        assertEquals("chl", plots[0].getVariablePair().satelliteAttributeName);

        assertEquals("Benguela", plots[1].getGroupName());
        assertEquals("ALGAL", plots[1].getVariablePair().referenceAttributeName);
        assertEquals("algal", plots[1].getVariablePair().satelliteAttributeName);

        assertEquals("Boussole", plots[2].getGroupName());
        assertEquals("CHL", plots[2].getVariablePair().referenceAttributeName);
        assertEquals("chl", plots[2].getVariablePair().satelliteAttributeName);

        assertEquals("Boussole", plots[3].getGroupName());
        assertEquals("ALGAL", plots[3].getVariablePair().referenceAttributeName);
        assertEquals("algal", plots[3].getVariablePair().satelliteAttributeName);
    }
}
