package com.bc.calvalus.b3.job;

import com.bc.calvalus.b3.AggregatorAverage;
import com.bc.calvalus.b3.AggregatorAverageML;
import com.bc.calvalus.b3.AggregatorMinMax;
import com.bc.calvalus.b3.AggregatorOnMaxSet;
import com.bc.calvalus.b3.BinManager;
import com.bc.calvalus.b3.BinningGrid;
import com.bc.calvalus.b3.IsinBinningGrid;
import com.bc.calvalus.b3.VariableContext;
import org.junit.Before;
import org.junit.Test;

import java.awt.geom.Rectangle2D;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static org.junit.Assert.*;

public class L3ConfigTest {
    private L3Config l3Config;

    @Before
    public void createL3Config() {
        l3Config = loadConfig("job.properties");
    }

    @Test
    public void testBinningGrid() {
        BinningGrid grid = l3Config.getBinningGrid();
        assertEquals(4320, grid.getNumRows());
        assertEquals(IsinBinningGrid.class, grid.getClass());
    }

    @Test
    public void testBBoxParameter() {
        final Properties properties = new Properties();
        final L3Config l3Config = new L3Config(properties);
         Rectangle2D boundingBox;

        boundingBox = l3Config.getBoundingBox();
        assertNull(boundingBox); // null means full globe

        properties.setProperty("calvalus.l3.bbox", "-60.0, 13.4, -20.0, 23.4");
        boundingBox = l3Config.getBoundingBox();
        assertEquals(-60.0, boundingBox.getX(), 1e-6);
        assertEquals(13.4, boundingBox.getY(), 1e-6);
        assertEquals(40.0, boundingBox.getWidth(), 1e-6);
        assertEquals(10.0, boundingBox.getHeight(), 1e-6);

        properties.remove("calvalus.l3.bbox");
        boundingBox = l3Config.getBoundingBox();
        assertNull(boundingBox); // null means full globe

        properties.setProperty("calvalus.l3.bbox", "-180,-90,+180,+90");
        boundingBox = l3Config.getBoundingBox();
        assertNull(boundingBox); // null means full globe

        try {
            properties.setProperty("calvalus.l3.bbox", "-60.0, 13.4, 23.4");
            l3Config.getBoundingBox();
            fail("IllegalArgumentException?");
        } catch (IllegalArgumentException e) {
        }

        try {
            properties.setProperty("calvalus.l3.bbox", "-60.0, 13.A, -20.0, 23.A");
            l3Config.getBoundingBox();
            fail("IllegalArgumentException?");
        } catch (IllegalArgumentException e) {
        }
    }

    @Test
    public void testVariableContext() {
        VariableContext varCtx = l3Config.getVariableContext();

        assertEquals(8, varCtx.getVariableCount());

        assertEquals(0, varCtx.getVariableIndex("ndvi"));
        assertEquals(1, varCtx.getVariableIndex("tsm"));
        assertEquals(2, varCtx.getVariableIndex("algal1"));
        assertEquals(3, varCtx.getVariableIndex("algal2"));
        assertEquals(4, varCtx.getVariableIndex("chl"));
        assertEquals(5, varCtx.getVariableIndex("reflec_3"));
        assertEquals(6, varCtx.getVariableIndex("reflec_7"));
        assertEquals(7, varCtx.getVariableIndex("reflec_8"));
        assertEquals(-1, varCtx.getVariableIndex("reflec_6"));
        assertEquals(-1, varCtx.getVariableIndex("reflec_10"));

        assertEquals("!l2_flags.INVALID && l2_flags.WATER", varCtx.getMaskExpr());

        assertEquals("ndvi", varCtx.getVariableName(0));
        assertEquals("(reflec_10 - reflec_6) / (reflec_10 + reflec_6)", varCtx.getVariableExpr(0));

        assertEquals("algal2", varCtx.getVariableName(3));
        assertEquals(null, varCtx.getVariableExpr(3));

        assertEquals("reflec_7", varCtx.getVariableName(6));
        assertEquals(null, varCtx.getVariableExpr(6));

    }

    @Test
    public void testBinManager() {
        BinManager binManager = l3Config.getBinManager();
        assertEquals(6, binManager.getAggregatorCount());
        assertEquals(AggregatorAverage.class, binManager.getAggregator(0).getClass());
        assertEquals(AggregatorAverageML.class, binManager.getAggregator(1).getClass());
        assertEquals(AggregatorAverageML.class, binManager.getAggregator(2).getClass());
        assertEquals(AggregatorAverageML.class, binManager.getAggregator(3).getClass());
        assertEquals(AggregatorOnMaxSet.class, binManager.getAggregator(4).getClass());
        assertEquals(AggregatorMinMax.class, binManager.getAggregator(5).getClass());
    }

    private L3Config loadConfig(String configPath) {
        return new L3Config(loadConfigProperties(configPath));
    }

    private Properties loadConfigProperties(String configPath) {
        final InputStream is = getClass().getResourceAsStream(configPath);
        try {
            try {
                final Properties properties = new Properties();
                properties.load(is);
                return properties;
            } finally {
                is.close();
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

}
