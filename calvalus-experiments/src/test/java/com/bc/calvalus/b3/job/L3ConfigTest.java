package com.bc.calvalus.b3.job;

import com.bc.calvalus.b3.AggregatorAverage;
import com.bc.calvalus.b3.AggregatorAverageML;
import com.bc.calvalus.b3.AggregatorMinMax;
import com.bc.calvalus.b3.AggregatorOnMaxSet;
import com.bc.calvalus.b3.BinManager;
import com.bc.calvalus.b3.BinningGrid;
import com.bc.calvalus.b3.IsinBinningGrid;
import com.bc.calvalus.b3.VariableContext;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class L3ConfigTest {

    @Test
    public void testBinningGrid() {
        BinningGrid grid = L3Config.getBinningGrid(loadConfig("job.properties"));
        assertEquals(4320, grid.getNumRows());
        assertEquals(IsinBinningGrid.class, grid.getClass());
    }

    @Test
    public void testVariableContext() {
        VariableContext varCtx = L3Config.getVariableContext(loadConfig("job.properties"));
        assertEquals(8, varCtx.getVariableCount());
        assertEquals(0, varCtx.getVariableIndex("algal1"));
        assertEquals(1, varCtx.getVariableIndex("algal2"));
        assertEquals(2, varCtx.getVariableIndex("chl"));
        assertEquals(3, varCtx.getVariableIndex("ndvi"));
        assertEquals(4, varCtx.getVariableIndex("reflec_3"));
        assertEquals(5, varCtx.getVariableIndex("reflec_7"));
        assertEquals(6, varCtx.getVariableIndex("reflec_8"));
        assertEquals(7, varCtx.getVariableIndex("tsm"));
        assertEquals(-1, varCtx.getVariableIndex("reflec_6"));
        assertEquals(-1, varCtx.getVariableIndex("reflec_10"));
    }

    @Test
    public void testBinManager() {
        BinManager binManager = L3Config.getBinManager(loadConfig("job.properties"));
        assertEquals(6, binManager.getAggregatorCount());
        assertEquals(AggregatorAverage.class, binManager.getAggregator(0).getClass());
        assertEquals(AggregatorAverageML.class, binManager.getAggregator(1).getClass());
        assertEquals(AggregatorAverageML.class, binManager.getAggregator(2).getClass());
        assertEquals(AggregatorAverageML.class, binManager.getAggregator(3).getClass());
        assertEquals(AggregatorOnMaxSet.class, binManager.getAggregator(4).getClass());
        assertEquals(AggregatorMinMax.class, binManager.getAggregator(5).getClass());
    }

    private Configuration loadConfig(String configPath) {
        final Properties properties = loadConfigProperties(configPath);
        Configuration configuration = new Configuration();
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            configuration.set(entry.getKey().toString(), entry.getValue().toString());
        }
        return configuration;
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
