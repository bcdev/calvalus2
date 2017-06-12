package com.bc.calvalus.processing.combinations;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;

public class CombinationsInputFormatTest {

    private CombinationsConfig.Variable a1;
    private CombinationsConfig.Variable a3;
    private CombinationsConfig.Variable b1;
    private CombinationsConfig.Variable c1;
    private CombinationsConfig.Variable t3;

    @Before
    public void setUp() throws Exception {
        a1 = new CombinationsConfig.Variable("a", "job", "A1");
        a3 = new CombinationsConfig.Variable("a", "job", "A1", "A2", "A3");
        b1 = new CombinationsConfig.Variable("b", "job", "B1");
        c1 = new CombinationsConfig.Variable("c", "job", "C1");
        t3 = new CombinationsConfig.Variable("t", "task", "T1", "T2", "T3");
    }


    @Test
    public void name() throws Exception {
        String[] xValues = new String[702];
        for (int x = 739; x <= 1440; x++) {
            xValues[x - 739] = x + "";
        }
        String[] yValues = new String[770];
        for (int y = 0; y < 770; y++) {
            yValues[y] = y + "";
        }
        CombinationsConfig.Variable x = new CombinationsConfig.Variable("x", "job", xValues);
        CombinationsConfig.Variable y = new CombinationsConfig.Variable("y", "job", yValues);

        List<InputSplit> inputSplits = getInputSplits(x, y);
        System.out.println();
    }

    @Test
    public void testCreateInputSplits_a1() throws Exception {
        List<InputSplit> inputSplits = getInputSplits(a1);
        assertNotNull(inputSplits);
        assertEquals(1, inputSplits.size());
        assertSplitEquals(inputSplits.get(0), "A1");
    }

    @Test
    public void testCreateInputSplits_a3() throws Exception {
        List<InputSplit> inputSplits = getInputSplits(a3);
        assertNotNull(inputSplits);
        assertEquals(3, inputSplits.size());
        assertSplitEquals(inputSplits.get(0), "A1");
        assertSplitEquals(inputSplits.get(1), "A2");
        assertSplitEquals(inputSplits.get(2), "A3");
    }

    @Test
    public void testCreateInputSplits_a1b1c1() throws Exception {
        List<InputSplit> inputSplits = getInputSplits(a1, b1, c1);
        assertNotNull(inputSplits);
        assertEquals(1, inputSplits.size());
        assertSplitEquals(inputSplits.get(0), "A1", "B1", "C1");
    }

    @Test
    public void testCreateInputSplits_a3b1() throws Exception {
        List<InputSplit> inputSplits = getInputSplits(a3, b1);
        assertNotNull(inputSplits);
        assertEquals(3, inputSplits.size());
        assertSplitEquals(inputSplits.get(0), "A1", "B1");
        assertSplitEquals(inputSplits.get(1), "A2", "B1");
        assertSplitEquals(inputSplits.get(2), "A3", "B1");
    }

    @Test
    public void testCreateInputSplits_b1a3() throws Exception {
        List<InputSplit> inputSplits = getInputSplits(b1, a3);
        assertNotNull(inputSplits);
        assertEquals(3, inputSplits.size());
        assertSplitEquals(inputSplits.get(0), "B1", "A1");
        assertSplitEquals(inputSplits.get(1), "B1", "A2");
        assertSplitEquals(inputSplits.get(2), "B1", "A3");
    }

    @Test
    public void testCreateInputSplits_a1t3() throws Exception {
        List<InputSplit> inputSplits = getInputSplits(a1, t3);
        assertNotNull(inputSplits);
        assertEquals(1, inputSplits.size());
        assertSplitEquals(inputSplits.get(0), "A1", "T1,T2,T3");
    }

    @Test
    public void testCreateInputSplits_a3t3() throws Exception {
        List<InputSplit> inputSplits = getInputSplits(a3, t3);
        assertNotNull(inputSplits);
        assertEquals(3, inputSplits.size());
        assertSplitEquals(inputSplits.get(0), "A1", "T1,T2,T3");
        assertSplitEquals(inputSplits.get(1), "A2", "T1,T2,T3");
        assertSplitEquals(inputSplits.get(2), "A3", "T1,T2,T3");
    }

    @Test
    public void testCreateSplitOutputPath() throws Exception {
        assertEquals("dir/a_b", create("dir", "a_%s", "b"));
        assertEquals("dir/a_3", create("dir", "a_%d", "3"));
        assertEquals("dir/a_4.50", create("dir", "a_%04.2f", "4.5"));
        assertEquals("dir/a_a", create("dir", "a_%s", "a, b"));
        assertEquals("dir/a_a", create("dir", "a_%s", "'a', b"));

        String modtranFormat = "AVHRR_AC_aertype%s_surfref%04.2f_aerdepth%05.3f_wv%04d_o3c0.33176_co2m380_sza%02d_vza%02d_azi%03d_alt%03.1f_band%01d.zip";
        String[] modtranValues = new String[]{"'___rural', 'foo'", "0.15", "0.7", "500", "0", "0", "30", "0.0", "1"};
        String modtranOutput = create("modtran", modtranFormat, modtranValues);

        String expected = "modtran/AVHRR_AC_aertype___rural_surfref0.15_aerdepth0.700_wv0500_o3c0.33176_co2m380_sza00_vza00_azi030_alt0.0_band1.zip";
        assertEquals(expected, modtranOutput);
    }

    public String create(String path, String formatName, String...values) {
        return CombinationsInputFormat.createSplitOutputPath(new Path(path), formatName, values).toString();
    }


    private static void assertSplitEquals(InputSplit inputSplit, String... values) {
        assertSame(CombinationsInputSplit.class, inputSplit.getClass());
        CombinationsInputSplit split = (CombinationsInputSplit) inputSplit;
        assertArrayEquals(values, split.getValues());
    }

    private static List<InputSplit> getInputSplits(CombinationsConfig.Variable... variables) {
        return CombinationsInputFormat.createInputSplits(new CombinationsConfig(variables));
    }
}