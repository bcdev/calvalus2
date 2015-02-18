package com.bc.calvalus.processing.combinations;

import org.apache.hadoop.mapreduce.InputSplit;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

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

    private static void assertSplitEquals(InputSplit inputSplit, String... values) {
        assertSame(CombinationsInputSplit.class, inputSplit.getClass());
        CombinationsInputSplit split = (CombinationsInputSplit) inputSplit;
        assertArrayEquals(values, split.getValues());
    }

    private static List<InputSplit> getInputSplits(CombinationsConfig.Variable... variables) {
        return CombinationsInputFormat.createInputSplits(new CombinationsConfig(variables));
    }
}