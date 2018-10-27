package com.bc.calvalus.processing.fire.format.grid.avhrr;

import com.bc.calvalus.processing.fire.format.grid.Majority;
import org.esa.snap.core.dataop.resamp.Resampling;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class MajorityTest {

    final Resampling resampling = new Majority();
    final TestRaster raster = new TestRaster();

    @Test
    public void testCreateIndex() {
        final Resampling.Index index = resampling.createIndex();
        assertNotNull(index);
        assertNotNull(index.i);
        assertNotNull(index.j);
        assertNotNull(index.ki);
        assertNotNull(index.kj);
        assertEquals(19, index.i.length);
        assertEquals(19, index.j.length);
        assertEquals(1, index.ki.length);
        assertEquals(1, index.kj.length);
    }

    @Test
    public void testComputeIndexAndGetSample() throws Exception {
        final Resampling.Index index = resampling.createIndex();
        test(index, 0.5f, 0.0f, 0.0, 0.0, 10f);
        test(index, 0.5f, 2.0f, 0.0, 2.0, 10f);
        test(index, 4.5f, 0.0f, 4.0, 0.0, 50f);
        test(index, 0.5f, 3.9f, 0.0, 3.0, 10f);
        test(index, 2.5f, 1.0f, 2.0, 1.0, 10f);
        test(index, 4.5f, 4.0f, 4.0, 4.0, 10f);
        test(index, 2.9f, 2.9f, 2.0, 2.0, 10f);
    }

    private void test(final Resampling.Index index, float x, float y, double iExp, double jExp, float sampleExp) throws Exception {
        resampling.computeIndex(x, y, raster.getWidth(), raster.getHeight(), index);
        assertEquals(iExp, index.i0, 1E-5);
        assertEquals(jExp, index.j0, 1E-5);
        double sample = resampling.resample(raster, index);
        assertEquals(sampleExp, sample, 1e-5f);
    }

    @Test
    public void testCornerBasedIndex() throws Exception {
        testCornerIndex(0.5f, 0.0f);
        testCornerIndex(0.5f, 2.0f);
        testCornerIndex(4.0f, 0.0f);
        testCornerIndex(0.5f, 3.9f);
        testCornerIndex(2.5f, 1.0f);
        testCornerIndex(4.0f, 4.0f);
        testCornerIndex(2.9f, 2.9f);
    }

    private void testCornerIndex(final float x, final float y) throws Exception {

        final Resampling.Index index = resampling.createIndex();
        resampling.computeCornerBasedIndex(x, y, raster.getWidth(), raster.getHeight(), index);

        final Resampling.Index indexExp = resampling.createIndex();
        computeExpectedIndex(x, y, raster.getWidth(), raster.getHeight(), indexExp);

        assertEquals(indexExp.i0, index.i0, 1E-5);
        assertEquals(indexExp.j0, index.j0, 1E-5);
    }

    private void computeExpectedIndex(
            final double x, final double y, final int width, final int height, final Resampling.Index index) {
        index.x = x;
        index.y = y;
        index.width = width;
        index.height = height;

        index.i0 = Resampling.Index.crop((int) Math.round(x), width - 1);
        index.j0 = Resampling.Index.crop((int) Math.round(y), height - 1);
    }

}