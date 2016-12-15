package com.bc.calvalus.processing.fire.format.grid.s2;

import org.apache.commons.math3.complex.Complex;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertArrayEquals;

public class UncertaintyEngineTest {

    @Test
    public void testDft() throws Exception {
        Complex c = UncertaintyEngine.dft(1, 10);
        assertEquals(c, new Complex(0.84125353283118121, 0.54064081745559756));
    }

    @Test
    public void testProduct() throws Exception {
        Complex c = UncertaintyEngine.product(new double[]{0.8, 0.3, 0.2, 0.9}, 2);
        assertEquals(c, new Complex(0.097174128647468702, -0.13886971673499082));

        c = UncertaintyEngine.product(new double[]{0.2, 0.4, 0.1}, 6);
        assertEquals(c, new Complex(0.095999999999999974, 8.670499337963262e-17));
    }

    @Test
    public void testPoisson() throws Exception {
        double[] result = UncertaintyEngine.poisson_binomial(new double[]{0.8, 0.3, 0.2, 0.9});
        assertArrayEquals(new double[]{0.12503483, 0.14455178, 0.38402028, 0.2241911}, result, 1E-6);

    }
}