package com.bc.calvalus.processing.fire.format.grid.s2;

import org.apache.commons.math3.complex.Complex;

import java.util.Arrays;

class UncertaintyEngine {

    private UncertaintyEngine() {
    }

    public static Complex dft(int r, int n) {
        // return np.exp ( 2.j*r*np.pi/(n+1.))
        return new Complex(0, 2.0).multiply(r * Math.PI).divide(n + 1.0).exp();
    }

    public static Complex product(double[] p, int l) {
        /*
            n = len(p)
            C_to_l = dft(l, n)
            w = np.zeros(n, dtype=np.complex_)
            for i, pm in enumerate(p):
                w[i] = (C_to_l-1.)*pm + 1.0
            return np.prod(w)
         */

        int n = p.length;
        Complex C_to_l = dft(l, n);
        Complex result = new Complex(1, 0);
        for (double pm : p) {
            result = result.multiply(C_to_l.subtract(1.0).multiply(pm).add(1.0));
        }
        return result;
    }

    public static double[] poisson_binomial(double[] p) {
        /*
            n = len(p)
            pdf = np.zeros(n, dtype=np.complex_)
            for k in xrange(n):
                for l in xrange(n):
                    pdf[k] += dft(-k*l, n) * product (p_b, l)
                pdf[k] = pdf[k]/(n+1.)
            pdf = pdf.real
            return pdf
         */

        int n = p.length;
        Complex[] pdf = new Complex[n];
        Arrays.fill(pdf, new Complex(0, 0));
        for (int k = 0; k < n; k++) {
            for (int l = 0; l < n; l++) {
                pdf[k] = pdf[k].add(dft(-k * l, n).multiply(product(p, l)));
            }
            pdf[k] = pdf[k].divide(n + 1.0);
        }
        double[] result = new double[n];
        for (int i = 0; i < pdf.length; i++) {
            Complex complex = pdf[i];
            result[i] = complex.getReal();
        }

        return result;
    }
}
