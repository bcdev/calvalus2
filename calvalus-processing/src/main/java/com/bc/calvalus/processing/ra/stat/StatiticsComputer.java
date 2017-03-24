package com.bc.calvalus.processing.ra.stat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * computes a variety of statsitics from float values (NaN free)
 */
class StatiticsComputer {

    private final String bandname;
    private final int numValid;
    private double min;
    private double max;
    private final double arithMean;
    private final double sigma;
    private final double geomMean;
    private final double p5;
    private final double p25;
    private final double p50;
    private final double p75;
    private final double p95;
//        private final double mode; //TODO

    public StatiticsComputer(String name, float... values) {
        this.bandname = name;
        this.numValid = values.length;
        if (numValid > 0) {
            min = +Double.MAX_VALUE;
            max = -Double.MAX_VALUE;
            double sum = 0;
            double sumSQ = 0;
            double product = 1;
            boolean geomMeanValid = true;
            for (float value : values) {
                min = Math.min(min, value);
                max = Math.max(max, value);
                sum += value;
                sumSQ += value * value;
                if (geomMeanValid) {
                    if (value > 0) {
                        product *= value;
                    } else {
                        geomMeanValid = false;
                    }
                }
            }
            arithMean = sum / numValid;
            double sigmaSqr = sumSQ / numValid - arithMean * arithMean;
            sigma = sigmaSqr > 0.0 ? Math.sqrt(sigmaSqr) : 0.0;
            geomMean = geomMeanValid ? Math.pow(product, 1.0 / numValid) : Double.NaN;
            Arrays.sort(values);
            p5 = computePercentile(5, values);
            p25 = computePercentile(25, values);
            p50 = computePercentile(50, values);
            p75 = computePercentile(75, values);
            p95 = computePercentile(95, values);
        } else {
            min = Double.NaN;
            max = Double.NaN;
            arithMean = Double.NaN;
            sigma = Double.NaN;
            geomMean = Double.NaN;
            p5 = Double.NaN;
            p25 = Double.NaN;
            p50 = Double.NaN;
            p75 = Double.NaN;
            p95 = Double.NaN;
        }
    }

    /**
     * Computes the p-th percentile of an array of measurements following
     * the "Engineering Statistics Handbook: Percentile". NIST.
     * http://www.itl.nist.gov/div898/handbook/prc/section2/prc252.htm.
     * Retrieved 2011-03-16.
     *
     * @param p            The percentage in percent ranging from 0 to 100.
     * @param measurements Sorted array of measurements.
     * @return The  p-th percentile.
     */
    static double computePercentile(int p, float[] measurements) {
        int N = measurements.length;
        double n = (p / 100.0) * (N + 1);
        int k = (int) Math.floor(n);
        double d = n - k;
        double yp;
        if (k == 0) {
            yp = measurements[0];
        } else if (k >= N) {
            yp = measurements[N - 1];
        } else {
            yp = measurements[k - 1] + d * (measurements[k] - measurements[k - 1]);
        }
        return yp;
    }

    public List<String> getHeader() {
        List<String> header = new ArrayList<>();
        header.add(bandname + "_count");
        header.add(bandname + "_min");
        header.add(bandname + "_max");
        header.add(bandname + "_arithMean");
        header.add(bandname + "_sigma");
        header.add(bandname + "_geomMean");
        header.add(bandname + "_p5");
        header.add(bandname + "_p25");
        header.add(bandname + "_p50");
        header.add(bandname + "_p75");
        header.add(bandname + "_p95");
        return header;
    }

    public List<String> getStats() {
        List<String> stats = new ArrayList<>();
        stats.add(Integer.toString(numValid));
        stats.add(Double.toString(min));
        stats.add(Double.toString(max));
        stats.add(Double.toString(arithMean));
        stats.add(Double.toString(sigma));
        stats.add(Double.toString(geomMean));
        stats.add(Double.toString(p5));
        stats.add(Double.toString(p25));
        stats.add(Double.toString(p50));
        stats.add(Double.toString(p75));
        stats.add(Double.toString(p95));
        return stats;
    }
}
