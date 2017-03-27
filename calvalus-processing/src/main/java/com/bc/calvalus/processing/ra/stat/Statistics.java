package com.bc.calvalus.processing.ra.stat;

import javax.media.jai.Histogram;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * computes a variety of statsitics from float values
 */
class Statistics {

    interface Stat {

        void process(float... samples);

        void reset();

        List<String> getHeaders(String bandName);

        List<String> getStats();
    }

    /**
     * Statistics that can be gathered without acquiring all data
     */
    static class StatStreaming implements Stat {

        private int numValid;
        private double min;
        private double max;
        private double sum;
        private double sumSQ;
        private double geomMeanProduct;
        private boolean geomMeanValid;


        StatStreaming() {
            reset();
        }

        @Override
        public void process(float... samples) {
            for (float value : samples) {
                if (!Float.isNaN(value)) {
                    numValid++;
                    min = Math.min(min, value);
                    max = Math.max(max, value);
                    sum += value;
                    sumSQ += value * value;
                    if (geomMeanValid) {
                        if (value > 0) {
                            geomMeanProduct *= value;
                        } else {
                            geomMeanValid = false;
                        }
                    }
                }
            }
        }

        @Override
        public void reset() {
            numValid = 0;
            min = +Double.MAX_VALUE;
            max = -Double.MAX_VALUE;
            sum = 0;
            sumSQ = 0;
            geomMeanProduct = 1;
            geomMeanValid = true;
        }

        @Override
        public List<String> getHeaders(String bandName) {
            List<String> header = new ArrayList<>();
            header.add(bandName + "_count");
            header.add(bandName + "_min");
            header.add(bandName + "_max");
            header.add(bandName + "_arithMean");
            header.add(bandName + "_sigma");
            header.add(bandName + "_geomMean");
            return header;
        }

        @Override
        public List<String> getStats() {
            List<String> stats = new ArrayList<>();
            stats.add(Integer.toString(numValid));
            if (numValid > 0) {
                final double arithMean = sum / numValid;
                final double sigmaSqr = sumSQ / numValid - arithMean * arithMean;
                final double sigma = sigmaSqr > 0.0 ? Math.sqrt(sigmaSqr) : 0.0;
                final double geomMean = geomMeanValid ? Math.pow(geomMeanProduct, 1.0 / numValid) : Double.NaN;

                stats.add(Double.toString(min));
                stats.add(Double.toString(max));
                stats.add(Double.toString(arithMean));
                stats.add(Double.toString(sigma));
                stats.add(Double.toString(geomMean));
            } else {
                stats.add(Double.toString(Double.NaN));
                stats.add(Double.toString(Double.NaN));
                stats.add(Double.toString(Double.NaN));
                stats.add(Double.toString(Double.NaN));
                stats.add(Double.toString(Double.NaN));
            }
            return stats;
        }
    }

    /**
     * The histogram of a dataset
     */
    static class StatHisto implements Stat {

        private final Histogram histogram;
        private int belowHistogram;
        private int aboveHistogram;

        StatHisto(int numBins,
                  double lowValue,
                  double highValue) {
            histogram = new Histogram(numBins, lowValue, highValue, 1);
        }

        @Override
        public void process(float... samples) {
            final int[] bins = histogram.getBins(0);
            final double lowValue = histogram.getLowValue(0);
            final double highValue = histogram.getHighValue(0);
            final double binWidth = (highValue - lowValue) / bins.length;
            for (float value : samples) {
                if (!Float.isNaN(value)) {
                    if (value < lowValue) {
                        belowHistogram++;
                    } else if (value > highValue) {
                        aboveHistogram++;
                    } else {
                        int i = (int) ((value - lowValue) / binWidth);
                        if (i == bins.length) {
                            i--;
                        }
                        bins[i]++;
                    }
                }
            }
        }

        @Override
        public void reset() {
            histogram.clearHistogram();
        }

        @Override
        public List<String> getHeaders(String bandName) {
            List<String> header = new ArrayList<>();
            header.add("belowHistogram");
            header.add("aboveHistogram");
            header.add("numBins");
            header.add("lowValue");
            header.add("highValue");
            for (int i = 0; i < histogram.getNumBins(0); i++) {
                header.add("bin_" + i);
            }
            return header;
        }

        @Override
        public List<String> getStats() {
            List<String> stats = new ArrayList<>();
            stats.add(Integer.toString(belowHistogram));
            stats.add(Integer.toString(aboveHistogram));
            stats.add(Integer.toString(histogram.getNumBins(0)));
            stats.add(Double.toString(histogram.getLowValue(0)));
            stats.add(Double.toString(histogram.getHighValue(0)));
            int[] bins = histogram.getBins(0);
            for (int bin : bins) {
                stats.add(Integer.toString(bin));
            }
            return stats;
        }
    }

    /**
     * Statistics that require the complete dataset.
     */
    static class StatAccu implements Stat {

        private final Accumulator accu;

        StatAccu() {
            accu = new Accumulator();
        }

        @Override
        public void process(float... samples) {
            accu.accumulate(samples);
        }

        @Override
        public void reset() {
            accu.clear();
        }

        @Override
        public List<String> getHeaders(String bandName) {
            List<String> header = new ArrayList<>();
            header.add(bandName + "_p5");
            header.add(bandName + "_p25");
            header.add(bandName + "_p50");
            header.add(bandName + "_p75");
            header.add(bandName + "_p95");
            return header;
        }

        @Override
        public List<String> getStats() {
            float[] values = accu.getValues();
            Arrays.sort(values);
            List<String> stats = new ArrayList<>();
            stats.add(Double.toString(computePercentile(5, values)));
            stats.add(Double.toString(computePercentile(25, values)));
            stats.add(Double.toString(computePercentile(50, values)));
            stats.add(Double.toString(computePercentile(75, values)));
            stats.add(Double.toString(computePercentile(95, values)));
            return stats;
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
        if (N == 0) {
            return Double.NaN;
        }
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
}
