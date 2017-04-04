package com.bc.calvalus.processing.ra.stat;

import javax.media.jai.Histogram;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Computes a variety of statistics from float values
 */
class Statistics {


    private int numValid;
    private double min;
    private double max;
    private double sum;
    private double sumSQ;
    private double geomMeanProduct;
    private boolean geomMeanValid;

    private final Histogram histogram;
    private int belowHistogram;
    private int aboveHistogram;

    private final int[] percentiles;
    private final Accumulator accu;

    Statistics() {
        this(0, Double.NaN, Double.NaN);
    }

    Statistics(int numBins,
                   double lowValue,
                   double highValue) {
        this(numBins, lowValue, highValue, new int[]{5,25,50,75,95});
    }

    Statistics(int numBins,
               double lowValue,
               double highValue,
               int[] percentiles) {
        if (numBins > 0) {
            histogram = new Histogram(numBins, lowValue, highValue, 1);
        } else {
            histogram = null;
        }
        if (percentiles != null && percentiles.length > 0) {
            this.percentiles = percentiles;
            this.accu = new Accumulator();
        } else {
            this.percentiles = null;
            this.accu = null;
        }
        reset();
    }

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
                        // the geometric mean is only defined, if all values are bigger than zero
                        geomMeanValid = false;
                    }
                }
            }
        }
        if (histogram != null) {
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
        if (accu != null) {
            accu.accumulate(samples);
        }
    }

    public void reset() {
        numValid = 0;
        min = +Double.MAX_VALUE;
        max = -Double.MAX_VALUE;
        sum = 0;
        sumSQ = 0;
        geomMeanProduct = 1;
        geomMeanValid = true;
        if (histogram != null) {
            histogram.clearHistogram();
        }
        if (accu != null) {
            accu.clear();
        }
    }

    public List<String> getStatisticsHeaders(String bandName) {
        List<String> header = new ArrayList<>();
        header.add(bandName + "_numValid");
        header.add(bandName + "_min");
        header.add(bandName + "_max");
        header.add(bandName + "_arithMean");
        header.add(bandName + "_sigma");
        header.add(bandName + "_geomMean");

        if (percentiles != null) {
            for (int percentile : percentiles) {
                header.add(String.format("%s_p%02d", bandName, percentile));
            }
        }
        return header;
    }

    public List<String> getHistogramHeaders(String bandName) {
        List<String> header = new ArrayList<>();
        if (histogram != null) {
            header.add(bandName + "_belowHistogram");
            header.add(bandName + "_aboveHistogram");
            header.add(bandName + "_numBins");
            header.add(bandName + "_lowValue");
            header.add(bandName + "_highValue");
            for (int i = 0; i < histogram.getNumBins(0); i++) {
                header.add(bandName + "_bin_" + i);
            }
        }
        return header;
    }


    public List<String> getStatisticsRecords() {
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
        if (accu != null) {
            float[] values = accu.getValues();
            Arrays.sort(values);
            for (int percentile : percentiles) {
                stats.add(Double.toString(computePercentile(percentile, values)));
            }
        }
        return stats;
    }

    public List<String> getHistogramRecords() {
        List<String> stats = new ArrayList<>();
        if (histogram != null) {
            stats.add(Integer.toString(belowHistogram));
            stats.add(Integer.toString(aboveHistogram));
            stats.add(Integer.toString(histogram.getNumBins(0)));
            stats.add(Double.toString(histogram.getLowValue(0)));
            stats.add(Double.toString(histogram.getHighValue(0)));
            int[] bins = histogram.getBins(0);
            for (int bin : bins) {
                stats.add(Integer.toString(bin));
            }
        }
        return stats;
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
