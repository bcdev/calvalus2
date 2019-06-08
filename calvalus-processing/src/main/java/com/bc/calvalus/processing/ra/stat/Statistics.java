package com.bc.calvalus.processing.ra.stat;

//import javax.media.jai.Histogram;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Computes a variety of statistics from float values
 *
 * For geometric mean:
 * see: https://en.wikipedia.org/wiki/Geometric_mean#Relationship_with_logarithms
 *
 * The log form of the geometric mean is generally the preferred alternative for implementation
 * in computer languages because calculating the product of many numbers can lead to an
 * arithmetic overflow or arithmetic underflow.
 * This is less likely to occur with the sum of the logarithms for each number.
 *
 */
class Statistics {


    private long numValid;
    private double min;
    private double max;
    private double sum;
    private double sumSQ;
    private long geomNumValid;
    private double geomLogSum;

    private final Histogram64 histogram;
    private long belowHistogram;
    private long aboveHistogram;

    private final int[] percentiles;
    private final Accumulator accu;
    private final boolean binValuesAsRatio;

    Statistics() {
        this(0, Double.NaN, Double.NaN);
    }

    Statistics(int numBins,
                   double lowValue,
                   double highValue) {
        this(numBins, lowValue, highValue, new int[]{5,25,50,75,95}, false);
    }

    Statistics(int numBins,
               double lowValue,
               double highValue,
               int[] percentiles,
               boolean binValuesAsRatio) {
        if (numBins > 0) {
            histogram = new Histogram64(numBins, lowValue, highValue, 1);
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
        this.binValuesAsRatio = binValuesAsRatio;
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
                if (value > 0) {
                    // the geometric mean is only defined, if for values bigger than zero
                    geomNumValid++;
                    geomLogSum += Math.log(value);
                }
            }
        }
        if (histogram != null) {
            final long[] bins = histogram.getBins(0);
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
            accu.accumulateNoNaN(samples);
        }
    }

    public void reset() {
        numValid = 0;
        min = +Double.MAX_VALUE;
        max = -Double.MAX_VALUE;
        sum = 0;
        sumSQ = 0;
        geomNumValid = 0;
        geomLogSum = 0;
        if (histogram != null) {
            histogram.clearHistogram();
            belowHistogram = 0;
            aboveHistogram = 0;
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
        stats.add(Long.toString(numValid));
        if (numValid > 0) {
            final double arithMean = sum / numValid;
            final double sigmaSqr = sumSQ / numValid - arithMean * arithMean;
            final double sigma = sigmaSqr > 0.0 ? Math.sqrt(sigmaSqr) : 0.0;
            final double geomMean = geomNumValid > 0 ? Math.exp(geomLogSum / geomNumValid) : Double.NaN;

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
            if (! binValuesAsRatio) {
                stats.add(Long.toString(belowHistogram));
                stats.add(Long.toString(aboveHistogram));
            } else {
                stats.add(Double.toString(((double)belowHistogram) / numValid));
                stats.add(Double.toString(((double)aboveHistogram) / numValid));
            }
            stats.add(Integer.toString(histogram.getNumBins(0)));
            stats.add(Double.toString(histogram.getLowValue(0)));
            stats.add(Double.toString(histogram.getHighValue(0)));
            long[] bins = histogram.getBins(0);
            for (long bin : bins) {
                if (! binValuesAsRatio) {
                    stats.add(Long.toString(bin));
                } else {
                    stats.add(Double.toString(((double) bin) / numValid));
                    
                }
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
