package com.bc.calvalus.processing.ra.stat;

//import javax.media.jai.JaiI18N;
import javax.media.jai.PixelAccessor;
import javax.media.jai.ROI;
import javax.media.jai.UnpackedImageData;
import java.awt.Rectangle;
import java.awt.image.ColorModel;
import java.awt.image.Raster;
import java.awt.image.SampleModel;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.ListIterator;

/**
 * TODO add API doc
 *
 * @author Martin Boettcher
 */
public class Histogram64 implements Serializable {
    private int[] numBins;
    private double[] lowValue;
    private double[] highValue;
    private int numBands;
    private double[] binWidth;
    private long[][] bins;
    private long[] totals;
    private double[] mean;

    private static final int[] fill(int[] array, int newLength) {
        int[] newArray = null;
        if (array != null && array.length != 0) {
            if (newLength > 0) {
                newArray = new int[newLength];
                int oldLength = array.length;

                for(int i = 0; i < newLength; ++i) {
                    if (i < oldLength) {
                        newArray[i] = array[i];
                    } else {
                        newArray[i] = array[0];
                    }
                }
            }

            return newArray;
        } else {
            throw new IllegalArgumentException("JaiI18N.getString of " + ("Generic0"));
        }
    }

    private static final double[] fill(double[] array, int newLength) {
        double[] newArray = null;
        if (array != null && array.length != 0) {
            if (newLength > 0) {
                newArray = new double[newLength];
                int oldLength = array.length;

                for(int i = 0; i < newLength; ++i) {
                    if (i < oldLength) {
                        newArray[i] = array[i];
                    } else {
                        newArray[i] = array[0];
                    }
                }
            }

            return newArray;
        } else {
            throw new IllegalArgumentException("JaiI18N.getString of " + ("Generic0"));
        }
    }

    public Histogram64(int[] numBins, double[] lowValue, double[] highValue) {
        this.bins = (long[][])null;
        this.totals = null;
        this.mean = null;
        if (numBins != null && lowValue != null && highValue != null) {
            this.numBands = numBins.length;
            if (lowValue.length == this.numBands && highValue.length == this.numBands) {
                if (this.numBands == 0) {
                    throw new IllegalArgumentException("JaiI18N.getString of " + ("Histogram1"));
                } else {
                    int i;
                    for(i = 0; i < this.numBands; ++i) {
                        if (numBins[i] <= 0) {
                            throw new IllegalArgumentException("JaiI18N.getString of " + ("Histogram2"));
                        }

                        if (lowValue[i] >= highValue[i]) {
                            throw new IllegalArgumentException("JaiI18N.getString of " + ("Histogram3"));
                        }
                    }

                    this.numBins = (int[])numBins.clone();
                    this.lowValue = (double[])lowValue.clone();
                    this.highValue = (double[])highValue.clone();
                    this.binWidth = new double[this.numBands];

                    for(i = 0; i < this.numBands; ++i) {
                        this.binWidth[i] = (highValue[i] - lowValue[i]) / (double)numBins[i];
                    }

                }
            } else {
                throw new IllegalArgumentException("JaiI18N.getString of " + ("Histogram0"));
            }
        } else {
            throw new IllegalArgumentException("JaiI18N.getString of " + ("Generic0"));
        }
    }

    public Histogram64(int[] numBins, double[] lowValue, double[] highValue, int numBands) {
        this(fill(numBins, numBands), fill(lowValue, numBands), fill(highValue, numBands));
    }

    public Histogram64(int numBins, double lowValue, double highValue, int numBands) {
        this.bins = (long[][])null;
        this.totals = null;
        this.mean = null;
        if (numBands <= 0) {
            throw new IllegalArgumentException("JaiI18N.getString of " + ("Histogram1"));
        } else if (numBins <= 0) {
            throw new IllegalArgumentException("JaiI18N.getString of " + ("Histogram2"));
        } else if (lowValue >= highValue) {
            throw new IllegalArgumentException("JaiI18N.getString of " + ("Histogram3"));
        } else {
            this.numBands = numBands;
            this.numBins = new int[numBands];
            this.lowValue = new double[numBands];
            this.highValue = new double[numBands];
            this.binWidth = new double[numBands];
            double bw = (highValue - lowValue) / (double)numBins;

            for(int i = 0; i < numBands; ++i) {
                this.numBins[i] = numBins;
                this.lowValue[i] = lowValue;
                this.highValue[i] = highValue;
                this.binWidth[i] = bw;
            }

        }
    }

    public int[] getNumBins() {
        return (int[])this.numBins.clone();
    }

    public int getNumBins(int band) {
        return this.numBins[band];
    }

    public double[] getLowValue() {
        return (double[])this.lowValue.clone();
    }

    public double getLowValue(int band) {
        return this.lowValue[band];
    }

    public double[] getHighValue() {
        return (double[])this.highValue.clone();
    }

    public double getHighValue(int band) {
        return this.highValue[band];
    }

    public int getNumBands() {
        return this.numBands;
    }

    public synchronized long[][] getBins() {
        if (this.bins == null) {
            this.bins = new long[this.numBands][];

            for(int i = 0; i < this.numBands; ++i) {
                this.bins[i] = new long[this.numBins[i]];
            }
        }

        return this.bins;
    }

    public long[] getBins(int band) {
        this.getBins();
        return this.bins[band];
    }

    public long getBinSize(int band, int bin) {
        this.getBins();
        return this.bins[band][bin];
    }

    public double getBinLowValue(int band, int bin) {
        return this.lowValue[band] + (double)bin * this.binWidth[band];
    }

    public void clearHistogram() {
        if (this.bins != null) {
            long[][] var1 = this.bins;
            synchronized(this.bins) {
                for(int i = 0; i < this.numBands; ++i) {
                    long[] b = this.bins[i];
                    int length = b.length;

                    for(int j = 0; j < length; ++j) {
                        b[j] = 0;
                    }
                }
            }
        }

    }

    public long[] getTotals() {
        if (this.totals == null) {
            this.getBins();
            synchronized(this) {
                this.totals = new long[this.numBands];

                for(int i = 0; i < this.numBands; ++i) {
                    long[] b = this.bins[i];
                    int length = b.length;
                    int t = 0;

                    for(int j = 0; j < length; ++j) {
                        t += b[j];
                    }

                    this.totals[i] = t;
                }
            }
        }

        return this.totals;
    }

    public int getSubTotal(int band, int minBin, int maxBin) {
        if (minBin >= 0 && maxBin < this.numBins[band]) {
            if (minBin > maxBin) {
                throw new IllegalArgumentException("JaiI18N.getString of " + ("Histogram10"));
            } else {
                long[] b = this.getBins(band);
                int total = 0;

                for(int i = minBin; i <= maxBin; ++i) {
                    total += b[i];
                }

                return total;
            }
        } else {
            throw new ArrayIndexOutOfBoundsException("JaiI18N.getString of " + ("Histogram5"));
        }
    }

    public double[] getMean() {
        if (this.mean == null) {
            this.getTotals();
            synchronized(this) {
                this.mean = new double[this.numBands];

                for(int i = 0; i < this.numBands; ++i) {
                    long[] counts = this.getBins(i);
                    int nBins = this.numBins[i];
                    double level = this.getLowValue(i);
                    double bw = this.binWidth[i];
                    double mu = 0.0D;
                    double total = (double)this.totals[i];

                    for(int b = 0; b < nBins; ++b) {
                        mu += (double)counts[b] / total * level;
                        level += bw;
                    }

                    this.mean[i] = mu;
                }
            }
        }

        return this.mean;
    }

    public void countPixels(Raster raster, ROI roi, int xStart, int yStart, int xPeriod, int yPeriod) {
        if (raster == null) {
            throw new IllegalArgumentException("JaiI18N.getString of " + ("Generic0"));
        } else {
            SampleModel sampleModel = raster.getSampleModel();
            if (sampleModel.getNumBands() != this.numBands) {
                throw new IllegalArgumentException("JaiI18N.getString of " + ("Histogram4"));
            } else {
                Rectangle bounds = raster.getBounds();
                LinkedList rectList;
                if (roi == null) {
                    rectList = new LinkedList();
                    rectList.addLast(bounds);
                } else {
                    rectList = roi.getAsRectangleList(bounds.x, bounds.y, bounds.width, bounds.height);
                    if (rectList == null) {
                        return;
                    }
                }

                PixelAccessor accessor = new PixelAccessor(sampleModel, (ColorModel)null);
                ListIterator iterator = rectList.listIterator(0);

                while(iterator.hasNext()) {
                    Rectangle r = (Rectangle)iterator.next();
                    int tx = r.x;
                    int ty = r.y;
                    r.x = this.startPosition(tx, xStart, xPeriod);
                    r.y = this.startPosition(ty, yStart, yPeriod);
                    r.width = tx + r.width - r.x;
                    r.height = ty + r.height - r.y;
                    if (r.width > 0 && r.height > 0) {
                        switch(accessor.sampleType) {
                        case -1:
                        case 0:
                            this.countPixelsByte(accessor, raster, r, xPeriod, yPeriod);
                            break;
                        case 1:
                            this.countPixelsUShort(accessor, raster, r, xPeriod, yPeriod);
                            break;
                        case 2:
                            this.countPixelsShort(accessor, raster, r, xPeriod, yPeriod);
                            break;
                        case 3:
                            this.countPixelsInt(accessor, raster, r, xPeriod, yPeriod);
                            break;
                        case 4:
                            this.countPixelsFloat(accessor, raster, r, xPeriod, yPeriod);
                            break;
                        case 5:
                            this.countPixelsDouble(accessor, raster, r, xPeriod, yPeriod);
                            break;
                        default:
                            throw new RuntimeException("JaiI18N.getString of " + ("Histogram11"));
                        }
                    }
                }

            }
        }
    }

    private void countPixelsByte(PixelAccessor accessor, Raster raster, Rectangle rect, int xPeriod, int yPeriod) {
        UnpackedImageData uid = accessor.getPixels(raster, rect, 0, false);
        byte[][] byteData = uid.getByteData();
        int pixelStride = uid.pixelStride * xPeriod;
        int lineStride = uid.lineStride * yPeriod;
        int[] offsets = uid.bandOffsets;

        for(int b = 0; b < this.numBands; ++b) {
            byte[] data = byteData[b];
            int lineOffset = offsets[b];
            int[] bin = new int[this.numBins[b]];
            double low = this.lowValue[b];
            double high = this.highValue[b];
            double bwidth = this.binWidth[b];

            for(int h = 0; h < rect.height; h += yPeriod) {
                int pixelOffset = lineOffset;
                lineOffset += lineStride;

                for(int w = 0; w < rect.width; w += xPeriod) {
                    int d = data[pixelOffset] & 255;
                    pixelOffset += pixelStride;
                    if ((double)d >= low && (double)d < high) {
                        int i = (int)(((double)d - low) / bwidth);
                        ++bin[i];
                    }
                }
            }

            this.mergeBins(b, bin);
        }

    }

    private void countPixelsUShort(PixelAccessor accessor, Raster raster, Rectangle rect, int xPeriod, int yPeriod) {
        UnpackedImageData uid = accessor.getPixels(raster, rect, 1, false);
        short[][] shortData = uid.getShortData();
        int pixelStride = uid.pixelStride * xPeriod;
        int lineStride = uid.lineStride * yPeriod;
        int[] offsets = uid.bandOffsets;

        for(int b = 0; b < this.numBands; ++b) {
            short[] data = shortData[b];
            int lineOffset = offsets[b];
            int[] bin = new int[this.numBins[b]];
            double low = this.lowValue[b];
            double high = this.highValue[b];
            double bwidth = this.binWidth[b];

            for(int h = 0; h < rect.height; h += yPeriod) {
                int pixelOffset = lineOffset;
                lineOffset += lineStride;

                for(int w = 0; w < rect.width; w += xPeriod) {
                    int d = data[pixelOffset] & '\uffff';
                    pixelOffset += pixelStride;
                    if ((double)d >= low && (double)d < high) {
                        int i = (int)(((double)d - low) / bwidth);
                        ++bin[i];
                    }
                }
            }

            this.mergeBins(b, bin);
        }

    }

    private void countPixelsShort(PixelAccessor accessor, Raster raster, Rectangle rect, int xPeriod, int yPeriod) {
        UnpackedImageData uid = accessor.getPixels(raster, rect, 2, false);
        short[][] shortData = uid.getShortData();
        int pixelStride = uid.pixelStride * xPeriod;
        int lineStride = uid.lineStride * yPeriod;
        int[] offsets = uid.bandOffsets;

        for(int b = 0; b < this.numBands; ++b) {
            short[] data = shortData[b];
            int lineOffset = offsets[b];
            int[] bin = new int[this.numBins[b]];
            double low = this.lowValue[b];
            double high = this.highValue[b];
            double bwidth = this.binWidth[b];

            for(int h = 0; h < rect.height; h += yPeriod) {
                int pixelOffset = lineOffset;
                lineOffset += lineStride;

                for(int w = 0; w < rect.width; w += xPeriod) {
                    int d = data[pixelOffset];
                    pixelOffset += pixelStride;
                    if ((double)d >= low && (double)d < high) {
                        int i = (int)(((double)d - low) / bwidth);
                        ++bin[i];
                    }
                }
            }

            this.mergeBins(b, bin);
        }

    }

    private void countPixelsInt(PixelAccessor accessor, Raster raster, Rectangle rect, int xPeriod, int yPeriod) {
        UnpackedImageData uid = accessor.getPixels(raster, rect, 3, false);
        int[][] intData = uid.getIntData();
        int pixelStride = uid.pixelStride * xPeriod;
        int lineStride = uid.lineStride * yPeriod;
        int[] offsets = uid.bandOffsets;

        for(int b = 0; b < this.numBands; ++b) {
            int[] data = intData[b];
            int lineOffset = offsets[b];
            int[] bin = new int[this.numBins[b]];
            double low = this.lowValue[b];
            double high = this.highValue[b];
            double bwidth = this.binWidth[b];

            for(int h = 0; h < rect.height; h += yPeriod) {
                int pixelOffset = lineOffset;
                lineOffset += lineStride;

                for(int w = 0; w < rect.width; w += xPeriod) {
                    int d = data[pixelOffset];
                    pixelOffset += pixelStride;
                    if ((double)d >= low && (double)d < high) {
                        int i = (int)(((double)d - low) / bwidth);
                        ++bin[i];
                    }
                }
            }

            this.mergeBins(b, bin);
        }

    }

    private void countPixelsFloat(PixelAccessor accessor, Raster raster, Rectangle rect, int xPeriod, int yPeriod) {
        UnpackedImageData uid = accessor.getPixels(raster, rect, 4, false);
        float[][] floatData = uid.getFloatData();
        int pixelStride = uid.pixelStride * xPeriod;
        int lineStride = uid.lineStride * yPeriod;
        int[] offsets = uid.bandOffsets;

        for(int b = 0; b < this.numBands; ++b) {
            float[] data = floatData[b];
            int lineOffset = offsets[b];
            int[] bin = new int[this.numBins[b]];
            double low = this.lowValue[b];
            double high = this.highValue[b];
            double bwidth = this.binWidth[b];

            for(int h = 0; h < rect.height; h += yPeriod) {
                int pixelOffset = lineOffset;
                lineOffset += lineStride;

                for(int w = 0; w < rect.width; w += xPeriod) {
                    float d = data[pixelOffset];
                    pixelOffset += pixelStride;
                    if ((double)d >= low && (double)d < high) {
                        int i = (int)(((double)d - low) / bwidth);
                        ++bin[i];
                    }
                }
            }

            this.mergeBins(b, bin);
        }

    }

    private void countPixelsDouble(PixelAccessor accessor, Raster raster, Rectangle rect, int xPeriod, int yPeriod) {
        UnpackedImageData uid = accessor.getPixels(raster, rect, 5, false);
        double[][] doubleData = uid.getDoubleData();
        int pixelStride = uid.pixelStride * xPeriod;
        int lineStride = uid.lineStride * yPeriod;
        int[] offsets = uid.bandOffsets;

        for(int b = 0; b < this.numBands; ++b) {
            double[] data = doubleData[b];
            int lineOffset = offsets[b];
            int[] bin = new int[this.numBins[b]];
            double low = this.lowValue[b];
            double high = this.highValue[b];
            double bwidth = this.binWidth[b];

            for(int h = 0; h < rect.height; h += yPeriod) {
                int pixelOffset = lineOffset;
                lineOffset += lineStride;

                for(int w = 0; w < rect.width; w += xPeriod) {
                    double d = data[pixelOffset];
                    pixelOffset += pixelStride;
                    if (d >= low && d < high) {
                        int i = (int)((d - low) / bwidth);
                        ++bin[i];
                    }
                }
            }

            this.mergeBins(b, bin);
        }

    }

    private int startPosition(int pos, int start, int Period) {
        int t = (pos - start) % Period;
        return t == 0 ? pos : pos + (Period - t);
    }

    private void mergeBins(int band, int[] bin) {
        this.getBins();
        long[][] var3 = this.bins;
        synchronized(this.bins) {
            long[] b = this.bins[band];
            int length = b.length;

            for(int i = 0; i < length; ++i) {
                b[i] += bin[i];
            }

        }
    }

    public double[] getMoment(int moment, boolean isAbsolute, boolean isCentral) {
        if (moment < 1) {
            throw new IllegalArgumentException("JaiI18N.getString of " + ("Histogram6"));
        } else {
            if ((moment == 1 || isCentral) && this.mean == null) {
                this.getMean();
            }

            if (moment == 1 && !isAbsolute && !isCentral) {
                return this.mean;
            } else {
                double[] moments = new double[this.numBands];
                int band;
                if (moment == 1 && isCentral) {
                    for(band = 0; band < this.numBands; ++band) {
                        moments[band] = 0.0D;
                    }
                } else {
                    this.getTotals();

                    for(band = 0; band < this.numBands; ++band) {
                        long[] counts = this.getBins(band);
                        int nBins = this.numBins[band];
                        double level = this.getLowValue(band);
                        double bw = this.binWidth[band];
                        double total = (double)this.totals[band];
                        double mmt = 0.0D;
                        if (isCentral) {
                            double mu = this.mean[band];
                            int b;
                            if (isAbsolute && moment % 2 == 0) {
                                for(b = 0; b < nBins; ++b) {
                                    mmt += Math.pow(level - mu, (double)moment) * (double)counts[b] / total;
                                    level += bw;
                                }
                            } else {
                                for(b = 0; b < nBins; ++b) {
                                    mmt += Math.abs(Math.pow(level - mu, (double)moment)) * (double)counts[b] / total;
                                    level += bw;
                                }
                            }
                        } else {
                            int b;
                            if (isAbsolute && moment % 2 != 0) {
                                for(b = 0; b < nBins; ++b) {
                                    mmt += Math.abs(Math.pow(level, (double)moment)) * (double)counts[b] / total;
                                    level += bw;
                                }
                            } else {
                                for(b = 0; b < nBins; ++b) {
                                    mmt += Math.pow(level, (double)moment) * (double)counts[b] / total;
                                    level += bw;
                                }
                            }
                        }

                        moments[band] = mmt;
                    }
                }

                return moments;
            }
        }
    }

    public double[] getStandardDeviation() {
        this.getMean();
        double[] variance = this.getMoment(2, false, false);
        double[] stdev = new double[this.numBands];

        for(int i = 0; i < variance.length; ++i) {
            stdev[i] = Math.sqrt(variance[i] - this.mean[i] * this.mean[i]);
        }

        return stdev;
    }

    public double[] getEntropy() {
        this.getTotals();
        double log2 = Math.log(2.0D);
        double[] entropy = new double[this.numBands];

        for(int band = 0; band < this.numBands; ++band) {
            long[] counts = this.getBins(band);
            int nBins = this.numBins[band];
            double total = (double)this.totals[band];
            double H = 0.0D;

            for(int b = 0; b < nBins; ++b) {
                double p = (double)counts[b] / total;
                if (p != 0.0D) {
                    H -= p * (Math.log(p) / log2);
                }
            }

            entropy[band] = H;
        }

        return entropy;
    }

    public Histogram64 getSmoothed(boolean isWeighted, int k) {
        if (k < 0) {
            throw new IllegalArgumentException("JaiI18N.getString of " + ("Histogram7"));
        } else if (k == 0) {
            return this;
        } else {
            Histogram64 smoothedHistogram = new Histogram64(this.getNumBins(), this.getLowValue(), this.getHighValue());
            long[][] smoothedBins = smoothedHistogram.getBins();
            this.getTotals();
            double[] weights = null;
            int band;
            int nBins;
            if (isWeighted) {
                band = 2 * k + 1;
                double denom = (double)(band * band);
                weights = new double[band];

                for(nBins = 0; nBins <= k; ++nBins) {
                    weights[nBins] = (double)(nBins + 1) / denom;
                }

                for(nBins = k + 1; nBins < band; ++nBins) {
                    weights[nBins] = weights[band - 1 - nBins];
                }
            }

            for(band = 0; band < this.numBands; ++band) {
                long[] counts = this.getBins(band);
                long[] smoothedCounts = smoothedBins[band];
                nBins = smoothedHistogram.getNumBins(band);
                int sum = 0;
                int b;
                int min;
                int max;
                int acc;
                if (isWeighted) {
                    for(b = 0; b < nBins; ++b) {
                        min = Math.max(b - k, 0);
                        max = Math.min(b + k, nBins);
                        acc = k > b ? k - b : 0;
                        double accd = 0.0D;
                        double weightTotal = 0.0D;

                        for(int i = min; i < max; ++i) {
                            double w = weights[acc++];
                            accd += (double)counts[i] * w;
                            weightTotal += w;
                        }

                        smoothedCounts[b] = (int)(accd / weightTotal + 0.5D);
                        sum += smoothedCounts[b];
                    }
                } else {
                    for(b = 0; b < nBins; ++b) {
                        min = Math.max(b - k, 0);
                        max = Math.min(b + k, nBins);
                        acc = 0;

                        for(int i = min; i < max; ++i) {
                            acc += counts[i];
                        }

                        smoothedCounts[b] = (int)((double)acc / (double)(max - min + 1) + 0.5D);
                        sum += smoothedCounts[b];
                    }
                }

                double factor = (double)this.totals[band] / (double)sum;

                for(max = 0; max < nBins; ++max) {
                    smoothedCounts[max] = (int)((double)smoothedCounts[max] * factor + 0.5D);
                }
            }

            return smoothedHistogram;
        }
    }

    public Histogram64 getGaussianSmoothed(double standardDeviation) {
        if (standardDeviation < 0.0D) {
            throw new IllegalArgumentException("JaiI18N.getString of " + ("Histogram8"));
        } else if (standardDeviation == 0.0D) {
            return this;
        } else {
            Histogram64 smoothedHistogram = new Histogram64(this.getNumBins(), this.getLowValue(), this.getHighValue());
            long[][] smoothedBins = smoothedHistogram.getBins();
            this.getTotals();
            int numWeights = (int)(5.16D * standardDeviation + 0.5D);
            if (numWeights % 2 == 0) {
                ++numWeights;
            }

            double[] weights = new double[numWeights];
            int m = numWeights / 2;
            double var = standardDeviation * standardDeviation;
            double gain = 1.0D / Math.sqrt(6.283185307179586D * var);
            double exp = -1.0D / (2.0D * var);

            int band;
            for(band = m; band < numWeights; ++band) {
                double del = (double)(band - m);
                weights[band] = weights[numWeights - 1 - band] = gain * Math.exp(exp * del * del);
            }

            for(band = 0; band < this.numBands; ++band) {
                long[] counts = this.getBins(band);
                long[] smoothedCounts = smoothedBins[band];
                int nBins = smoothedHistogram.getNumBins(band);
                int sum = 0;

                int b;
                for(b = 0; b < nBins; ++b) {
                    int min = Math.max(b - m, 0);
                    b = Math.min(b + m, nBins);
                    int offset = m > b ? m - b : 0;
                    double acc = 0.0D;
                    double weightTotal = 0.0D;

                    for(int i = min; i < b; ++i) {
                        double w = weights[offset++];
                        acc += (double)counts[i] * w;
                        weightTotal += w;
                    }

                    smoothedCounts[b] = (int)(acc / weightTotal + 0.5D);
                    sum += smoothedCounts[b];
                }

                double factor = (double)this.totals[band] / (double)sum;

                for(b = 0; b < nBins; ++b) {
                    smoothedCounts[b] = (int)((double)smoothedCounts[b] * factor + 0.5D);
                }
            }

            return smoothedHistogram;
        }
    }

    public double[] getPTileThreshold(double p) {
        if (p > 0.0D && p < 1.0D) {
            double[] thresholds = new double[this.numBands];
            this.getTotals();

            for(int band = 0; band < this.numBands; ++band) {
                int var10000 = this.numBins[band];
                long[] counts = this.getBins(band);
                long totalCount = this.totals[band];
                int numBinWidths = 0;
                long count = counts[0];

                for(int idx = 0; (double)count / (double)totalCount < p; count += counts[idx]) {
                    ++numBinWidths;
                    ++idx;
                }

                thresholds[band] = this.getLowValue(band) + (double)numBinWidths * this.binWidth[band];
            }

            return thresholds;
        } else {
            throw new IllegalArgumentException("JaiI18N.getString of " + ("Histogram9"));
        }
    }

    public double[] getModeThreshold(double power) {
        double[] thresholds = new double[this.numBands];
        this.getTotals();

        for(int band = 0; band < this.numBands; ++band) {
            int nBins = this.numBins[band];
            long[] counts = this.getBins(band);
            int mode1 = 0;
            long mode1Count = counts[0];

            int mode2;
            for(mode2 = 1; mode2 < nBins; ++mode2) {
                if (counts[mode2] > mode1Count) {
                    mode1 = mode2;
                    mode1Count = counts[mode2];
                }
            }

            mode2 = -1;
            double mode2count = 0.0D;

            for(int b = 0; b < nBins; ++b) {
                double d = (double)counts[b] * Math.pow((double)Math.abs(b - mode1), power);
                if (d > mode2count) {
                    mode2 = b;
                    mode2count = d;
                }
            }

            long minCount = counts[mode1];

            for(int b = mode1 + 1; b <= mode2; ++b) {
                if (counts[b] < minCount) {
                    minCount = counts[b];
                }
            }

            thresholds[band] = (double)((int)((double)(mode1 + mode2) / 2.0D + 0.5D));
        }

        return thresholds;
    }

    public double[] getIterativeThreshold() {
        double[] thresholds = new double[this.numBands];
        this.getTotals();

        for(int band = 0; band < this.numBands; ++band) {
            int nBins = this.numBins[band];
            long[] counts = this.getBins(band);
            double bw = this.binWidth[band];
            double threshold = 0.5D * (this.getLowValue(band) + this.getHighValue(band));
            double mid1 = 0.5D * (this.getLowValue(band) + threshold);
            double mid2 = 0.5D * (threshold + this.getHighValue(band));
            if (this.totals[band] == 0) {
                thresholds[band] = threshold;
            } else {
                int countDown = 1000;

                do {
                    thresholds[band] = threshold;
                    double total = (double)this.totals[band];
                    double level = this.getLowValue(band);
                    double mean1 = 0.0D;
                    double mean2 = 0.0D;
                    int count1 = 0;

                    for(int b = 0; b < nBins; ++b) {
                        if (level <= threshold) {
                            long c = counts[b];
                            mean1 += (double)c * level;
                            count1 += c;
                        } else {
                            mean2 += (double)counts[b] * level;
                        }

                        level += bw;
                    }

                    if (count1 != 0) {
                        mean1 /= (double)count1;
                    } else {
                        mean1 = mid1;
                    }

                    if (total != (double)count1) {
                        mean2 /= total - (double)count1;
                    } else {
                        mean2 = mid2;
                    }

                    threshold = 0.5D * (mean1 + mean2);
                    if (Math.abs(threshold - thresholds[band]) <= 1.0E-6D) {
                        break;
                    }

                    --countDown;
                } while(countDown > 0);
            }
        }

        return thresholds;
    }

    public double[] getMaxVarianceThreshold() {
        double[] thresholds = new double[this.numBands];
        this.getTotals();
        this.getMean();
        double[] variance = this.getMoment(2, false, false);

        for(int band = 0; band < this.numBands; ++band) {
            int nBins = this.numBins[band];
            long[] counts = this.getBins(band);
            double total = (double)this.totals[band];
            double mBand = this.mean[band];
            double bw = this.binWidth[band];
            double prob0 = 0.0D;
            double mean0 = 0.0D;
            double lv = this.getLowValue(band);
            double level = lv;
            double maxRatio = -1.7976931348623157E308D;
            int maxIndex = 0;
            int runLength = 0;

            for(int t = 0; t < nBins; level += bw) {
                double p = (double)counts[t] / total;
                prob0 += p;
                if (prob0 != 0.0D) {
                    double m0 = (mean0 += p * level) / prob0;
                    double prob1 = 1.0D - prob0;
                    if (prob1 != 0.0D) {
                        double m1 = (mBand - mean0) / prob1;
                        double var0 = 0.0D;
                        double g = lv;

                        for(int b = 0; b <= t; g += bw) {
                            double del = g - m0;
                            var0 += del * del * (double)counts[b];
                            ++b;
                        }

                        var0 /= total;
                        double var1 = 0.0D;

                        for(int b = t + 1; b < nBins; g += bw) {
                            double del = g - m1;
                            var1 += del * del * (double)counts[b];
                            ++b;
                        }

                        var1 /= total;
                        if (var0 == 0.0D && var1 == 0.0D && m1 != 0.0D) {
                            maxIndex = (int)(((m0 + m1) / 2.0D - this.getLowValue(band)) / bw + 0.5D);
                            runLength = 0;
                            break;
                        }

                        if (var0 / prob0 >= 0.5D && var1 / prob1 >= 0.5D) {
                            double mdel = m0 - m1;
                            double ratio = prob0 * prob1 * mdel * mdel / (var0 + var1);
                            if (ratio > maxRatio) {
                                maxRatio = ratio;
                                maxIndex = t;
                                runLength = 0;
                            } else if (ratio == maxRatio) {
                                ++runLength;
                            }
                        }
                    }
                }

                ++t;
            }

            thresholds[band] = this.getLowValue(band) + ((double)maxIndex + (double)runLength / 2.0D + 0.5D) * bw;
        }

        return thresholds;
    }

    public double[] getMaxEntropyThreshold() {
        double[] thresholds = new double[this.numBands];
        this.getTotals();
        double[] entropy = this.getEntropy();
        double log2 = Math.log(2.0D);

        for(int band = 0; band < this.numBands; ++band) {
            int nBins = this.numBins[band];
            long[] counts = this.getBins(band);
            double total = (double)this.totals[band];
            double H = entropy[band];
            double P1 = 0.0D;
            double H1 = 0.0D;
            double maxCriterion = -1.7976931348623157E308D;
            int maxIndex = 0;
            int runLength = 0;

            for(int t = 0; t < nBins; ++t) {
                double p = (double)counts[t] / total;
                if (p != 0.0D) {
                    P1 += p;
                    H1 -= p * Math.log(p) / log2;
                    double max1 = 0.0D;

                    for(int b = 0; b <= t; ++b) {
                        if ((double)counts[b] > max1) {
                            max1 = (double)counts[b];
                        }
                    }

                    if (max1 != 0.0D) {
                        double max2 = 0.0D;

                        for(int b = t + 1; b < nBins; ++b) {
                            if ((double)counts[b] > max2) {
                                max2 = (double)counts[b];
                            }
                        }

                        if (max2 != 0.0D) {
                            double ratio = H1 / H;
                            double criterion = ratio * Math.log(P1) / Math.log(max1 / total) + (1.0D - ratio) * Math.log(1.0D - P1) / Math.log(max2 / total);
                            if (criterion > maxCriterion) {
                                maxCriterion = criterion;
                                maxIndex = t;
                                runLength = 0;
                            } else if (criterion == maxCriterion) {
                                ++runLength;
                            }
                        }
                    }
                }
            }

            thresholds[band] = this.getLowValue(band) + ((double)maxIndex + (double)runLength / 2.0D + 0.5D) * this.binWidth[band];
        }

        return thresholds;
    }

    public double[] getMinErrorThreshold() {
        double[] thresholds = new double[this.numBands];
        this.getTotals();
        this.getMean();

        for(int band = 0; band < this.numBands; ++band) {
            int nBins = this.numBins[band];
            long[] counts = this.getBins(band);
            double total = (double)this.totals[band];
            double lv = this.getLowValue(band);
            double bw = this.binWidth[band];
            int total1 = 0;
            long total2 = this.totals[band];
            double sum1 = 0.0D;
            double sum2 = this.mean[band] * total;
            double level = lv;
            double minCriterion = 1.7976931348623157E308D;
            int minIndex = 0;
            int runLength = 0;
            double J0 = 1.7976931348623157E308D;
            double J1 = 1.7976931348623157E308D;
            double J2 = 1.7976931348623157E308D;
            int Jcount = 0;

            for(int t = 0; t < nBins; level += bw) {
                long c = counts[t];
                total1 += c;
                total2 -= c;
                double incr = level * (double)c;
                sum1 += incr;
                sum2 -= incr;
                if (total1 != 0 && sum1 != 0.0D) {
                    if (total2 == 0 || sum2 == 0.0D) {
                        break;
                    }

                    double m1 = sum1 / (double)total1;
                    double m2 = sum2 / (double)total2;
                    double s1 = 0.0D;
                    double g = lv;

                    for(int b = 0; b <= t; g += bw) {
                        double v = g - m1;
                        s1 += (double)counts[b] * v * v;
                        ++b;
                    }

                    s1 /= (double)total1;
                    if (s1 >= 0.5D) {
                        double s2 = 0.0D;

                        for(int b = t + 1; b < nBins; g += bw) {
                            double v = g - m2;
                            s2 += (double)counts[b] * v * v;
                            ++b;
                        }

                        s2 /= (double)total2;
                        if (s2 >= 0.5D) {
                            double P1 = (double)total1 / total;
                            double P2 = (double)total2 / total;
                            double J = 1.0D + P1 * Math.log(s1) + P2 * Math.log(s2) - 2.0D * (P1 * Math.log(P1) + P2 * Math.log(P2));
                            ++Jcount;
                            J0 = J1;
                            J1 = J2;
                            J2 = J;
                            if (Jcount >= 3 && J1 <= J0 && J1 <= J) {
                                if (J1 < minCriterion) {
                                    minCriterion = J1;
                                    minIndex = t - 1;
                                    runLength = 0;
                                } else if (J1 == minCriterion) {
                                    ++runLength;
                                }
                            }
                        }
                    }
                }

                ++t;
            }

            thresholds[band] = minIndex == 0 ? this.mean[band] : this.getLowValue(band) + ((double)minIndex + (double)runLength / 2.0D + 0.5D) * bw;
        }

        return thresholds;
    }

    public double[] getMinFuzzinessThreshold() {
        double[] thresholds = new double[this.numBands];
        this.getTotals();
        this.getMean();

        for(int band = 0; band < this.numBands; ++band) {
            int nBins = this.numBins[band];
            long[] counts = this.getBins(band);
            double total = (double)this.totals[band];
            double bw = this.binWidth[band];
            long total1 = 0;
            long total2 = this.totals[band];
            double sum1 = 0.0D;
            double sum2 = this.mean[band] * total;
            double lv = this.getLowValue(band);
            double level = lv;
            double C = this.getHighValue(band) - lv;
            double minCriterion = 1.7976931348623157E308D;
            int minIndex = 0;
            int runLength = 0;

            for(int t = 0; t < nBins; level += bw) {
                long c = counts[t];
                total1 += c;
                total2 -= c;
                double incr = level * (double)c;
                sum1 += incr;
                sum2 -= incr;
                if (total1 != 0 && total2 != 0) {
                    double m1 = sum1 / (double)total1;
                    double m2 = sum2 / (double)total2;
                    double g = lv;
                    double E = 0.0D;

                    for(int b = 0; b < nBins; g += bw) {
                        double u = b <= t ? 1.0D / (1.0D + Math.abs(g - m1) / C) : 1.0D / (1.0D + Math.abs(g - m2) / C);
                        double v = 1.0D - u;
                        E += (-u * Math.log(u) - v * Math.log(v)) * ((double)counts[b] / total);
                        ++b;
                    }

                    if (E < minCriterion) {
                        minCriterion = E;
                        minIndex = t;
                        runLength = 0;
                    } else if (E == minCriterion) {
                        ++runLength;
                    }
                }

                ++t;
            }

            thresholds[band] = lv + ((double)minIndex + (double)runLength / 2.0D + 0.5D) * bw;
        }

        return thresholds;
    }
}
