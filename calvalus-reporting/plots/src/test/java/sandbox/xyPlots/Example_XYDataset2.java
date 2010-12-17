package sandbox.xyPlots;

import org.jfree.data.DomainInfo;
import org.jfree.data.Range;
import org.jfree.data.RangeInfo;
import org.jfree.data.xy.AbstractXYDataset;
import org.jfree.data.xy.XYDataset;

/**
 * Random data for a scatter plot demo.
 * <p/>
 * Note that the aim of this class is to create a self-contained data source
 * for demo purposes - it is NOT intended to show how you should go about
 * writing your own datasets.
 */
public class Example_XYDataset2 extends AbstractXYDataset implements XYDataset, DomainInfo, RangeInfo {

    private static final int DEFAULT_SERIES_COUNT = 2;
    private static final int DEFAULT_ITEM_COUNT = 10;
    private static final double DEFAULT_RANGE = 200;
    private Double[][] xValues;
    private Double[][] yValues;
    private int seriesCount;
    private int itemCount;
    private Number domainMin;
    private Number domainMax;
    private Number rangeMin;
    private Number rangeMax;

    public Range getDomainRange() {
        return domainRange;
    }

    public Range getRange() {
        return range;
    }

    public Number getDomainMin() {
        return domainMin;
    }

    public Number getDomainMax() {
        return domainMax;
    }

    public Number getRangeMin() {
        return rangeMin;
    }

    public Number getRangeMax() {
        return rangeMax;
    }

    private Range domainRange;
    private Range range;

    public Example_XYDataset2() {
        this(DEFAULT_SERIES_COUNT, DEFAULT_ITEM_COUNT);
    }

    public Example_XYDataset2(int seriesCount, int itemCount) {

        this.xValues = new Double[seriesCount][itemCount];
        this.yValues = new Double[seriesCount][itemCount];
        this.seriesCount = seriesCount;
        this.itemCount = itemCount;

        double minX = Double.POSITIVE_INFINITY;
        double maxX = Double.NEGATIVE_INFINITY;
        double minY = Double.POSITIVE_INFINITY;
        double maxY = Double.NEGATIVE_INFINITY;

        for (int series = 0; series < seriesCount; series++) {
            for (int item = 0; item < itemCount; item++) {

                double x = (Math.random() - 0.5) * DEFAULT_RANGE;
                this.xValues[series][item] = new Double(x);
                if (x < minX) {
                    minX = x;
                }
                if (x > maxX) {
                    maxX = x;
                }

                double y = (Math.random() + 0.5) * 6 * x + x;
                this.yValues[series][item] = new Double(y);
                if (y < minY) {
                    minY = y;
                }
                if (y > maxY) {
                    maxY = y;
                }

            }
        }

        this.domainMin = new Double(minX);
        this.domainMax = new Double(maxX);
        this.domainRange = new Range(minX, maxX);

        this.rangeMin = new Double(minY);
        this.rangeMax = new Double(maxY);
        this.range = new Range(minY, maxY);

    }

    /**
     * Returns the x-value for the specified series and item.  Series are numbered 0, 1, ...
     *
     * @param series the index (zero-based) of the series.
     * @param item   the index (zero-based) of the required item.
     * @return the x-value for the specified series and item.
     */
    public Number getX(int series, int item) {
        return this.xValues[series][item];
    }

    /**
     * Returns the y-value for the specified series and item.  Series are numbered 0, 1, ...
     *
     * @param series the index (zero-based) of the series.
     * @param item   the index (zero-based) of the required item.
     * @return the y-value for the specified series and item.
     */
    public Number getY(int series, int item) {
        return this.yValues[series][item];
    }

    public int getSeriesCount() {
        return this.seriesCount;
    }

    /**
     * Returns the key for the series.
     *
     * @param series the index (zero-based) of the series.
     * @return The key for the series.
     */
    public Comparable getSeriesKey(int series) {
        return "Sample " + series;
    }

    /**
     * Returns the number of items in the specified series.
     *
     * @param series the index (zero-based) of the series.
     * @return the number of items in the specified series.
     */
    public int getItemCount(int series) {
        return this.itemCount;
    }


    public double getDomainLowerBound(boolean includeInterval) {
        return this.domainMin.doubleValue();
    }

    /**
     * Returns the upper bound for the domain.
     *
     * @param includeInterval include the x-interval?
     * @return The upper bound.
     */
    public double getDomainUpperBound(boolean includeInterval) {
        return this.domainMax.doubleValue();
    }

    /**
     * Returns the bounds for the domain.
     *
     * @param includeInterval include the x-interval?
     * @return The bounds.
     */
    public Range getDomainBounds(boolean includeInterval) {
        return this.domainRange;
    }

    /**
     * Returns the lower bound for the range.
     *
     * @param includeInterval include the y-interval?
     * @return The lower bound.
     */
    public double getRangeLowerBound(boolean includeInterval) {
        return this.rangeMin.doubleValue();
    }

    public double getRangeUpperBound() {
        return this.rangeMax.doubleValue();
    }

    /**
     * Returns the upper bound for the range.
     *
     * @param includeInterval include the y-interval?
     * @return The upper bound.
     */
    public double getRangeUpperBound(boolean includeInterval) {
        return this.rangeMax.doubleValue();
    }

    /**
     * Returns the range of values in the range (y-values).
     *
     * @param includeInterval include the y-interval?
     * @return The range.
     */
    public Range getRangeBounds(boolean includeInterval) {
        return this.range;
    }
}
