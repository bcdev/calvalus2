package sandbox.xyPlots;

import org.jfree.data.xy.AbstractXYDataset;

/**
 * Random data for a scatter plot demo.
 * <p/>
 * Note that the aim of this class is to create a self-contained data source
 * for demo purposes - it is NOT intended to show how you should go about
 * writing your own datasets.
 */
public class Example_XYDatasetSimple extends AbstractXYDataset  {

    static final int DEFAULT_SERIES_COUNT = 6;
    static final int DEFAULT_ITEM_COUNT = 20;
    private static final double DEFAULT_RANGE = 200;
    private Double[][] xValues;
    private Double[][] yValues;
    private int seriesCount;
    private int itemCount;

    public Example_XYDatasetSimple() {
        this(DEFAULT_SERIES_COUNT, DEFAULT_ITEM_COUNT);
    }

    public Example_XYDatasetSimple(int seriesCount, int itemCount) {

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
        return "Bla " + series;
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



 
}
