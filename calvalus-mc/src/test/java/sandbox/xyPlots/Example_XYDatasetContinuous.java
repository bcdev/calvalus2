package sandbox.xyPlots;

import org.jfree.data.Range;
import org.jfree.data.xy.AbstractXYDataset;
import org.jfree.data.xy.XYDataset;

import java.util.Arrays;

public class Example_XYDatasetContinuous extends AbstractXYDataset implements XYDataset {
    private int LENGTH_DATA = 20;
    private double[] xValues; //domain
    private double[] yValues; //range

    public Example_XYDatasetContinuous(int lengthData) {
        this.LENGTH_DATA = lengthData;
        xValues = new double[LENGTH_DATA];
        yValues = new double[LENGTH_DATA];
        fillDataset();
    }

    private void fillDataset() {
        for (int i = 0; i < LENGTH_DATA; i++) {
            xValues[i] = i + Math.random();
            yValues[i] = i + Math.random();
        }
    }

    public Range getDomainRange() { //xRange
        Arrays.sort(xValues);
        return new Range(xValues[0], xValues[xValues.length - 1]);
    }

    public Range getRangeRange() { //yRange
        Arrays.sort(yValues);
        return new Range(yValues[0], yValues[yValues.length - 1]);
    }

    @Override
    public int indexOf(Comparable seriesKey) {
        return 1;
    }

    @Override
    public Comparable getSeriesKey(int series) {
        return "my measurement";
    }

    @Override
    public int getSeriesCount() {
        return 1;
    }

    @Override
    public Number getY(int series, int item) {
        return yValues[item];
    }

    @Override
    public Number getX(int series, int item) {
        return xValues[item];
    }

    @Override
    public int getItemCount(int series) {
        return xValues.length;
    }

}
