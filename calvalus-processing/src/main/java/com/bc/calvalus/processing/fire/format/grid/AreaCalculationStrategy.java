package com.bc.calvalus.processing.fire.format.grid;

/**
 * Compute the area of pixels in a grid defined by a {@link org.esa.snap.core.datamodel.GeoCoding}.
 * The implementing class is responsible for managing the definition of the grid.
 *
 * @author Hannes Neuschmidt
 */
public interface AreaCalculationStrategy {
    /**
     * Compute the area of a single pixel at coordinates {@code x} and {@code y} in the grid.
     *
     * @param x row index into the grid
     * @param y column index into the grid
     * @return the pixel area in square metres
     */
    public double calculatePixelSize(int x, int y);

    /**
     * Compute the area of a single pixel at coordinates {@code x} and {@code y} in the grid.
     * The {@code maxX} and {@code maxY} parameters allow special handling of pixel coordinates
     * at the border of the grid. See {@link AreaCalculator} for an example.
     *
     * @param x row index into the grid
     * @param y column index into the grid
     * @param maxX the max x value of the scene
     * @param maxY the max y value of the scene
     * @return the pixel area in square metres
     */
    public double calculatePixelSize(int x, int y, int maxX, int maxY);
    // public double calculateRowPixelSizes(int y);
    //public double calculateColumnPixelSizes(int y);
}