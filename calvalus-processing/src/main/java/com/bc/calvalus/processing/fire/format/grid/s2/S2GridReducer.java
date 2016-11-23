package com.bc.calvalus.processing.fire.format.grid.s2;

import com.bc.calvalus.processing.fire.format.grid.AbstractGridReducer;
import com.bc.calvalus.processing.fire.format.grid.GridCell;
import com.bc.calvalus.processing.fire.format.grid.GridFormatUtils;
import org.apache.hadoop.io.Text;
import ucar.ma2.InvalidRangeException;

import java.io.IOException;
import java.util.Arrays;

public class S2GridReducer extends AbstractGridReducer {

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        prepareTargetProducts(context);

        float[] buffer = new float[40 * 40];
        Arrays.fill(buffer, -2);

        try {
            for (int y = 0; y < 18; y++) {
                for (int x = 0; x < 36; x++) {
                    writeFloatChunk(x * 40, y * 40, ncFirst, "observed_area_fraction", buffer);
                    writeFloatChunk(x * 40, y * 40, ncSecond, "observed_area_fraction", buffer);
                }
            }
        } catch (InvalidRangeException e) {
            throw new IOException(e);
        }
    }

    @Override
    protected void reduce(Text key, Iterable<GridCell> values, Context context) throws IOException, InterruptedException {
        super.reduce(key, values, context);
        GridCell currentGridCell = getCurrentGridCell();
        try {
            writeFloatChunk(getX(key.toString()), getY(key.toString()), ncFirst, "burnable_area_fraction", currentGridCell.burnableFraction);
            writeFloatChunk(getX(key.toString()), getY(key.toString()), ncSecond, "burnable_area_fraction", currentGridCell.burnableFraction);
        } catch (InvalidRangeException e) {
            throw new IOException(e);
        }

    }

    @Override
    protected String getFilename(String year, String month, String version, boolean firstHalf) {
        return GridFormatUtils.createS2Filename(year, month, version, firstHalf);
    }
}
