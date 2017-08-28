package com.bc.calvalus.processing.fire.format.grid.modis;

import com.bc.calvalus.processing.fire.format.grid.AbstractGridReducer;
import com.bc.calvalus.processing.fire.format.grid.GridCell;
import org.apache.hadoop.io.Text;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.NetcdfFileWriter;

import java.io.IOException;

public class ModisGridReducer extends AbstractGridReducer {

    private ModisNcFileFactory modisNcFileFactory;

    public ModisGridReducer() {
        this.modisNcFileFactory = new ModisNcFileFactory();
    }

    @Override
    protected void reduce(Text key, Iterable<GridCell> values, Context context) throws IOException, InterruptedException {
        super.reduce(key, values, context);
        GridCell currentGridCell = getCurrentGridCell();
        try {
            int x = getX(key.toString());
            int y = getY(key.toString());
            writeFloatChunk(x, y, ncFirst, "burnable_area_fraction", currentGridCell.burnableFraction);
            writeFloatChunk(x, y, ncSecond, "burnable_area_fraction", currentGridCell.burnableFraction);

            writeFloatChunk(x, y, ncFirst, "observed_area_fraction", currentGridCell.coverageFirstHalf);
            writeFloatChunk(x, y, ncSecond, "observed_area_fraction", currentGridCell.coverageSecondHalf);
        } catch (InvalidRangeException e) {
            throw new IOException(e);
        }
    }

    @Override
    protected String getFilename(String year, String month, String version, boolean firstHalf) {
        return String.format("%s%s%s-ESACCI-L4_FIRE-BA-MODIS-f%s.nc", year, month, firstHalf ? "07" : "22", version);
    }

    @Override
    protected NetcdfFileWriter createNcFile(String filename, String version, String timeCoverageStart, String timeCoverageEnd, int numberOfDays) throws IOException {
        return modisNcFileFactory.createNcFile(filename, version, timeCoverageStart, timeCoverageEnd, numberOfDays);
    }

    @Override
    protected int getTargetSize() {
        return ModisGridMapper.WINDOW_SIZE;
    }

    @Override
    protected int getX(String key) {
        // key == "2001-02-735,346"
        return Integer.parseInt(key.split("-")[2].split(",")[0]);
    }

    @Override
    protected int getY(String key) {
        // key == "2001-02-735,46"
        return Integer.parseInt(key.split("-")[2].split(",")[1]);
    }
}
