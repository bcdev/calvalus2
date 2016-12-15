package com.bc.calvalus.processing.fire.format.grid.s2;

import com.bc.calvalus.processing.fire.format.grid.AbstractGridReducer;
import com.bc.calvalus.processing.fire.format.grid.GridCell;
import com.bc.calvalus.processing.fire.format.grid.NcFileFactory;
import org.apache.hadoop.io.Text;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.NetcdfFileWriter;

import java.io.IOException;

public class S2GridReducer extends AbstractGridReducer {

    private static final int S2_CHUNK_SIZE = 8;
    private final NcFileFactory s2NcFileFactory;

    public S2GridReducer() {
        this.s2NcFileFactory = new S2NcFileFactory();
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
        return String.format("%s%s%s-ESACCI-L4_FIRE-BA-MSI-f%s.nc", year, month, firstHalf ? "07" : "22", version);
    }

    @Override
    protected NetcdfFileWriter createNcFile(String filename, String version, String timeCoverageStart, String timeCoverageEnd, int numberOfDays) throws IOException {
        return s2NcFileFactory.createNcFile(filename, version, timeCoverageStart, timeCoverageEnd, numberOfDays);
    }

    @Override
    protected int getTargetSize() {
        return S2_CHUNK_SIZE;
    }
}
