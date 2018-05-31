package com.bc.calvalus.processing.fire.format.grid.avhrr;

import com.bc.calvalus.processing.fire.format.grid.AbstractGridReducer;
import com.bc.calvalus.processing.fire.format.grid.GridCells;
import com.bc.calvalus.processing.fire.format.grid.NcFileFactory;
import org.apache.hadoop.io.Text;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.NetcdfFileWriter;

import java.io.IOException;

public class AvhrrGridReducer extends AbstractGridReducer {

    private final NcFileFactory avhrrNcFileFactory;

    public AvhrrGridReducer() {
        this.avhrrNcFileFactory = new AvhrrNcFileFactory();
    }

    @Override
    protected void reduce(Text key, Iterable<GridCells> values, Context context) throws IOException, InterruptedException {
        super.reduce(key, values, context);
        GridCells currentGridCells = getCurrentGridCells();
        try {
            int x = getX(key.toString());
            int y = getY(key.toString());
            writeFloatChunk(x, y, ncFile, "fraction_of_burnable_area", currentGridCells.burnableFraction);
            writeFloatChunk(x, y, ncFile, "fraction_of_observed_area", currentGridCells.coverage);
        } catch (InvalidRangeException e) {
            throw new IOException(e);
        }
    }

    @Override
    protected String getFilename(String year, String month, String version) {
        return String.format("%s%s%s-ESACCI-L4_FIRE-BA-LTDR-f%s.nc", year, month, "01", version);
    }

    @Override
    protected NetcdfFileWriter createNcFile(String filename, String version, String timeCoverageStart, String timeCoverageEnd, int numberOfDays) throws IOException {
        return avhrrNcFileFactory.createNcFile(filename, version, timeCoverageStart, timeCoverageEnd, numberOfDays);
    }

    @Override
    protected int getTargetWidth() {
        return 80;
    }

    @Override
    protected int getTargetHeight() {
        return 80;
    }

    @Override
    protected int getX(String key) {
        int tileIndex = Integer.parseInt(key.split("-")[2]);
        return (tileIndex % 18) * 80;
    }

    @Override
    protected int getY(String key) {
        int tileIndex = Integer.parseInt(key.split("-")[2]);
        return (tileIndex / 18) * 80;
    }

}
