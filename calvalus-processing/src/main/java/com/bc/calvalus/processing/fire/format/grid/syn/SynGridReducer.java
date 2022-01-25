package com.bc.calvalus.processing.fire.format.grid.syn;

import com.bc.calvalus.processing.fire.format.grid.GridCells;
import com.bc.calvalus.processing.fire.format.grid.olci.OlciGridReducer;
import org.apache.hadoop.io.Text;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.NetcdfFileWriter;

import java.io.IOException;

public class SynGridReducer extends OlciGridReducer {

    private SynNcFileFactory fileFactory;

    public SynGridReducer() {
        this.fileFactory = new SynNcFileFactory();
    }

    @Override
    protected void reduce(Text key, Iterable<GridCells> values, Context context) throws IOException, InterruptedException {
        super.reduce(key, values, context);
        GridCells currentGridCells = getCurrentGridCells();
        try {
            int x = getX(key.toString());
            int y = getY(key.toString());
            writeFloatChunk(x, y, ncFile, "fraction_of_burnable_area", currentGridCells.burnableFraction);
        } catch (InvalidRangeException e) {
            throw new IOException(e);
        }
    }
    @Override
    protected String getFilename(String year, String month, String version) {
        String paddedMonth = String.format("%02d", Integer.parseInt(month));
        return String.format("%s%s01-C3S-L4_FIRE-BA-OLCI-fv%s.nc", year, paddedMonth, version);
    }

    @Override
    protected NetcdfFileWriter createNcFile(String filename, String version, String timeCoverageStart, String timeCoverageEnd, int numberOfDays) throws IOException {
        return fileFactory.createNcFile(filename, "v" + version, timeCoverageStart, timeCoverageEnd, numberOfDays, 18, numRowsGlobal);
    }


}
