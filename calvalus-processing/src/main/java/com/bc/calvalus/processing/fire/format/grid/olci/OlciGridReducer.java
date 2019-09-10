package com.bc.calvalus.processing.fire.format.grid.olci;

import com.bc.calvalus.processing.fire.format.grid.AbstractGridReducer;
import com.bc.calvalus.processing.fire.format.grid.GridCells;
import org.apache.hadoop.io.Text;
import ucar.ma2.Array;
import ucar.ma2.DataType;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.NetcdfFileWriter;
import ucar.nc2.Variable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class OlciGridReducer extends AbstractGridReducer {

    private static final int CHUNK_SIZE = 40;
    private OlciNcFileFactory fileFactory;

    public OlciGridReducer() {
        this.fileFactory = new OlciNcFileFactory();
    }

    @Override
    protected void writeVegetationClasses(NetcdfFileWriter ncFile) throws IOException, InvalidRangeException {
        Variable vegetationClass = ncFile.findVariable("vegetation_class");
        int[] array = new int[]{
                10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130,
                140, 150, 160, 170, 180
        };
        Array values = Array.factory(DataType.INT, new int[]{18}, array);
        ncFile.write(vegetationClass, values);
    }

    @Override
    protected void writeVegetationClassNames(NetcdfFileWriter ncFile) throws IOException, InvalidRangeException {
        Variable vegetationClass = ncFile.findVariable("vegetation_class_name");
        List<String> names = new ArrayList<>();
        names.add("Cropland, rainfed");
        names.add("Cropland, irrigated or post-flooding");
        names.add("Mosaic cropland (>50%) / natural vegetation (tree, shrub, herbaceous cover) (<50%)");
        names.add("Mosaic natural vegetation (tree, shrub, herbaceous cover) (>50%) / cropland (<50%)");
        names.add("Tree cover, broadleaved, evergreen, closed to open (>15%)");
        names.add("Tree cover, broadleaved, deciduous, closed to open (>15%)");
        names.add("Tree cover, needleleaved, evergreen, closed to open (>15%)");
        names.add("Tree cover, needleleaved, deciduous, closed to open (>15%)");
        names.add("Tree cover, mixed leaf type (broadleaved and needleleaved)");
        names.add("Mosaic tree and shrub (>50%) / herbaceous cover (<50%)");
        names.add("Mosaic herbaceous cover (>50%) / tree and shrub (<50%)");
        names.add("Shrubland");
        names.add("Grassland");
        names.add("Lichens and mosses");
        names.add("Sparse vegetation (tree, shrub, herbaceous cover) (<15%)");
        names.add("Tree cover, flooded, fresh or brakish water");
        names.add("Tree cover, flooded, saline water");
        names.add("Shrub or herbaceous cover, flooded, fresh/saline/brakish water");
        for (int i = 0; i < names.size(); i++) {
            String name = names.get(i);
            char[] array = name.toCharArray();
            Array values = Array.factory(DataType.CHAR, new int[]{1, name.length()}, array);
            ncFile.write(vegetationClass, new int[]{i, 0}, values);
        }
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
        return fileFactory.createNcFile(filename, "v" + version, timeCoverageStart, timeCoverageEnd, numberOfDays, 18);
    }

    @Override
    protected int getTargetWidth() {
        return CHUNK_SIZE;
    }

    @Override
    protected int getTargetHeight() {
        return CHUNK_SIZE;
    }

    @Override
    protected int getX(String key) {
        // key == "2018-01-h10v08"
        int h = new Integer(key.substring("2018-01-h".length(), "2018-01-h".length() + 2));
        return h * 40;
    }

    @Override
    protected int getY(String key) {
        // key == "2018-01-h10v08"
        int v = new Integer(key.substring("2018-01-hxxv".length(), "2018-01-hxxv".length() + 2));
        return v * 40;
    }

}
