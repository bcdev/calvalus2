package com.bc.calvalus.processing.fire.format.grid.s2;

import com.bc.calvalus.processing.fire.format.grid.AbstractGridReducer;
import com.bc.calvalus.processing.fire.format.grid.GridCells;
import com.bc.calvalus.processing.fire.format.grid.NcFileFactory;
import org.apache.hadoop.io.Text;
import ucar.ma2.Array;
import ucar.ma2.DataType;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.NetcdfFileWriter;
import ucar.nc2.Variable;

import java.io.IOException;

public class S2GridReducer extends AbstractGridReducer {

    private static final int S2_CHUNK_SIZE = 4;
    private final NcFileFactory s2NcFileFactory;

    public S2GridReducer() {
        this.s2NcFileFactory = new S2NcFileFactory();
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
        return String.format("%s%s%s-ESACCI-L4_FIRE-BA-MSI-f%s.nc", year, month, "01", version);
    }

    @Override
    protected NetcdfFileWriter createNcFile(String filename, String version, String timeCoverageStart, String timeCoverageEnd, int numberOfDays) throws IOException {
        return s2NcFileFactory.createNcFile(filename, version, timeCoverageStart, timeCoverageEnd, numberOfDays, 6);
    }

    @Override
    protected int getTargetWidth() {
        return S2_CHUNK_SIZE;
    }

    @Override
    protected int getTargetHeight() {
        return getTargetWidth();
    }

    @Override
    protected int getX(String key) {
        key = key.split("-")[2]; // x210y40
        String xPart = key.split("y")[0].substring(1); // 210
        return Integer.parseInt(xPart) * 4;
    }

    @Override
    protected int getY(String key) {
        key = key.split("-")[2];
        int y = Integer.parseInt(key.split("y")[1]);
        return (180 - y - 1) * 4;
    }

    @Override
    protected void writeVegetationClasses(NetcdfFileWriter ncFile) throws IOException, InvalidRangeException {
        Variable vegetationClass = ncFile.findVariable("vegetation_class");
        int[] array = new int[]{
                1, 2, 3, 4, 5, 6
        };
        Array values = Array.factory(DataType.INT, new int[]{6}, array);
        ncFile.write(vegetationClass, values);
    }

    @Override
    protected void writeVegetationClassNames(NetcdfFileWriter ncFile) throws IOException, InvalidRangeException {

        return;

        /*
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
        */
    }

}
