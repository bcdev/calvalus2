package com.bc.calvalus.processing.fire.format.grid.meris;

import com.bc.calvalus.processing.fire.format.grid.AbstractGridReducer;
import org.apache.hadoop.mapreduce.Reducer;
import ucar.ma2.Array;
import ucar.ma2.DataType;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.NetcdfFileWriter;
import ucar.nc2.Variable;

import java.io.IOException;

public class MerisGridReducer extends AbstractGridReducer {

    private static final int MERIS_CHUNK_SIZE = 40;
    private MerisNcFileFactory merisNcFileFactory;

    public MerisGridReducer() {
        this.merisNcFileFactory = new MerisNcFileFactory();
    }

    @Override
    protected void setup(Reducer.Context context) throws IOException, InterruptedException {
        super.setup(context);

//        String year = context.getConfiguration().get("calvalus.year");
//        String month = context.getConfiguration().get("calvalus.month");
//        String basePath = "hdfs://calvalus/calvalus/projects/fire/aux/psd-grid-for-oaf/";
//        String firstHalfFilename = year + month + "07-ESACCI-L4_FIRE-BA-MERIS-fv04.1.nc";
//        String secondHalfFilename = year + month + "22-ESACCI-L4_FIRE-BA-MERIS-fv04.1.nc";
//        Path firstHalfGridPath = new Path(basePath, firstHalfFilename);
//        Path secondHalfGridPath = new Path(basePath, secondHalfFilename);
//        File firstHalfGridFile = CalvalusProductIO.copyFileToLocal(firstHalfGridPath, context.getConfiguration());
//        File secondHalfGridFile = CalvalusProductIO.copyFileToLocal(secondHalfGridPath, context.getConfiguration());
//
//        Product firstHalfGridProduct = ProductIO.readProduct(firstHalfGridFile);
//        Product secondHalfGridProduct = ProductIO.readProduct(secondHalfGridFile);
//
//        float[] buffer = new float[MERIS_CHUNK_SIZE * MERIS_CHUNK_SIZE];
//
//        try {
//            for (int y = 0; y < 18; y++) {
//                for (int x = 0; x < 36; x++) {
//                    firstHalfGridProduct.getBand("observed_area_fraction").readPixels(x * MERIS_CHUNK_SIZE, y * MERIS_CHUNK_SIZE, MERIS_CHUNK_SIZE, MERIS_CHUNK_SIZE, buffer);
//                    writeFloatChunk(x * MERIS_CHUNK_SIZE, y * MERIS_CHUNK_SIZE, ncFile, "observed_area_fraction", buffer);
//                    secondHalfGridProduct.getBand("observed_area_fraction").readPixels(x * MERIS_CHUNK_SIZE, y * MERIS_CHUNK_SIZE, MERIS_CHUNK_SIZE, MERIS_CHUNK_SIZE, buffer);
//                    writeFloatChunk(x * MERIS_CHUNK_SIZE, y * MERIS_CHUNK_SIZE, ncSecond, "observed_area_fraction", buffer);
//                }
//            }
//        } catch (InvalidRangeException e) {
//            throw new IOException(e);
//        }
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

    }

    @Override
    protected String getFilename(String year, String month, String version) {
        return String.format("%s%s%s-ESACCI-L4_FIRE-BA-MERIS-f%s.nc", year, month, "01", version);
    }

    @Override
    protected NetcdfFileWriter createNcFile(String filename, String version, String timeCoverageStart, String timeCoverageEnd, int numberOfDays) throws IOException {
        return merisNcFileFactory.createNcFile(filename, version, timeCoverageStart, timeCoverageEnd, numberOfDays, 18);
    }

    @Override
    protected int getTargetWidth() {
        return MERIS_CHUNK_SIZE;
    }

    @Override
    protected int getTargetHeight() {
        return getTargetWidth();
    }
}
