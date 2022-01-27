package com.bc.calvalus.processing.fire;

import com.bc.ceres.core.ProgressMonitor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Reducer;
import org.esa.snap.core.dataio.ProductWriter;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.CrsGeoCoding;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.dataio.bigtiff.BigGeoTiffProductWriterPlugIn;
import org.esa.snap.runtime.Config;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.NetcdfFileWriter;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

public class FireSynPixelReducer extends FirePixelReducer {

    private ProductWriter jdWriter;
    private Band jdBand;
    private ProductWriter clWriter;
    private Band clBand;
    private ProductWriter lcWriter;
    private Band lcBand;
    private String jdOutputFilename;
    private String clOutputFilename;
    private String lcOutputFilename;
    private Configuration configuration;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Config.instance().preferences().putBoolean("snap.dataio.bigtiff.support.pushprocessing", true);
        super.setup(context);
        configuration = context.getConfiguration();
    }

    @Override
    protected void prepareTargetProduct(String regionName, String timeCoverageStart, String timeCoverageEnd, String year, String month, String version) throws IOException {
        String actualYear = configuration.get("calvalus.year");
        String actualMonth = configuration.get("calvalus.month");

        jdOutputFilename = String.format(filenamePattern, actualYear, Integer.parseInt(actualMonth), regionName, version) + "-JD.tif";
        clOutputFilename = String.format(filenamePattern, actualYear, Integer.parseInt(actualMonth), regionName, version) + "-CL.tif";
        lcOutputFilename = String.format(filenamePattern, actualYear, Integer.parseInt(actualMonth), regionName, version) + "-LC.tif";

        Product jdProduct = new Product("jd", "fire-cci-jd", continentalRectangle.width, continentalRectangle.height);
        Product clProduct = new Product("cl", "fire-cci-cl", continentalRectangle.width, continentalRectangle.height);
        Product lcProduct = new Product("lc", "fire-cci-lc", continentalRectangle.width, continentalRectangle.height);

        try {
            jdProduct.setSceneGeoCoding(new CrsGeoCoding(DefaultGeographicCRS.WGS84, numRowsGlobal * 2, numRowsGlobal, getEasting(regionName), getNorthing(regionName), 360.0 / (numRowsGlobal * 2), 180.0 / numRowsGlobal, 0.0, 0.0));
            clProduct.setSceneGeoCoding(new CrsGeoCoding(DefaultGeographicCRS.WGS84, numRowsGlobal * 2, numRowsGlobal, getEasting(regionName), getNorthing(regionName), 360.0 / (numRowsGlobal * 2), 180.0 / numRowsGlobal, 0.0, 0.0));
            lcProduct.setSceneGeoCoding(new CrsGeoCoding(DefaultGeographicCRS.WGS84, numRowsGlobal * 2, numRowsGlobal, getEasting(regionName), getNorthing(regionName), 360.0 / (numRowsGlobal * 2), 180.0 / numRowsGlobal, 0.0, 0.0));
        } catch (FactoryException | TransformException e) {
            throw new IllegalStateException(e);
        }

        jdWriter = new BigGeoTiffProductWriterPlugIn().createWriterInstance();
        jdBand = jdProduct.addBand("JD", ProductData.TYPE_INT16);
        jdBand.setNoDataValue(-2);
        jdBand.setNoDataValueUsed(true);
        jdWriter.writeProductNodes(jdProduct, jdOutputFilename);

        clWriter = new BigGeoTiffProductWriterPlugIn().createWriterInstance();
        clBand = clProduct.addBand("CL", ProductData.TYPE_INT8);
        clWriter.writeProductNodes(clProduct, clOutputFilename);

        lcWriter = new BigGeoTiffProductWriterPlugIn().createWriterInstance();
        lcBand = lcProduct.addBand("LC", ProductData.TYPE_UINT8);
        lcWriter.writeProductNodes(lcProduct, lcOutputFilename);

        LOG.info("Product headers written");

        if (regionName.equals("AREA_1") || regionName.equals("AREA_3") || regionName.equals("AREA_4")) {
            final int[] pixels = new int[jdBand.getRasterWidth() * 1024];
            Arrays.fill(pixels, -2);
            jdBand.writePixels(0, 0, jdBand.getRasterWidth(), 1024, pixels);
        }
        if (regionName.equals("AREA_6")) {
            final int[] pixels = new int[jdBand.getRasterWidth() * 1031];
            Arrays.fill(pixels, -2);
            jdBand.writePixels(0, 18048, jdBand.getRasterWidth(), 1031, pixels);
        }

    }

    private double getEasting(String regionName) {
        switch (regionName) {
            case "AREA_1": return ContinentalArea.northamerica.left - 180.0;
            case "AREA_2": return ContinentalArea.southamerica.left - 180.0;
            case "AREA_3": return ContinentalArea.europe.left - 180.0;
            case "AREA_4": return ContinentalArea.asia.left - 180.0;
            case "AREA_5": return ContinentalArea.africa.left - 180.0;
            case "AREA_6": return ContinentalArea.australia.left - 180.0;
            case "AREA_7": return ContinentalArea.greenland.left - 180.0;
        }
        throw new IllegalArgumentException("Invalid area: " + regionName);
    }

    private double getNorthing(String regionName) {
        switch (regionName) {
            case "AREA_1": return 90 - ContinentalArea.northamerica.top;
            case "AREA_2": return 90 - ContinentalArea.southamerica.top;
            case "AREA_3": return 90 - ContinentalArea.europe.top;
            case "AREA_4": return 90 - ContinentalArea.asia.top;
            case "AREA_5": return 90 - ContinentalArea.africa.top;
            case "AREA_6": return 90 - ContinentalArea.australia.top;
            case "AREA_7": return 90 - ContinentalArea.greenland.top;
        }
        throw new IllegalArgumentException("Invalid area: " + regionName);
    }

    protected void writeShortChunk(int x, int y, NetcdfFileWriter ncFile, String varName, short[] data, int width, int height) throws IOException, InvalidRangeException {
        LOG.info(String.format("Writing data: x=%d, y=%d, %d*%d into variable %s", x, y, width, height, varName));
        jdWriter.writeBandRasterData(jdBand, x, y, width, height, new ProductData.Short(data), ProgressMonitor.NULL);
    }

    protected void writeByteChunk(int x, int y, NetcdfFileWriter ncFile, String varName, byte[] data, int width, int height) throws IOException, InvalidRangeException {
        LOG.info(String.format("Writing data: x=%d, y=%d, %d*%d into variable %s", x, y, width, height, varName));
        clWriter.writeBandRasterData(clBand, x, y, width, height, new ProductData.Byte(data), ProgressMonitor.NULL);
    }

    protected void writeUByteChunk(int x, int y, NetcdfFileWriter ncFile, String varName, byte[] data, int width, int height) throws IOException, InvalidRangeException {
        LOG.info(String.format("Writing data: x=%d, y=%d, %d*%d into variable %s", x, y, width, height, varName));
        lcWriter.writeBandRasterData(lcBand, x, y, width, height, new ProductData.UByte(data), ProgressMonitor.NULL);

    }

    @Override
    protected void cleanup(Reducer.Context context) throws IOException {
        String outputDir = context.getConfiguration().get("calvalus.output.dir");
        jdWriter.close();
        clWriter.close();
        lcWriter.close();

        LOG.info("writing finished");

        for (String filename : new String[]{jdOutputFilename, clOutputFilename, lcOutputFilename}) {
            File fileLocation = new File(filename);
            Path path = new Path(outputDir + "/" + filename);
            LOG.info(outputDir + "/" + filename);
            FileSystem fs = path.getFileSystem(context.getConfiguration());
            if (!fs.exists(path)) {
                FileUtil.copy(fileLocation, fs, path, false, context.getConfiguration());
                LOG.info(String.format("output file %s archived in %s", filename, outputDir));
            } else {
                LOG.warning(String.format("output file %s not archived in %s, file exists", filename, outputDir));
            }
        }
    }
}
