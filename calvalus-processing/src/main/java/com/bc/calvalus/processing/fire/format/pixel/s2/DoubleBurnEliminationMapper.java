package com.bc.calvalus.processing.fire.format.pixel.s2;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.beam.CalvalusProductIO;
import com.bc.calvalus.processing.hadoop.ProductSplit;
import com.bc.ceres.core.ProgressMonitor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.dataio.ProductWriter;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.dataop.barithm.BandArithmetic;
import org.esa.snap.core.gpf.GPF;
import org.esa.snap.core.image.VirtualBandOpImage;
import org.esa.snap.core.jexp.ParseException;
import org.esa.snap.core.jexp.Term;
import org.esa.snap.core.util.ProductUtils;
import org.esa.snap.dataio.bigtiff.BigGeoTiffProductWriterPlugIn;

import java.awt.Dimension;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

public class DoubleBurnEliminationMapper extends Mapper {

    private static final Logger LOG = CalvalusLogger.getLogger();
    static final int TILE_SIZE = 256;

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis();
        System.getProperties().put("snap.dataio.bigtiff.compression.type", "LZW");
        System.getProperties().put("snap.dataio.bigtiff.tiling.width", "" + TILE_SIZE);
        System.getProperties().put("snap.dataio.bigtiff.tiling.height", "" + TILE_SIZE);
        System.getProperties().put("snap.dataio.bigtiff.force.bigtiff", "true");

        ProductSplit inputSplit = (ProductSplit) context.getInputSplit();

        Path inputJdLocation = inputSplit.getPath();
        Set<String> prevJdPaths = getPrevJdPaths(inputJdLocation);
        Path inputLcLocation = new Path(inputJdLocation.toString().replace("JD", "LC"));

        LOG.info("Fixing files '" + inputJdLocation + "' and '" + inputLcLocation + "'");

        File localJdFile = CalvalusProductIO.copyFileToLocal(inputJdLocation, configuration);
        File localLcFile = CalvalusProductIO.copyFileToLocal(inputLcLocation, configuration);

        Product inputJd = ProductIO.readProduct(localJdFile);
        Product inputLc = ProductIO.readProduct(localLcFile);

        inputJd.setPreferredTileSize(TILE_SIZE, TILE_SIZE);
        inputLc.setPreferredTileSize(TILE_SIZE, TILE_SIZE);

        Product newJd = new Product(inputJd.getName(), inputJd.getProductType(), inputJd.getSceneRasterWidth(), inputJd.getSceneRasterHeight());
        Product newLc = new Product(inputLc.getName(), inputLc.getProductType(), inputLc.getSceneRasterWidth(), inputLc.getSceneRasterHeight());

        ProductUtils.copyGeoCoding(inputJd, newJd);
        ProductUtils.copyGeoCoding(inputLc, newLc);

        inputJd.setRefNo(1);
        inputLc.setRefNo(2);

        List<Product> prevJDs = getPrevJdProducts(configuration, prevJdPaths);

        if (prevJDs.isEmpty()) {
            LOG.info("No previous files found.");
            return;
        }

        Product[] namespaceProducts = getNamespaceProducts(inputJd, inputLc, prevJDs);
        LOG.info("Found previous files: " + Arrays.toString(prevJDs.toArray(new Product[0])));

        Term jdTerm;
        try {
            StringBuilder expressionBuilder = new StringBuilder("if $1.JD > 0 and (");
            for (int i = 0; i < prevJDs.size(); i++) {
                expressionBuilder.append("$").append(i + 3).append(".JD > 0");
                if (i < prevJDs.size() - 1) {
                    expressionBuilder.append(" or ");
                }
            }
            expressionBuilder.append(") then 0 else $1.JD");
            LOG.info(expressionBuilder.toString());
            jdTerm = BandArithmetic.parseExpression(expressionBuilder.toString(), namespaceProducts, 0);
        } catch (ParseException e) {
            throw new IOException(e);
        }
        VirtualBandOpImage.Builder jdBuilder = VirtualBandOpImage.builder(jdTerm);
        jdBuilder.sourceSize(new Dimension(inputJd.getSceneRasterWidth(), inputJd.getSceneRasterHeight()));
        jdBuilder.tileSize(new Dimension(256, 256));
        Band newJdBand = newJd.addBand("JD", inputJd.getBand("JD").getDataType());
        newJdBand.setSourceImage(jdBuilder.create());

        Term lcTerm;
        try {
            StringBuilder expressionBuilder = new StringBuilder("if $1.JD > 0 and (");
            for (int i = 0; i < prevJDs.size(); i++) {
                expressionBuilder.append("$").append(i + 3).append(".JD > 0");
                if (i < prevJDs.size() - 1) {
                    expressionBuilder.append(" or ");
                }
            }
            expressionBuilder.append(") then 0 else $2.LC");
            lcTerm = BandArithmetic.parseExpression(expressionBuilder.toString(), namespaceProducts, 0);
        } catch (ParseException e) {
            throw new IOException(e);
        }
        VirtualBandOpImage.Builder lcBuilder = VirtualBandOpImage.builder(lcTerm);
        lcBuilder.sourceSize(new Dimension(inputLc.getSceneRasterWidth(), inputLc.getSceneRasterHeight()));
        lcBuilder.tileSize(new Dimension(256, 256));
        Band newLcBand = newLc.addBand("LC", inputLc.getBand("LC").getDataType());
        newLcBand.setSourceImage(lcBuilder.create());

        newJd.setPreferredTileSize(TILE_SIZE, TILE_SIZE);
        newLc.setPreferredTileSize(TILE_SIZE, TILE_SIZE);

        FileSystem fileSystem = FileSystem.get(configuration);

        CalvalusLogger.getLogger().info("Writing new JD...");
        final ProductWriter jdGeotiffWriter = ProductIO.getProductWriter(BigGeoTiffProductWriterPlugIn.FORMAT_NAME);
        jdGeotiffWriter.writeProductNodes(newJd, localJdFile.getName() + ".tif");
        jdGeotiffWriter.writeBandRasterData(newJd.getBandAt(0), 0, 0, 0, 0, null, ProgressMonitor.NULL);
        CalvalusLogger.getLogger().info(String.format("...done. Copying final product to %s...", inputJdLocation.getParent().toString()));

        fileSystem.delete(inputJdLocation, false);
        FileUtil.copy(new File(localJdFile.getName() + ".tif"), fileSystem, inputJdLocation, false, configuration);

        CalvalusLogger.getLogger().info("...done.");

        CalvalusLogger.getLogger().info("Writing new LC...");
        final ProductWriter lcGeotiffWriter = ProductIO.getProductWriter(BigGeoTiffProductWriterPlugIn.FORMAT_NAME);
        lcGeotiffWriter.writeProductNodes(newLc, localLcFile.getName() + ".tif");
        lcGeotiffWriter.writeBandRasterData(newLc.getBandAt(0), 0, 0, 0, 0, null, ProgressMonitor.NULL);
        CalvalusLogger.getLogger().info(String.format("...done. Copying final product to %s...", inputLcLocation.getParent().toString()));

        fileSystem.delete(inputLcLocation, false);

        FileUtil.copy(new File(localLcFile.getName() + ".tif"), fileSystem, inputLcLocation, false, configuration);
    }

    protected static List<Product> getPrevJdProducts(Configuration configuration, Set<String> prevJdPaths) throws IOException {
        List<Product> prevJDs = new ArrayList<>();
        int index = 0;
        for (String prevJdPath : prevJdPaths) {
            LOG.info(prevJdPath);
            Path prevJdLocation = new Path(prevJdPath);
            if (!FileSystem.get(configuration).exists(prevJdLocation)) {
                continue;
            }
            File prevJdFile = CalvalusProductIO.copyFileToLocal(prevJdLocation, configuration);
            Product prevJD = ProductIO.readProduct(prevJdFile);
            prevJD.setPreferredTileSize(TILE_SIZE, TILE_SIZE);
            prevJD.setRefNo(index + 3);
            prevJDs.add(prevJD);
            index++;
        }
        return prevJDs;
    }

    private static Product[] getNamespaceProducts(Product inputJd, Product inputLc, List<Product> prevJDs) {
        Product[] namespaceProducts = new Product[prevJDs.size() + 2];
        namespaceProducts[0] = inputJd;
        namespaceProducts[1] = inputLc;
        System.arraycopy(prevJDs.toArray(new Product[0]), 0, namespaceProducts, 2, prevJDs.size());
        return namespaceProducts;
    }

    private static Set<String> getPrevJdPaths(Path inputJdLocation) {
        String month = inputJdLocation.getName().substring(4, 6);
        Set<String> previousJdPaths = new HashSet<>();
        for (int i = 1; i <= 4; i++) {
            int prevMonth = Integer.parseInt(month) - i;
            String prevMonthString = prevMonth < 10 ? "0" + prevMonth : "" + prevMonth;
            previousJdPaths.add(inputJdLocation.toString()
                    .replace("2016" + month, "2016" + prevMonthString)
                    .replace("2016-" + month + "-Fire", "2016-" + prevMonthString + "-Fire"));
        }
        for (int i = 1; i <= 4; i++) {
            int prevMonth = Integer.parseInt(month) - i;
            String prevMonthString = prevMonth < 10 ? "0" + prevMonth : "" + prevMonth;
            previousJdPaths.add(inputJdLocation.toString()
                    .replace("2016" + month, "2016" + prevMonthString)
                    .replace("2016-" + month + "-Fire", "2016-" + prevMonthString + "-Fire")
                    .replace("-Formatting", "-Formatting_format"));
        }
        for (int i = 1; i <= 4; i++) {
            int prevMonth = Integer.parseInt(month) - i;
            String prevMonthString = prevMonth < 10 ? "0" + prevMonth : "" + prevMonth;
            previousJdPaths.add(inputJdLocation.toString()
                    .replace("2016" + month, "2016" + prevMonthString)
                    .replace("2016-" + month + "-Fire", "2016-" + prevMonthString + "-Fire")
                    .replace("-Formatting_format", "-Formatting"));
        }

        return previousJdPaths;
    }
}
