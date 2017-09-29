package com.bc.calvalus.processing.fire.format.grid.modis;

import com.bc.calvalus.processing.analysis.QuicklookGenerator;
import com.bc.calvalus.processing.analysis.Quicklooks;
import com.bc.calvalus.processing.beam.CalvalusProductIO;
import com.bc.calvalus.processing.fire.format.grid.AbstractGridReducer;
import com.bc.calvalus.processing.fire.format.grid.GridCell;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Product;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.NetcdfFileWriter;

import javax.imageio.ImageIO;
import java.awt.image.RenderedImage;
import java.io.File;
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
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);

        createQuicklook(context, new File("./" + firstHalfFile));
        createQuicklook(context, new File("./" + secondHalfFile));
    }

    private static void createQuicklook(Context context, File fileLocation) throws IOException {
        Quicklooks.QLConfig qlConfig = new Quicklooks.QLConfig();
        qlConfig.setImageType("png");
        qlConfig.setBandName("burned_area");
        File localCpd = fetchAuxFile(context, "fire-modis-grid.cpd");
        qlConfig.setCpdURL(localCpd.toURI().toURL().toExternalForm());
        File localOverlay = fetchAuxFile(context, "world.png");
        qlConfig.setOverlayURL(localOverlay.toURI().toURL().toExternalForm());
        Product product = ProductIO.readProduct(fileLocation);
        RenderedImage image = QuicklookGenerator.createImage(context, product, qlConfig);
        String outputDir = context.getConfiguration().get("calvalus.output.dir");
        FileSystem fileSystem = FileSystem.get(context.getConfiguration());
        if (image != null) {
            File output = new File(product.getName() + ".png");
            ImageIO.write(image, "png", output);
            Path pngPath = new Path(outputDir + "/" + product.getName() + ".png");
            FileUtil.copy(output, fileSystem, pngPath, false, context.getConfiguration());
        }
    }

    private static File fetchAuxFile(Context context, String filename) throws IOException {
        File localFile = new File(filename);
        if (!localFile.exists()) {
            CalvalusProductIO.copyFileToLocal(new Path("/calvalus/projects/fire/aux/" + filename), localFile, context.getConfiguration());
        }
        return localFile;
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
