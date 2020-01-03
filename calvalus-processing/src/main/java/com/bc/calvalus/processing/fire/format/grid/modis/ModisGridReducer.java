package com.bc.calvalus.processing.fire.format.grid.modis;

import com.bc.calvalus.processing.analysis.QuicklookGenerator;
import com.bc.calvalus.processing.analysis.Quicklooks;
import com.bc.calvalus.processing.beam.CalvalusProductIO;
import com.bc.calvalus.processing.fire.format.grid.AbstractGridReducer;
import com.bc.calvalus.processing.fire.format.grid.GridCells;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Product;
import ucar.ma2.Array;
import ucar.ma2.DataType;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.NetcdfFileWriter;
import ucar.nc2.Variable;

import javax.imageio.ImageIO;
import java.awt.image.RenderedImage;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ModisGridReducer extends AbstractGridReducer {

    private ModisNcFileFactory modisNcFileFactory;

    public ModisGridReducer() {
        this.modisNcFileFactory = new ModisNcFileFactory();
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
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);

        createQuicklook(context, new File("./" + outputFilename));
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
        RenderedImage image = new QuicklookGenerator(context, product, qlConfig).createImage();
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
    protected String getFilename(String year, String month, String version) {
        return String.format("%s%s%s-ESACCI-L4_FIRE-BA-MODIS-f%s.nc", year, month, "01", version);
    }

    @Override
    protected NetcdfFileWriter createNcFile(String filename, String version, String timeCoverageStart, String timeCoverageEnd, int numberOfDays) throws IOException {
        return modisNcFileFactory.createNcFile(filename, version, timeCoverageStart, timeCoverageEnd, numberOfDays, 18);
    }

    @Override
    protected int getTargetWidth() {
        return ModisGridMapper.WINDOW_SIZE;
    }

    @Override
    protected int getTargetHeight() {
        return getTargetWidth();
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
