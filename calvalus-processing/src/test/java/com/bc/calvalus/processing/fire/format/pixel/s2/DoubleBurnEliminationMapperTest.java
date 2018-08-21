package com.bc.calvalus.processing.fire.format.pixel.s2;

import com.bc.ceres.core.ProgressMonitor;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.dataio.ProductWriter;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.dataop.barithm.BandArithmetic;
import org.esa.snap.core.image.VirtualBandOpImage;
import org.esa.snap.core.jexp.ParseException;
import org.esa.snap.core.jexp.Term;
import org.esa.snap.core.util.ProductUtils;
import org.esa.snap.dataio.bigtiff.BigGeoTiffProductWriterPlugIn;
import org.junit.Ignore;
import org.junit.Test;

import java.awt.*;
import java.io.IOException;

import static com.bc.calvalus.processing.fire.format.pixel.s2.DoubleBurnEliminationMapper.TILE_SIZE;

public class DoubleBurnEliminationMapperTest {

    @Test
    @Ignore
    public void writeTestFile() throws Exception {
        System.getProperties().put("snap.dataio.bigtiff.compression.type", "LZW");
        System.getProperties().put("snap.dataio.bigtiff.tiling.width", "" + TILE_SIZE);
        System.getProperties().put("snap.dataio.bigtiff.tiling.height", "" + TILE_SIZE);
        System.getProperties().put("snap.dataio.bigtiff.force.bigtiff", "true");

        Product orig = ProductIO.readProduct("C:\\ssd\\20160701-ESACCI-L3S_FIRE-BA-MSI-AREA_h37v16-fv1.1-JD.tif");
        Product prev = ProductIO.readProduct("C:\\ssd\\20160601-ESACCI-L3S_FIRE-BA-MSI-AREA_h37v16-fv1.1-JD.tif");

        orig.setRefNo(1);
        prev.setRefNo(2);

        Product newJd = new Product(orig.getName(), orig.getProductType(), orig.getSceneRasterWidth(), orig.getSceneRasterHeight());
        ProductUtils.copyGeoCoding(orig, newJd);

        Term jdTerm;
        try {
            jdTerm = BandArithmetic.parseExpression("if $1.JD > 0 and $2.JD > 0 then 0 else $1.JD", new Product[]{orig, prev}, 0);
        } catch (ParseException e) {
            throw new IOException(e);
        }
        VirtualBandOpImage.Builder jdBuilder = VirtualBandOpImage.builder(jdTerm);
        jdBuilder.sourceSize(new Dimension(orig.getSceneRasterWidth(), orig.getSceneRasterHeight()));
        jdBuilder.tileSize(new Dimension(256, 256));
        Band newJdBand = newJd.addBand("JD", orig.getBand("JD").getDataType());
        newJdBand.setSourceImage(jdBuilder.create());

        newJd.setPreferredTileSize(TILE_SIZE, TILE_SIZE);
        orig.setPreferredTileSize(TILE_SIZE, TILE_SIZE);
        prev.setPreferredTileSize(TILE_SIZE, TILE_SIZE);

        final ProductWriter jdGeotiffWriter = ProductIO.getProductWriter(BigGeoTiffProductWriterPlugIn.FORMAT_NAME);
        jdGeotiffWriter.writeProductNodes(newJd, "C:\\ssd\\" + newJd.getName() + "-new.tif");
        jdGeotiffWriter.writeBandRasterData(newJd.getBandAt(0), 0, 0, 0, 0, null, ProgressMonitor.NULL);
    }
}