package com.bc.calvalus.processing.fire;

import com.bc.ceres.core.ProgressMonitor;
import org.esa.snap.core.dataio.ProductWriter;
import org.esa.snap.core.dataio.ProductWriterPlugIn;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.datamodel.ProductNode;

import java.io.IOException;

public class FrpL3ProductWriter implements ProductWriter {

    private final ProductWriterPlugIn plugin;

    public FrpL3ProductWriter(ProductWriterPlugIn writerPlugIn) {
        this.plugin = writerPlugIn;
    }

    @Override
    public ProductWriterPlugIn getWriterPlugIn() {
        return plugin;
    }

    @Override
    public Object getOutput() {
        return null;
    }

    @Override
    public void writeProductNodes(Product product, Object output) throws IOException {

    }

    @Override
    public void writeBandRasterData(Band sourceBand, int sourceOffsetX, int sourceOffsetY, int sourceWidth, int sourceHeight, ProductData sourceBuffer, ProgressMonitor pm) throws IOException {

    }

    @Override
    public void flush() throws IOException {

    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public boolean shouldWrite(ProductNode node) {
        return false;
    }

    @Override
    public boolean isIncrementalMode() {
        return false;
    }

    @Override
    public void setIncrementalMode(boolean enabled) {

    }

    @Override
    public void deleteOutput() throws IOException {

    }

    @Override
    public void removeBand(Band band) {

    }

    @Override
    public void setFormatName(String formatName) {

    }
}
