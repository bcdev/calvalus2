package org.esa.beam.binning;

import com.bc.calvalus.binning.BinRasterizer;
import com.bc.calvalus.binning.BinnerContext;
import com.bc.calvalus.binning.TemporalBin;
import com.bc.calvalus.binning.WritableVector;
import com.bc.ceres.core.ProgressMonitor;
import org.esa.beam.framework.dataio.ProductWriter;
import org.esa.beam.framework.datamodel.Band;
import org.esa.beam.framework.datamodel.ProductData;

import java.io.IOException;

/**
* @author Norman Fomferra
*/
public final class ProductDataWriter extends BinRasterizer {
    private final int width;
    private final ProductData numObsLine;
    private final ProductData numPassesLine;
    private final Band[] outputBands;
    private final ProductData[] outputLines;
    private final ProductWriter productWriter;
    private final Band numObsBand;
    private final Band numPassesBand;
    private final float[] fillValues;
    private int yLast;

    public ProductDataWriter(ProductWriter productWriter,
                             Band numObsBand,
                             ProductData numObsLine,
                             Band numPassesBand,
                             ProductData numPassesLine,
                             Band[] outputBands,
                             ProductData[] outputLines) {
        this.numObsLine = numObsLine;
        this.numPassesLine = numPassesLine;
        this.outputBands = outputBands;
        this.outputLines = outputLines;
        this.productWriter = productWriter;
        this.numObsBand = numObsBand;
        this.numPassesBand = numPassesBand;
        this.width = numObsBand.getSceneRasterWidth();
        this.yLast = 0;
        this.fillValues = new float[outputBands.length];
        for (int i = 0; i < outputBands.length; i++) {
            fillValues[i] = (float) outputBands[i].getNoDataValue();
        }
        initLine();
    }

    @Override
    public void processBin(int x, int y, TemporalBin temporalBin, WritableVector outputVector) throws Exception {
        setData(x, temporalBin, outputVector);
        if (y != yLast) {
            completeLine();
            yLast = y;
        }
    }

    @Override
    public void processMissingBin(int x, int y) throws Exception {
        setNoData(x);
        if (y != yLast) {
            completeLine();
            yLast = y;
        }
    }

    @Override
    public void end(BinnerContext ctx) throws Exception {
        completeLine();
        productWriter.close();
    }

    private void completeLine() throws IOException {
        writeLine(yLast);
        initLine();
    }

    private void writeLine(int y) throws IOException {
        productWriter.writeBandRasterData(numObsBand, 0, y, width, 1, numObsLine, ProgressMonitor.NULL);
        productWriter.writeBandRasterData(numPassesBand, 0, y, width, 1, numPassesLine, ProgressMonitor.NULL);
        for (int i = 0; i < outputBands.length; i++) {
            productWriter.writeBandRasterData(outputBands[i], 0, y, width, 1, outputLines[i], ProgressMonitor.NULL);
        }
    }

    private void initLine() {
        for (int x = 0; x < width; x++) {
            setNoData(x);
        }
    }

    private void setData(int x, TemporalBin temporalBin, WritableVector outputVector) {
        numObsLine.setElemIntAt(x, temporalBin.getNumObs());
        numPassesLine.setElemIntAt(x, temporalBin.getNumPasses());
        for (int i = 0; i < outputBands.length; i++) {
            outputLines[i].setElemFloatAt(x, outputVector.get(i));
        }
    }

    private void setNoData(int x) {
        numObsLine.setElemIntAt(x, -1);
        numPassesLine.setElemIntAt(x, -1);
        for (int i = 0; i < outputBands.length; i++) {
            outputLines[i].setElemFloatAt(x, fillValues[i]);
        }
    }
}
