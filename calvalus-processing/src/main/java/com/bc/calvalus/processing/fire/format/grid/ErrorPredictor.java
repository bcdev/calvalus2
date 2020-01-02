package com.bc.calvalus.processing.fire.format.grid;

import org.renjin.sexp.DoubleVector;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;

/**
 * @author thomas
 * @author marcop
 */
public class ErrorPredictor {

    private ScriptEngine engine;
    private final File resFile;

    public ErrorPredictor() throws IOException {
        ScriptEngineManager manager = new ScriptEngineManager();
        engine = manager.getEngineByName("Renjin");
        if (engine == null) {
            throw new IllegalStateException("Could not create script engine \"Renjin\"");
        }
        resFile = new File("./codiR_cecr_GridProd_onlyMERIS.RData");
        try (InputStream resourceAsStream = getClass().getResourceAsStream("meris/codiR_cecr_GridProd_onlyMERIS.RData")) {
            if (!resFile.exists()) {
                Files.copy(resourceAsStream, resFile.getAbsoluteFile().toPath());
            }
        }
    }

    public float[] predictError(double[] burnedAreaInSquareMeters, double[] cellSizeInSquareMeters) throws ScriptException {
        if (burnedAreaInSquareMeters.length != cellSizeInSquareMeters.length) {
            throw new IllegalArgumentException("For each burned area pixel there must be exactly one cell size value.");
        }

        String absolutePath = resFile.getAbsolutePath().replace("\\", "\\\\");
        engine.eval("load(file = \"" + absolutePath + "\")");
        double[] burnedAreaPerSquareMeter = computeBAPerSquareMeter(burnedAreaInSquareMeters, cellSizeInSquareMeters);
        engine.put("bap", burnedAreaPerSquareMeter);
        engine.eval("nwd = data.frame(gpg=bap)");
        DoubleVector pred = (DoubleVector) engine.eval("predict(ugwi, nwd)");
        double[] predictionResult = pred.toDoubleArray();
        for (int i = 0; i < predictionResult.length; i++) {
            predictionResult[i] = cellSizeInSquareMeters[i] * predictionResult[i];
        }
        float[] result = new float[predictionResult.length];
        for (int i = 0; i < predictionResult.length; i++) {
            result[i] = (float) predictionResult[i];

        }
        return result;
    }

    public void dispose() {
        resFile.delete();
    }

    private double[] computeBAPerSquareMeter(double[] burnedAreaInSquareMeters, double[] cellSizeInSquareMeters) {
        double[] result = new double[burnedAreaInSquareMeters.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = burnedAreaInSquareMeters[i] / cellSizeInSquareMeters[i];
        }
        return result;
    }
}
