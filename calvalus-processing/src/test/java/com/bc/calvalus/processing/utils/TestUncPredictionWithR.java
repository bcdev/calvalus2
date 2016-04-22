package com.bc.calvalus.processing.utils;

import org.junit.BeforeClass;
import org.junit.Test;
import org.renjin.sexp.DoubleVector;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.io.File;
import java.net.URISyntaxException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


/**
 * @author Marco Peters
 */
public class TestUncPredictionWithR {

    private static ScriptEngine engine;

    @BeforeClass
    public static void setUp() throws Exception {
        ScriptEngineManager manager = new ScriptEngineManager();
        // create a Renjin engine:
        engine = manager.getEngineByName("Renjin");
        if (engine == null) {
            fail("Could not create R Scripting Engine");
        }
    }

    @Test
    public void testPrediction() throws URISyntaxException, ScriptException {
        File resFile = new File(getClass().getResource("codiR_cecr_GridProd.RData").toURI());
        String absolutePath = resFile.getAbsolutePath().replace("\\", "\\\\");
        engine.eval("load(file = \"" + absolutePath + "\")");
        engine.put("bap", new double[]{1, 2, 3, 4, 5, 1, 2, 3, 4, 5});
        engine.eval("nwd = data.frame(gpg=bap)");
        DoubleVector pred = (DoubleVector) engine.eval("predict(ug, nwd)");
        double[] predResult = pred.toDoubleArray();
        for (int i = 0; i < predResult.length / 2; i++) {
            double v0 = predResult[i];
            double v1 = predResult[i + 5];
            assertEquals(v0, v1, 1.0e-6);
        }
    }
}