package com.bc.calvalus.processing.utils;

import org.junit.BeforeClass;
import org.junit.Test;
import org.renjin.sexp.DoubleArrayVector;
import org.renjin.sexp.SEXP;
import org.renjin.sexp.Vector;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


/**
 * @author Marco Peters
 */
public class RScriptExecutionTest {

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
    public void testMethod() throws ScriptException {

        engine.eval("df <- data.frame(x=1:10, y=(1:10)+rnorm(n=10))");
        engine.eval("print(df)");
        engine.eval("print(lm(y ~ x, df))");


        engine.eval("rVector=c(1,2,3,4,5)");
        engine.eval("meanVal=mean(rVector)");
        SEXP meanVal = (SEXP) engine.eval("meanVal");
        double mean = meanVal.asReal();
        assertEquals(3.0, mean, 1.0e-6);

        Vector x = (Vector) engine.eval("x <- c(6, 7, 8, 9)");
        assertEquals(4, x.length());
        for (int i = 0; i < x.length(); i++) {
            assertEquals(i + 6, x.getElementAsDouble(i), 1.0e-6);
        }

        engine.put("y", new double[]{1d, 2d, 3d, 4d});
        engine.put("z", new DoubleArrayVector(1, 2, 3, 4, 5));
        assertEquals(10, ((SEXP) engine.eval("out <- sum(y)")).asReal(), 1.0e-8);
        assertEquals(15, ((SEXP) engine.eval("out <- sum(z)")).asReal(), 1.0e-8);
    }
}