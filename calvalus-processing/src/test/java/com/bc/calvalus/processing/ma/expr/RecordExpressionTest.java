package com.bc.calvalus.processing.ma.expr;

import com.bc.calvalus.processing.ma.AggregatedNumber;
import com.bc.calvalus.processing.ma.Header;
import com.bc.calvalus.processing.ma.Record;
import com.bc.calvalus.processing.ma.RecordUtils;
import com.bc.calvalus.processing.ma.TestHeader;
import org.esa.snap.core.jexp.Symbol;
import org.esa.snap.core.jexp.Term;
import org.esa.snap.core.jexp.impl.ParserImpl;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author Norman Fomferra
 */
public class RecordExpressionTest {

    @Test
    public void testMedian() throws Exception {
        Header header = new TestHeader("*rho_1", "*rho_2", "*rho_3", "*rho_4", "*rho_5");
        HeaderNamespace namespace = new HeaderNamespace(header);
        RecordEvalEnv recordEvalEnv = new RecordEvalEnv(namespace);

        Record record = RecordUtils.create(new AggregatedNumber(24, 25, 16, 0.1, 4.0, 2.6, 0.1),
                                           new AggregatedNumber(24, 25, 16, 0.1, 4.0, 2.6, 0.5),
                                           new AggregatedNumber(24, 25, 16, 0.1, 4.0, 2.6, 0.4),
                                           new AggregatedNumber(24, 25, 16, 0.1, 4.0, 2.6, 0.2),
                                           new AggregatedNumber(24, 25, 16, 0.1, 4.0, 2.6, 0.3));

        ParserImpl parser = new ParserImpl(namespace);

        recordEvalEnv.setContext(record);

        // 0 arguments
        assertEquals(0.0, parser.parse("median()").evalD(recordEvalEnv), 1e-6);

        // 1 argument
        assertEquals(0.1 / 2.6, parser.parse("rho_1.cv").evalD(recordEvalEnv), 1e-6);
        assertEquals(0.1 / 2.6, parser.parse("median(rho_1.cv)").evalD(recordEvalEnv), 1e-6);

        // 4 (even) arguments
        assertEquals(0.5 * (0.2 / 2.6 + 0.3 / 2.6), parser.parse("0.5 * (rho_4.cv + rho_5.cv)").evalD(recordEvalEnv), 1e-6);
        assertEquals(0.5 * (0.2 / 2.6 + 0.3 / 2.6), parser.parse("median(rho_1.cv, rho_3.cv, rho_4.cv, rho_5.cv)").evalD(recordEvalEnv), 1e-6);

        // 5 (odd) arguments
        assertEquals(0.3 / 2.6, parser.parse("rho_5.cv").evalD(recordEvalEnv), 1e-6);
        assertEquals(0.3 / 2.6, parser.parse("median(rho_1.cv, rho_2.cv, rho_3.cv, rho_4.cv, rho_5.cv)").evalD(recordEvalEnv), 1e-6);
    }

    @Test
    public void testThatHeaderNamespaceIgnoresAggregationPrefix() throws Exception {
        Header header = new TestHeader("lat", "lon", "*conc_chl", "*kd_460", "*l1p_flags.CC_LAND");
        HeaderNamespace namespace = new HeaderNamespace(header);
        assertNotNull(namespace.resolveSymbol("lat"));
        assertNotNull(namespace.resolveSymbol("lon"));
        assertNull(namespace.resolveSymbol("*conc_chl"));
        assertNotNull(namespace.resolveSymbol("conc_chl"));
        assertNull(namespace.resolveSymbol("*kd_460"));
        assertNotNull(namespace.resolveSymbol("kd_460"));
        assertNull(namespace.resolveSymbol("*l1p_flags.CC_LAND"));
        assertNotNull(namespace.resolveSymbol("l1p_flags.CC_LAND"));
    }

    @Test
    public void testThatDotsCanBePartOfNameOfAggregatedVariables() throws Exception {
        Header header = new TestHeader("*l1p_flags.CC_LAND", "ref.CONC_CHL");
        HeaderNamespace namespace = new HeaderNamespace(header);

        Symbol sym1 = namespace.resolveSymbol("l1p_flags.CC_LAND");
        assertNotNull(sym1);
        assertEquals(RecordSymbol.class, sym1.getClass());
        RecordSymbol fieldSym1 = (RecordSymbol) sym1;
        assertEquals("l1p_flags.CC_LAND", fieldSym1.getName());
        assertEquals("l1p_flags.CC_LAND", fieldSym1.getVariableName());

        Symbol sym2 = namespace.resolveSymbol("l1p_flags.CC_LAND.mean");
        assertNotNull(sym2);
        assertEquals(RecordFieldSymbol.class, sym2.getClass());
        RecordFieldSymbol fieldSym2 = (RecordFieldSymbol) sym2;
        assertEquals("l1p_flags.CC_LAND.mean", fieldSym2.getName());
        assertEquals("l1p_flags.CC_LAND", fieldSym2.getVariableName());
        assertEquals("mean", fieldSym2.getFieldName());

        Symbol sym3 = namespace.resolveSymbol("ref.CONC_CHL");
        assertNotNull(sym3);
        assertEquals(RecordSymbol.class, sym3.getClass());
        RecordSymbol fieldSym3 = (RecordSymbol) sym3;
        assertEquals("ref.CONC_CHL", fieldSym3.getName());
        assertEquals("ref.CONC_CHL", fieldSym3.getVariableName());

        Symbol sym4 = namespace.resolveSymbol("ref.CONC_CHL.mean");
        assertNull(sym4);
    }

    @Test
    public void testRecordsWithAggregatedNumbers() throws Exception {
        Header header = new TestHeader("*conc_chl");
        HeaderNamespace namespace = new HeaderNamespace(header);
        RecordEvalEnv recordEvalEnv = new RecordEvalEnv(namespace);

        Record record1 = RecordUtils.create(new AggregatedNumber(24, 25, 4, 0.1, 4.0, 2.6, 0.4));
        Record record2 = RecordUtils.create(new AggregatedNumber(16, 25, 3, 0.2, 4.1, 3.7, 0.3));

        ParserImpl parser = new ParserImpl(namespace);

        Term t0 = parser.parse("conc_chl");
        Term t1 = parser.parse("conc_chl.n");
        Term t2 = parser.parse("conc_chl.nT");
        Term t3 = parser.parse("conc_chl.nF");
        Term t4 = parser.parse("conc_chl.min");
        Term t5 = parser.parse("conc_chl.max");
        Term t6 = parser.parse("conc_chl.mean");
        Term t7 = parser.parse("conc_chl.sigma");
        Term t8 = parser.parse("conc_chl.cv");
        Term t9 = parser.parse("conc_chl.cv < 0.15 && conc_chl.n > 5");

        recordEvalEnv.setContext(record1);
        assertEquals(2.6, t0.evalD(recordEvalEnv), 1e-10);
        assertEquals(24, t1.evalI(recordEvalEnv));
        assertEquals(25, t2.evalI(recordEvalEnv));
        assertEquals(4, t3.evalI(recordEvalEnv));
        assertEquals(0.1, t4.evalD(recordEvalEnv), 1e-10);
        assertEquals(4.0, t5.evalD(recordEvalEnv), 1e-10);
        assertEquals(2.6, t6.evalD(recordEvalEnv), 1e-10);
        assertEquals(0.4, t7.evalD(recordEvalEnv), 1e-10);
        assertEquals(0.4 / 2.6, t8.evalD(recordEvalEnv), 1e-10);
        assertEquals(false, t9.evalB(recordEvalEnv));

        recordEvalEnv.setContext(record2);
        assertEquals(16, t1.evalI(recordEvalEnv));
        assertEquals(25, t2.evalI(recordEvalEnv));
        assertEquals(3, t3.evalI(recordEvalEnv));
        assertEquals(0.2, t4.evalD(recordEvalEnv), 1e-10);
        assertEquals(4.1, t5.evalD(recordEvalEnv), 1e-10);
        assertEquals(3.7, t6.evalD(recordEvalEnv), 1e-10);
        assertEquals(0.3, t7.evalD(recordEvalEnv), 1e-10);
        assertEquals(0.3 / 3.7, t8.evalD(recordEvalEnv), 1e-10);
        assertEquals(true, t9.evalB(recordEvalEnv));
    }

    @Test
    public void testRecordsWithScalars() throws Exception {
        Header header = new TestHeader("b", "s", "i", "f");
        HeaderNamespace namespace = new HeaderNamespace(header);
        RecordEvalEnv recordEvalEnv = new RecordEvalEnv(namespace);

        Record record1 = RecordUtils.create(false, "x", 4, 0.6F);
        Record record2 = RecordUtils.create(true, "y", 3, 0.5F);

        ParserImpl parser = new ParserImpl(namespace);

        Term t1 = parser.parse("i + 1.5 * f");
        Term t2 = parser.parse("feq(f, 0.6)");
        Term t3 = parser.parse("b");
        Term t4 = parser.parse("i > 3");

        recordEvalEnv.setContext(record1);
        assertEquals(4 + 1.5 * 0.6, t1.evalD(recordEvalEnv), 1e-6);
        assertEquals(true, t2.evalB(recordEvalEnv));
        assertEquals(false, t3.evalB(recordEvalEnv));
        assertEquals(true, t4.evalB(recordEvalEnv));

        recordEvalEnv.setContext(record2);
        assertEquals(3 + 1.5 * 0.5, t1.evalD(recordEvalEnv), 1e-6);
        assertEquals(false, t2.evalB(recordEvalEnv));
        assertEquals(true, t3.evalB(recordEvalEnv));
        assertEquals(false, t4.evalB(recordEvalEnv));
    }
}
