package com.bc.calvalus.processing.ma.expr;

import com.bc.calvalus.processing.ma.AggregatedNumber;
import com.bc.calvalus.processing.ma.DefaultHeader;
import com.bc.calvalus.processing.ma.DefaultRecord;
import com.bc.jexp.Term;
import com.bc.jexp.impl.ParserImpl;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Norman Fomferra
 */
public class RecordExpressionTest {

    @Test
    public void testMedian() throws Exception {
        DefaultHeader header = new DefaultHeader("rho_1", "rho_2", "rho_3", "rho_4", "rho_5");
        RecordEvalEnv recordEvalEnv = new RecordEvalEnv(header);
        HeaderNamespace namespace = new HeaderNamespace(header);

        DefaultRecord record = new DefaultRecord(new AggregatedNumber(24, 25, 16, 0.1, 4.0, 2.6, 0.1),
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
    public void testRecordsWithAggregatedNumbers() throws Exception {
        DefaultHeader header = new DefaultHeader("chl");
        RecordEvalEnv recordEvalEnv = new RecordEvalEnv(header);
        HeaderNamespace namespace = new HeaderNamespace(header);

        DefaultRecord record1 = new DefaultRecord(new AggregatedNumber(24, 25, 4, 0.1, 4.0, 2.6, 0.4));
        DefaultRecord record2 = new DefaultRecord(new AggregatedNumber(16, 25, 3, 0.2, 4.1, 3.7, 0.3));

        ParserImpl parser = new ParserImpl(namespace);

        Term t0 = parser.parse("chl");
        Term t1 = parser.parse("chl.n");
        Term t2 = parser.parse("chl.nT");
        Term t3 = parser.parse("chl.nF");
        Term t4 = parser.parse("chl.min");
        Term t5 = parser.parse("chl.max");
        Term t6 = parser.parse("chl.mean");
        Term t7 = parser.parse("chl.sigma");
        Term t8 = parser.parse("chl.cv");
        Term t9 = parser.parse("chl.cv < 0.15 && chl.n > 5");

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
        DefaultHeader header = new DefaultHeader("b", "s", "i", "f");
        RecordEvalEnv recordEvalEnv = new RecordEvalEnv(header);
        HeaderNamespace namespace = new HeaderNamespace(header);

        DefaultRecord record1 = new DefaultRecord(false, "x", 4, 0.6F);
        DefaultRecord record2 = new DefaultRecord(true, "y", 3, 0.5F);

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
