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

        DefaultRecord record = new DefaultRecord(new AggregatedNumber(25, 0.1, 4.0, 2.6, 0.4, 24, 2.6, 0.1, 16),
                                                 new AggregatedNumber(25, 0.1, 4.0, 2.6, 0.4, 24, 2.7, 0.2, 16),
                                                 new AggregatedNumber(25, 0.1, 4.0, 2.6, 0.4, 24, 2.5, 0.1, 16),
                                                 new AggregatedNumber(25, 0.1, 4.0, 2.6, 0.4, 24, 2.6, 0.2, 16),
                                                 new AggregatedNumber(25, 0.1, 4.0, 2.6, 0.4, 24, 2.8, 0.3, 16));

        ParserImpl parser = new ParserImpl(namespace);

        Term term = parser.parse("median(rho_1.cv, rho_2.cv, rho_3.cv, rho_4.cv, rho_5.cv)");

        recordEvalEnv.setContext(record);
        assertEquals(0.04, term.evalD(recordEvalEnv), 1e-6);
    }


    @Test
    public void testRecordsWithArrays() throws Exception {
        DefaultHeader header = new DefaultHeader("chl");
        RecordEvalEnv recordEvalEnv = new RecordEvalEnv(header);
        HeaderNamespace namespace = new HeaderNamespace(header);

        DefaultRecord record1 = new DefaultRecord(new AggregatedNumber(25, 0.1, 4.0, 2.6, 0.4, 24, 2.4, 0.2, 16));
        DefaultRecord record2 = new DefaultRecord(new AggregatedNumber(25, 0.2, 4.1, 1.7, 0.3, 16, 2.5, 0.1, 12));

        ParserImpl parser = new ParserImpl(namespace);

        Term t1 = parser.parse("chl.filteredMean - 1.5 * chl.filteredStdDev");
        Term t2 = parser.parse("feq(chl.mean, 2.6)");
        Term t3 = parser.parse("chl.cv");
        Term t4 = parser.parse("chl.cv > 0.15");
        Term t5 = parser.parse("chl.max - chl.min");

        recordEvalEnv.setContext(record1);
        assertEquals(2.4 - 1.5 * 0.2, t1.evalD(recordEvalEnv), 1e-6);
        assertEquals(true, t2.evalB(recordEvalEnv));
        assertEquals(0.2 / 2.4, t3.evalD(recordEvalEnv), 1e-6);
        assertEquals(false, t4.evalB(recordEvalEnv));
        assertEquals(4.0 - 0.1, t5.evalD(recordEvalEnv), 1e-6);

        recordEvalEnv.setContext(record2);
        assertEquals(2.5 - 1.5 * 0.1, t1.evalD(recordEvalEnv), 1e-6);
        assertEquals(false, t2.evalB(recordEvalEnv));
        assertEquals(0.1 / 2.5, t3.evalD(recordEvalEnv), 1e-6);
        assertEquals(false, t4.evalB(recordEvalEnv));
        assertEquals(4.1 - 0.2, t5.evalD(recordEvalEnv), 1e-6);
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
