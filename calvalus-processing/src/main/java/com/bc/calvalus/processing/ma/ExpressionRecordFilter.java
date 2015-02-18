package com.bc.calvalus.processing.ma;

import com.bc.calvalus.processing.ma.expr.HeaderNamespace;
import com.bc.calvalus.processing.ma.expr.RecordEvalEnv;
import com.bc.jexp.ParseException;
import com.bc.jexp.Term;
import com.bc.jexp.impl.ParserImpl;

/**
 * A record filter that uses an expression to decide if a record is accepted or not.
 *
 * @author Norman
 */
public class ExpressionRecordFilter implements RecordFilter {
    private final Term compiledExpr;
    private final RecordEvalEnv recordEvalEnv;

    public static RecordFilter create(Header header, String expr) throws ParseException {
        HeaderNamespace namespace = new HeaderNamespace(header);
        ParserImpl parser = new ParserImpl(namespace, false);
        return new ExpressionRecordFilter(new RecordEvalEnv(namespace), parser.parse(expr));
    }

    private ExpressionRecordFilter(RecordEvalEnv recordEvalEnv, Term compiledExpr) {
        this.recordEvalEnv = recordEvalEnv;
        this.compiledExpr = compiledExpr;
    }

    @Override
    public boolean accept(Record record) {
        recordEvalEnv.setContext(record);
        return compiledExpr.evalB(recordEvalEnv);
    }
}
