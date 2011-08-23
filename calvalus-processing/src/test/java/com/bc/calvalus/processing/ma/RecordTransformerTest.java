package com.bc.calvalus.processing.ma;

import com.bc.jexp.EvalEnv;
import com.bc.jexp.EvalException;
import com.bc.jexp.Function;
import com.bc.jexp.Namespace;
import com.bc.jexp.Symbol;
import com.bc.jexp.Term;
import com.bc.jexp.impl.DefaultNamespace;
import com.bc.jexp.impl.ParserImpl;
import org.esa.beam.framework.datamodel.GeoPos;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.*;

/**
 * @author Norman
 */
public class RecordTransformerTest {

    @Test(expected = IllegalArgumentException.class)
    public void testExpandChecksZeroLengthArrays() throws Exception {
        new RecordTransformer(-1).expand(ExtractorTest.newRecord(null, null,
                                                                 "x",
                                                                 new float[0]));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testExpandChecksVaryingLengthArrays() throws Exception {
        new RecordTransformer(-1).expand(ExtractorTest.newRecord(null, null,
                                                                 "x",
                                                                 new float[3],
                                                                 new int[2]));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testExpandChecksSupportedArrayTypes() throws Exception {
        new RecordTransformer(-1).expand(ExtractorTest.newRecord(null, null,
                                                                 "x",
                                                                 new Date[3]));
    }

    @Test
    public void testExpand() throws Exception {
        List<Record> flattenedRecords = new RecordTransformer(-1).expand(ExtractorTest.newRecord(new GeoPos(53.0F, 13.3F), new Date(128L),
                                                                                                 "africa",
                                                                                                 new float[]{1.1F, 1.2F, 1.4F, 1.8F},
                                                                                                 new int[]{64, 32, 16, 8}));
        assertNotNull(flattenedRecords);
        assertEquals(4, flattenedRecords.size());

        Record r1 = flattenedRecords.get(0);
        assertEquals(6, r1.getAttributeValues().length);
        assertEquals(53.0F, (Float) r1.getAttributeValues()[0], 1E-5F);
        assertEquals(13.3F, (Float) r1.getAttributeValues()[1], 1E-5F);
        assertEquals(new Date(128L), r1.getAttributeValues()[2]);
        assertEquals("africa", r1.getAttributeValues()[3]);
        assertEquals(1.1F, (Float) r1.getAttributeValues()[4], 1E-5F);
        assertEquals(64, r1.getAttributeValues()[5]);

        Record r4 = flattenedRecords.get(3);
        assertEquals(6, r4.getAttributeValues().length);
        assertEquals(53.0F, (Float) r4.getAttributeValues()[0], 1E-5F);
        assertEquals(13.3F, (Float) r4.getAttributeValues()[1], 1E-5F);
        assertEquals(new Date(128L), r4.getAttributeValues()[2]);
        assertEquals("africa", r4.getAttributeValues()[3]);
        assertEquals(1.8F, (Float) r4.getAttributeValues()[4], 1E-5F);
        assertEquals(8, r4.getAttributeValues()[5]);
    }

    @Test
    public void testExpandWithMask() throws Exception {
        List<Record> flattenedRecords = new RecordTransformer(6).expand(ExtractorTest.newRecord(new GeoPos(53.0F, 13.3F), new Date(128L),
                                                                                                "africa",
                                                                                                new float[]{1.1F, 1.2F, 1.4F, 1.8F},
                                                                                                new int[]{64, 32, 16, 8},
                                                                                                new int[]{1, 0, 0, 1}));
        assertNotNull(flattenedRecords);
        assertEquals(2, flattenedRecords.size());

        Record r1 = flattenedRecords.get(0);
        assertEquals(7, r1.getAttributeValues().length);
        assertEquals(53.0F, (Float) r1.getAttributeValues()[0], 1E-5F);
        assertEquals(13.3F, (Float) r1.getAttributeValues()[1], 1E-5F);
        assertEquals(new Date(128L), r1.getAttributeValues()[2]);
        assertEquals("africa", r1.getAttributeValues()[3]);
        assertEquals(1.1F, (Float) r1.getAttributeValues()[4], 1E-5F);
        assertEquals(64, r1.getAttributeValues()[5]);

        Record r4 = flattenedRecords.get(1);
        assertEquals(7, r4.getAttributeValues().length);
        assertEquals(53.0F, (Float) r4.getAttributeValues()[0], 1E-5F);
        assertEquals(13.3F, (Float) r4.getAttributeValues()[1], 1E-5F);
        assertEquals(new Date(128L), r4.getAttributeValues()[2]);
        assertEquals("africa", r4.getAttributeValues()[3]);
        assertEquals(1.8F, (Float) r4.getAttributeValues()[4], 1E-5F);
        assertEquals(8, r4.getAttributeValues()[5]);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAggregateChecksZeroLengthArrays() throws Exception {
        new RecordTransformer(-1).aggregate(ExtractorTest.newRecord(null, null,
                                                                    "x",
                                                                    new float[0]));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAggregateChecksVaryingLengthArrays() throws Exception {
        new RecordTransformer(-1).aggregate(ExtractorTest.newRecord(null, null,
                                                                    "x",
                                                                    new float[3],
                                                                    new int[2]));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAggregateChecksSupportedArrayTypes() throws Exception {
        new RecordTransformer(-1).aggregate(ExtractorTest.newRecord(null, null,
                                                                    "x",
                                                                    new Date[3]));
    }

    @Test
    public void testAggregate() throws Exception {
        int maskAttributeIndex = -1;
        Record aggregatedRecord = new RecordTransformer(maskAttributeIndex).aggregate(
                ExtractorTest.newRecord(new GeoPos(53.0F, 13.3F), new Date(128L),
                                        "africa",
                                        new float[]{1.1F, 1.2F, 1.4F, 1.8F},
                                        new int[]{64, 32, 16, 8},
                                        new int[]{0, 1, 1, 0}));
        assertNotNull(aggregatedRecord);

        assertEquals(53.0F, (Float) aggregatedRecord.getAttributeValues()[0], 1E-5F);
        assertEquals(13.3F, (Float) aggregatedRecord.getAttributeValues()[1], 1E-5F);
        assertEquals(new Date(128L), aggregatedRecord.getAttributeValues()[2]);
        assertEquals("africa", aggregatedRecord.getAttributeValues()[3]);

        Object v5 = aggregatedRecord.getAttributeValues()[4];
        assertEquals(AggregatedNumber.class, v5.getClass());
        AggregatedNumber a5 = (AggregatedNumber) v5;
        assertEquals(4, a5.numGoodPixels);
        assertEquals(4, a5.numTotalPixels);
        assertEquals((1.1F + 1.2F + 1.4F + 1.8F) / 4.0F, a5.floatValue(), 1E-5F);

        Object v6 = aggregatedRecord.getAttributeValues()[5];
        assertEquals(AggregatedNumber.class, v6.getClass());
        AggregatedNumber a6 = (AggregatedNumber) v6;
        assertEquals(4, a6.numGoodPixels);
        assertEquals(4, a6.numTotalPixels);
        assertEquals((64 + 32 + 16 + 8) / 4, a6.floatValue(), 1E-5F);

        Object v7 = aggregatedRecord.getAttributeValues()[6];
        assertEquals(AggregatedNumber.class, v7.getClass());
        AggregatedNumber a7 = (AggregatedNumber) v7;
        assertEquals(4, a7.numGoodPixels);
        assertEquals(4, a7.numTotalPixels);
        assertEquals(0.5F, a7.floatValue(), 1E-5F);
    }

    @Test
    public void testAggregateWithMask() throws Exception {
        int maskAttributeIndex = 6;
        Record aggregatedRecord = new RecordTransformer(maskAttributeIndex).aggregate(
                ExtractorTest.newRecord(new GeoPos(53.0F, 13.3F), new Date(128L),
                                        "africa",
                                        new float[]{1.1F, 1.2F, Float.NaN, 1.8F},
                                        new int[]{64, 32, 16, 8},
                                        new int[]{1, 1, 1, 0}));
        assertNotNull(aggregatedRecord);

        assertEquals(53.0F, (Float) aggregatedRecord.getAttributeValues()[0], 1E-5F);
        assertEquals(13.3F, (Float) aggregatedRecord.getAttributeValues()[1], 1E-5F);
        assertEquals(new Date(128L), aggregatedRecord.getAttributeValues()[2]);
        assertEquals("africa", aggregatedRecord.getAttributeValues()[3]);

        Object v5 = aggregatedRecord.getAttributeValues()[4];
        assertEquals(AggregatedNumber.class, v5.getClass());
        AggregatedNumber a5 = (AggregatedNumber) v5;
        assertEquals(2, a5.numGoodPixels);
        assertEquals(3, a5.numTotalPixels);
        assertEquals((1.1F + 1.2F) / 2, a5.floatValue(), 1E-5F);

        Object v6 = aggregatedRecord.getAttributeValues()[5];
        assertEquals(AggregatedNumber.class, v6.getClass());
        AggregatedNumber a6 = (AggregatedNumber) v6;
        assertEquals(3, a6.numGoodPixels);
        assertEquals(4, a6.numTotalPixels);
        assertEquals((64F + 32F + 16F) / 3, a6.floatValue(), 1E-5F);

        Object v7 = aggregatedRecord.getAttributeValues()[6];
        assertEquals(AggregatedNumber.class, v7.getClass());
        AggregatedNumber a7 = (AggregatedNumber) v7;
        assertEquals(4, a7.numGoodPixels);
        assertEquals(4, a7.numTotalPixels);
        assertEquals(3F / 4, a7.floatValue(), 1E-5F);
    }

    @Test
    public void testAggregateWithFilters() throws Exception {
        int maskAttributeIndex = -1;
        AggregatedNumberFilter filter = new AggregatedNumberFilter() {
            @Override
            public boolean accept(int attributeIndex, AggregatedNumber number) {
                return attributeIndex == 4 && number.mean < 1.0F;
            }
        };
        Record goodRecord = new RecordTransformer(maskAttributeIndex, filter).aggregate(
                ExtractorTest.newRecord(new GeoPos(53.0F, 13.3F), new Date(128L),
                                        "africa",
                                        new float[]{0.5F, 0.5F, 0.5F})); // mean = 0.5
        assertNotNull(goodRecord);

        Record badRecord = new RecordTransformer(maskAttributeIndex, filter).aggregate(
                ExtractorTest.newRecord(new GeoPos(53.0F, 13.3F), new Date(128L),
                                        "africa",
                                        new float[]{2.0F, 2.0F, 2.0F})); // mean = 2.0
        assertNull(badRecord);
    }

    @Test
    public void testExpr() throws Exception {
        HashMap<String, AggregatedNumber> values = new HashMap<String, AggregatedNumber>();
         values.put("chl", new AggregatedNumber(25, 24, 16, 2.6f, 0.4f, 2.4f, 0.2f));
        MyNamespace namespace = new MyNamespace(values);

        ParserImpl parser = new ParserImpl(namespace);
        assertEquals(2.4 - 1.5 * 0.2, parser.parse("chl.filteredMean - 1.5 * chl.filteredStdDev").evalD(null), 1e-6);
        assertEquals(true, parser.parse("feq(chl.mean, 2.6)").evalB(null));
        assertEquals(0.2 / 2.4, parser.parse("chl.CV").evalD(null), 1e-6);
        assertEquals(false, parser.parse("chl.CV > 0.15").evalB(null));
    }

    private static abstract class PifiSymbol implements Symbol {
        String variableName;
        Field field;
        AggregatedNumber aggregatedNumber;
        String name;

        private PifiSymbol(AggregatedNumber aggregatedNumber, Field field, String variableName) {
            this.aggregatedNumber = aggregatedNumber;
            this.field = field;
            this.variableName = variableName;
            name = variableName + "." + field.getName();
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public boolean evalB(EvalEnv env) throws EvalException {
            return evalI(env) != 0;
        }

    }

    private static class IntSymbol extends PifiSymbol {

        public IntSymbol(String variableName, Field field, AggregatedNumber aggregatedNumber) {
            super(aggregatedNumber, field, variableName);
        }

        @Override
        public int getRetType() {
            return Term.TYPE_I;
        }

        @Override
        public int evalI(EvalEnv env) throws EvalException {
            try {
                return field.getInt(aggregatedNumber);
            } catch (IllegalAccessException e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public double evalD(EvalEnv env) throws EvalException {
            return evalI(env);
        }

        @Override
        public String evalS(EvalEnv env) throws EvalException {
            return String.valueOf(evalI(env));
        }
    }
    private static class FloatSymbol extends PifiSymbol {

        public FloatSymbol(String variableName, Field field, AggregatedNumber aggregatedNumber) {
            super(aggregatedNumber, field, variableName);
        }

        @Override
        public int getRetType() {
            return Term.TYPE_D;
        }

        @Override
        public double evalD(EvalEnv env) throws EvalException {
            try {
                return field.getFloat(aggregatedNumber);
            } catch (IllegalAccessException e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public int evalI(EvalEnv env) throws EvalException {
            return (int) evalD(env);
        }

        @Override
        public String evalS(EvalEnv env) throws EvalException {
            return String.valueOf(evalD(env));
        }
    }

    private static class MyNamespace implements Namespace {
        private final DefaultNamespace namespace;

        private HashMap<String, AggregatedNumber> values;

        private MyNamespace(HashMap<String, AggregatedNumber> values) {
            this.namespace = new DefaultNamespace();
            this.values = values;
        }

        @Override
        public Function resolveFunction(String name, Term[] args) {
            return namespace.resolveFunction(name, args);
        }

        @Override
        public Symbol resolveSymbol(String name) {
            Symbol symbol = namespace.resolveSymbol(name);
            if (symbol != null) {
                return symbol;
            }
            int pos = name.indexOf('.');
            if (pos <= 0) {
                return null;
            }
            String variableName = name.substring(0, pos);
            AggregatedNumber aggregatedNumber = values.get(variableName);
            if (aggregatedNumber == null) {
                return null;
            }
            String fieldName = name.substring(pos + 1);
            Field field;
            try {
                field = AggregatedNumber.class.getDeclaredField(fieldName);
            } catch (NoSuchFieldException e) {
                return null;
            }
            if (field.getType().equals(Integer.TYPE)) {
                symbol= new IntSymbol(variableName, field, aggregatedNumber);
                namespace.registerSymbol(symbol);
            } else if (field.getType().equals(Float.TYPE)) {
                symbol= new FloatSymbol(variableName, field, aggregatedNumber);
                namespace.registerSymbol(symbol);
            }
            return symbol;
        }
    }
}
