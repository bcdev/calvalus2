/*
 * Copyright (C) 2014 Brockmann Consult GmbH (info@brockmann-consult.de)
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, see http://www.gnu.org/licenses/
 */

package com.bc.calvalus.processing.ma;

import com.bc.jexp.ParseException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * A record transformer that annotates the record ith the given annotation,
 * if the filter does not accept it.
 */
public class RecordFilterTransformer implements RecordTransformer {

    public static final String EXCLUSION_REASON_EXPRESSION = "RECORD_EXPRESSION";

    private final RecordFilter recordFilter;
    private final int exclusionIndex;
    private final String reason;

    public static RecordTransformer createExpressionFilter(Header header, String goodRecordExpression) {
        if (shallApplyGoodRecordExpression(goodRecordExpression)) {
            final RecordFilter recordFilter;
            try {
                recordFilter = ExpressionRecordFilter.create(header, goodRecordExpression);
            } catch (ParseException e) {
                String msg = "Illegal configuration: goodRecordExpression '" + goodRecordExpression + "' is invalid: " + e.getMessage();
                throw new IllegalStateException(msg, e);
            }
            return new RecordFilterTransformer(header, recordFilter, EXCLUSION_REASON_EXPRESSION);
        } else {
            return new NoneTransformer();
        }
    }


    private static boolean shallApplyGoodRecordExpression(String goodRecordExpression) {
        return goodRecordExpression != null && !goodRecordExpression.isEmpty();
    }

    RecordFilterTransformer(Header header, RecordFilter recordFilter, String reason) {
        this.recordFilter = recordFilter;
        this.reason = reason;
        exclusionIndex = header.getAnnotationIndex(DefaultHeader.ANNOTATION_EXCLUSION_REASON);
    }

    @Override
    public Iterable<Record> transform(Iterable<Record> recordIterable) {
        return new Iterable<Record>() {
            @Override
            public Iterator<Record> iterator() {
                return new RecIt(recordIterable.iterator());
            }
        };
    }

    private class RecIt extends RecordIterator {

        private final Iterator<Record> inputIt;

        private RecIt(Iterator<Record> inputIt) {
            this.inputIt = inputIt;
        }

        @Override
        protected Record getNextRecord() {
            if (inputIt.hasNext()) {
                Record next = inputIt.next();
                String currentReason = (String) next.getAnnotationValues()[exclusionIndex];
                if (currentReason.isEmpty() && !recordFilter.accept(next)) {
                    next.getAnnotationValues()[exclusionIndex] = reason;
                }
                return next;
            }
            return null;
        }
    }
}
