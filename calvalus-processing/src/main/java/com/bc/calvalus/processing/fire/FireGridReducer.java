package com.bc.calvalus.processing.fire;

import org.apache.hadoop.mapreduce.Reducer;
import org.esa.snap.core.datamodel.Product;

import java.io.IOException;

/**
 * TODO fill in or delete
 *
 * @author thomas
 */
public class FireGridReducer extends Reducer {

    Product result;

    @Override
    protected void reduce(Object key, Iterable values, Context context) throws IOException, InterruptedException {
//        context.write(result, result);
    }
}
