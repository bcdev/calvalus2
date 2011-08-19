package com.bc.calvalus.processing.ma;

import org.apache.hadoop.io.Writable;
import org.esa.beam.util.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.DateFormat;
import java.util.Date;

/**
 * A Hadoop writable for {@link Record}s.
 *
 * @author MarcoZ
 * @author Norman
 */
public class RecordWritable implements Writable {

    private String[] values;

    public RecordWritable() {
    }

    public RecordWritable(String[] values) {
        this.values = values;
    }

    public RecordWritable(Object[] values, DateFormat dateFormat) {
        this(convertValuesToText(values, dateFormat));
    }

    private static String[] convertValuesToText(Object[] values, DateFormat dateFormat) {
        String[] strings = new String[values.length];
        for (int i = 0; i < values.length; i++) {
            Object value = values[i];
            if (value == null) {
                strings[i] = "";
            } else if (value instanceof Date) {
                strings[i] =  dateFormat.format((Date) value);
            } else {
                strings[i] = value.toString();
            }
        }
        return strings;
    }

    public String[] getValues() {
        return values;
    }

    public void setValues(String[] values) {
        this.values = values;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(values.length);
        for (String value : values) {
            out.writeUTF(value);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int n = in.readInt();
        if (values == null || values.length != n) {
            values = new String[n];
        }
        for (int i = 0; i < n; i++) {
            values[i] = in.readUTF();
        }
    }

    @Override
    public String toString() {
        return StringUtils.join(values, "\t");
    }
}
