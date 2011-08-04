package com.bc.calvalus.processing.ma;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

    public RecordWritable(Record... records) {
        List<String> strings = new ArrayList<String>();
        for (Record record : records) {
            Object[] values = record.getAttributeValues();
            for (Object value : values) {
                strings.add(value.toString());
            }
        }
        values = strings.toArray(new String[strings.size()]);
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
}
