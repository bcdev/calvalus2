package com.bc.calvalus.processing.ma;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;

/**
 * A Hadoop writable for {@link com.bc.calvalus.processing.ma.Record}s.
 *
 * @author Norman
 */
public class RecordWritable implements Writable {

    public enum Type {
        NULL('_'),
        INTEGER('I'),
        LONG('L'),
        FLOAT('F'),
        DOUBLE('D'),
        DATE('T'),
        AGG_NUM('A'),
        STRING('S');

        private final char id;

        Type(char id) {
            this.id = id;
        }

        public char getId() {
            return id;
        }
    }

    private static final HashMap<Character, Type> TYPE_ID_TO_TYPE_MAP = new HashMap<Character, Type>();
    private static final HashMap<Class, Type> CLASS_TO_TYPE_MAP = new HashMap<Class, Type>();

    private Object[] values;

    public RecordWritable() {
    }

    public RecordWritable(Object[] values) {
        this.values = values;
    }

    public Object[] getValues() {
        return values;
    }

    public void setValues(Object[] values) {
        this.values = values;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(values.length);
        for (Object value : values) {
            Type type = getValueType(value);
            if (type == null) {
                throw new IllegalStateException("Received illegal data type: " + value.getClass());
            }
            out.writeChar(type.getId());
            if (type == Type.INTEGER) {
                out.writeInt((Integer) value);
            } else if (type == Type.LONG) {
                out.writeLong((Long) value);
            } else if (type == Type.FLOAT) {
                out.writeFloat((Float) value);
            } else if (type == Type.DOUBLE) {
                out.writeDouble((Double) value);
            } else if (type == Type.DATE) {
                out.writeLong(((Date) value).getTime());
            } else if (type == Type.STRING) {
                out.writeUTF((String) value);
            } else if (type == Type.AGG_NUM) {
                AggregatedNumber aggregatedNumber = (AggregatedNumber) value;
                out.writeInt(aggregatedNumber.n);
                out.writeInt(aggregatedNumber.nT);
                out.writeInt(aggregatedNumber.nF);
                out.writeDouble(aggregatedNumber.min);
                out.writeDouble(aggregatedNumber.max);
                out.writeDouble(aggregatedNumber.mean);
                out.writeDouble(aggregatedNumber.sigma);
            } else if (type == Type.NULL) {
                // ok, do nothing
            } else {
                throw new IllegalStateException("Unhandled value type: " + type);
            }
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int valueCount = in.readInt();
        if (values == null || values.length != valueCount) {
            values = new Object[valueCount];
        }
        for (int i = 0; i < valueCount; i++) {
            char typeId = in.readChar();
            Type type = TYPE_ID_TO_TYPE_MAP.get(typeId);
            if (type == null) {
                throw new IllegalStateException("Read illegal type ID: '" + typeId + "'");
            }
            if (type == Type.INTEGER) {
                values[i] = in.readInt();
            } else if (type == Type.LONG) {
                values[i] = in.readLong();
            } else if (type == Type.FLOAT) {
                values[i] = in.readFloat();
            } else if (type == Type.DOUBLE) {
                values[i] = in.readDouble();
            } else if (type == Type.STRING) {
                values[i] = in.readUTF();
            } else if (type == Type.DATE) {
                values[i] = new Date(in.readLong());
            } else if (type == Type.AGG_NUM) {
                int n = in.readInt();
                int nT = in.readInt();
                int nF = in.readInt();
                double min = in.readDouble();
                double max = in.readDouble();
                double mean = in.readDouble();
                double sigma = in.readDouble();
                values[i] = new AggregatedNumber(n, nT, nF, min, max, mean, sigma);
            } else if (type == Type.NULL) {
                // ok, do nothing
            } else {
                throw new IllegalStateException("Unhandled value type: " + type);
            }
        }
    }

    /**
     * Used by Hadoop's TextOutputFormat.
     *
     * @return A textual representation of this record.
     */
    @Override
    public String toString() {
        return TextUtils.toString(values);
    }

    private static Type getValueType(Object value) {
        if (value == null) {
            return Type.NULL;
        } else {
            return CLASS_TO_TYPE_MAP.get(value.getClass());
        }
    }

    static {
        CLASS_TO_TYPE_MAP.put(Integer.class, Type.INTEGER);
        CLASS_TO_TYPE_MAP.put(Long.class, Type.LONG);
        CLASS_TO_TYPE_MAP.put(Float.class, Type.FLOAT);
        CLASS_TO_TYPE_MAP.put(Double.class, Type.DOUBLE);
        CLASS_TO_TYPE_MAP.put(String.class, Type.STRING);
        CLASS_TO_TYPE_MAP.put(Date.class, Type.DATE);
        CLASS_TO_TYPE_MAP.put(AggregatedNumber.class, Type.AGG_NUM);

        for (Type type : Type.values()) {
            TYPE_ID_TO_TYPE_MAP.put(type.getId(), type);
        }
    }
}
