package com.bc.calvalus.processing.ma;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;

/**
 * A Hadoop writable for {@link com.bc.calvalus.processing.ma.Record}s. The supported valuze types are given
 * in the enum {@link RecordWritable.Type}.
 * <p/>
 * <p/>
 * <i>
 * Implementation note: For each record value, a 16-bit type identifier is written.
 * A {@code null} value is represented by the special type  {@link RecordWritable.Type#NULL}.
 * Thus, it makes sense to encode missing data in a record, such as Float.NaN or Double.NaN values,
 * using {@code null} values.
 * </i>
 *
 * @author Norman
 */
public class RecordWritable implements Writable {
    /**
     * The supported value types.
     * Basically a mapping between a 16-bit type code and the its corresponding Java class.
     */
    public enum Type {
        NULL('\0', Void.class),
        INTEGER('I', Integer.class),
        LONG('L', Long.class),
        FLOAT('F', Float.class),
        DOUBLE('D', Double.class),
        STRING('S', String.class),
        DATE('T', Date.class),
        AGGREGATED_NUMBER('A', AggregatedNumber.class);

        private static final HashMap<Character, Type> TYPE_ID_TO_TYPE_MAP = new HashMap<Character, Type>();
        private static final HashMap<Class, Type> CLASS_TO_TYPE_MAP = new HashMap<Class, Type>();

        private final char id;
        private final Class type;

        Type(char id, Class type) {
            this.id = id;
            this.type = type;
        }

        public char getId() {
            return id;
        }

        public Class getType() {
            return type;
        }

        public static Type getType(Object value) {
            if (value == null) {
                return Type.NULL;
            } else {
                return CLASS_TO_TYPE_MAP.get(value.getClass());
            }
        }

        public static Type getType(char typeId) {
            // Performance note: typeId is converted to Character object here,
            // maybe use a fixed size lookup array here? (nf)
            return TYPE_ID_TO_TYPE_MAP.get(typeId);
        }

        static {
            for (Type type : Type.values()) {
                CLASS_TO_TYPE_MAP.put(type.getType(), type);
                TYPE_ID_TO_TYPE_MAP.put(type.getId(), type);
            }
        }
    }


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
            writeValue(out, value);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        final int valueCount = in.readInt();
        if (values == null || values.length != valueCount) {
            values = new Object[valueCount];
        }
        for (int i = 0; i < valueCount; i++) {
            values[i] = readValue(in);
        }
    }

    private void writeValue(DataOutput out, Object value) throws IOException {
        final Type type = Type.getType(value);
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
        } else if (type == Type.AGGREGATED_NUMBER) {
            final AggregatedNumber aggregatedNumber = (AggregatedNumber) value;
            out.writeInt(aggregatedNumber.n);
            out.writeInt(aggregatedNumber.nT);
            out.writeInt(aggregatedNumber.nF);
            out.writeDouble(aggregatedNumber.min);
            out.writeDouble(aggregatedNumber.max);
            out.writeDouble(aggregatedNumber.mean);
            out.writeDouble(aggregatedNumber.sigma);
            float[] data = aggregatedNumber.data;
            if (data != null) {
                out.writeShort(data.length);
                for (int i = 0; i < data.length; i++) {
                    out.writeFloat(data[i]);
                }
            } else {
                out.writeShort(0);
            }
        } else if (type == Type.NULL) {
            // ok, do nothing
        } else {
            throw new IllegalStateException("Unhandled value type: " + type);
        }
    }

    private Object readValue(DataInput in) throws IOException {
        final Object value;
        final char typeId = in.readChar();
        final Type type = Type.getType(typeId);
        if (type == null) {
            throw new IllegalStateException("Read illegal type ID: '" + typeId + "'");
        }
        if (type == Type.INTEGER) {
            value = in.readInt();
        } else if (type == Type.LONG) {
            value = in.readLong();
        } else if (type == Type.FLOAT) {
            value = in.readFloat();
        } else if (type == Type.DOUBLE) {
            value = in.readDouble();
        } else if (type == Type.STRING) {
            value = in.readUTF();
        } else if (type == Type.DATE) {
            value = new Date(in.readLong());
        } else if (type == Type.AGGREGATED_NUMBER) {
            final int n = in.readInt();
            final int nT = in.readInt();
            final int nF = in.readInt();
            final double min = in.readDouble();
            final double max = in.readDouble();
            final double mean = in.readDouble();
            final double sigma = in.readDouble();
            final int length = in.readShort();
            final float[] data = length > 0 ? new float[length] : null;
            if (data != null) {
                for (int i = 0; i < data.length; i++) {
                    data[i] = in.readFloat();
                }
            }
            value = new AggregatedNumber(n, nT, nF, min, max, mean, sigma, data);
        } else if (type == Type.NULL) {
            value = null;
        } else {
            throw new IllegalStateException("Unhandled value type: " + type);
        }
        return value;
    }

    /**
     * Used by Hadoop's TextOutputFormat.
     *
     * @return A textual representation of this record.
     */
    @Override
    public String toString() {
        return CsvRecordWriter.recordToString(values);
    }

}
