package com.bc.calvalus.processing.hadoop;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class RasterStackWritable implements Writable {

    public int width;
    public int height;

    public Type[] bandTypes;
    public Object[] data;

    public RasterStackWritable() {
    }

    public RasterStackWritable(int width, int height, int numBands) {
        this.width = width;
        this.height = height;
        bandTypes = new Type[numBands];
        data = new Object[numBands];
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.write(width);
        out.write(height);
        out.write(bandTypes.length);
        for (Type bandType : bandTypes) {
            out.write(bandType.id);
        }

        for (int i = 0; i < bandTypes.length; i++) {
            Type type = bandTypes[i];
            Object sourceArray = data[i];

            int pixelIndex = 0;
            for (int y = 0; y < height; y++) {
                for (int x = 0; x < width; x++) {
                    if (type == Type.INTEGER) {
                        out.writeInt(((int[]) sourceArray)[pixelIndex]);
                    } else if (type == Type.LONG) {
                        out.writeLong(((long[]) sourceArray)[pixelIndex]);
                    } else if (type == Type.FLOAT) {
                        out.writeFloat(((float[]) sourceArray)[pixelIndex]);
                    } else if (type == Type.DOUBLE) {
                        out.writeDouble(((double[]) sourceArray)[pixelIndex]);
                    } else if (type == Type.SHORT) {
                        out.writeShort(((short[]) sourceArray)[pixelIndex]);
                    }
                    pixelIndex++;
                }
            }
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        width = in.readInt();
        height = in.readInt();
        int numBands = in.readInt();
        bandTypes = new Type[numBands];
        data = new Object[numBands];
        for (int i = 0; i < numBands; i++) {
            bandTypes[i] = Type.getType(in.readChar());
            switch (bandTypes[i]) {
                case INTEGER:
                    data[i] = new int[width * height];
                    break;
                case LONG:
                    data[i] = new long[width * height];
                    break;
                case FLOAT:
                    data[i] = new float[width * height];
                    break;
                case DOUBLE:
                    data[i] = new double[width * height];
                    break;
                case SHORT:
                    data[i] = new short[width * height];
                    break;
                case BYTE:
                    data[i] = new byte[width * height];
                    break;
                default:
                    throw new IllegalStateException("Unknown band type '" + bandTypes[i].id + "'");
            }
        }

        for (int i = 0; i < numBands; i++) {
            Type type = bandTypes[i];
            Object sourceArray = data[i];

            int pixelIndex = 0;
            for (int y = 0; y < height; y++) {
                for (int x = 0; x < width; x++) {
                    if (type == Type.INTEGER) {
                        ((int[]) sourceArray)[pixelIndex] = in.readInt();
                    } else if (type == Type.LONG) {
                        ((long[]) sourceArray)[pixelIndex] = in.readLong();
                    } else if (type == Type.FLOAT) {
                        ((float[]) sourceArray)[pixelIndex] = in.readFloat();
                    } else if (type == Type.DOUBLE) {
                        ((double[]) sourceArray)[pixelIndex] = in.readDouble();
                    } else if (type == Type.SHORT) {
                        ((short[]) sourceArray)[pixelIndex] = in.readShort();
                    } else if (type == Type.BYTE) {
                        ((byte[]) sourceArray)[pixelIndex] = in.readByte();
                    }
                    pixelIndex++;
                }
            }
        }

    }

    public void setBandType(int index, Type bandType) {
        this.bandTypes[index] = bandType;
    }

    public void setData(int index, Object array, Type type) {
        this.bandTypes[index] = type;
        this.data[index] = array;
    }

    public enum Type {
        INTEGER('I', Integer.class),
        LONG('L', Long.class),
        FLOAT('F', Float.class),
        DOUBLE('D', Double.class),
        SHORT('S', Short.class),
        BYTE('B', Byte.class);

        private static final Map<Character, Type> TYPE_ID_TO_TYPE_MAP = new HashMap<>();

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

        public static Type getType(char typeId) {
            // Performance note: typeId is converted to Character object here,
            // maybe use a fixed size lookup array here? (nf)
            return TYPE_ID_TO_TYPE_MAP.get(typeId);
        }

        static {
            for (Type type : Type.values()) {
                TYPE_ID_TO_TYPE_MAP.put(type.getId(), type);
            }
        }
    }
}
