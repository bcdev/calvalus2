package com.bc.calvalus.experiments.format.seq;

import org.apache.hadoop.io.BytesWritable;

public class ByteArrayFactory {
    public static Class getType(boolean compressed) {
        if (compressed) {
            return  MyByteArrayWritable.class;
        } else {
            return  MyBytesWritable.class;
        }
    }

    public static ByteArray createByteArray(boolean compressed, byte[] data) {
        if (compressed) {
            return new MyByteArrayWritable(data);
        } else {
            return new MyBytesWritable(data);
        }
    }

    public static ByteArray createByteArray(boolean compressed) {
        if (compressed) {
            return new MyByteArrayWritable();
        } else {
            return new MyBytesWritable();
        }
    }
    public static class MyBytesWritable extends BytesWritable implements ByteArray {

        private MyBytesWritable() {
        }

        private MyBytesWritable(byte[] bytes) {
            super(bytes);
        }
    }

    public static class MyByteArrayWritable extends ByteArrayWritable implements ByteArray {
        private MyByteArrayWritable() {
        }

        public MyByteArrayWritable(byte[] data) {
            super(data);
        }
    }

}
