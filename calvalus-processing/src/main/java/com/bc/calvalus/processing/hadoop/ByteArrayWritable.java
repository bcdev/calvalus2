package com.bc.calvalus.processing.hadoop;

import org.apache.hadoop.io.CompressedWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.BytesWritable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A {@link Writable} very similar to {@link BytesWritable}
 * but with in-built compression.
 *
 * @author Norman Fomferra
 * @since 0.1
 */
public class ByteArrayWritable extends CompressedWritable {
    private int length;
    private byte[] array;

    public ByteArrayWritable() {
        this(0);
    }

    public ByteArrayWritable(byte[] array) {
        this.length = array.length;
        this.array = array;
    }

    public ByteArrayWritable(int length) {
        this.length = length;
        this.array = new byte[length];
    }

    public int getLength() {
        ensureInflated();
        return length;
    }

    public byte[] getArray() {
        ensureInflated();
        return array;
    }

    /**
     * Serialize the fields of this object to <code>out</code>.
     *
     * @param out <code>DataOuput</code> to serialize this object into.
     * @throws java.io.IOException
     */
    @Override
    public void writeCompressed(DataOutput out) throws IOException {
        out.writeInt(length);
        out.write(array);
    }

    /**
     * Deserialize the fields of this object from <code>in</code>.
     *
     * @param in <code>DataInput</code> to deseriablize this object from.
     * @throws java.io.IOException
     */
    @Override
    public void readFieldsCompressed(DataInput in) throws IOException {
        int length = in.readInt();
        byte[] array = this.array;
        if (array == null || array.length != length) {
            array = new byte[length];
        }
        in.readFully(array);
        this.length = length;
        this.array = array;
    }

    /**
     * Deserialize the fields of this object from <code>in</code>.
     *
     * @param in <code>DataInput</code> to deseriablize this object from.
     * @return The writable.
     * @throws java.io.IOException if an I/O error occurs
     */
    public static ByteArrayWritable read(DataInput in) throws IOException {
        ByteArrayWritable writable = new ByteArrayWritable();
        writable.readFields(in);
        return writable;
    }

    public String toString() {
        return "ByteArrayWritable length=" + getLength();
    }
}
