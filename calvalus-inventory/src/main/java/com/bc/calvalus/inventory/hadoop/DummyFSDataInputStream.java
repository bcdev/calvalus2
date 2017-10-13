package com.bc.calvalus.inventory.hadoop;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.ReadOption;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.ByteBufferPool;

import java.io.ByteArrayInputStream;
import java.io.FileDescriptor;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.EnumSet;

/**
 * Dummy implementation of a formally seekable stream to obey the Hadoop FileSystem interface.
 * We know that we read sequentially from the process output.
 *
 * @author Martin Boettcher
 */
public class DummyFSDataInputStream extends FSDataInputStream {

    static class DummyByteArrayInputStream extends ByteArrayInputStream implements Seekable, PositionedReadable {

        public DummyByteArrayInputStream(byte[] buf) {
            super(buf);
        }

        @Override
        public void seek(long pos) throws IOException {
             throw new UnsupportedOperationException();
        }

        @Override
        public long getPos() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean seekToNewSource(long targetPos) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int read(long position, byte[] buffer, int offset, int length) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void readFully(long position, byte[] buffer) throws IOException {
            throw new UnsupportedOperationException();
        }
    }

    public DummyFSDataInputStream(InputStream in) {
        super(new DummyByteArrayInputStream(new byte[2]));
        this.in = in;
    }

    @Override
    public void seek(long desired) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getPos() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void readFully(long position, byte[] buffer) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public InputStream getWrappedStream() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int read(ByteBuffer buf) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public FileDescriptor getFileDescriptor() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setReadahead(Long readahead) throws IOException, UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setDropBehind(Boolean dropBehind) throws IOException, UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuffer read(ByteBufferPool bufferPool, int maxLength, EnumSet<ReadOption> opts) throws IOException, UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void releaseBuffer(ByteBuffer buffer) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void unbuffer() {
        throw new UnsupportedOperationException();
    }
}
