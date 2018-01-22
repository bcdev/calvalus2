package com.bc.calvalus.inventory.hadoop;

import com.bc.calvalus.commons.CalvalusLogger;
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
import java.util.logging.Level;

/**
 * Dummy implementation of a formally seekable stream to obey the Hadoop FileSystem interface.
 * We know that we read sequentially from the process output.
 *
 * @author Martin Boettcher
 */
public class DummyFSDataInputStream extends FSDataInputStream {

    private String path;
    private CalvalusShFileSystem fileSystem;

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

    static class DummyCountingInputStream extends InputStream {

        private InputStream in;
        private long pos = 0;

        public DummyCountingInputStream(InputStream in) {
            super();
            this.in = in;
        }

        public void setIn(InputStream in) {
            this.in = in;
            pos = 0;
        }

        @Override
        public int read() throws IOException {
            ++pos;
            return in.read();
        }

        @Override
        public int read(byte[] b) throws IOException {
            int count = in.read(b);
            pos += count;
            return count;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            int count = in.read(b, off, len);
            pos += count;
            return count;
        }

        @Override
        public long skip(long n) throws IOException {
            return in.skip(n);
        }

        @Override
        public int available() throws IOException {
            return in.available();
        }

        @Override
        public void close() throws IOException {
            in.close();
        }

        @Override
        public synchronized void mark(int readlimit) {
            in.mark(readlimit);
        }

        @Override
        public synchronized void reset() throws IOException {
            in.reset();
        }

        @Override
        public boolean markSupported() {
            return in.markSupported();
        }

        public long getPos() {
            return pos;
        }
    }

    public DummyFSDataInputStream(InputStream in, String path, CalvalusShFileSystem fileSystem) {
        super(new DummyByteArrayInputStream(new byte[2]));
        this.in = new DummyCountingInputStream(in);
        this.path = path;
        this.fileSystem = fileSystem;
    }

    /**
     * Verbatim copy from: java.io.InputStream#skip(long)
     * to prevent FileInputstream from skipping wrongly
     */
    @Override
    public long skip(long n) throws IOException {
        CalvalusLogger.getLogger().log(Level.INFO, "Start skip " + n +
                                                   " with current position " +
                                                   ((DummyCountingInputStream) this.in).getPos());

        long remaining = n;
        int nr;

        if (n <= 0) {
            CalvalusLogger.getLogger().log(Level.INFO, "n = " + n);
            return 0;
        }

        int size = (int) Math.min(8192, remaining);
        byte[] skipBuffer = new byte[size];
        while (remaining > 0) {
            CalvalusLogger.getLogger().log(Level.INFO, "Math.min(size, remaining) : " + Math.min(size, remaining));
            CalvalusLogger.getLogger().log(Level.INFO, "remaining : " + remaining);
            nr = read(skipBuffer, 0, (int) Math.min(size, remaining));
            CalvalusLogger.getLogger().log(Level.INFO, "nr : " + nr);
            if (nr < 0) {
                break;
            }
            remaining -= nr;
            CalvalusLogger.getLogger().log(Level.INFO, "remaining stream : " + remaining);
        }
        return n - remaining;
    }

    @Override
    public void seek(long desired) throws IOException {
        CalvalusLogger.getLogger().log(Level.INFO, "Start seek " + desired + " with current position " +
                                                   ((DummyCountingInputStream) this.in).getPos());
        DummyCountingInputStream in = (DummyCountingInputStream) this.in;
        long pos = in.getPos();
        if (desired >= pos) {
            long skip = skip(desired - pos);
        } else {
            InputStream inputStream = fileSystem.callUnixCommand("cat", path).getInputStream();
            in.setIn(inputStream);
            long skip = skip(desired);
        }
    }

    @Override
    public int read() throws IOException {
        return super.read();
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
    public ByteBuffer read(ByteBufferPool bufferPool, int maxLength, EnumSet<ReadOption> opts)
                throws IOException, UnsupportedOperationException {
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
