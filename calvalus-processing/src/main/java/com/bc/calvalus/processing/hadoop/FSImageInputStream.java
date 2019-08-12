package com.bc.calvalus.processing.hadoop;

import org.apache.hadoop.fs.FSDataInputStream;

import javax.imageio.stream.ImageInputStreamImpl;
import java.io.IOException;


/**
 * An {@link javax.imageio.stream.ImageInputStream} that can read from a Hadoop {@link FSImageInputStream}.
 *
 * @author Marco Zuehlke
 * @since 0.1
 */
public class FSImageInputStream extends ImageInputStreamImpl {

    private FSDataInputStream fsInStream;
    private final long length;
    private final String path;

    public FSImageInputStream(FSDataInputStream fsInStream, long length, String path) {
        this.fsInStream = fsInStream;
        this.length = length;
        this.path = path;
    }

    public int read() throws IOException {
        checkClosed();
        bitOffset = 0;
        int val = fsInStream.read();
        if (val != -1) {
            ++streamPos;
        }
        return val;
    }

    public int read(byte[] b, int off, int len) throws IOException {
        checkClosed();
        bitOffset = 0;
        int nbytes = fsInStream.read(b, off, len);
        if (nbytes != -1) {
            streamPos += nbytes;
        }
        return nbytes;
    }

    public long length() {
        return length;
    }

    public void seek(long pos) throws IOException {
        checkClosed();
        if (pos < flushedPos) {
            throw new IndexOutOfBoundsException("pos < flushedPos!");
        }
        bitOffset = 0;
        fsInStream.seek(pos);
        streamPos = fsInStream.getPos();
    }

    public void close() throws IOException {
        try {
            super.close();
            fsInStream.close();
        } finally {
            fsInStream = null;
        }
    }

    @Override
    public String toString() {
        return String.format("FSImageInputStream{path='%s'} ", path);
    }
}
