package com.bc.calvalus.portal.server;

import java.io.OutputStream;
import java.io.PrintStream;

/**
 * @author Hans.
 */
public class CountablePrintStream extends PrintStream {

    private long count = 0;

    public CountablePrintStream(OutputStream out) {
        super(out);
    }

    @Override
    public void write(byte[] buf, int off, int len) {
        count += len;
        super.write(buf, off, len);
    }

    @Override
    public void print(String s) {
        count += s.length();
        super.print(s);
    }

    @Override
    public void println(String x) {
        count += x.length();
        super.println(x);
    }

    public long getCount() {
        return count;
    }
}
