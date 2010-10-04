package com.bc.calvalus.experiments.format.seq;

import org.apache.hadoop.io.Writable;

interface ByteArray extends Writable {
    byte[] getBytes();
    int getLength();
}
