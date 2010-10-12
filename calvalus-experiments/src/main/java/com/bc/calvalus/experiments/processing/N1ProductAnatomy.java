package com.bc.calvalus.experiments.processing;

import com.bc.calvalus.hadoop.io.FSImageInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.esa.beam.dataio.envisat.DSD;
import org.esa.beam.dataio.envisat.ProductFile;
import org.esa.beam.dataio.envisat.RecordReader;
import javax.imageio.stream.ImageInputStream;
import java.io.IOException;

/**
 * Determines header and granule (record) sizes of line-interleaved N1 products by reading
 * the DSDs of the product file.
 */
public class N1ProductAnatomy {

    private final long headerSize;
    private final long granuleSize;

    public N1ProductAnatomy(Path path, JobContext job) throws IOException {
        ProductFile productFile = null;
        FSDataInputStream fileIn = null;
        try {
            FileSystem fs = path.getFileSystem(job.getConfiguration());
            fileIn = fs.open(path);
            final FileStatus status = fs.getFileStatus(path);
            ImageInputStream imageInputStream = new FSImageInputStream(fileIn, status.getLen());
            productFile = ProductFile.open(imageInputStream);
            final RecordReader[] mdsRecordReaders = getMdsRecordReaders(productFile);
            headerSize = mdsRecordReaders[0].getDSD().getDatasetOffset();
            granuleSize = computeGranuleSize(mdsRecordReaders);
        } finally {
            productFile.close();
            fileIn.close();
        }
    }

    public long getHeaderSize() {
        return headerSize;
    }

    public long getGranuleSize() {
        return granuleSize;
    }

    public String toString() {
        return "anatomy(header=" + getHeaderSize() + ",granule=" + getGranuleSize() + ")";
    }

    static int computeGranuleSize(RecordReader[] mdsRecordReaders) {
        int granuleSize = 0;
        for (RecordReader recordReader : mdsRecordReaders) {
            DSD dsd = recordReader.getDSD();
            granuleSize += dsd.getRecordSize();
        }
        return granuleSize;
    }

    static RecordReader[] getMdsRecordReaders(ProductFile productFile) throws IOException {
        String[] mdsNames = productFile.getValidDatasetNames('M');
        RecordReader[] recordReaders = new RecordReader[mdsNames.length];
        for (int i = 0; i < mdsNames.length; i++) {
            RecordReader recordReader = productFile.getRecordReader(mdsNames[i]);
            recordReaders[i] = recordReader;
        }
        return recordReaders;
    }
}
