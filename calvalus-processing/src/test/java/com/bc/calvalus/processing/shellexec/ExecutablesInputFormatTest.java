package com.bc.calvalus.processing.shellexec;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import static org.junit.Assert.*;

// todo - ignore it for now (Martin to solve that)
@Ignore
public class ExecutablesInputFormatTest {

    //final static String INPUT_PATH = "hdfs://cvmaster00:9000/calvalus/eodata/MER_FRS_1P/r03/2009/01/01";
    private final static String INPUT_PATH = "/calvalus-processing/src/test/resources";
    private final static String REQUEST_PATH = "/calvalus-l2gen/bin/geochildgen-request.xml";
    private static final String CALVALUS_MARKER = "/calvalus";

//    @Test
    public void findFiles() throws Exception {
        Path path = new Path(getAbsPath(INPUT_PATH));
        FileSystem fs = path.getFileSystem(new Configuration());
        final FileStatus[] files = fs.listStatus(path);
        final Pattern filter = Pattern.compile(".*N1");
        List<FileSplit> accu = new ArrayList<FileSplit>();
        ExecutablesInputFormat.collectInputFiles(files, fs, filter, accu);
        assertTrue("files found", accu.size() > 0);
    }

//    @Test
    public void getSplits() throws Exception {
        final String requestContent = FileUtil.readFile(getAbsPath(REQUEST_PATH));
        final Job job = new Job(new Configuration(), REQUEST_PATH);
        job.getConfiguration().set("calvalus.request", requestContent);
        final List<FileSplit> splits = new ExecutablesInputFormat().getSplits(job);
        assertTrue("splits created", splits.size() > 0);
    }

    public void testNothing() {}

    private static String getAbsPath(String resourcePath) throws Exception {
        String path = ExecutablesInputFormatTest.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        int idx = path.lastIndexOf(CALVALUS_MARKER);
        String substring = path.substring(0, idx);
        return substring + CALVALUS_MARKER + resourcePath;
    }
}
