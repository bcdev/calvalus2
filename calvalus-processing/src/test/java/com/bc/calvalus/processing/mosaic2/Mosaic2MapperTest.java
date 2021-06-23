package com.bc.calvalus.processing.mosaic2;

import com.bc.calvalus.processing.mosaic.TileIndexWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.security.Credentials;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

import static org.junit.Assert.*;

/**
 * TODO add API doc
 *
 * @author Martin Boettcher
 */
public class Mosaic2MapperTest extends Mosaic2Mapper {

    private Configuration conf;
    private Context context;
    private Mosaic2Mapper.SpatialBinMicroTileEmitter spatialBinMicroTileEmitter;

    @Test
    public void testMicroTileEmitterParameters() throws Exception {
        assertEquals(3240, spatialBinMicroTileEmitter.numRowsGlobal);
        assertEquals(18, spatialBinMicroTileEmitter.microTileWidth);
        assertEquals(18, spatialBinMicroTileEmitter.microTileHeight);
        assertEquals(360, spatialBinMicroTileEmitter.microTileCols);
        assertEquals(180, spatialBinMicroTileEmitter.microTileRows);
        assertEquals(180, spatialBinMicroTileEmitter.macroTileWidth);
        assertEquals(180, spatialBinMicroTileEmitter.macroTileHeight);
        assertEquals(0, spatialBinMicroTileEmitter.numBinsTotal);
        assertEquals(0, spatialBinMicroTileEmitter.numObsTotal);
        int mtp = spatialBinMicroTileEmitter.microTilePosition(1080L * spatialBinMicroTileEmitter.numRowsGlobal * 2 + 3245L);
        assertEquals(5, mtp);
        long mtr = spatialBinMicroTileEmitter.microTileRow(1080L/spatialBinMicroTileEmitter.microTileHeight * spatialBinMicroTileEmitter.microTileCols + 3245L/spatialBinMicroTileEmitter.microTileWidth);
        assertEquals(60, mtr);
        long mti = spatialBinMicroTileEmitter.microTileIndex(1080L * spatialBinMicroTileEmitter.numRowsGlobal * 2 + 3245L);
        assertEquals(360*60+180, mti);
        TileIndexWritable tiw = spatialBinMicroTileEmitter.tileIndex(1080L/spatialBinMicroTileEmitter.microTileHeight * spatialBinMicroTileEmitter.microTileCols + 3245L/spatialBinMicroTileEmitter.microTileWidth);
        assertEquals(18, tiw.getMacroTileX());
        assertEquals(6, tiw.getMacroTileY());
        assertEquals(180, tiw.getTileX());
        assertEquals(60, tiw.getTileY());
    }

    @Before
    public void setUp() throws Exception {
        final int numRowsGlobal = 3240;
        conf = new Configuration();
        conf.set("tileSize", "180");
        conf.set("microTileSize", "18");
        context = new Context() {
            @Override
            public void progress() {

            }

            @Override
            public Configuration getConfiguration() {
                return conf;
            }

            @Override
            public Credentials getCredentials() {
                return null;
            }

            @Override
            public JobID getJobID() {
                return null;
            }

            @Override
            public int getNumReduceTasks() {
                return 0;
            }

            @Override
            public Path getWorkingDirectory() throws IOException {
                return null;
            }

            @Override
            public Class<?> getOutputKeyClass() {
                return null;
            }

            @Override
            public Class<?> getOutputValueClass() {
                return null;
            }

            @Override
            public Class<?> getMapOutputKeyClass() {
                return null;
            }

            @Override
            public Class<?> getMapOutputValueClass() {
                return null;
            }

            @Override
            public String getJobName() {
                return null;
            }

            @Override
            public Class<? extends InputFormat<?, ?>> getInputFormatClass() throws ClassNotFoundException {
                return null;
            }

            @Override
            public Class<? extends Mapper<?, ?, ?, ?>> getMapperClass() throws ClassNotFoundException {
                return null;
            }

            @Override
            public Class<? extends Reducer<?, ?, ?, ?>> getCombinerClass() throws ClassNotFoundException {
                return null;
            }

            @Override
            public Class<? extends Reducer<?, ?, ?, ?>> getReducerClass() throws ClassNotFoundException {
                return null;
            }

            @Override
            public Class<? extends OutputFormat<?, ?>> getOutputFormatClass() throws ClassNotFoundException {
                return null;
            }

            @Override
            public Class<? extends Partitioner<?, ?>> getPartitionerClass() throws ClassNotFoundException {
                return null;
            }

            @Override
            public RawComparator<?> getSortComparator() {
                return null;
            }

            @Override
            public String getJar() {
                return null;
            }

            @Override
            public RawComparator<?> getCombinerKeyGroupingComparator() {
                return null;
            }

            @Override
            public RawComparator<?> getGroupingComparator() {
                return null;
            }

            @Override
            public boolean getJobSetupCleanupNeeded() {
                return false;
            }

            @Override
            public boolean getTaskCleanupNeeded() {
                return false;
            }

            @Override
            public boolean getProfileEnabled() {
                return false;
            }

            @Override
            public String getProfileParams() {
                return null;
            }

            @Override
            public Configuration.IntegerRanges getProfileTaskRange(boolean isMap) {
                return null;
            }

            @Override
            public String getUser() {
                return null;
            }

            @Override
            public boolean getSymlink() {
                return false;
            }

            @Override
            public Path[] getArchiveClassPaths() {
                return new Path[0];
            }

            @Override
            public URI[] getCacheArchives() throws IOException {
                return new URI[0];
            }

            @Override
            public URI[] getCacheFiles() throws IOException {
                return new URI[0];
            }

            @Override
            public Path[] getLocalCacheArchives() throws IOException {
                return new Path[0];
            }

            @Override
            public Path[] getLocalCacheFiles() throws IOException {
                return new Path[0];
            }

            @Override
            public Path[] getFileClassPaths() {
                return new Path[0];
            }

            @Override
            public String[] getArchiveTimestamps() {
                return new String[0];
            }

            @Override
            public String[] getFileTimestamps() {
                return new String[0];
            }

            @Override
            public int getMaxMapAttempts() {
                return 0;
            }

            @Override
            public int getMaxReduceAttempts() {
                return 0;
            }

            @Override
            public TaskAttemptID getTaskAttemptID() {
                return null;
            }

            @Override
            public void setStatus(String msg) {

            }

            @Override
            public String getStatus() {
                return null;
            }

            @Override
            public float getProgress() {
                return 0;
            }

            @Override
            public Counter getCounter(Enum<?> counterName) {
                return null;
            }

            @Override
            public Counter getCounter(String groupName, String counterName) {
                return null;
            }

            @Override
            public boolean nextKeyValue() throws IOException, InterruptedException {
                return false;
            }

            @Override
            public NullWritable getCurrentKey() throws IOException, InterruptedException {
                return null;
            }

            @Override
            public NullWritable getCurrentValue() throws IOException, InterruptedException {
                return null;
            }

            @Override
            public void write(TileIndexWritable key, L3SpatialBinMicroTileWritable value) throws IOException, InterruptedException {

            }

            @Override
            public OutputCommitter getOutputCommitter() {
                return null;
            }

            @Override
            public InputSplit getInputSplit() {
                return null;
            }
        };
        spatialBinMicroTileEmitter = new Mosaic2Mapper.SpatialBinMicroTileEmitter(context, numRowsGlobal);
    }
}