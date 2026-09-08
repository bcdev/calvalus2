package com.bc.calvalus.processing.mosaic2;

import com.bc.calvalus.processing.hadoop.ParameterizedSplit;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.counters.GenericCounter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.security.Credentials;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * TODO add API doc
 *
 * @author Martin Boettcher
 */
public class CubeMapperTest extends TestCase {

    Map<CubeIndexWritable, CubeChunkWritable> mrData = new HashMap<>();

    public class MapperProxy extends CubeMapper {
        Configuration conf;
        public MapperProxy(Configuration conf) {
            this.conf = conf;
            conf.set("calvalus.l3.parameters", "<parameters><aggregators><aggregator><type>TOptCube</type><varNames>CHL,CHL_algo,Turbidity,Turbidity_algo,SDD,adg,TIME</varNames><timeAxisLength>30</timeAxisLength><yAxisLength>160</yAxisLength><xAxisLength>200</xAxisLength><chunkSizeX>64</chunkSizeX><chunkSizeY>64</chunkSizeY><chunkSizeT>6</chunkSizeT><encoding>littleendian</encoding></aggregator></aggregators></parameters>");
            conf.set("calvalus.output.dir", "test.zarr");
        }
        public class CubeMapperContext extends Context {

            @Override
            public InputSplit getInputSplit() {
                return new ParameterizedSplit(
                        new Path(this.getClass().getClassLoader().getResource("eodata/subset_0_of_20260621-P1D-L3-norge-300m-v0.nc").getPath()),
                        965216L,
                        new String[] { "localhost" },
                        new String[] { "timeIndex", "3" }
                );
            }

            @Override
            public void write(CubeIndexWritable cubeIndex, CubeChunkWritable cubeChunk) throws IOException, InterruptedException {
                System.out.println("writing " + cubeIndex);
                mrData.put(cubeIndex, cubeChunk);
            }

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

            Counter counter = new GenericCounter("mycounter", "mycounter1");

            @Override
            public Counter getCounter(Enum<?> counterName) {
                return counter;
            }

            @Override
            public Counter getCounter(String groupName, String counterName) {
                return counter;
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
            public OutputCommitter getOutputCommitter() {
                return null;
            }
        }
    }

    public class ReducerProxy extends CubeReducer {
        Configuration conf;
        public ReducerProxy(Configuration conf) {
            this.conf = conf;
            conf.set("calvalus.l3.parameters", "<parameters><aggregators><aggregator><type>TOptCube</type><varNames>CHL,CHL_algo,Turbidity,Turbidity_algo,SDD,adg,TIME</varNames><timeAxisLength>30</timeAxisLength><yAxisLength>160</yAxisLength><xAxisLength>200</xAxisLength><chunkSizeX>64</chunkSizeX><chunkSizeY>64</chunkSizeY><chunkSizeT>6</chunkSizeT><encoding>littleendian</encoding></aggregator></aggregators></parameters>");
            conf.set("calvalus.output.dir", "test.zarr");
        }
        public class CubeReducerContext extends Context {

            List<CubeIndexWritable> sequence = null;
            int cursor = -1;

            private void initialise() {
                sequence = mrData.keySet().stream().sorted().collect(Collectors.toList());
            }

            @Override
            public boolean nextKey() throws IOException, InterruptedException {
                if (sequence == null) {
                    initialise();
                }
                return ++cursor < sequence.size();
            }

            @Override
            public CubeIndexWritable getCurrentKey() throws IOException, InterruptedException {
                return sequence.get(cursor);
            }

            @Override
            public CubeChunkWritable getCurrentValue() throws IOException, InterruptedException {
                return mrData.get(getCurrentKey());
            }

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

            Counter counter = new GenericCounter("mycounter", "mycounter1");

            @Override
            public Counter getCounter(Enum<?> counterName) {
                return counter;
            }

            @Override
            public Counter getCounter(String groupName, String counterName) {
                return counter;
            }

            @Override
            public boolean nextKeyValue() throws IOException, InterruptedException {
                return false;
            }

            @Override
            public void write(NullWritable nullWritable, NullWritable nullWritable2) throws IOException, InterruptedException {

            }

            @Override
            public OutputCommitter getOutputCommitter() {
                return null;
            }

            @Override
            public Iterable<CubeChunkWritable> getValues() throws IOException, InterruptedException {
                return null;
            }
        }
    }


    public void testRun() throws IOException, InterruptedException {
        Configuration conf = new Configuration();
        MapperProxy mapper = new MapperProxy(conf);
        MapperProxy.CubeMapperContext context = mapper.new CubeMapperContext();
        mapper.run(context);

        ReducerProxy reducer = new ReducerProxy(conf);
        ReducerProxy.CubeReducerContext context2 = reducer.new CubeReducerContext();
        reducer.run(context2);
    }
}