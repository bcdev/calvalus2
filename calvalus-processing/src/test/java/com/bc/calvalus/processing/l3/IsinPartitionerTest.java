package com.bc.calvalus.processing.l3;

import com.sun.tracing.dtrace.ProviderAttributes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class IsinPartitionerTest {

    private IsinPartitioner isinPartitioner;

    @Before
    public void setUp(){
        isinPartitioner = new IsinPartitioner();
    }

    @Test
    public void test_3_partitions() {
        final int numPartitions = 3;

        // (1, 1) in tile (0, 0)
        assertEquals(0, isinPartitioner.getPartition(new LongWritable(10001L), null, numPartitions));
        // (2, 2) in tile (1, 0)
        assertEquals(1, isinPartitioner.getPartition(new LongWritable(100020002L), null, numPartitions));
        // (3, 3) in tile (2, 0)
        assertEquals(2, isinPartitioner.getPartition(new LongWritable(200030003L), null, numPartitions));
        // (4, 4) in tile (3, 0)
        assertEquals(0, isinPartitioner.getPartition(new LongWritable(300040004L), null, numPartitions));

        // (5, 5) in tile (4, 16)
        assertEquals(1, isinPartitioner.getPartition(new LongWritable(160400050005L), null, numPartitions));
        // (6, 6) in tile (5, 16)
        assertEquals(2, isinPartitioner.getPartition(new LongWritable(160500060006L), null, numPartitions));
        // (7, 7) in tile (6, 16)
        assertEquals(0, isinPartitioner.getPartition(new LongWritable(160600070007L), null, numPartitions));
    }

    @Test
    public void test_4_partitions() {
        final int numPartitions = 4;

        // (8, 8) in tile (0, 0)
        assertEquals(0, isinPartitioner.getPartition(new LongWritable(80008L), null, numPartitions));
        // (9, 9) in tile (1, 0)
        assertEquals(1, isinPartitioner.getPartition(new LongWritable(100090009L), null, numPartitions));
        // (10, 10) in tile (2, 0)
        assertEquals(2, isinPartitioner.getPartition(new LongWritable(200100010L), null, numPartitions));
        // (11, 11) in tile (3, 0)
        assertEquals(3, isinPartitioner.getPartition(new LongWritable(300110011L), null, numPartitions));
        // (12, 12) in tile (4, 0)
        assertEquals(0, isinPartitioner.getPartition(new LongWritable(400120012L), null, numPartitions));

        // (13, 13) in tile (5, 17)
        assertEquals(1, isinPartitioner.getPartition(new LongWritable(170500130013L), null, numPartitions));
        // (14, 14) in tile (6, 17)
        assertEquals(2, isinPartitioner.getPartition(new LongWritable(170600140014L), null, numPartitions));
        // (15, 15) in tile (7, 17)
        assertEquals(3, isinPartitioner.getPartition(new LongWritable(170700150015L), null, numPartitions));
        // (16, 16) in tile (8, 17)
        assertEquals(0, isinPartitioner.getPartition(new LongWritable(170800160016L), null, numPartitions));
    }

    @Test
    public void testSetGetConf() {
        final Configuration configuration = new Configuration();

        isinPartitioner.setConf(configuration);
        assertSame(configuration, isinPartitioner.getConf());
    }
}
