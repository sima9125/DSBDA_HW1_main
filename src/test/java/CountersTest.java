
import bdtc.lab1.CounterType;
import bdtc.lab1.HW1Mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/* test for counters in mapreduce*/
public class CountersTest {

    private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;

    private final String testMalformed = "mama mila ramu";
    private final String testMap = "2,1616188268301,136";
    private final String testRed = "Node2CPU,1616188260000,10s";
    private final int scale = 10; // диапозон времени агрегации сырых метрик

    @Before
    public void setUp() {
        HW1Mapper mapper = new HW1Mapper(scale);
        mapDriver = MapDriver.newMapDriver(mapper);
    }
    /* 1 bad*/
    @Test
    public void testMapperCounterOne() throws IOException  {
        mapDriver
                .withInput(new LongWritable(), new Text(testMalformed))
                .runTest();
        assertEquals("Expected 1 counter increment", 1, mapDriver.getCounters()
                .findCounter(CounterType.MALFORMED).getValue());
    }
    /*1 good*/
    @Test
    public void testMapperCounterZero() throws IOException {
        mapDriver
                .withInput(new LongWritable(), new Text(testMap))
                .withOutput(new Text(testRed), new IntWritable(136))
                .runTest();
        assertEquals("Expected 1 counter increment", 0, mapDriver.getCounters()
                .findCounter(CounterType.MALFORMED).getValue());
    }
    /*2 bad, 1 good*/
    @Test
    public void testMapperCounters() throws IOException {
        mapDriver
                .withInput(new LongWritable(), new Text(testMap))
                .withInput(new LongWritable(), new Text(testMalformed))
                .withInput(new LongWritable(), new Text(testMalformed))
                .withOutput(new Text(testRed), new IntWritable(136))
                .runTest();

        assertEquals("Expected 2 counter increment", 2, mapDriver.getCounters()
                .findCounter(CounterType.MALFORMED).getValue());
    }
}

