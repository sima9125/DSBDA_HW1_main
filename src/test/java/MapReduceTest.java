
import bdtc.lab1.HW1Mapper;
import bdtc.lab1.HW1Reducer;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class MapReduceTest {

    private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
    private ReduceDriver<Text, IntWritable, Text, FloatWritable> reduceDriver;
    private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, FloatWritable> mapReduceDriver;

    private final String testMap = "2,1616188268301,136";
    private final String testMap2 = "2,1616188268301,14";
    private final String testRed = "Node2CPU,1616188260000,10s";
    private final int scale = 10; // диапозон времени агрегации сырых метрик

    @Before
    public void setUp()
    {
        HW1Mapper mapper = new HW1Mapper(scale);
        HW1Reducer reducer = new HW1Reducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver
                .withInput(new LongWritable(), new Text(testMap))
                .withOutput(new Text(testRed), new IntWritable(136))
                .runTest();
    }

    @Test
    public void testReducer() throws IOException {
        List<IntWritable> values = new ArrayList<IntWritable>();
        values.add(new IntWritable(40));
        values.add(new IntWritable(60));
        reduceDriver
                .withInput(new Text(testRed), values)
                .withOutput(new Text(testRed), new FloatWritable(50.0f))
                .runTest();
    }

    @Test
    public void testMapReduce() throws IOException {
        mapReduceDriver
                .withInput(new LongWritable(), new Text(testMap))
                .withInput(new LongWritable(), new Text(testMap2))
                .withOutput(new Text(testRed), new FloatWritable(75.0f))
                .runTest();
    }
}