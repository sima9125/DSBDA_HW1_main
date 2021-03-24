package bdtc.lab1;

import lombok.extern.log4j.Log4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

@Log4j
public class MapReduceApplication {

    //public static int timeRangeAggregationMetric;

    /**
     * @param args должны прийти три аргумента:
     *             1) имя обрабатываемой папки
     *             2) имя выходной папки
     *             3) диапазон времени, за который будут агрегироваться сырые метрики, в секундах c типом int
     */

    public static void main(String[] args) throws Exception {

        if (args.length < 3) {
            throw new RuntimeException("Необходимо задать входной и выходной файл, диапазон времени агрегации сырых метрик в секундах!");
        }
        Configuration conf = new Configuration();
        // задаём выходной файл, разделенный запятыми - формат CSV в соответствии с заданием
        conf.set("mapreduce.output.textoutputformat.separator", ",");
        conf.set("timeRangeAggregationMetric", args[2]);

        Job job = Job.getInstance(conf, "avg metric");
        job.setJarByClass(MapReduceApplication.class);
        job.setMapperClass(HW1Mapper.class);
        job.setReducerClass(HW1Reducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        // указываем, что выходной файл должен быть в формате SequenceFile со Snappy сжатием
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
        SequenceFileOutputFormat.setCompressOutput(job, true);
        SequenceFileOutputFormat.setOutputCompressorClass(job, org.apache.hadoop.io.compress.SnappyCodec.class);

        Path outputDirectory = new Path(args[1]);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        //timeRangeAggregationMetric = Integer.parseInt(args[2]);
        //log.info(timeRangeAggregationMetric);

        FileOutputFormat.setOutputPath(job, outputDirectory);
        log.info("=====================JOB STARTED=====================");
        job.waitForCompletion(true);
        log.info("=====================JOB ENDED=====================");
        // проверяем статистику по счётчикам
        Counter counter = job.getCounters().findCounter(CounterType.MALFORMED);
        log.info("=====================COUNTERS " + counter.getName() + ": " + counter.getValue() + "=====================");
    }
}
