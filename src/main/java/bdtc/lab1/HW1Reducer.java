package bdtc.lab1;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Редьюсер: суммирует все значения метрики полученные от {@link HW1Mapper}, делит на количество полученных метрик по ключу и выдаёт среднее зачение по метрикам
 */

public class HW1Reducer extends Reducer<Text, IntWritable, Text, FloatWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        int count = 0;
        while (values.iterator().hasNext()) {
            sum += values.iterator().next().get();
            count++;
        }
        context.write(key, new FloatWritable((float)sum/count));
    }
}