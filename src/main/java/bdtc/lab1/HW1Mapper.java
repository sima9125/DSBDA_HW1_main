package bdtc.lab1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper: составляет ключ (metricName,timestamp,scale), значение (value)
 */

public class HW1Mapper extends Mapper<LongWritable, Text, Text, IntWritable>
{

    private Text word = new Text();
    private long timeRangeAggregationMetric;
    private boolean testFlag = false; // Введен для запуска юнит-тестов, потому что иначе он падает на строчке this.timeRangeAggregationMetric = Long.parseLong(conf.get("timeRangeAggregationMetric"));

    // конструктор добавлен, чтобы не сломать старую логику
    public HW1Mapper()
    {
        super();
    }

    // конструктор для юнит-тестов, чтобы проинициализировать параметр timeRangeAggregationMetric (диапазон времени агрегации сырых метрик)
    public HW1Mapper(long timeRangeAggregationMetric)
    {
        super();
        this.testFlag = true;
        this.timeRangeAggregationMetric = timeRangeAggregationMetric;
    }

    // справочник обозначений: metricId-metricName
    private static final String[] valuesKeys = {"Node1CPU", "Node2CPU", "Node3CPU", "Node4CPU", "Node12HDD", "Node34HDD", "Node1Cache", "Node2Cache", "Node3Cache", "Node4Cache"};

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] strings = line.split(",");
        Configuration conf = context.getConfiguration();

        // для юнит-тестов данная строчка не будет выполняться
        if(!testFlag)
            this.timeRangeAggregationMetric = Long.parseLong(conf.get("timeRangeAggregationMetric"));
        if(strings.length==3)
        {
            if (!strings[0].isEmpty() && !strings[1].isEmpty() && !strings[2].isEmpty())
            {
                try
                {
                    // вычисление начального времени агрегирования
                    // диапазон времени агрегации сырых метрик переводится с сек на мс
                    long startTime = Long.parseLong(strings[1]) - Long.parseLong(strings[1]) % (this.timeRangeAggregationMetric * 1000);

                    int metricId = Integer.parseInt(strings[0]) - 1;

                    // ключ будет составлять из:
                    // -названия метрики (которое берется из массива по номеру из файла). Есть обработка выхода за пределы массива
                    // -начального времени агрегирования
                    // -диапазон времени агрегации сырых метрик
                    word.set(String.join(",", metricId < valuesKeys.length && metricId >= 0 ? valuesKeys[metricId] : "other", String.valueOf(startTime), String.valueOf(this.timeRangeAggregationMetric) + "s"));

                    context.write(word, new IntWritable(Integer.parseInt(strings[2])));
                }
                catch (IOException e)
                {
                    context.getCounter(CounterType.MALFORMED).increment(1);
                }
            }
            else{
                context.getCounter(CounterType.MALFORMED).increment(1);
            }
        }
        else{
            context.getCounter(CounterType.MALFORMED).increment(1);
        }
    }
}
