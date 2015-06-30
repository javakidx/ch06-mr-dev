package idv.jk.hadoop.ed4.ch06.v4;

import idv.jk.hadoop.ed4.ch06.v4.NcdcRecordParser;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by javakid on 6/30/15.
 */
public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
    enum Temperature
    {
        MALFORMED
    }

    private NcdcRecordParser parser = new NcdcRecordParser();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        parser.parse(value);

        if(parser.isValidTemperature())
        {
            context.write(new Text(parser.getYear()), new IntWritable(parser.getAirTemperature()));
        }
        else if(parser.isMalformedTemperature())
        {
            System.err.println("Ignoring possibly corrupt input: " + value);
            context.getCounter(Temperature.MALFORMED).increment(1);
        }
    }
}
