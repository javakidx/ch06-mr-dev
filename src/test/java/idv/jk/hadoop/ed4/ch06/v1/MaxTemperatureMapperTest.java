package idv.jk.hadoop.ed4.ch06.v1;

//import idv.jk.hadoop.ed3.ch02.oldapi.*;
//import idv.jk.hadoop.ed3.ch02.oldapi.MaxTemperatureMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by javakid on 6/25/15.
 */
public class MaxTemperatureMapperTest
{
    @Test
    public void processValidRecord() throws IOException, InterruptedException
    {
        Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" +
                                      // Year ^^^^
                "99999V0203201N00261220001CN9999999N9-00111+99999999999");
                                       // Temperature ^^^^^
        new MapDriver<LongWritable, Text, Text, IntWritable>()
                .withMapper(new MaxTemperatureMapper())
                .withInput(new LongWritable(0), value)
                .withOutput(new Text("1950"), new IntWritable(-11))
                .runTest();
    }

    @Test
    public void ignoresMissingTemperatureRecord() throws IOException, InterruptedException
    {
        Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" +
                                      // Year ^^^^
                "99999V0203201N00261220001CN9999999N9+99991+99999999999");
                                       // Temperature ^^^^^

        new MapDriver<LongWritable, Text, Text, IntWritable>()
                .withMapper(new MaxTemperatureMapper())
                .withInput(new LongWritable(0), value)
                .runTest();
    }
}
