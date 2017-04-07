import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Rajiv on 4/7/17.
 */
public class CIFMapper extends Mapper<LongWritable, Text, Text, NullWritable> {


    public void setup(Mapper.Context context) throws IOException, InterruptedException {

        System.out.println("A new Mapper Triggered");
    }


    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        System.out.println(value);
        context.write(value, NullWritable.get());

    }


    public void cleanup(Mapper.Context context) throws IOException, InterruptedException {

        System.out.println("======================");

    }


}
