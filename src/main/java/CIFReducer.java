import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Rajiv on 4/7/17.
 */
public class CIFReducer extends Reducer<Text, NullWritable, Text, IntWritable> {


    public int lnNumber = 1;

    public void reduce(Text key, Iterable<NullWritable> value, Context context) throws IOException, InterruptedException {
        context.write(key, new IntWritable(lnNumber));
        lnNumber++;
    }


}
