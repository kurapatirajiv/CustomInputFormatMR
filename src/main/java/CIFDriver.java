import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by Rajiv on 4/7/17.
 */
public class CIFDriver {

    public static void main(String args[]) throws IOException,InterruptedException, ClassNotFoundException{

        if(args.length!=2){
            System.out.println("Not enough arguments");
            System.exit(-1);
        }

        Configuration conf = new Configuration();

        // A new map is triggered for each two lines
        conf.setInt("mapred.line.input.format.linespermap", 2);

        Job job = new Job(conf);

        job.setJarByClass(CIFDriver.class);
        job.setJobName("TextPair Count Application");

        job.setMapperClass(CIFMapper.class);
        job.setReducerClass(CIFReducer.class);

        // Define Mapper output classes
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        // Specify key / value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        job.setInputFormatClass(NLineInputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(new Path(args[1])))
            hdfs.delete(new Path(args[1]), true);


        System.exit(job.waitForCompletion(true) ? 0 : 1);


    }


}
