import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Hw3 {

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Hw3");
    job.setJarByClass(Hw3.class);

    String task = args[0];

    if(task.equals("cap")){
        job.setMapperClass(Capacity.CapacityMapper.class);
        job.setCombinerClass(Capacity.CapacityReducer.class);
        job.setReducerClass(Capacity.CapacityReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
    }
    else if(task.equals("pass")){
        job.setMapperClass(Pass.PassMapper.class);
        job.setCombinerClass(Pass.PassReducer.class);
        job.setReducerClass(Pass.PassReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
    }
    else if(task.equals("avg")){
        job.setMapperClass(Average.AverageMapper.class);
        job.setReducerClass(Average.AverageReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
    }
    
    else if(task.equals("twolist")){
        job.setNumReduceTasks(2);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Twolist.TwolistMapper.class);
        job.setReducerClass(Twolist.TwolistReducer.class);
        job.setPartitionerClass(Twolist.TwolistPartitioner.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

    }

    FileInputFormat.addInputPath(job, new Path(args[1]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  
  }
}
