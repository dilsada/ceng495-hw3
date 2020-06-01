import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

public class Twolist {

    public static class TwolistMapper
            extends Mapper<Object, Text, Text, IntWritable> {
        
        private Text student = new Text();
        private Text course = new Text();
        private Text grade = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                student.set(itr.nextToken());
                course.set(itr.nextToken());
                grade.set(itr.nextToken());

                IntWritable newGrade = new IntWritable(Integer.parseInt(grade.toString()));
                context.write(course, newGrade);
            }
        }
    }

    public static class TwolistPartitioner extends Partitioner<Text,IntWritable> {
        public int getPartition(Text key, IntWritable value, int numReduceTasks){
            Integer grade = value.get();
            
            if(grade >= 60)
                return 0;
            else
                return 1;

        }
    }

    public static class TwolistReducer
            extends Reducer<Text,IntWritable,Text,DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            int size = 0;
            for (IntWritable val : values) {
                size +=1;
                sum += val.get();
            }

            double avg = ((double)sum)/((double)size);
            System.out.println(key.toString() + " : " + avg);
            
            result.set(avg);
            context.write(key, result);
        }
    }
}