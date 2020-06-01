import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Average {

    public static class AverageMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        //private final static IntWritable one = new IntWritable(1);
        
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
                context.write(student, newGrade);
            }
        }
    }

    public static class AverageReducer
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