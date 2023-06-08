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

public class PiCalculation {

  public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{

    private static final IntWritable one = new IntWritable(1);
    private static final Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      line = line.replace("(","");
      line = line.replace(")","");
      line = line.replace(","," ");

      StringTokenizer itr = new StringTokenizer(line);
      int radius = 200;
      while (itr.hasMoreTokens()) {
        int xvalue = Integer.parseInt(itr.nextToken());
        int yvalue = (itr.hasMoreTokens()) ? Integer.parseInt(itr.nextToken()) : 0;

        double check = Math.sqrt(Math.pow((radius-xvalue), 2) + Math.pow((radius-yvalue), 2));

        word.set((check < radius) ? "inside" : "outside");
        context.write(word, one);
      }
    }
  }

  public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    private final IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "pi calculation");
    job.setJarByClass(PiCalculation.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.waitForCompletion(true);
    
    double inside = Double.valueOf(job.getCounters().findCounter("inside").getValue());
    double outside = Double.valueOf(job.getCounters().findCounter("outside").getValue());
    double pi = 4 * (inside / (inside + outside));

    System.out.println("PI: " + pi);
  }
}
