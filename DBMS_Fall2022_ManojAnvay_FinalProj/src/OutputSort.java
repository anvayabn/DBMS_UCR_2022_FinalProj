import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import java.io.File;
import org.apache.hadoop.fs.FileSystem;

public class OutputSort{
    public static class Map1 extends Mapper<Object, Text, FloatWritable, Text> {

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException
        {
            FloatWritable difference = new FloatWritable();
            String line = value.toString();
            line = line.replaceAll("\\s+", ",");
            String[] part = line.split(",");

            float diff = Float.parseFloat(part[5]);
            difference.set(diff);
            context.write(difference, value);
        }
    }

    public static class Red1 extends Reducer<FloatWritable, Text, Text, NullWritable> {
        public void reduce(FloatWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException
        {
            for (Text t : values) {
                context.write(t, NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        Path inputPath = new Path(args[0]);
        Path partitionFile = new Path(args[1] + "_stage1.lst");
        Path outputStage = new Path(args[1] + "_stage2");
        Path outputOrder = new Path(args[1]);

        double sampling_rate = 0.001;

        FileSystem.get(new Configuration()).delete(partitionFile, true);
        FileSystem.get(new Configuration()).delete(outputStage, true);
        FileSystem.get(new Configuration()).delete(outputOrder, true);

        Job sampleJob = new Job(conf, "Staging");
        sampleJob.setJarByClass(OutputSort.class);

        sampleJob.setMapperClass(Map1.class);
        sampleJob.setNumReduceTasks(0);

        sampleJob.setOutputKeyClass(FloatWritable.class);
        sampleJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(sampleJob, inputPath);

        sampleJob.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(sampleJob, outputStage);

        int code = sampleJob.waitForCompletion(true) ? 0 : 1;
        if(code == 0){
            Job orderJob = new Job(conf, "Sorting the Output");
            orderJob.setJarByClass(OutputSort.class);
            orderJob.setMapperClass(Mapper.class);
            orderJob.setReducerClass(Red1.class);
            orderJob.setNumReduceTasks(1);
            orderJob.setPartitionerClass(TotalOrderPartitioner.class);
            TotalOrderPartitioner.setPartitionFile(orderJob.getConfiguration(), partitionFile);

            orderJob.setOutputKeyClass(FloatWritable.class);
            orderJob.setOutputValueClass(Text.class);
            orderJob.setInputFormatClass(SequenceFileInputFormat.class);
            SequenceFileInputFormat.setInputPaths(orderJob, outputStage);

            TextOutputFormat.setOutputPath(orderJob, outputOrder);
            orderJob.getConfiguration().set("mapreduce.textoutputformat.separator", "");
            InputSampler.writePartitionFile(orderJob, new InputSampler.RandomSampler(.001, 10000, 1));
            code = orderJob.waitForCompletion(true) ? 0 : 2;
        }
        FileSystem.get(new Configuration()).delete(partitionFile, false);
        FileSystem.get(new Configuration()).delete(outputStage, true);
        System.exit(code);
    }
}
