import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.time.Month;

public class MeanTemppMonthpState {
    public static class Map1 extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            Text stateAndMonth = new Text();
            Text avgTemp = new Text();
            String line = value.toString();
            line = line.replaceAll("\\s+", ",");
            String[] part = line.split(",");
// Loading each part of the output lines into variables
            String date = part[0];
            String temperature = part[1];
            String readings = part[2];

            String state = part[3];
            String station_id = part[4];
            String year = date.substring(0, 4);
            String month = date.substring(4, 6);
            int monthInt = Integer.parseInt(month);
//            To display the name of the month
            String date_with_month = year + "_" + Month.of(monthInt);
            stateAndMonth.set(state + date_with_month);

            float temp = Float.parseFloat(temperature);
            float numberOfReadings = Float.parseFloat(readings);
            float product = temp * numberOfReadings;
//            Setting Precision
            String values = String.format("%.2f,%.2f", product, numberOfReadings);
            avgTemp.set(values);
            context.write(stateAndMonth, avgTemp);
        }
    }

    public static class Red1 extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            float total_temp = 0;
            float total_read = 0;
            for (Text t : values) {
                String tmp = t.toString();
                String[] product_with_readings = tmp.split(",");
                float product_temp = Float.parseFloat(product_with_readings[0]);
                float n_readings = Float.parseFloat(product_with_readings[1]);
                total_read += n_readings;
                total_temp += product_temp;
            }
            total_temp = total_temp / total_read;
            String total_temp_sum_value = String.format("%.2f", total_temp);
            context.write(key, new Text(total_temp_sum_value));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Calculating_MeanTemp_perMonth_perState");
        job.setJarByClass(MeanTemppMonthpState.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Map1.class);
        job.setReducerClass(Red1.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        Path out = new Path(args[1]);
        out.getFileSystem(conf).delete(out);
        job.waitForCompletion(true);
    }
}

