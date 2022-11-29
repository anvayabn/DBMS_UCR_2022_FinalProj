import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.DataInput;
import java.io.DataOutput;
import java.text.DecimalFormat;

public class MonthHiLo{
    public static class MinMaxTemperature implements Writable {
        private String minTemDate;
        private String maxTemDate;
        private float difference;


        DecimalFormat df = new DecimalFormat("0.00");



        public String getMinTemDate() {
            return minTemDate;
        }

        public void setMinTemDate(String mintempWithDate){
            this.minTemDate=mintempWithDate;
        }

        public String getMaxTemDate(){
            return maxTemDate;
        }

        public void setMaxTemDate(String maxtempWithDate){
            this.maxTemDate=maxtempWithDate;
        }

        public float getDifference() {
            return difference;
        }
        public void setDifference(float difference) {
            this.difference = Float.parseFloat(df.format(difference));
        }

        public void readFields(DataInput in) throws IOException {
            minTemDate=new String(in.readUTF());
            maxTemDate=new String(in.readUTF());
            difference=new Float(in.readFloat());
        }

        public void write(DataOutput out) throws IOException {
            out.writeUTF(minTemDate);
            out.writeUTF(maxTemDate);
            out.writeFloat(difference);
        }

        public String toString() {
            return minTemDate + "," + maxTemDate + "," + difference;
        }
    }

    public static class Map1 extends Mapper<Object, Text, Text, MinMaxTemperature> {
        private Text statekey = new Text();
        private String minTemDate;
        private String maxTemDate;
        private MinMaxTemperature outvalue = new MinMaxTemperature();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException
        {
            String line = value.toString();
            line = line.replaceAll("\\s+", ",");
            String[] part = line.split(",");

            String state = part[0].substring(0,2);
            String date = part[0].substring(7);
            String temperature = part[1];

            statekey.set(state);
            minTemDate = date + "," + temperature;
            maxTemDate = date + "," + temperature;
            outvalue.setMinTemDate(minTemDate);
            outvalue.setMaxTemDate(maxTemDate);
            outvalue.setDifference(0);
            context.write(statekey, outvalue);
        }
    }

    public static class Red1 extends Reducer<Text, MinMaxTemperature, Text, MinMaxTemperature> {
        private MinMaxTemperature result = new MinMaxTemperature();
        public void reduce(Text key, Iterable<MinMaxTemperature> values, Context context)
                throws IOException, InterruptedException
        {
            float minTemperature = 0;
            float maxTemperature = 0;
            float resultminTemperature = 0;
            float resultmaxTemperature = 0;
            String minlocaldate = "";
            String maxlocaldate = "";
            float finalresultminTemperature = 0;
            float finalresultmaxTemperature = 0;

            result.setMinTemDate("");
            result.setMaxTemDate("");
            result.setDifference(0);

            for (MinMaxTemperature t : values) {
                if(t.getMinTemDate().length() > 0){
                    String[] date_with_min_temp = t.getMinTemDate().split(",");
                    if(date_with_min_temp.length > 0)
                    {
                        minTemperature = Float.parseFloat(date_with_min_temp[1]);
                        minlocaldate = date_with_min_temp[0];
                    }
                    else
                    {
                        minTemperature = 0;
                    }
                }

                if (t.getMaxTemDate().length() > 0)
                {
                    String[] date_with_max_temp = t.getMaxTemDate().split(",");
                    if(date_with_max_temp.length > 0)
                    {
                        maxTemperature = Float.parseFloat(date_with_max_temp[1]);
                        maxlocaldate = date_with_max_temp[0];
                    }
                    else
                    {
                        maxTemperature = 0;
                    }
                }

                if(result.getMinTemDate().length() > 0){
                    String[] result_date_with_min_temp = result.getMinTemDate().split(",");
                    if(result_date_with_min_temp.length > 0)
                    {
                        resultminTemperature = Float.parseFloat(result_date_with_min_temp[1]);
                    }
                    else
                    {
                        resultminTemperature = 0;
                    }
                }

                if(result.getMaxTemDate().length() > 0){
                    String[] result_date_with_max_temp = result.getMaxTemDate().split(",");
                    if(result_date_with_max_temp.length > 0)
                    {
                        resultmaxTemperature = Float.parseFloat(result_date_with_max_temp[1]);
                    }
                    else
                    {
                        resultmaxTemperature = 0;
                    }
                }


                if (resultminTemperature==0 || minTemperature < resultminTemperature)
                {
                    result.setMinTemDate(minlocaldate + "," + minTemperature);
                }

                if (resultmaxTemperature==0 || maxTemperature > resultmaxTemperature)
                {
                    result.setMaxTemDate(maxlocaldate + "," + maxTemperature);
                }

                if(result.getMinTemDate().length() > 0){
                    String[] final_result_date_with_min_temp = result.getMinTemDate().split(",");
                    if(final_result_date_with_min_temp.length > 0)
                    {
                        finalresultminTemperature = Float.parseFloat(final_result_date_with_min_temp[1]);
                    }
                    else
                    {
                        finalresultminTemperature = 0;
                    }
                }

                if(result.getMaxTemDate().length() > 0){
                    String[] final_result_date_with_max_temp = result.getMaxTemDate().split(",");
                    if(final_result_date_with_max_temp.length > 0)
                    {
                        finalresultmaxTemperature = Float.parseFloat(final_result_date_with_max_temp[1]);
                    }
                    else
                    {
                        finalresultmaxTemperature = 0;
                    }
                }
                float difference = finalresultmaxTemperature - finalresultminTemperature;
                result.setDifference(difference);
            }
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        @SuppressWarnings("deprecation")
        Job job = new Job(conf, "Minmax_temperature");
        job.setJarByClass(MonthHiLo.class);

        job.setMapperClass(Map1.class);
        job.setCombinerClass(Red1.class);
        job.setReducerClass(Red1.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MinMaxTemperature.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}