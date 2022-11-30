import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InputFileConstruction {
    //To map the csv file with key value pairs of stationID and state-name
    public static class Map1 extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            Text newKey = new Text();
            Text newValue = new Text();
            String line = value.toString();
            String[] lines = line.split(",");
            if (lines.length > 6) {
//                    For the Line is valid: there should be a state value, the country should be US and should not be the header
                if (lines[3].equals("US") && lines[4].length() > 0 && !(lines[0].equals("USAF"))) {
                    newKey.set(lines[0]);
                    String valuesState = String.format("%s,%s", lines[4], lines[0]);
                    newValue.set("D " + valuesState);
                    context.write(newKey, newValue);
                }
            }
        }
    }

    public static class Map2 extends Mapper<Object, Text, Text, Text> {
        //            This mapper is used to map the data in the format of stationID| date AvgTemp Numberof Readings
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            Text newKey = new Text();
            Text newValue = new Text();
            String line = value.toString();
            //            if line is valid and not header
            if (!(line.length() == 0) && !(line.substring(0, 6).equals("STN---"))) {
                String station_id = line.substring(0, 6);
                newKey.set(station_id);
                String date = line.substring(14, 22);
                float average_temperature = Float.parseFloat(line.substring(26, 30).trim());
                float number_of_reading = Float.parseFloat(line.substring(31, 33).trim());
                String valuesStations = String.format("%s,%.2f,%.2f", date, average_temperature, number_of_reading);
                newValue.set("T " + valuesStations);
                context.write(newKey, newValue);
            }
        }
    }

    public static class Map3 extends Mapper<Object, Text, Text, Text> {
        //            This mapper is used to map the data in the format of stationID| date AvgTemp Numberof Readings
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            Text newKey = new Text();
            Text newValue = new Text();
            String line = value.toString();
            //            if line is valid and not header
            if (!(line.length() == 0) && !(line.substring(0, 6).equals("STN---"))) {
                String station_id = line.substring(0, 6);
                newKey.set(station_id);
                String date = line.substring(14, 22);
                float average_temperature = Float.parseFloat(line.substring(26, 30).trim());
                float number_of_reading = Float.parseFloat(line.substring(31, 33).trim());
                String valuesStations = String.format("%s,%.2f,%.2f", date, average_temperature, number_of_reading);
                newValue.set("T " + valuesStations);
                context.write(newKey, newValue);
            }
        }
    }

    public static class Map4 extends Mapper<Object, Text, Text, Text> {
        //            This mapper is used to map the data in the format of stationID| date AvgTemp Numberof Readings
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            Text newKey = new Text();
            Text newValue = new Text();
            String line = value.toString();
            //            if line is valid and not header
            if (!(line.length() == 0) && !(line.substring(0, 6).equals("STN---"))) {
                String station_id = line.substring(0, 6);
                newKey.set(station_id);
                String date = line.substring(14, 22);
                float average_temperature = Float.parseFloat(line.substring(26, 30).trim());
                float number_of_reading = Float.parseFloat(line.substring(31, 33).trim());
                String valuesStations = String.format("%s,%.2f,%.2f", date, average_temperature, number_of_reading);
                newValue.set("T " + valuesStations);
                context.write(newKey, newValue);
            }
        }
    }

    public static class Map5 extends Mapper<Object, Text, Text, Text> {
        //            This mapper is used to map the data in the format of stationID| date AvgTemp Numberof Readings
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            Text newKey = new Text();
            Text newValue = new Text();
            String line = value.toString();
            //            if line is valid and not header
            if (!(line.length() == 0) && !(line.substring(0, 6).equals("STN---"))) {
                String station_id = line.substring(0, 6);
                newKey.set(station_id);
                String date = line.substring(14, 22);
                float average_temperature = Float.parseFloat(line.substring(26, 30).trim());
                float number_of_reading = Float.parseFloat(line.substring(31, 33).trim());
                String valuesStations = String.format("%s,%.2f,%.2f", date, average_temperature, number_of_reading);
                newValue.set("T " + valuesStations);
                context.write(newKey, newValue);
            }
        }
    }

    // The reduce takess the input from the above two mapper and outputs the key value pairs with StaionID and weatherData
    public static class Red1 extends Reducer<Text, Text, Text, Text> {


        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            ArrayList<Text> weatherList = new ArrayList<Text>();
            ArrayList<Text> stationList = new ArrayList<Text>();

            weatherList.clear();
            stationList.clear();
            for (Text t : values) {
                String tmp = t.toString();
                if (tmp.charAt(0) == 'T') {
                    stationList.add(new Text(tmp.substring(2)));
                } else if (tmp.charAt(0) == 'D') {
                    weatherList.add(new Text(tmp.substring(2)));
                }
            }

            if (!stationList.isEmpty() && !weatherList.isEmpty()) {
                for (Text A : stationList) {
                    for (Text B : weatherList) {
                        context.write(A, B);
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Input_File_Construction");
        job.setJarByClass(InputFileConstruction.class);
        job.setReducerClass(Red1.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, Map1.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, Map2.class);
        MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, Map3.class);
        MultipleInputs.addInputPath(job, new Path(args[3]), TextInputFormat.class, Map4.class);
        MultipleInputs.addInputPath(job, new Path(args[4]), TextInputFormat.class, Map5.class);
        Path outputPath = new Path(args[5]);

        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
