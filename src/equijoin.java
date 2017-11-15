import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;


public class equijoin
{
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

        private Text kjoin = new Text();
        private Text tuples = new Text();

        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            //Read the  row and Split
            // gets the relation name, joining key and tuple
            String row[] = value.toString().split(",");

            //The tuple added with its relation name
            StringBuilder tuple = new StringBuilder(row[0]);

            for (int i=1;i<row.length;i++)
                tuple.append(",").append(row[i]);

            //sets key and value
            kjoin.set(row[1]);
            tuples.set(tuple.toString());

            output.collect(kjoin, tuples);
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text,Text> output, Reporter reporter) throws IOException {

            List<String>  rArrList = new ArrayList<>();
            List<String>  sArrList = new ArrayList<>();

            Text result = new Text();
            String tempVal;
            while(values.hasNext())
            {
                tempVal = values.next().toString();
                String Split[] = tempVal.split(",");
                //checks for the joining key and add it separately in a table
                if(Split[0].equals("S")){
                    sArrList.add(tempVal);
                }
                else if(Split[0].equals("R")){
                    rArrList.add(tempVal);
                }
            }

            if(sArrList.isEmpty() || rArrList.isEmpty()){
                key.clear();
            }else{
                //Adds value of S Key to R Key
                for (String r : rArrList) {
                    for (String s : sArrList) {
                        result.set(r + ", " + s);
                        output.collect(new Text(""), result);
                    }
                }
            }
        }
    }

    public static void main(String[] args){
        try{
        JobConf jobConf = new JobConf(equijoin.class);
        jobConf.setJobName(args[0]);

        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(Text.class);
        jobConf.set("mapred.textoutputformat.separator"," ");
        jobConf.setMapperClass(Map.class);
        jobConf.setReducerClass(Reduce.class);
        FileInputFormat.setInputPaths(jobConf,new Path(args[1]));
        FileOutputFormat.setOutputPath(jobConf,new Path(args[2]));

        JobClient.runJob(jobConf);
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }
}
