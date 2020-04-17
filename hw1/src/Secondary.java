import java.io.*;
import java.util.*;

import com.opencsv.CSVReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Secondary{

    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {


        public void setup(Context context) throws IOException, InterruptedException {

        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            CSVReader R = new CSVReader(new StringReader(line));
            String[] f2 = R.readNext();
            R.close();

            if (f2[41].equals("1.00")) return;
            if (f2[43].equals("1.00")) return;
            if (f2[0].equals("Year")) return;

            String carrier = f2[8];
            String flightYear = f2[0];
            if (!flightYear.equals("2008")) return;
            String flightMonth = f2[2];

            String arrDelayMinutes = f2[37];
            context.write(new Text(carrier+","+flightMonth), new Text(arrDelayMinutes));


        }

        public void cleanup(Context context) throws IOException, InterruptedException {

        }
    }


    public static class IntSumReducer extends Reducer<Text, Text, Text, NullWritable> {

        Map<Integer,Integer> delay = new HashMap<>();
        String currentCarrier = "";

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String [] sa = key.toString().split(",");
            String carrier = sa[0];
            int month = Integer.parseInt(sa[1]);
            if (!carrier.equals(currentCarrier)){
                StringBuilder sb = new StringBuilder();
                sb.append(currentCarrier);
                for (int m=1;m<=12;m++){
                    if (!currentCarrier.equals("")) sb.append(",("+m+","+delay.get(m)+")");
                    delay.put(m,0);
                }
                if (!currentCarrier.equals("")) context.write(new Text(sb.toString()),NullWritable.get());
                currentCarrier=carrier;
            }

            double sum = 0;
            int count = 0;
            for (Text t:values){
                count++;
                sum += Double.parseDouble(t.toString());
            }
            delay.put(month,(int)Math.ceil(sum/count));
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (!currentCarrier.equals("")){
                StringBuilder sb = new StringBuilder();
                sb.append(currentCarrier);
                for (int m=1;m<=12;m++){
                    sb.append(",("+m+","+delay.get(m)+")");
                }
                context.write(new Text(sb.toString()),NullWritable.get());
            }
        }
    }

    public static class FirstPartitioner extends Partitioner<Text,Text>{
        public int getPartition(Text key, Text value, int numPartitions){
            String carrier = key.toString().split(",")[0];
            return carrier.hashCode()%numPartitions;
        }
    }

    public static class KeyComparator extends WritableComparator{
        protected KeyComparator(){
            super(Text.class,true);
        }
        public int compare(WritableComparable w1, WritableComparable w2){
            Text t1 = (Text)w1;
            Text t2 = (Text)w2;
            String[] s1 = t1.toString().split(",");
            String[] s2 = t2.toString().split(",");
            int cmp = s1[0].compareTo(s2[0]);
            if (cmp!=0) return cmp;
            return s1[1].compareTo(s2[1]);
        }
    }

    public static class GroupComparator extends WritableComparator{
        protected GroupComparator(){
            super(Text.class,true);
        }
        public int compare(WritableComparable w1, WritableComparable w2){
            Text t1 = (Text)w1;
            Text t2 = (Text)w2;
            return t1.toString().compareTo(t2.toString());
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "word count");

        job.setJarByClass(Secondary.class);
        job.setMapperClass(TokenizerMapper.class);

        job.setPartitionerClass(FirstPartitioner.class);
        job.setSortComparatorClass(KeyComparator.class);
        job.setGroupingComparatorClass(GroupComparator.class);

        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
