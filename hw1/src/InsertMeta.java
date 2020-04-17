import java.io.*;
import java.util.*;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.opencsv.CSVReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class InsertMeta {
    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
        private AmazonDynamoDB ddb;
        private Set<String> set;
        public void setup(Context context) throws IOException, InterruptedException {
            ddb = AmazonDynamoDBAsyncClientBuilder.defaultClient();
            set = new HashSet<>();
            Scanner sc = new Scanner(new File("1000UserResult/1000UserRating.csv"));
            while (sc.hasNextLine()){
                String l = sc.nextLine();
                set.add(l.split(",")[1]);
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            CSVReader R = new CSVReader((new StringReader(line)));
            String[] f2 = null;
            HashMap<String, AttributeValue> item_values = null;
            try {
                f2 = R.readNext();
                if (f2[0].equals("adult")) return;
                if (!set.contains(f2[5])) return;
                item_values = new HashMap<String,AttributeValue>();
                //System.out.println(f2[5]);
                item_values.put("id", new AttributeValue().withN(f2[5]));
                //System.out.println(f2[2]);
                item_values.put("budget", new AttributeValue().withN(f2[2]));
                //System.out.println(f2[8]);
                item_values.put("titile", new AttributeValue().withS(f2[8].equals("")?"Null":f2[8]));
                item_values.put("description", new AttributeValue().withS(f2[9].equals("")?"Null":f2[9]));
                //System.out.println(f2[4]);
                item_values.put("homepage", new AttributeValue().withS(f2[4].equals("")?"Null":f2[4]));
                //System.out.println("end");
            }catch(Exception e){
                return;
            }finally{
                R.close();
            }

            try {
                ddb.putItem("Movie", item_values);
            } catch (ResourceNotFoundException e) {
                System.err.format("Error: The table \"%s\" can't be found.\n", "Top");
                System.err.println("Be sure that it exists and that you've typed its name correctly!");
                System.exit(1);
            } catch (AmazonServiceException e) {
                System.err.println(e.getMessage());
                System.exit(1);
            }
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 1) { //One parameter the location of metadata
            System.err.println("Wrong input number");
            System.exit(2);
        }
        Job job = new Job(conf, "word count");

        job.setJarByClass(InsertMeta.class);
        job.setMapperClass(TokenizerMapper.class);

        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        Random rand = new Random();
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job,new Path("data"+rand.nextInt(10000)));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}