import com.opencsv.CSVReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.io.IOException;
import java.io.StringReader;



public class HPOPULATE {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
        Connection connection;
        Table hTable;

        public void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = HBaseConfiguration.create();
            connection = ConnectionFactory.createConnection(conf);
            hTable = connection.getTable(TableName.valueOf("flight"));
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            CSVReader R = new CSVReader(new StringReader(line));
            String[] f2 = R.readNext();
            R.close();

            if (f2[0].equals("Year")) return; //skip the head row

            String carrier = f2[8];
            String flightYear = f2[0];
            String flightMonth = f2[2];
            String arrDelayMinutes = f2[37];

            Put c = new Put(Bytes.toBytes(carrier+","+flightMonth+","+key.toString()));
            c.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("carrier"),Bytes.toBytes(carrier));
            c.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("year"),Bytes.toBytes(flightYear));
            c.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("month"),Bytes.toBytes(flightMonth));
            c.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("cancelled"),Bytes.toBytes(f2[41]));
            c.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("diverted"),Bytes.toBytes(f2[43]));
            c.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("arrDelayMinutes"),Bytes.toBytes(arrDelayMinutes));

            hTable.put(c);

        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            hTable.close();
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }

        Job job = new Job(conf, "Write csv file to hbase");
        job.setJarByClass(HPOPULATE.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setNumReduceTasks(0);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));


        Configuration config = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();

        if (admin.tableExists(TableName.valueOf("flight"))) {
            admin.disableTable(TableName.valueOf("flight"));
            admin.deleteTable(TableName.valueOf("flight"));
        }


        HTableDescriptor hd = new HTableDescriptor(TableName.valueOf("flight"));
        HColumnDescriptor fd = new HColumnDescriptor("f1");
        hd.addFamily(fd);
        admin.createTable(hd);
        System.out.println("Table Created");

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
