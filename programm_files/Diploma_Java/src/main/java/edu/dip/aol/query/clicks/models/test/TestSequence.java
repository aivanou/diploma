/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.dip.aol.query.clicks.models.test;

import edu.dip.aol.query.clicks.models.objects.RankedUrlWritable;
import edu.dip.aol.query.clicks.models.objects.UserSessionWritable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author alex
 */
public class TestSequence {

    public static class TestMapper extends MapReduceBase
            implements Mapper<IntWritable, Text, IntWritable, Text> {

        private Text outputKey = new Text();
        private RankedUrlWritable wrurl = new RankedUrlWritable();

        public void map(IntWritable key, Text value, OutputCollector<IntWritable, Text> oc, Reporter rprtr) throws IOException {
            oc.collect(new IntWritable(1), value);
        }
    }

    public static class TestReducer extends MapReduceBase
            implements Reducer<IntWritable, Text, IntWritable, Text> {

        private Text outputKey = new Text();
        private UserSessionWritable wusession = new UserSessionWritable();

        public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> oc, Reporter rprtr) throws IOException {
            int size = 0;
            while (values.hasNext()) {
                size += 1;
            }
            oc.collect(new IntWritable(size), new Text("100500values"));
        }
    }

    public class AolParseJob extends Configured implements Tool {

        public int run(String[] args) throws Exception {
            Configuration conf = getConf();
            JobConf jconf = new JobConf(conf, TestSequence.AolParseJob.class);
            Path in = new Path(args[0]);
            Path out = new Path(args[1]);
            FileSystem fs = FileSystem.get(conf);
            if (fs.exists(out)) {
                fs.delete(out, false);
            }
            SequenceFileInputFormat.setInputPaths(jconf, in);
            SequenceFileOutputFormat.setOutputPath(jconf, out);

            jconf.setMapperClass(TestMapper.class);
            jconf.setReducerClass(TestReducer.class);

            jconf.setInputFormat(SequenceFileInputFormat.class);
            jconf.setOutputFormat(SequenceFileOutputFormat.class);

            jconf.setOutputKeyClass(IntWritable.class);
            jconf.setOutputValueClass(Text.class);

            JobClient.runJob(jconf);
            return 0;
        }
    }

    public void run(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new TestSequence.AolParseJob(), args);
    }

    public static void main(String[] args) throws FileNotFoundException, IOException, Exception {
        new TestSequence().run(args);
//        writeData("/home/alex/opt/apache/hadoop/hadoop-1.0.4/testInput");
    }

    public static void writeData(String path) throws Exception {
        Path p = new Path(path);
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, p, IntWritable.class, Text.class);
        for (int i = 0; i < 1000; i++) {
            IntWritable k = new IntWritable(i);
            Text v = new Text("dfksljdfkl " + i);
            writer.append(k, v);
        }
        writer.close();
    }
}
