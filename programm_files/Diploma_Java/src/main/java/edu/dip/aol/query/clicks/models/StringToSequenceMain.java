/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.dip.aol.query.clicks.models;

import edu.dip.aol.query.clicks.models.objects.RankedUrl;
import edu.dip.aol.query.clicks.models.objects.RankedUrlWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;

/**
 * @author alex
 */
public class StringToSequenceMain {

    public static void main(String[] args) throws FileNotFoundException, IOException, Exception {
        new StringToSequenceMain().run(args);
    }

    public void run(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new StringToSequenceMain.AolParseJob(), args);
    }

    public static class StringToUserSessionMapper extends MapReduceBase
            implements Mapper<LongWritable, Text, Text, RankedUrlWritable> {

        private Text outputKey = new Text();
        private RankedUrlWritable wrurl = new RankedUrlWritable();

        public void map(LongWritable key, Text value, OutputCollector<Text, RankedUrlWritable> oc, Reporter rprtr) throws IOException {
            String strValue = value.toString().trim();
            if (strValue.isEmpty() || strValue.equalsIgnoreCase("\n") || strValue.equalsIgnoreCase("\t")) {
                return;
            }
            String[] items = parseInput(strValue);
            if (items.length != 5) {
                return;
            }
            String session = items[0].trim();
            String query = items[1].trim();
            int rank = Integer.parseInt(items[2]);
            String url = items[3].trim();
            int click = Integer.parseInt(items[4]);
            RankedUrl rurl = new RankedUrl(url, query, session, rank, click);

            wrurl.set(rurl);
            outputKey.set(session);
            oc.collect(outputKey, wrurl);
        }

        private String[] parseInput(String input) {
            String[] its = input.trim().split("\t");
            if (its.length != 2) {
                return new String[0];
            }
            String[] outValue = its[1].split(" +");
            return new String[]{its[0], outValue[0],
                    outValue[1].substring(1, outValue[1].length() - 1),
                    outValue[2], outValue[3]};
        }
    }

    public static class StringToUserSessionReducer extends MapReduceBase
            implements Reducer<Text, RankedUrlWritable, Text, RankedUrlWritable> {

        private Text outputKey = new Text();

        public void reduce(Text key, Iterator<RankedUrlWritable> values, OutputCollector<Text, RankedUrlWritable> oc, Reporter rprtr) throws IOException {
            String strKey = key.toString();
            String sess = strKey.split(" ")[0];
            if (!values.hasNext()) {
                return;
            }
            RankedUrlWritable wrurl = values.next();
            String query = wrurl.get().getQuery();
            outputKey.set(sess);
            while (values.hasNext()) {
//                usession.addRurl(values.next());
                oc.collect(outputKey, values.next());
            }
//            usession.setId(sess);
//            wusession.set(usession);
//            oc.collect(outputKey, wusession);
        }
    }

    public class AolParseJob extends Configured implements Tool {

        @Override
        public int run(String[] args) throws Exception {
            Configuration conf = getConf();
            JobConf jconf = new JobConf(conf, StringToSequenceMain.AolParseJob.class);
            Path in = new Path(args[0]);
            Path out = new Path(args[1]);
            FileSystem fs = FileSystem.get(conf);
            if (fs.exists(out)) {
                fs.delete(out, false);
            }
            FileInputFormat.setInputPaths(jconf, in);
            SequenceFileOutputFormat.setOutputPath(jconf, out);
            jconf.setJobName("string to sequence");

            jconf.setJarByClass(StringToSequenceMain.class);

            jconf.setMapperClass(StringToUserSessionMapper.class);
            jconf.setReducerClass(StringToUserSessionReducer.class);

            jconf.setInputFormat(TextInputFormat.class);
            jconf.setOutputFormat(SequenceFileOutputFormat.class);

            jconf.setNumReduceTasks(0);

            jconf.setMapOutputKeyClass(Text.class);
            jconf.setMapOutputValueClass(RankedUrlWritable.class);
            jconf.setOutputKeyClass(Text.class);
            jconf.setOutputValueClass(RankedUrlWritable.class);

            JobClient.runJob(jconf);
            return 0;
        }
    }
}
