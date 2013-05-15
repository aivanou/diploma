/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.dip.aol.query;

import edu.dip.aol.query.clicks.models.objects.RankedUrl;
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
public class MainAolPretty {

    public static void main(String[] args) throws FileNotFoundException, IOException, Exception {
        //        new Main().localrun();
        new MainAolPretty().run(args);
        //        String path = "/home/alex/opt/resourses/aol/AOL-user-ct-collection/txts/user-ct-test-collection-01.txt";
//        parseFile(path);
    }

    public void run(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new MainAolPretty.AolParseJob(), args);
    }

    public class AolPrettyMapper extends MapReduceBase
            implements Mapper<LongWritable, Text, Text, Text> {

        private final Text outputKey = new Text();
        private final Text outputValue = new Text();

        public void map(LongWritable key, Text value, OutputCollector<Text, Text> oc, Reporter rpr) throws IOException {
            String line = value.toString().trim();
            if (line.isEmpty() || line.toLowerCase().startsWith("anonid")) {
                return;
            }
            String[] items = line.split("\t");
            if (items.length != 5) {
                return;
            }
            outputKey.set(items[0]);
            outputValue.set(String.format("%s %s %s", items[1].trim(), items[3].trim(), items[4].trim()));
            oc.collect(outputKey, outputValue);
        }
    }

    public class AolPrettyReducer extends MapReduceBase
            implements Reducer<Text, Text, Text, Text> {

        private final Text outputKey = new Text();
        private final Text outputValue = new Text();
        //    private final Map<String, RankedUrl[]> queryMap = new HashMap<String, RankedUrl[]>();
        private final int maxRank = 20;

        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> oc, Reporter rpr) throws IOException {
//        while (values.hasNext()) {
//            Text value = values.next();
//            String[] items = value.toString().split(" ");
//            if (items.length != 3) {
//                continue;
//            }
//            int rank = Integer.parseInt(items[1]);
//            if (rank >= maxRank) {
//                continue;
//            }
//            String query = items[0];
//            if (!queryMap.containsKey(query)) {
//                queryMap.put(query, new RankedUrl[maxRank]);
//            }
//            RankedUrl rurl = new RankedUrl(items[2], rank);
//            queryMap.get(query)[rank] = rurl;
//        }
//        int sessionAdder = 1;
//        for (String query : queryMap.keySet()) {
//            String newSession = key.toString() + ":" + ++sessionAdder;
//            RankedUrl[] rurls = queryMap.get(query);
//            removeDublicateUrls(rurls);
//            for (int i = 0; i < rurls.length; i++) {
//                if (rurls[i] == null) {
//                    continue;
//                }
//                outputKey.set(newSession);
//                outputValue.set(query + " " + rurls[i].toString());
//                oc.collect(outputKey, outputValue);
//            }
//            outputKey.set("");
//            outputValue.set("\n");
//            oc.collect(outputKey, outputValue);
//        }
//        queryMap.clear();
        }

        private void removeDublicateUrls(RankedUrl[] rurls) {
//        for (int i = 0; i < rurls.length; i++) {
//            if (rurls[i] == null) {
//                continue;
//            }
//            for (int j = i + 1; j < rurls.length; j++) {
//                if (rurls[j] == null) {
//                    continue;
//                }
//                if (rurls[i].getUrl().equalsIgnoreCase(rurls[j].getUrl())) {
//                    rurls[j] = null;
//                }
//            }
//        }
        }
    }

    public class AolParseJob extends Configured implements Tool {

        public int run(String[] args) throws Exception {
            Configuration conf = getConf();
            JobConf jconf = new JobConf(conf, MainAolPretty.AolParseJob.class);
            Path in = new Path(args[0]);
            Path out = new Path(args[1]);
            FileSystem fs = FileSystem.get(conf);
            if (fs.exists(out)) {
                fs.delete(out, false);
            }
            FileInputFormat.setInputPaths(jconf, in);
            FileOutputFormat.setOutputPath(jconf, out);
            jconf.setJobName("aol pretty");

            jconf.setMapperClass(AolPrettyMapper.class);
            jconf.setReducerClass(AolPrettyReducer.class);

            jconf.setInputFormat(TextInputFormat.class);
            jconf.setOutputFormat(TextOutputFormat.class);

            jconf.setOutputKeyClass(Text.class);
            jconf.setOutputValueClass(Text.class);

            jconf.set("mapred.textoutputformat.separator", "\t");
//            jconf.setNumMapTasks(40);
//            jconf.setNumReduceTasks(40);
            //            jconf.set("key.value.separator.in.input.line", ",");
            JobClient.runJob(jconf);
            return 0;
        }
    }
}
