/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.dip.aol.query;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author alex
 */
public class MainAolStatisticsCounter {

    public class AolQueryStatisticsCounter extends Configured implements Tool {

        public int run(String[] args) throws Exception {
            Configuration conf = getConf();
            JobConf jconf = new JobConf(conf, MainAolStatisticsCounter.AolQueryStatisticsCounter.class);
            Path in = new Path(args[0]);
            Path out = new Path(args[1]);
            FileSystem fs = FileSystem.get(conf);
            if (fs.exists(out)) {
                fs.delete(out, false);
            }
            FileInputFormat.setInputPaths(jconf, in);
            FileOutputFormat.setOutputPath(jconf, out);
            jconf.setJobName("aol query filter");

            jconf.setMapperClass(AolStatisticsCounterMapper.class);
            jconf.setReducerClass(AolStatisticsCounterReducer.class);

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

    public void run(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new MainAolStatisticsCounter.AolQueryStatisticsCounter(), args);
    }

    public static void main(String[] args) throws FileNotFoundException, IOException, Exception {
        new MainAolStatisticsCounter().run(args);
    }
}
