/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.dip.aol;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.apache.commons.math.stat.ranking.RankingAlgorithm;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author alex
 */
public class MainAol {

    public class AolParseJob extends Configured implements Tool {

        public int run(String[] args) throws Exception {
            Configuration conf = getConf();
            JobConf jconf = new JobConf(conf, AolParseJob.class);
            Path in = new Path(args[0]);
            Path out = new Path(args[1]);
            FileSystem fs = FileSystem.get(conf);
            if (fs.exists(out)) {
                fs.delete(out, false);
            }
            FileInputFormat.setInputPaths(jconf, in);
            FileOutputFormat.setOutputPath(jconf, out);
            jconf.setJobName("aol parser");

            jconf.setMapperClass(AolMapper.class);
            jconf.setReducerClass(AolReducer.class);

            jconf.setInputFormat(TextInputFormat.class);
            jconf.setOutputFormat(TextOutputFormat.class);

            jconf.setOutputKeyClass(Text.class);
            jconf.setOutputValueClass(Text.class);

            jconf.set("mapred.textoutputformat.separator", "\n");
//            jconf.setNumMapTasks(40);
//            jconf.setNumReduceTasks(40);
            //            jconf.set("key.value.separator.in.input.line", ",");
            JobClient.runJob(jconf);
            return 0;
        }
    }

    public void run(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new AolParseJob(), args);
    }

    public static void main(String[] args) throws FileNotFoundException, IOException, Exception {
//        new Main().localrun();
        new MainAol().run(args);
        //        String path = "/home/alex/opt/resourses/aol/AOL-user-ct-collection/txts/user-ct-test-collection-01.txt";
//        parseFile(path);
    }

    public static void parseFile(String path) throws FileNotFoundException, IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path)));
        reader.readLine();
        String line = reader.readLine().trim();
        String[] items = line.split("\t");
        for (String item : items) {
            System.out.println(item);
        }
        reader.close();
    }
}
