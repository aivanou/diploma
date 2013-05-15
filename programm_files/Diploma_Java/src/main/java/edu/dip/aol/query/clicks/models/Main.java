/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.dip.aol.query.clicks.models;

import edu.dip.aol.query.clicks.models.objects.*;
import org.apache.hadoop.fs.FSDataOutputStream;

import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * @author alex
 */
public class Main {

    public static void main(String[] args) throws IOException, Exception {
        String clickTestFile = "/home/tierex/opt/resourses/letor/Fold1/transformedTest.txt";
        double[][] features = parseClickFile(clickTestFile);
        System.out.println(features.length + "  " + features[0].length);
        int threadCount = 8;
        Collection<Future<Collection<Double>>> threads = new ArrayList<Future<Collection<Double>>>();
        Collection<Pair> pairs = split(features[0].length, threadCount);
        ExecutorService exec = Executors.newCachedThreadPool();

        long startTime = System.currentTimeMillis();
        for (Pair pair : pairs) {
            System.out.println(pair.getV1() + "   " + pair.getV2());
            Future<Collection<Double>> fData = exec.submit(new ProbabilityJob(features[0], (int) pair.getV1(), (int) pair.getV2()));
            threads.add(fData);
        }
        for (Future<Collection<Double>> fData : threads) {
            Collection<Double> prs = fData.get();
            for (Double pr : prs) {
                System.out.println(pr);
            }

        }
        System.out.println((System.currentTimeMillis() - startTime));

//        Path path = new Path("/home/tierex/opt/apache/hadoop/hadoop-1.0.4/part-00000");
//        Configuration conf = new Configuration();
//        FileSystem fs = FileSystem.get(conf);
//        SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
//        Text key = new Text();
//        RankedUrlWritable rurl = new RankedUrlWritable();
//        int i = 0;
//        while (reader.next(key, rurl)) {
////            System.out.println("s: " + rurl.get().getSession() + "  "
////                    + rurl.get().getUrl() + "  " + rurl.get().getRank()
////                    + "  " + rurl.get().getAttractivness() + "  "
////                    + rurl.get().getAttrDistributionFactor() + "   "
////                    + rurl.get().getSatDistributionFactor());
//            System.out.println(String.format("%s %s  %s", rurl.get().getSession(), rurl.get().getUrl(), rurl.get().getClick()));
//            i += 1;
//        }
//        System.out.println(i);
    }

    public static void testRW(String filename) throws Exception {
        UserSessionWritable u = new UserSessionWritable();
        UserSession s = new UserSession("1", "2");
        for (int i = 0; i < 10; i++) {
            RankedUrlWritable r = new RankedUrlWritable();
            RankedUrl url = new RankedUrl("adf" + i, "sdf", "1", i, i);
            r.set(url);
            s.addRurl(r);
        }
        u.set(s);
        File out = new File(filename);
        out.deleteOnExit();
        FileOutputStream fs = new FileOutputStream(out);
        DataOutput output = new FSDataOutputStream(fs);
        u.write(output);
        fs.close();
        UserSessionWritable uwi = new UserSessionWritable();
        FileInputStream fi = new FileInputStream(out);
        DataInput in = new DataInputStream(fi);
        uwi.readFields(in);
        fi.close();
        System.out.println(uwi.get().getId());
        for (RankedUrlWritable r : uwi.get().getRurls()) {
            System.out.println(r.get().getUrl());
        }
    }

    private static double[][] parseClickFile(String filename) throws IOException {
        int size = 1000000;
        int featuresNumber = 136;
        double[][] features = new double[136][size];
        String line = "";
        BufferedReader reader = new BufferedReader(new FileReader(filename));
        int index = 0;
        while ((line = reader.readLine()) != null) {
            String[] parts = line.split(" ");
            for (int i = 0; i < featuresNumber; ++i) {
                features[i][index] = Double.parseDouble(parts[i + 2]);
            }
            index += 1;
            if (index >= size) return features;
        }

        return features;
    }

    private static Collection<Pair> split(int featuresLength, int splitLength) {
        Collection<Pair> pairs = new ArrayList<Pair>();
        int part = featuresLength / splitLength;
        int lowBound = 0;
        int highBound = 0;
        if (part <= 1) {
            splitLength = 1;
        }

        for (int i = 0; i < splitLength - 1; ++i) {
            highBound += part;
            pairs.add(new Pair(lowBound, highBound));
            lowBound += part;
        }
        pairs.add(new Pair(lowBound, featuresLength));
        return pairs;
    }

    protected static class ProbabilityJob implements Callable<Collection<Double>> {

        private double[] features;
        private int lowBound;
        private int highBound;

        public ProbabilityJob(double[] features, int lowBound, int highBound) {
            this.features = features;
            this.lowBound = lowBound;
            this.highBound = highBound;
        }

        @Override
        public Collection<Double> call() throws Exception {
            Collection<Double> probs = new ArrayList<Double>();

            for (int index = lowBound; index < highBound; ++index) {
                probs.add(computeProbability(features, features[index]));
            }

            return probs;  //To change body of implemented methods use File | Settings | File Templates.
        }

        private double computeProbability(double[] features, double value) {
            double probability = 0.0;
            for (int i = 0; i < features.length; ++i) {
                if (features[i] < value) {
                    probability += 1.0;
                }
            }
            return probability / (double) features.length;
        }
    }
}
