package edu.dip.aol.query.clicks.models.ccm;

import edu.dip.aol.query.clicks.models.objects.RankedUrlWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;

public class ClickChainModelMain {

    public static class InitMapper extends MapReduceBase
            implements Mapper<Text, RankedUrlWritable, Text, RankedUrlWritable> {
        @Override
        public void map(Text inpupKey, RankedUrlWritable rankedUrlWritable, OutputCollector<Text, RankedUrlWritable> outputCollector, Reporter reporter) throws IOException {
            //To change body of implemented methods use File | Settings | File Templates.
        }
    }

    public static class ComputeAlphaParametersMapper extends MapReduceBase
            implements Mapper<Text, RankedUrlWritable, Text, RankedUrlWritable> {
        @Override
        public void map(Text inputKey, RankedUrlWritable inputValue, OutputCollector<Text, RankedUrlWritable> oc, Reporter reporter) throws IOException {

            String query = inputValue.get().getQuery();

            oc.collect(new Text(query), inputValue);

        }
    }

    public static class ComputeAlphaParametersReducer extends MapReduceBase
            implements Reducer<Text, RankedUrlWritable, Text, Text> {

        private final HashMap<String, Collection<RankedUrlWritable>> localBuffer = new HashMap<String, Collection<RankedUrlWritable>>();

        @Override
        public void reduce(Text inputKey, Iterator<RankedUrlWritable> inputValues, OutputCollector<Text, Text> oc, Reporter reporter) throws IOException {

            while (inputValues.hasNext()) {
                RankedUrlWritable wrurl = null;
                try {
                    wrurl = RankedUrlWritable.copy(inputValues.next());
                } catch (CloneNotSupportedException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                    continue;
                }

                if (!localBuffer.containsKey(wrurl.get().getSession())) {
                    localBuffer.put(wrurl.get().getSession(), new ArrayList<RankedUrlWritable>());
                }
                localBuffer.get(wrurl.get().getSession()).add(wrurl);
            }

            int[] n_array = new int[5];
            for (String query : localBuffer.keySet()) {
                Collection<RankedUrlWritable> wrurls = localBuffer.get(query);
                int lastClickPosition = findLastClickPosition(wrurls);
                if (lastClickPosition == 1 || lastClickPosition == 0) {
                    n_array[4] += 1;
                    continue;
                }
                for (RankedUrlWritable wrurl : wrurls) {
                    if (wrurl.get().getRank() < lastClickPosition) {
                        if (wrurl.get().getClick() > 0) {
                            n_array[1] += 1;
                        } else {
                            n_array[0] += 1;
                        }
                    } else if (wrurl.get().getRank() == lastClickPosition) {
                        n_array[2] += 1;
                    } else if (wrurl.get().getRank() > lastClickPosition) {
                        n_array[3] += 1;
                    }
                }
            }

            String n_string = "";
            for (int n : n_array) {
                n_string += n + " ";
            }

            oc.collect(new Text(inputKey.toString()), new Text(n_string.trim()));

        }

        private int findLastClickPosition(Collection<RankedUrlWritable> wrurls) {
            int lastRank = 0;

            for (RankedUrlWritable wrurl : wrurls) {
                if (wrurl.get().getRank() > lastRank && wrurl.get().getClick() > 0) {
                    lastRank = wrurl.get().getRank();
                }
            }

            return lastRank;
        }
    }

}
