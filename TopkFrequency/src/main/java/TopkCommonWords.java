import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.StringTokenizer;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopkCommonWords {

    public static ArrayList<String> stopWords(Mapper.Context context) throws IOException {
        ArrayList<String> stopWords = new ArrayList<>();
        URI[] cacheFiles = context.getCacheFiles();
        if (cacheFiles != null && cacheFiles.length > 0) {
            try {
                String line = "";
                FileSystem fs = FileSystem.get(context.getConfiguration());
                Path getFilePath = new Path(cacheFiles[0].toString());
                BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(getFilePath)));
                while ((line = reader.readLine()) != null) {
                    String[] words = line.split("\n");
                    Collections.addAll(stopWords, words);
                }
            } catch (Exception e) {
                System.out.println("Unable to read the File");
                System.exit(1);
            }
        }
        return stopWords;
    }

    public static class MapperFile1 extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private final Text word = new Text();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            ArrayList<String> stopWords = TopkCommonWords.stopWords(context);
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                if (stopWords.contains(word.toString())) { // if token has a stop word
                    continue;
                }
                context.write(word, one);
            }
        }
    }

    public static class MapperFile2 extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable two = new IntWritable(2);
        private final Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            ArrayList<String> stopWords = TopkCommonWords.stopWords(context);
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                if (stopWords.contains(word.toString())) { // if token has a stop word
                    continue;
                }
                context.write(word, two);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, IntWritable, Text> {
        TreeSet<Pair> priorityQueue = new TreeSet<>();
        private final IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, Context context) {
            int sum_1 = 0;
            int sum_2 = 0;
            //System.out.println(key);
            String keyStr = key.toString();
            for (IntWritable val : values) {
                if (val.get() == 1) {
                    sum_1 += val.get();
                } else {
                    sum_2 += 1;
                }
            }
            //System.out.println(sum_1 + ", " + sum_2);
            int repeat = Math.max(sum_1, sum_2);
            if (repeat > 0 && Math.min(sum_1, sum_2) > 0) {
                result.set(repeat);
                //System.out.println(repeat);
                //context.write(key, result);
                System.out.println(key);
                System.out.println(sum_1 + ", " + sum_2);
                priorityQueue.add(new Pair(result.get(), keyStr));
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            int stop = 0;
            while (!priorityQueue.isEmpty()) {
                Pair pair = priorityQueue.pollLast();
                context.write(new IntWritable(pair.value),new Text(pair.key) );
                stop += 1;
                if (stop == 20) {
                    break;
                }
            }
        }
    }

    public static class Pair implements Comparable<Pair> {
        int value;
        String key;

        Pair(int value, String key) {
            this.key = key;
            this.value = value;
        }

        @Override
        public int compareTo(Pair pair) {
            if (this.value >= pair.value) {
                return 1;
            } else {
                return -1;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count1");
        job.addCacheFile(new Path(args[2]).toUri()); // stop words
        job.setJarByClass(TopkCommonWords.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        MultipleInputs.addInputPath(job, new Path(args[0]),
                TextInputFormat.class, MapperFile1.class);
        MultipleInputs.addInputPath(job, new Path(args[1]),
                TextInputFormat.class, MapperFile2.class);
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
