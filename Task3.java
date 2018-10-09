import java.io.IOException;
import java.util.StringTokenizer;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;

import java.net.URI;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.Vector;
import java.util.regex.Pattern;
import java.util.Collections;
import java.util.Map;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;


public class Task3 {


    // http://commons.apache.org/proper/commons-text/jacoco/org.apache.commons.text.similarity/JaccardSimilarity.java.html
    public static Double calculateJaccardSimilarity(final CharSequence left, final CharSequence right) {
        final int leftLength = left.length();
        final int rightLength = right.length();
        if (leftLength == 0 || rightLength == 0) {
            return 0d;
        }
        final Set<Character> leftSet = new HashSet<>();
        for (int i = 0; i < leftLength; i++) {
            leftSet.add(left.charAt(i));
        }
        final Set<Character> rightSet = new HashSet<>();
        for (int i = 0; i < rightLength; i++) {
            rightSet.add(right.charAt(i));
        }
        final Set<Character> unionSet = new HashSet<>(leftSet);
        unionSet.addAll(rightSet);
        final int intersectionSize = leftSet.size() + rightSet.size() - unionSet.size();
        return 1.0d * intersectionSize / unionSet.size();
    }


    // https://github.com/tdebatty/java-string-similarity/blob/master/src/main/java/info/debatty/java/stringsimilarity/Jaccard.java
    private static final Pattern SPACE_REG = Pattern.compile("\\s+");
    private static final int k = 3;
    public static final Map<String, Integer> getProfile(final String string) {
        HashMap<String, Integer> shingles = new HashMap<String, Integer>();

        String string_no_space = SPACE_REG.matcher(string).replaceAll(" ");
        for (int i = 0; i < (string_no_space.length() - k + 1); i++) {
            String shingle = string_no_space.substring(i, i + k);
            Integer old = shingles.get(shingle);
            if (old != null) {
                shingles.put(shingle, old + 1);
            } else {
                shingles.put(shingle, 1);
            }
        }

        return Collections.unmodifiableMap(shingles);
    }

    public static Double calculateJaccardSimilarityAlt(final String s1, final String s2) {
        if (s1 == null) {
            throw new NullPointerException("s1 must not be null");
        }

        if (s2 == null) {
            throw new NullPointerException("s2 must not be null");
        }

        if (s1.equals(s2)) {
            return 1.0;
        }

        Map<String, Integer> profile1 = getProfile(s1);
        Map<String, Integer> profile2 = getProfile(s2);


        Set<String> union = new HashSet<String>();
        union.addAll(profile1.keySet());
        union.addAll(profile2.keySet());

        int inter = profile1.keySet().size() + profile2.keySet().size()
                - union.size();

        return 1.0 - (1.0 * inter / union.size());
    }

    public static class PairGenerator extends Mapper<Object, Text, Text, DoubleWritable>{

        private final static DoubleWritable v = new DoubleWritable(1);
        private static Vector<String> File2Map = new Vector<String>();
        private String word = new String();
        private BufferedReader brReader;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            URI[] cacheFilesLocal = context.getCacheFiles();

            for (URI eachPath : cacheFilesLocal) {
                loadFile2(eachPath, context);
            }
        }

        private void loadFile2(URI filePath, Context context) throws IOException {
            String strLineRead = "";

            try {
                brReader = new BufferedReader(new FileReader(filePath.toString()));

                // Read each line, split and load to Vector
                while ((strLineRead = brReader.readLine()) != null) {
                    File2Map.add(strLineRead.trim().toLowerCase());
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }finally {
                if (brReader != null) {
                    brReader.close();
                }
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word = itr.nextToken().toLowerCase();
                for(String s : File2Map){
                    Double v = calculateJaccardSimilarityAlt(word,s);
                    if(v>=0 && v <=0.15) {
                        context.write(new Text(word + "||" + s), new DoubleWritable(v));
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Jaccardi");
        FileSystem fs = FileSystem.get(conf);
        Path p0 = new Path(args[0]);
        Path p1 = new Path(args[1]);
        Path p2 = new Path(args[2]);

        if(fs.exists(p2)){
            fs.delete(p2, true);
        }


        job.setJarByClass(Task3.class);
        job.addCacheFile(p1.toUri());

        job.setMapperClass(PairGenerator.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, p0);
        FileOutputFormat.setOutputPath(job, p2);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}