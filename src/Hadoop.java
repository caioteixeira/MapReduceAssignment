import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public abstract class Hadoop {
  private static ArrayList<Integer> paramSelect;
  private static HashMap<Integer, String> param =
      new HashMap<Integer, String>();
  private static int month = 0;

  public static class TokenizerMapper
      extends Mapper<Object, Text, Text, FloatWritable> {

    private final static FloatWritable vals = new FloatWritable();
    private Text title = new Text();

    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      StringTokenizer line = new StringTokenizer(value.toString(), "\n");
      while (line.hasMoreTokens()) {
        int i = 0;
        StringTokenizer dataLine = new StringTokenizer(line.nextToken());
        if (month != 0) {
          while (dataLine.hasMoreTokens() && i < 2) {
            dataLine.nextToken();
            i++;
          }
          try {
            int m = Integer.parseInt(dataLine.nextToken().substring(4, 6));
            i++;
            if (m != month)
              continue;
          } catch (NumberFormatException e) {
            continue;
          }
        }
        for (Integer p : paramSelect) {
          title.set(param.get(p));
          while (dataLine.hasMoreTokens() && i < p) {
            dataLine.nextToken();
            i++;
          }
          try {
            String data = dataLine.nextToken();
            data = data.replaceAll("[^0-9.,]+", "");
            float val = Float.parseFloat(data);
            val = verifyMissing(val, p);
            i++;
            vals.set(val);
            context.write(title, vals);
          } catch (NumberFormatException e) {
            break;
          }
        }
      }
    }

    public float verifyMissing(float val, int p) {
      if (val == (float) 9999.9
          && (p == 3 || p == 5 || p == 7 || p == 9 || p == 17 || p == 18)) {
        return 0;
      }
      if (val == (float) 999.9
          && (p == 11 || p == 13 || p == 15 || p == 16 || p == 20)) {
        return 0;
      }
      if (val == (float) 99.99 && p == 20) {
        return 0;
      }
      return val;
    }
  }

  public static class MeanReducer
      extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    private FloatWritable result = new FloatWritable();

    public void reduce(Text key, Iterable<FloatWritable> values,
        Context context) throws IOException, InterruptedException {
      float sum = 0;
      int length = 0;
        for (FloatWritable val : values) {
          sum += val.get();
          length++;
        }
        result.set(sum / length);
      context.write(key, result);
    }
  }

  public static boolean executeMean(int year1, int year2, int m,
      String input, String output, ArrayList<Integer> par) throws Exception {
    if (new File(output).exists()) {
      return false;
    }
    paramSelect = par;
    initializeParams();
    month = m;
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "hadoop");
    job.setJarByClass(Index.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(MeanReducer.class);
    job.setReducerClass(MeanReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FloatWritable.class);
    while (year1 <= year2) {
      FileInputFormat.addInputPath(job, new Path(input + "/" + year1));
      year1++;
    }
    FileOutputFormat.setOutputPath(job, new Path(output));
    job.waitForCompletion(true);
    return true;
  }

  private static void initializeParams() {
    param.put(3, "TEMP"); // 9999.9
    param.put(5, "DEWP"); // 9999.9
    param.put(7, "SLP"); // 9999.9
    param.put(9, "STP"); // 9999.9
    param.put(11, "VISIB");// 999.9
    param.put(13, "WDSP");// 999.9
    param.put(15, "MXSPD");// 999.9
    param.put(16, "GUST");// 999.9
    param.put(17, "MAX"); // 9999.9
    param.put(18, "MIN"); // 9999.9
    param.put(19, "PRCP"); // 99.99
    param.put(20, "SNDP");// 999.9
  }

  public static void deleteDir(File file) {
    File[] contents = file.listFiles();
    if (contents != null) {
      for (File f : contents) {
        deleteDir(f);
      }
    }
    file.delete();
  }
}
