import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
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
  private static int dayW = 0;

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
        while (dataLine.hasMoreTokens() && i < 2) {
          dataLine.nextToken();
          i++;
        }
        String date = dataLine.nextToken();
        i++;
        if (month != 0) {
          if (!monthCorrect(date))
            continue;
        } else {
          if (!dayOfWeekCorrect(date))
            continue;
        }
        for (Integer p : paramSelect) {
          title.set(param.get(p));
          while (dataLine.hasMoreTokens() && i < p) {
            dataLine.nextToken();
            i++;
          }
          try {
            if (!dataLine.hasMoreTokens())
              break;
            String data = dataLine.nextToken();
            i++;
            data = data.replaceAll("[^0-9.,]+", "");
            float val = Float.parseFloat(data);
            val = verifyMissing(val, p);
            vals.set(val);
            context.write(title, vals);
          } catch (NumberFormatException e) {
            break;
          }
        }
      }
    }

    public boolean dayOfWeekCorrect(String date) {
      if (dayW != 0) {
        int dw = 0;
        try {
          dw = getDayWeek(date);
        } catch (ParseException e) {
          return false;
        }
        if (dayW != dw)
          return false;
      }
      return true;
    }

    public boolean monthCorrect(String date) {
      try {
        int m = Integer.parseInt(date.substring(4, 6));
        if (m != month)
          return false;

        if (!dayOfWeekCorrect(date))
          return false;
      } catch (NumberFormatException e) {
        return false;
      }
      return true;
    }

    public int getDayWeek(String date) throws ParseException {
      Calendar c = Calendar.getInstance();
      SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
      Date dt = format.parse(date);
      c.setTime(dt);
      return c.get(Calendar.DAY_OF_WEEK);
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

  public static class StdDevReducer
      extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    private FloatWritable result = new FloatWritable();

    public void reduce(Text key, Iterable<FloatWritable> values,
        Context context) throws IOException, InterruptedException {
      ArrayList<Float> vals = new ArrayList<Float>();
      float sd = (float) Math.sqrt(getVariance(values, vals));
      result.set(sd);
      context.write(key, result);
    }

    public double getVariance(Iterable<FloatWritable> values,
        ArrayList<Float> vals) {
      float sum = 0;
      int length = 0;
      float mean = getMean(values, vals);
      for (float val : vals) {
        sum += (val - mean) * (val - mean);
        length++;
      }

      return (sum / (length - 1));
    }

    public float getMean(Iterable<FloatWritable> values,
        ArrayList<Float> vals) {
      float sum = 0;
      int length = 0;
      for (FloatWritable val : values) {
        vals.add(val.get());
        sum += val.get();
        length++;
      }
      return (sum / length);
    }
  }

  public static boolean executeMean(int m, int dw,
      String input, String output, ArrayList<Integer> par) throws Exception {
    if (new File(output).exists()) {
      return false;
    }
    Job job = initializeJob(m, dw, input, output, par);
    job.setCombinerClass(MeanReducer.class);
    job.setReducerClass(MeanReducer.class);

    job.waitForCompletion(true);
    return true;
  }

  public static boolean executeStdDev(int m, int dw,
      String input, String output, ArrayList<Integer> par) throws Exception {
    if (new File(output).exists()) {
      return false;
    }
    Job job = initializeJob(m, dw, input, output, par);
    job.setReducerClass(StdDevReducer.class);

    job.waitForCompletion(true);
    return true;
  }

  public static Job initializeJob(int m, int dw,
      String input, String output, ArrayList<Integer> par) throws Exception {
    paramSelect = par;
    initializeParams();
    month = m;
    dayW = dw;
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "hadoop");
    job.setJarByClass(Index.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FloatWritable.class);
    FileInputFormat.addInputPath(job, new Path(input));
    FileOutputFormat.setOutputPath(job, new Path(output));
    return job;
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
