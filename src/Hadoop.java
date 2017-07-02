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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
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
  private static int y1 = 0;
  private static int y2 = 0;

  public static class TokenizerMapper
      extends Mapper<Object, Text, Text, MapWritable> {

    private final MapWritable vals = new MapWritable();
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
            int ano = Integer.parseInt(date.substring(0, 4));
            String data = dataLine.nextToken();
            i++;
            data = data.replaceAll("[^0-9.,]+", "");
            float val = Float.parseFloat(data);
            val = verifyMissing(val, p);
            vals.put(new IntWritable(ano), new FloatWritable(val));
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
      extends Reducer<Text, MapWritable, Text, MapWritable> {
    private MapWritable result = new MapWritable();

    public void reduce(Text key, Iterable<MapWritable> values, Context context)
        throws IOException, InterruptedException {
      HashMap<Integer, Float> sum = new HashMap<Integer, Float>();
      HashMap<Integer, Integer> length = new HashMap<Integer, Integer>();
      int year1 = 0;
      for (MapWritable val : values) {
        year1 = y1;
        while (year1 <= y2) {
          if (val.get(new IntWritable(year1)) == null) {
            year1++;
            continue;
          }
          float value = ((FloatWritable) val.get(new IntWritable(year1))).get();

          if (sum.get(year1) == null) {
            sum.put(year1, value);
            length.put(year1, 1);
          } else {
            sum.put(year1, sum.get(year1) + value);
            length.put(year1, length.get(year1) + 1);
          }
          year1++;
        }
      }

      year1 = y1;
      while (year1 <= y2) {
        if (sum.get(year1) == null) {
          year1++;
          continue;
        }
        result.put(new IntWritable(year1),
            new FloatWritable((sum.get(year1) / length.get(year1))));
        year1++;
      }
      context.write(key, result);
    }
  }

  public static class StdDevReducer
      extends Reducer<Text, MapWritable, Text, MapWritable> {
    private MapWritable result = new MapWritable();

    public void reduce(Text key, Iterable<MapWritable> values,
        Context context) throws IOException, InterruptedException {
      HashMap<Integer, Float> sum = new HashMap<Integer, Float>();
      HashMap<Integer, Integer> length = new HashMap<Integer, Integer>();
      HashMap<Integer, ArrayList<Float>> vals =
          new HashMap<Integer, ArrayList<Float>>();

      int year1 = 0;

      for (MapWritable val : values) {
        year1 = y1;
        while (year1 <= y2) {
          if (val.get(new IntWritable(year1)) == null) {
            year1++;
            continue;
          }
          float value = ((FloatWritable) val.get(new IntWritable(year1))).get();
          if (sum.get(year1) == null) {
            sum.put(year1, value);
            length.put(year1, 1);
            vals.put(year1, new ArrayList<Float>());
            vals.get(year1).add(value);
          } else {
            sum.put(year1, sum.get(year1) + value);
            length.put(year1, length.get(year1) + 1);
            vals.get(year1).add(value);
          }
          year1++;
        }
      }

      year1 = y1;
      while (year1 <= y2) {
        if (sum.get(year1) == null) {
          year1++;
          continue;
        }
        double mean = (sum.get(year1) / length.get(year1));
        float std =
            (float) Math.sqrt(getVariance(vals.get(year1), mean, year1));
        result.put(new IntWritable(year1), new FloatWritable(std));
        year1++;
      }

      context.write(key, result);
    }

    public Double getVariance(ArrayList<Float> vals, double mean, int y) {
      double sum = 0;
      int length = 0;
      for (float val : vals) {
        sum += (val - mean) * (val - mean);
        length++;
      }
      return (sum / (length - 1));
    }

  }
  
  public static class MinimumSqrReducer
  extends Reducer<Text, MapWritable, Text, MapWritable> {
	private MapWritable result = new MapWritable();
	
	public void reduce(Text key, Iterable<MapWritable> values,
	    Context context) throws IOException, InterruptedException {
		HashMap<Integer, Float> sum = new HashMap<Integer, Float>();
	      HashMap<Integer, Integer> length = new HashMap<Integer, Integer>();
	      HashMap<Integer, ArrayList<Float>> vals =
	          new HashMap<Integer, ArrayList<Float>>();

	      for (MapWritable val : values) {    	  
	          int year = y1;
	  	      Writable entry = val.get(new IntWritable(year));
	  	      
	  	      if (entry == null)
	  	      {
	  	    	  year = y2;
	  	    	  entry = val.get(new IntWritable(year));
	  	      }
	  	      
	  	      if( entry == null)
	  	      {
	  	    	  continue;
	  	      }
	  	      
	  	      float value = ((FloatWritable) entry).get();
	  	      if (sum.get(year) == null) {
	  	        sum.put(year, value);
	  	        length.put(year, 1);
	  	        vals.put(year, new ArrayList<Float>());
	  	        vals.get(year).add(value);
	  	      } else {
	  	        sum.put(year, sum.get(year) + value);
	  	        length.put(year, length.get(year) + 1);
	  	        vals.get(year).add(value);
	  	      }
	      }

	      
	      float year1Mean = (sum.get(y1) / length.get(y1));
	      float year2Mean = (sum.get(y2) / length.get(y2));
	      float minimumSquareB =
	          (float) computeMinimumSquareB(vals.get(y1), vals.get(y2), year1Mean, year2Mean);
	        
	      float minimumSquareA = year2Mean - minimumSquareB * year1Mean;
	        
	      result.put(new FloatWritable(minimumSquareB), new FloatWritable(minimumSquareA));

	      context.write(key, result);
	}
	
	public float computeMinimumSquareB(ArrayList<Float> y1Vals, ArrayList<Float> y2Vals, float y1Mean, float y2Mean)
	{
		float sum1 = 0;
		float sum2 = 0;
		
		int lenght = Math.min(y1Vals.size(), y2Vals.size());
		
		for(int i = 0; i < lenght; i++)
		{
			Float x = y1Vals.get(i);
			Float y = y2Vals.get(i);
			sum1 += x * (y - y2Mean);
			sum2 += x * (x - y1Mean);
		}
		
		return sum1/sum2;
	}
	
	public Double getVariance(ArrayList<Float> vals, double mean, int y) {
	  double sum = 0;
	  int length = 0;
	  for (float val : vals) {
	    sum += (val - mean) * (val - mean);
	    length++;
	  }
	  return (sum / (length - 1));
	}
	
  }
  
  public static boolean executeMinimumSquare(int m, int dw, int year1, int year2, String input,
		  String output, ArrayList<Integer> par) throws Exception
  {
	  if(new File(output).exists()) {
		  return false;
	  }
	  
	  //Job for year1
	  Job job1 = initializeJob(m, dw, year1, year2, input, output , par);
	  job1.setReducerClass(MinimumSqrReducer.class);
	  
	  job1.waitForCompletion(true);
	  
	  return true;
  }

  public static boolean executeMean(int m, int dw, int year1, int year2,
      String input, String output, ArrayList<Integer> par) throws Exception {
    if (new File(output).exists()) {
      return false;
    }
    Job job = initializeJob(m, dw, year1, year2, input, output, par);
    job.setReducerClass(MeanReducer.class);

    job.waitForCompletion(true);
    return true;
  }

  public static boolean executeStdDev(int m, int dw, int year1, int year2,
      String input, String output, ArrayList<Integer> par) throws Exception {
    if (new File(output).exists()) {
      return false;
    }
    Job job = initializeJob(m, dw, year1, year2, input, output, par);
    job.setReducerClass(StdDevReducer.class);

    job.waitForCompletion(true);
    return true;
  }

  public static Job initializeJob(int m, int dw, int year1, int year2,
      String input, String output, ArrayList<Integer> par) throws Exception {
    paramSelect = par;
    initializeParams();
    month = m;
    dayW = dw;
    y1 = year1;
    y2 = year2;
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "hadoop");
    job.setJarByClass(Index.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(MapWritable.class);
    while (year1 <= y2) {
      if (!new File(input + "/" + year1).exists()) {
        continue;
      }
      FileInputFormat.addInputPath(job, new Path(input + "/" + year1));
      year1++;
    }
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

