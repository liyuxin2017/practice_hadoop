package hw5part3;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class MySecondReducer extends Reducer<LongWritable, Text, Text, Text> {
	private final static SimpleDateFormat frmt = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss");

	@Override
	protected void reduce(LongWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		Date date = new Date(key.get());
		String strDate = frmt.format(date);
		for (Text value : values) {
			context.write(value, new Text(strDate));
		}
	}

}
