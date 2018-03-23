package hw5part3;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class MySecondMapper extends Mapper<Object, Text, LongWritable, Text> {
	private final static SimpleDateFormat frmt = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss");

	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		StringTokenizer line = new StringTokenizer(value.toString());
		String ip = line.nextToken();
		String strDate = line.nextToken();
		String count = line.nextToken();
		Date date = new Date();
		try {
			date = frmt.parse(strDate);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		context.write(new LongWritable(date.getTime()), new Text(ip + "   " + count));
	}

}
