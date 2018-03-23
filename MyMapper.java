package hw5part3;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<Object, Text, IPAndDate, IntWritable> {
	private IPAndDate result;
	private IntWritable one = new IntWritable(1);
	private final static SimpleDateFormat frmt = new SimpleDateFormat("[dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH);

	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		result = new IPAndDate();
		String[] line = value.toString().split(" ");
		result.setIp(line[0]);
		String strDate = line[3];
		try {
			result.setAccessDate(frmt.parse(strDate));
		} catch (ParseException e) {
			e.printStackTrace();
		}
		context.write(result, one);
	}
}
