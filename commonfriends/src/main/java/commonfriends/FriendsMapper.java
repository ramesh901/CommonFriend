package commonfriends;
import java.io.IOException;
//import java.util.logging.Logger;
import java.util.StringTokenizer;

import org.apache.log4j.Logger;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Mapper.Context;


public class FriendsMapper extends Mapper<LongWritable,Text,Text,Text> {
	private static final Logger LOGGER = Logger.getLogger(FriendsMapper.class);
	
 public FriendsMapper() {
	 LOGGER.info("FriendsMapper()");
 }
 //private final static IntWritable one = new IntWritable(1);
 private Text word = new Text();
	
 
 public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException {
	 LOGGER.info("mapper.map(-,-,-)");
	 String data[] = value.toString().split(",");
	 //value = 100, 200 300 400 500 600
	 String id = data[0];
	 //int temp = Integer.parseInt(data[1]);
	 //context.write(new Text(year), new IntWritable(temp));
	 
	 /////////////////
	 StringTokenizer itr = new StringTokenizer(data[1].toString());
	 int idInt = Integer.parseInt(id);
		while(itr.hasMoreTokens()) {
			int nextToken = Integer.parseInt(itr.nextToken());
			if(idInt < nextToken)
				word.set(id + "," + nextToken);
			else
				word.set(nextToken + "," + id);
		
			LOGGER.info(word + "####" + data[1].toString());
			context.write(word,new Text(data[1].toString()));
		}
 }
 
}
