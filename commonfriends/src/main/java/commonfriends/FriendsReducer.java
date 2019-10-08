package commonfriends;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class FriendsReducer extends Reducer<Text, Text, Text, Text> {
	public static final Logger LOGGER = Logger.getLogger(FriendsReducer.class);

	public FriendsReducer() {
		LOGGER.info("FriendsReducer");
	}

	Text valueToEmit = new Text();

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		LOGGER.info("Reducer.reduce(-,-,-)");
		LOGGER.info("key: " + key);
		// key 100,200 values [ [200 300 400 500 600] [100 300 400]]
		Integer minLength = Integer.MAX_VALUE;
		String[] firstArr = {};
		String[] secondArr = {};
		StringBuilder common = new StringBuilder();
		for (Text value : values) {
			String[] tempVal = value.toString().split(" ");
			if (firstArr.length == 0)
				firstArr = tempVal;
			else
				secondArr = tempVal;
			if (minLength > tempVal.length)
				minLength = tempVal.length;
			LOGGER.info("minLength:" + minLength);
			if (secondArr.length != 0) {
				for (String felement : firstArr) {
					for (String selement : secondArr) {
						if (Integer.parseInt(felement) == Integer.parseInt(selement)) {
							common.append(felement).append(",");
						}

					}
				}
				
				
			}


		}

		LOGGER.info("Common:" + common);

//		int len = common.length();
//		StringBuilder commonFriend = common.deleteCharAt(len - 1);
		valueToEmit.set(common.toString());
		context.write(key, valueToEmit);
	}
}
