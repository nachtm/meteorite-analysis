import java.io.IOException;
import java.io.StringReader;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.IntWritable;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

public class MaxMassMapper 
		extends Mapper<LongWritable, Text, IntWritable, Text>{

	private static final int NAME_INDEX = 0;
	private static final int MASS_INDEX = 4;
	private static final IntWritable ONE = new IntWritable(1);
	private static final IntWritable TWO = new IntWritable(2);
	private static final Text ID = new Text("ID");

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		CSVParser parser = new CSVParser(new StringReader(value.toString()), CSVFormat.DEFAULT);
		CSVRecord line = parser.getRecords().get(0);
		String name = line.get(NAME_INDEX);
		String massStr = line.get(MASS_INDEX);

		// try{
		if(!(massStr.contains("mass (g)") || massStr.isEmpty())){
			double mass = Double.parseDouble(massStr);
			context.write(ONE, new Text(String.valueOf(mass)));
		// } else{
			// System.out.println(name);
		// } catch(NumberFormatException e) {
		} 
		// else {
		// String toWrite = name + "|" + massStr;
		// context.write(TWO, new Text(toWrite));
		// }
	}
}