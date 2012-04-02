import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class PageRankFirstMapper extends MapReduceBase implements
		Mapper<Object, Text, Text, Text>
{
	/*
	 * Building the adjacency list
	 * (non-Javadoc)
	 * @see org.apache.hadoop.mapred.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
	 */
	public void map(Object key, Text value, OutputCollector<Text, Text> output,
			Reporter reporter) throws IOException
	{
		String[] nodes = value.toString().split(",");
		output.collect(new Text(nodes[0].trim()), new Text(nodes[1].trim()));
	}
}