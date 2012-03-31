import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class PageRankFirstMapper extends MapReduceBase implements
		Mapper<Object, Text, Text, Text>
{

	ArrayList<Text> adjList = new ArrayList<Text>();
	Text currentL;
	int lineNum = 0;

	public void map(Object key, Text value, OutputCollector<Text, Text> output,
			Reporter reporter) throws IOException
	{
		String[] nodes = value.toString().split(", ");
		output.collect(new Text(nodes[0]), new Text(nodes[1]));
	}
}