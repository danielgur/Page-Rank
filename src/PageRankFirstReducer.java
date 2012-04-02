import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class PageRankFirstReducer extends MapReduceBase implements
		Reducer<Object, Text, Text, Text>
{
	ArrayList<Text> adjList = new ArrayList<Text>();
	long NODE_COUNT;

	public void reduce(Object _key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException
	{
		adjList.clear();

		while (values.hasNext())
		{
			adjList.add(new Text(((Text) values.next()).toString()));
		}
		
		output.collect(new Text(_key + ""), new Text(1.0 / NODE_COUNT + "P"+ adjList.toString()));
	}

	public void configure(JobConf job)
	{
		NODE_COUNT = Long.parseLong(job.get("NODE_COUNT"));
	}
}
