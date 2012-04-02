import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class PageRankSecondMapper extends MapReduceBase implements
		Mapper<Object, Text, Text, Text>
{
	public void map(Object key, Text value, OutputCollector<Text, Text> output,
			Reporter reporter) throws IOException
	{
		String[] entry = value.toString().split("\t");
		String nID = entry[0];
		if (nID.toString().trim().equals(""))
			return;

		String[] pageRank_adjList = entry[1].split("P");
		String[] following = pageRank_adjList[1].substring(1,pageRank_adjList[1].length() - 1).split(", ");

		double currentPageRank = Double.parseDouble(pageRank_adjList[0]);

		currentPageRank /= following.length;

		output.collect(new Text(nID), new Text(currentPageRank + "P" + pageRank_adjList[1]));

		for (int i = 0; i < following.length; i++)
		{
			output.collect(new Text(following[i]), new Text(currentPageRank + ""));
		}
	}
}