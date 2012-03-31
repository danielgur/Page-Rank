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
		
		System.out.println(value);
		String[] nodes = value.toString().split(", ");
		output.collect(new Text(nodes[0]), new Text(nodes[1]));

	}
}

	/*	
		 * FIRST ITERATION	
		 
		if (PageRankDriver.isFirstIteration)
		{
			lineNum++;
			String[] nodes = value.toString().split(", ");
			Text l = new Text(nodes[0]);
			System.out.println(nodes[0]+nodes[1]);
			Text r = new Text(nodes[1]);

			if (currentL == null) // initial case
			{
				currentL = l;
			}
			else if (currentL.toString().compareTo(l.toString()) != 0) // new
																		// l-value
			{
				if (adjList.isEmpty())
					adjList.add(r);

				double page_rank = HW3Q2Driver.init_page_rank;
				output.collect(currentL, new Text(page_rank + "P" + adjList));

				for (Text node : adjList)
				{
					output.collect(node, new Text(page_rank / adjList.size()
							+ ""));
				}

				currentL = l;
				adjList.clear();
			}

			adjList.add(r);

			if (lineNum == HW3Q2Driver.line_numbers)
			{
				double page_rank = HW3Q2Driver.init_page_rank;
				output.collect(currentL, new Text(page_rank + "P" + adjList));

				for (Text node : adjList)
				{
					output.collect(node, new Text(page_rank / adjList.size()
							+ ""));
				}
			}
		}
		
		 * NOT FIRST ITERATION	
		 
		else
		{
			adjList.clear();
			
			String[] entry=value.toString().split("\t");
			String nID=entry[0];
						
			String[] pageRank_adjList = entry[1].split("P");
			String[] following=pageRank_adjList[1].substring(1,pageRank_adjList[1].length()-1).split(", ");			
			
			double  currentPageRank= Double.parseDouble(pageRank_adjList[0]);
			
			currentPageRank/=following.length;
			
			output.collect(new Text(nID), new Text(currentPageRank+"P"+pageRank_adjList[1]));
			for(int i=0;i<following.length;i++)
			{
				output.collect(new Text(following[i]), new Text(currentPageRank+""));

			}

			
			
		}
	}*/


