import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class PageRankSecondReducer extends MapReduceBase implements Reducer<Object, Text, Text, Text>
{
	boolean valid=true;
	ArrayList<Text> adjList=new ArrayList<Text>();	
	long NODE_COUNT;

	public void reduce(	Object _key, 
						Iterator<Text> values,
						OutputCollector<Text, Text> output, 
						Reporter reporter) throws IOException
	{
		double pageRankSum=0;
		String pagesFollowing="";
		while (values.hasNext())
		{
			Text value = (Text) values.next();
			if(value.toString().contains("P"))
			{
				String [] data= value.toString().split("P");
				pagesFollowing = data[1];
			}
			else
			{
				pageRankSum+=Double.parseDouble(value.toString());
			}
		}
		
		//This is to check for dangling nodes
		if(pagesFollowing.length()>=1)
			output.collect(new Text(_key+""),new Text(pageRankSum+"P"+pagesFollowing));
		else if(_key.toString().compareTo("")!=0&&_key!=null)	
			output.collect(new Text(_key+""),new Text(pageRankSum+"P[]"));

	}
}
