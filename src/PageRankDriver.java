import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.GenericOptionsParser;

@SuppressWarnings("deprecation")
public class PageRankDriver
{
	public static void main(String[] args) throws IOException
	{
		if (args.length != 4||Integer.parseInt(args[3])<0)
		{
			System.err.println("Usage: PageRank <nodes path> <edges path> <output path> <# of iterations> ");
			System.exit(-1);
		}

		/*
		 * Here we do the initial iteration of the edges
		 */
		JobConf conf = new JobConf(PageRankDriver.class);

		FileSystem fs = LocalFileSystem.getLocal(conf);

		System.out.println(fs.getWorkingDirectory().toString());

		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		/*
		 * Get the number of nodes
		 */
		FSDataInputStream nodesFile = fs.open(new Path(otherArgs[0]));
		int count = 0;
		while (nodesFile.readLine() != null)
		{
			count++;
		}
		conf.set("NODE_COUNT", count + "");

		//delete old output folder
		fs.delete(new Path(otherArgs[2]),true);

		conf.setJobName("PageRank");

		FileOutputFormat.setOutputPath(conf, new Path(otherArgs[2]));
		FileInputFormat.setInputPaths(conf, new Path(otherArgs[1]));

		conf.setMapperClass(PageRankFirstMapper.class);
		conf.setReducerClass(PageRankFirstReducer.class);
		
		//so ec2 doesnt compress the files
		conf.setBoolean("mapred.output.compress", false);
		conf.setJarByClass(PageRankDriver.class);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setJarByClass(PageRankDriver.class);
		//For ec2 use:
		//conf.setJar(fs.getWorkingDirectory().toString()+"/PageRank.jar");
		JobClient.runJob(conf);

		/*
		 * Now onto the next few iterations
		 */
		
		//For ec2 use:
		//fs.rename(new Path(otherArgs[2]+"/part-00000"), new Path("/temp.txt"));
		fs.rename(new Path(otherArgs[2]+"/part-00000"), new Path("temp.txt"));
		fs.delete(new Path(otherArgs[2]),true);
		
		for (int i = 0; i < Integer.parseInt(otherArgs[3]); i++)
		{
			JobConf confNext = new JobConf(PageRankDriver.class);

			confNext.setJobName("PageRank");
			confNext.setBoolean("mapred.output.compress", false);

			FileOutputFormat.setOutputPath(confNext, new Path(otherArgs[2]));
			//For ec2 use:
			//FileInputFormat.setInputPaths(confNext, new Path("/temp.txt"));
			FileInputFormat.setInputPaths(confNext, new Path("temp.txt"));
			//For ec2 use:
			//confNext.setJar(fs.getWorkingDirectory().toString()+"/PageRank.jar");

			confNext.setMapperClass(PageRankSecondMapper.class);
			confNext.setReducerClass(PageRankSecondReducer.class);

			confNext.setOutputKeyClass(Text.class);
			confNext.setOutputValueClass(Text.class);

			confNext.setJarByClass(PageRankDriver.class);

			JobClient.runJob(confNext);
			//For ec2 use:
			//fs.rename(new Path(otherArgs[2]+"/part-00000"), new Path("/temp.txt"));
			fs.rename(new Path(otherArgs[2]+"/part-00000"), new Path("temp.txt"));
			fs.delete(new Path(otherArgs[2]),true);
		}
	}
}
