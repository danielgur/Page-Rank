# Page Rank (Hadoop)

## To run on on Eclipse

Program Arguments: 'nodes' 'edges' 'output_folder' 'number_of_iterations'

Example: "nodes.txt edges.txt output 5"

## To run on EC2
Steps to run on EC2:

1.  Login to cluster
2.  Send files from local machine to remote cluster
3.  (Optional) wget http://socialcomputing.asu.edu/uploads/1296759055/Twitter-dataset.zip and unzip the Twitter dataset. 
4.  Copy your nodes and edges dataset's to HDFS
5.  To run (example), type: hadoop jar PageRank.jar nodes.txt edges.txt output 5


### Enjoy, and happy hadooping :)

