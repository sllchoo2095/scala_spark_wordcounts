# scala_spark_wordcounts
To execute the program please create an empty folder called input on the Hadoop File System (HDFS) on HUE. 

Then load the ``NAME_BDP_A3_2021-0.0.1-SNAPSHOT-jar-with-dependencies.jar`` onto HUE 

Use this command to download the .jar file to your local system. 

``hadoop fs -copyToLocal /user/NAME/NAME_BDP_A3_2021-0.0.1-SNAPSHOT-jar-with-dependencies.jar ~\``

If on your command line it shows >, hit ENTER again and use ls to ensure that the .jar file in in the local directory. 
Also for the following commands <input path > and <output path> do not have "/" at the end. 

Execute Task A

``spark-submit --class streaming.NAME_BDP_A3_2021.Tasks --master yarn --deploy-mode client NAME_BDP_A3_2021-0.0.1-SNAPSHOT-jar-with-dependencies.jar <input path> <output path> 1``

After putting in the command put text files into the folder input one at a time. It is expected that only rdds with data will output non-empty files. Stop the spark streaming using either control C or command C  in the command line.

Execute Task B

``spark-submit --class streaming.NAME_BDP_A3_2021.Tasks --master yarn --deploy-mode client NAME_BDP_A3_2021-0.0.1-SNAPSHOT-jar-with-dependencies.jar <input path> <output path> 2``

After putting in the command put text files into the folder input one at a time. It is expected that only rdds with data will output non-empty files. Stop the spark streaming using either control C or command C  in the command line.

Execute Task C

``spark-submit --class streaming.NAME_BDP_A3_2021.Tasks --master yarn --deploy-mode client NAME_BDP_A3_2021-0.0.1-SNAPSHOT-jar-with-dependencies.jar <input path> <output path> 3``

After putting in the command put text files into the folder input one at a time. Wait for 5 minutes after putting the last file into the input folder
There may be some empty folders. When there are multiple files in the input folder, this program finds all the common word pairs within all documents. If you only want to see the common wordpairs in all documents only downlaod the last file. 

