Author: @mkhullar

=====Versions=====
Java: javac 1.8.0_151

=====Dependencies=====
Hadoop: hadoop-core-1.2.1.jar

EquiJoin using Map-Reduce
Java and Hadoop Framework are used.

The code takes three inputs:
1) Name of the Job
2) hdfs location of the input file.(the file on which equijoin is to be performed)
3) hdfs location of the output file.(the file on result of equijoin performed is stored)

Execution:
Add the hadoop dependencies.
Build Artifact to generate Jar and execute the Jar with appropriate inputs.

Code Approach taken :

Map:
1) Read the input from the hdfs input file
2) Line by line and sets the key value pairs
3) The key is Joining Key and the value has the relation name and sets key and value.

Reduce:

1) Separates R tuples and Relation S tuples based on relation name.
2) Based on joining key,the tuples of S are added to tuples of R for the matching Key.
3) Output of EquiJoin is written in output hdfs file.
