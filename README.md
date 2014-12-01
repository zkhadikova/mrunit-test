# Task definition

1. Create map-reduce job to count number of tracks listened by user.
2. Create MRUnit (https://mrunit.apache.org/) tests for created job.
3. Process sample input file and print results as: user_id: tracks_count.

# Usage

To run project tests run:

    mvn clean test

To generate html report of test result run:

    mvn clean surefire-report:report

# MR Unit features

-Test Map and Reduce separately.
-Test Map-Reduce job altogether.
-Test series of Map-Reduce jobs.
-Test counters.
	
# Benefits of using MR Unit

-Faster testing (no I/O required).
-No input/output files required(everything can be programatically specified).
-Less test harness code.

# Problems with MR Unit

-Lacking documentation.
-Tests are not executed in distributed way.
	
# Links

MR Unit:
https://mrunit.apache.org/ 

Javadoc
https://mrunit.apache.org/documentation/javadocs/1.1.0/index.html 

MRUnit Tutorial
https://cwiki.apache.org/confluence/display/MRUNIT/MRUnit+Tutorial