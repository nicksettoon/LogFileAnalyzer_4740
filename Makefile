DIR=LastRequest

all: # displays this file
	cat ./Makefile

lastrequest: # runs the main lastrequest job and displays results
	-make cleanreq 2> /dev/null
	-./commands.sh ${DIR}
	hdfs dfs -cat ${DIR}/part-r-00000 

cleanreq: # cleans up previous lastrequest run
	hdfs dfs -rm -r ${DIR} 2> /dev/null