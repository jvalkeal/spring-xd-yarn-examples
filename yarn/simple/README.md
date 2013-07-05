Spring XD Yarn Simple Example
=============================

This example demonstrates how to deploy spring-xd as a Hadoop Yarn managed application.

Yarn managed application will fire up Spring XD Admin as Application master and one container as Yarn managed container.

# Prerequisites for Running Application in Hadoop

Build depends on master code from spring-hadoop, spring-xd and testing code from github.com/jvalkeal/spring-hadoop/tree/SHDP-140-mr-compat branch.

If application is run on a real Hadoop cluster file dependencies needs to be copied into HDFS. Dependent files are resolved during the test phase.

		$hadoop/bin/hdfs dfs -copyFromLocal build/libs/* /xd
		$hadoop/bin/hdfs dfs -copyFromLocal build/dependency-libs/* /xd
		

# Application Unit Testing

To run test of this example, open a command window, go to the the spring-xd-yarn-examples root directory, and type:

		./gradlew clean yarn-xd-examples-common:yarn-xd-examples-simple:build
		
If test succeeds you should be able to see timestamps logged in container log:

```
$ pwd
/repos/spring-xd-yarn-examples/yarn/simple
$ find target | grep std
target/yarn--1502101888/yarn--1502101888-logDir-nm-0_0/application_1373043225189_0001/container_1373043225189_0001_01_000002/Container.stdout
target/yarn--1502101888/yarn--1502101888-logDir-nm-0_0/application_1373043225189_0001/container_1373043225189_0001_01_000002/Container.stderr
target/yarn--1502101888/yarn--1502101888-logDir-nm-0_0/application_1373043225189_0001/container_1373043225189_0001_01_000001/Appmaster.stderr
target/yarn--1502101888/yarn--1502101888-logDir-nm-0_0/application_1373043225189_0001/container_1373043225189_0001_01_000001/Appmaster.stdout

$ cat target/yarn--1502101888/yarn--1502101888-logDir-nm-0_0/application_1373043225189_0001/container_1373043225189_0001_01_000002/Container.stdout
...
2013-07-05 17:56:25,581 WARN [LoggingHandler] - 2013-07-05 17:56:25
2013-07-05 17:56:26,591 WARN [LoggingHandler] - 2013-07-05 17:56:26
2013-07-05 17:56:27,605 WARN [LoggingHandler] - 2013-07-05 17:56:27
...
```


# Running Application in Hadoop

To run this example, open a command window, go to the the spring-xd-yarn-examples root directory, and type:

		./gradlew -q run-yarn-xd-examples-simple

Follow the application logs on a Hadoop cluster. After application is running execute request to add ticktock:

		$curl -X PUT -H 'Content-Type: text/plain' -d "time | log" http://localhost:8282/streams/ticktock

After this you should be able to see timestamp messages on a container log. Application is not killed automatically so you need to kill it manually.
	
