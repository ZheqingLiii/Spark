README

1. open project in your IDE, such as IntelliJ
2. change directory path for saving results in line 52 and 63
	batch size can be changed to a different time, current one is 5s
3. open a new terminal window, generate data-generator.jar in port 9999
	e.g. java -jar data-generator.jar --destIPaddress 127.0.0.1 --destPortNumber 9999 --transmissionTime 31 --transmissionRate 10
4. execute src/main/java/assignment4/DOctect.java
	OR src/main/java/assignment/SlidingWindow.java

output:
(hours:minutes, avg/max: value)
