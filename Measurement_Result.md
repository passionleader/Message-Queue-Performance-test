# Result of measurement
## description
* Some message transfer protocols did not provided wanted data type(json, dataframe...) 
* When data is transmitted by message transfer protocol, performance is mostly better than when direct input using MongoDB Client's own TCP/IP.
* In particular, it can be seen that sending data in JSON format through Apache Kafka, which supports multi-brokers, has the best performance.
* Except Redis, Most Message protocols shown that sending data in JSON format is more efficient than sending byte-serialized data.
* However, since compression is possible when serialized in Byte format, it is expected that the larger the size of data to be sent at one time, the more advantageous
## result chart
![image](https://user-images.githubusercontent.com/55945939/144736063-a8234aa7-5f08-4592-b893-673b67578e1b.png)
![image](https://user-images.githubusercontent.com/55945939/144736064-3f58e57c-01b4-445d-bb4e-f7ec04434a08.png)
