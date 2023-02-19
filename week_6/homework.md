## **Q1**: Which option allows Kafka to scale?
Partitions

## **Q2**: Which option provide fault tolerance to kafka?
Replication

## **Q3**: What is a compact topic?
Compact topic in Apache Kafka refers to a feature that allows the deletion of older records in a topic, while retaining the latest version of the records with the same key. This helps to reduce the size of the topic, which can be particularly useful when working with high-volume data streams where storage space is limited.

With compacted topics, Kafka retains only the latest value for each key and discards older values. This means that if there are multiple records in the topic with the same key, only the most recent one will be kept. The compaction process happens in the background and is performed by the broker, freeing up space and making it possible to manage large amounts of data efficiently.

Compacted topics are often used in scenarios where the data is constantly changing, and it is more important to have the latest value for each key, rather than a complete history of all values. However, it's important to note that once a record is deleted as a result of compaction, it is not recoverable, so it's crucial to ensure that the compaction process is properly configured to avoid data loss.

## **Q4**: Role of schemas in Kafka
The schema in Apache Kafka defines the structure of the data in topics, ensuring consistency and integrity of the data being produced, consumed, and stored. It allows for data serialization and deserialization, enabling efficient processing and seamless interaction between applications using different data formats. Schemas can be defined using languages like Avro or JSON Schema and can evolve over time.

## **Q5**: Which configuration should a producer set to provide guarantee that a message is never lost?
To guarantee that a message is never lost in Apache Kafka, you should set the following configurations on the producer:

acks: This configuration determines the number of acknowledgements the producer requires the broker to receive before considering a write to be successful. Setting acks to all ensures that the message is written to all in-sync replicas before it is considered sent.

min.insync.replicas: This configuration sets the minimum number of in-sync replicas that must receive the message for it to be considered as written. When combined with acks=all, it ensures that the message is not lost even if a broker fails.

retries: The producer can retry sending a message if the write fails due to a temporary error, such as a network issue. By setting retries to a non-zero value, you can ensure that the message will be retried until it is successfully written.

compression.type: Enabling compression can help to reduce the network bandwidth used by the producer, but it can also slow down the write speed. To ensure that the message is written as quickly as possible, you may want to set compression.type to none.

By setting these configurations, you can ensure that the message is never lost and that it is written to the broker as quickly as possible. However, it is important to note that these configurations can also have a trade-off with performance, so you may need to balance the need for reliability with the requirement for high performance.

## **Q6**: From where all can a consumer start consuming messages from?
A consumer in Apache Kafka can start consuming messages from a specific point in the topic's partition, depending on the value of the auto.offset.reset configuration. The options are:

earliest: The consumer will start consuming messages from the earliest offset in the partition, effectively re-reading all the data in the partition.

latest: The consumer will start consuming messages from the latest offset in the partition, effectively only reading new messages that are produced after the consumer starts.

none: If the consumer tries to consume from an offset that does not exist, it will receive an error. This is useful when you want to ensure that the consumer only consumes from a specific offset that you know exists.

By default, auto.offset.reset is set to latest, which means that the consumer will start consuming from the latest offset in the partition. However, if you want the consumer to start consuming from a specific offset, you can set auto.offset.reset to none and then seek to the desired offset using the seek method on the consumer.

## **Q7**: What key structure is seen when producing messages via 10 minutes Tumbling window in Kafka stream?
In Apache Kafka streams, a tumbling window is a fixed-size, non-overlapping time-based window that slides forward in time by a specified interval. When producing messages via a 10-minute tumbling window, the key structure that is seen depends on how the data is being processed in the stream.

If the data is being aggregated or grouped by a specific key, then the key structure of the output messages produced by the window will be based on the key used for the aggregation or grouping. For example, if you're aggregating data by user ID, then the key in the output messages will be the user ID.

If the data is not being aggregated or grouped by a specific key, then the key structure of the output messages produced by the window can be a composite key that includes the window start time and any other relevant information, such as the partition and topic. This composite key can be used to uniquely identify the messages produced by a specific window and allows for easy partitioning of the data.

In either case, the key structure is determined by the processing logic in the stream and can be customized to meet the specific needs of your use case. The choice of key structure can impact the efficiency and scalability of the stream processing, so it is important to carefully consider the key structure when designing a Kafka streams application.

## **Q8**: Benefit of Global KTable?
A Global KTable in Apache Kafka Streams is a representation of a materialized view of a stream that is available to all instances of a Kafka Streams application in a cluster. Some of the benefits of using a Global KTable are:

Scalability: Global KTables are stored in a distributed manner across the cluster, allowing for horizontal scaling as the volume of data increases.

Consistency: Global KTables provide a consistent view of the data to all instances of the Kafka Streams application, even if the data is being updated by multiple instances at the same time.

Performance: Global KTables are optimized for fast lookups, which makes them ideal for use cases where you need to perform lookups on large amounts of data, such as in an e-commerce application where you need to retrieve information about a product.

Ease of use: Global KTables can be created and updated using simple stream processing operations, which makes it easy to implement complex data processing logic and to update the data as it changes.

Overall, the use of Global KTables can greatly improve the performance, scalability, and consistency of a Kafka Streams application. It allows you to store and access large amounts of data in a distributed and scalable manner, making it ideal for use cases where you need to perform lookups on large amounts of data.

## **Q9**: When joining KTable with KTable partitions should be...
When joining two KTables in Apache Kafka Streams, the partitions of the input KTables should be co-partitioned in order to ensure that records with the same key are processed by the same stream task. This is important because join operations in Kafka Streams only work within a single partition, and if the input KTables are not co-partitioned, some keys may not be present in the join output.

Co-partitioning can be achieved by either repartitioning the input KTables or by ensuring that they have the same partitioning scheme when they are created. Repartitioning can be done using operations such as through, groupByKey, or repartition on the KStream.

If you are joining a KTable with a KStream, the KStream should be grouped by the join key and repartitioned so that it has the same number of partitions as the KTable. This can be done using the groupByKey or repartition operations.

In summary, to ensure that a KTable join is performed correctly, the input KTables should be co-partitioned based on the join key. This can be achieved through repartitioning or by ensuring that they have the same partitioning scheme when they are created.

## **Q10**: When joining KStream with Global KTable partitions should be...
When joining a KStream with a Global KTable in Apache Kafka Streams, the partitions of the KStream should be co-partitioned with the Global KTable. This is because the Global KTable is distributed across all instances of the Kafka Streams application and is optimized for fast lookups, so it is important to ensure that records with the same key are processed by the same instance of the application.

## **Q11**: (Attach code link) Practical: Create two streams with same key and provide a solution where you are joining the two keys