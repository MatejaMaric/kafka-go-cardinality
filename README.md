# About this project:

This project is a solution to the [Data Engineer Challenge](https://github.com/tamediadigital/hiring-challenges/tree/master/data-engineer-challenge).
It sends statistics to the output topic every 5 seconds and the counting error margin is less than 1%.

## How to run this project:

You can simply use `go run`, or if you want you can use Docker.
If you are using Docker, just make sure that the container has access to the host network so it can use the local Kafka instance.
There are a few environment variables that this project uses, but they have sensible defaults, so you don't have to bother with them.
Here's the list of the used environment variables and their default values:

```
KAFKA_BROKER=localhost:9092
USERS_TOPIC=users
STATS_TOPIC=stats
```

## My thought process while making this project:

First thing I did was research what algorithms and data structures I should use, deciding on HyperLogLog for counting.

After that I was trying to figure out how to efficiently aggregate stream windows that are constantly changing (last minute, day, week...).
I didn't find much information from search engine results, so I turned to Slack and IRC communities.
There I got recommendations to use [ksqlDB](https://ksqldb.io/), [ClickHouse](https://clickhouse.com/) or [Kafka Streams](https://kafka.apache.org/documentation/streams/).
Since they are pre-built solutions and the challenge requirement is to not use any stream-processing frameworks, I continued my search on the Web.
Later, I found a video presentation about [efficient stream window aggregation](https://www.youtube.com/watch?v=K1y5dJvP1jM),
which made me realise that there probably aren't any windowing solutions that I could easily implement.

After, that I started prototyping.

At first, I wanted to use Redis for storing my HyperLogLogs, because I didn't want to use any random package from GitHub.
I wanted my service to access a local Kafka instance and a Redis container, however Docker Compose networking options didn't really want to cooperate with me.
And I wasted quite a lot of time trying to fix that.
In the end, I decided to not use Docker Compose at all, but to deploy Redis as a standalone container.
I still don't know what caused the issue, and my best guess is that it had something to do with the fact that I'm using Apple M1...
After I finished writing the first prototype, I realised that I'm wasting way too much time with network calls to Redis.
Then I decided to simply use some third-party HyperLogLog package and be done with it, effectively nullifying hours and hours of work...

Now in retrospective, I guess, I could have just added some buffering option to Redis client.
I don't regret scraping the Redis idea given that using native Go library is not only faster, but it also simplifies entire project setup and reduces boilerplate.

When it comes to the code itself, there is a context I use for cancellation propagation when the user sends ^C (interrupt) or terminates the program.
There are two functions for initializing and closing Kafka client, respectively.
There are two goroutines.
One for reading events from input Kafka topic and inserting them into a map of minutes as keys and HyperLogLog pointers as values.
Other goroutine for aggregating minutes and sending them to output Kafka topic once every 5 seconds.
Basically, every time an event arrives, the service calculates to which minute it belongs (`ts-(ts%60)`) and writes its `uid` to the corresponding HyperLogLog.
Every 5s services goes through all currently tracked minutes, checks if they are in a corresponding time range, aggregates them and sends the response to a Kafka topic.

In conclusion, I learned quite a bit from this project.
It was both interesting and quite challenging, given that I never before worked with Kafka and stream-processing.
