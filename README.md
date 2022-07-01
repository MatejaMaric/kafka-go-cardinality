# About this project:

This project is a solution to the [Data Engineer Challenge](https://github.com/tamediadigital/hiring-challenges/tree/master/data-engineer-challenge).
It sends statistics to the output topic and the counting error margin is less than 1%.
It doesn't deal with latency, but it does work with historically data (you simply re-run it on a Kafka topic, since the entire point is for it to be stateless).

The original solution that got me a job is at commit: `56103583efeb057546bd402882908f7a0fda9ba2`

The current and improved solution is for my personal satisfaction...

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
