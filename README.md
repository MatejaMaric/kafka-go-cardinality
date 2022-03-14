# About this project:

This project is a solution to the [Data Engineer Challenge](https://github.com/tamediadigital/hiring-challenges/tree/master/data-engineer-challenge).
It sends statistics to the output topic every 5 seconds and the counting error
margin is less than 1%.

## How to run this project:

You can simply use `go run`, or if you want you can use Docker.  If you are
using Docker, just make sure that the container has access to the host network
so it can use the local Kafka instance. There are a few environment variables
that this project uses, but they have sensible defaults, so you don't have to
bother with them. Here's the list of the used environment variables and their
default values:

```
KAFKA_BROKER=localhost:9092
USERS_TOPIC=users
STATS_TOPIC=stats
```
