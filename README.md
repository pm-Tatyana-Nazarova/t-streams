# T-streams

T-streams are scalable, transactional persistent queues for exactly-once, batch messaging.

# Introduction

T-streams (transactional streams) is a Scala library which implements transactional messaging for Producers and Consumers, which is important for CEP (complex event processing) systems that must handle an event exactly-once, at-least-once or at-most-once. T-streams implementation is inspired by Apache Kafka.

T-streams library uses next data management systems for operation:

 * Apache Cassandra – metadata backend, data backend;
 * Aerospike – data backend (preferred);
 * Redis/Apache Zookeeper – distributed lock management and pub-sub features. Redis is preferred.

Data backends are pluggable and can be developed for other key-value storages, e.g. Redis or even Ceph, AWS S3, etc.

# T-streams features

Usually developers choose Apache Kafka for storing data in data processing systems – it is robust, it scales well and fits in many cases.

We started T-streams as a replacement for Apache Kafka, which is perfect but has lack of  features which exist in T-streams. Kafka is widely used but doesn’t fit well for CEP systems where processing chain can be complex and involve kafka-to-kafka processing.

Kafka allows to place somewhere (usually Zookeeper) an offset from which consumer read data, but it doesn’t provide any mechanism for producers, as a result exactly-once processing is impossible with Kafka when it’s used in kafka-to-kafka processing.

Also, with Kafka it is impossible to start consuming from point of time natively because Kafka’s offset doesn’t match to time.

Another problem which exists in Kafka begins from the way it stores queues, it doesn’t fit well for storing millions or even billions of queues because queues are matched to separate files.

# T-streams library fits good if…

T-streams library is designed to solve the problems, mentioned before and provides following features:

 * exactly-once and at-least-once processing models when source and destination queues are T-streams;
 * gives developers the way to implement exactly-once and at-least-once processing models when source is T-streams and destination is any key-value store (or full-featured DBMS, e.g. Oracle, MySQL, Postgres, Elasticsearch);
 * is designed for batch processing where batch includes some items (events, messages, etc.);
 * scales perfectly horizontally (Aerospike clustering, Cassandra clustering);
 * scales perfectly in direction of amount of queues (supports millions of queues without problems);
 * relies on Cassandra replication and Aerospike replication for replication and consistency;
 * allows consumers read from most recent offset, arbitrary date-time offset, most ancient offset.
 * provides Kafka-like features:
    * It allows to read data by multiple consumers from one queue;
    * It allows to write data by multiple producers t one queue;
    * It allows to use streams with one partition or many partitions.
    * It stores data until expiration date-time set by stream configuration.



T-streams is free, light [ZeroMQ](http://zeromq.org/)-based Dealer-Router python server. It's developed to be run under GNU/Linux environment.

Moebius is inspired by Tornado web-server, which is high performance HTTP and WebSocket server. Like Tornado, Moebius employs the approach with python generators to create light and single-threaded server.

Name "Moebius" (Möbius) is choosen because of nature of handlers which are python generators and could be run infinitely like "Möbius stripe".

## ZeroMQ as transport

Moebius uses well known and robust ZeroMQ (0mq) library to implement transport. It uses DEALER-ROUTER 0mq pattern to implement two-way communication without specific limitations. DEALER-ROUTER pattern allows to implement three types of communication:
 
* 1 request - 1 response 
* 1 request - N responses
* 1 request - 0 responses

## Architecture

Moebius is single-threaded. It uses python generators to implement parallel processing of multiple clients. Developers can use either standard python methods to implement simple fast routines which just work or python generators to implement long-living routines which can run for seconds or minutes until completion.


## Installation from sources

To install Moebius, download the sources from the [github repository](https://github.com/bwsw/moebius) and perform the following steps:
```
# create a virtualenv.
# skip these tests if you already set up one for your project.
virtualenv moebius_venv
source moebius_venv/bin/activate

# installation
pip install .
```

## Dependencies

1. Python 2.7+ (we didn't tested before 2.7)
2. libzmq 4.0.4+
3. python-zmq library
4. Tornado ioloop (Tornado framework installed)

Please note that, actually, Moebius does not work with Python 3.x.

## Platforms

Moebius is tested and works well under next GNU/Linux distributions:

1. Ubuntu 14.04.3 LTS (all from repos)
2. [Debian 7 wheezy](https://github.com/bwsw/moebius/blob/master/tests/Debian7.md)

## Documentation

See [doc](https://github.com/bwsw/moebius/tree/master/doc) directory for documentation and [tests](https://github.com/bwsw/moebius/tree/master/examples) for examples.

## Performance

There is tests/sync_req_rep_perf.py script which should be run like (tests REQ-REP-like communication):

```bash
$ python sync_req_rep_perf.py 12
```
from tests directory, which takes argument (amount of concurrent synchronous clients to server), 12 in the sample above. 

Keep in mind, that Moebius is single-threaded so you should use HA-proxy, BalanceNG or similar software to distribute load and utilize all cores of your system.

We have benchmarked Moebius on Intel(R) Xeon(R) CPU E3-1230 V2 @ 3.30GHz and it gives us about 22K q/s for 1 server/12 clients, so for 4 servers (1 per core for Xeon E3) You should take about 70-80K q/s.

## License

Moebius is licensed under [Apache 2.0 license](https://github.com/bwsw/moebius/blob/master/LICENSE).

## Author

Moebius is created by [Bitworks Software, Ltd.](http://bw-sw.com)

mailto: bitworks (at) bw-sw.com
