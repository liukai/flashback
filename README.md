# What is Flashback

Flashback is a MongoDB benchmark framework consisting of a set of scripts that:
1. records the operations(_ops_) that happens during a stretch of time;
2. replays the recorded ops.

The two parts are not necessarily coupled and can be used independently for different purposes.

# How it works

## Record

How can you know which ops are performed by MongoDB? There are a lot of ways to do this. But in Flashback, we record the ops by enabling MongoDB's [profiling](http://docs.mongodb.org/manual/reference/command/profile/).

By setting the profile level to _2_ (profile all ops), we'll be able to fetched the ops information that is detailed enough for future replay -- except for _insert_ ops.

MongoDB intentionally avoid putting insertion details in profiling results because they don't want to have the insertion being written several times. Luckily, if a MongoDB instance is working in a "replica set", then we can complete the missing information through _oplog_.

Thus, we record the ops with the following steps:

1. Script starts two threads to pull the profiling results and oplog entries for collections that we are interested in. 2 threads are working independently.
2. After fetching the entries, we'll merge the results from these two sources and have a full pictures of all the operations.

NOTE: If the oplog size is huge, fetching the first entry from oplog may take a long time (several hours). This is because oplog is unindexed. After that it will catch up with current time quickly.

## Replay

With the ops being recorded, we also have replayer to replay them in differnet ways:

* Replay ops with "best effort", which diligently sends these ops to databases as fast as possible. This style can help us to measure the limits of databases. Please note to reduce the overhead for loading ops, we'll preload the ops to the memory and replay them as fast as possible.
* Reply ops in accordance to their original timestamps, which allows us to imitate regular traffic.

The replay module is written in Go because Python doesn't do a good job in concurrent cpu intensive tasks.

# How to use it

## Record

### Prerequisites

* The "record" module is written in python. You'll need to have pymongo, mongodb's python driver, installed.
* Set MongoDB profiling level to be _2_, which captures all the ops.
* Run MongoDB in a replica set mode (even there is only one node), which allows us to access the oplog.

### Configuration

* If you are the first time user, please run `cp config.py.example config.py`.
* In `config.py`, modify it based on your need. Here are some notes:
    * We intentionally separate the servers for oplog pulling and profiling results pulling. As a good practice, it's better to pull oplog from secondaries. However profiling results must be pulled from primary server.
    * `duration_secs` indicates the length for the recording.

### Start Recording

After configuration, please simply run `python record.py`.

## Replay

### Prerequisites

Install `mgo` as it is the mongodb go driver.

### Command

    go run main.go \
        --host=<hostname:[port]> \
        --style=[real|max] \
        --ops_num=N \
        --threads=THREADS
