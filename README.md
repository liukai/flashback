# What is Flashback

Flashback is a MongoDB benchmark framework consisting of a set of scripts that:
1. records the operations(_ops_) that happens during a stretch of time;
2. replays the recorded ops [Completed but in an immature status].

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

[Currently the replay scripts are having some performnce issues. I wrote a Go version of it but haven't made it public]

# How to use it

1. Set MongoDB profiling level to be _2_, meaning capture all the ops.

## Example
TBD

## Dependencies

* pymongo
