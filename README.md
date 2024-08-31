# id-ttl-ordered-storage

The repo allows doing the following:
1. Store byte payloads on a disk with a limited total storage size.
2. The repo associates each written payload with a key that can be later used to access the written value.

It was designed to solve a case where all the keys could be stored in memory, while all the payloads had to be moved to disk to limit the maximum memory used.
The keys usually got associated with some other value (larger than the key, much smaller than the payload; multiple keys were associated with a single other value) on the user side.

Writes are inexpensive: they append the given payload to an internal buffer, which gets flushed occasionally.
Reads are also cheap: the returned key allows directly accessing the original value (open + seek + read).

The implementation worked faster than embedded key-value storages for that case, yet I don't have any numbers to share.
