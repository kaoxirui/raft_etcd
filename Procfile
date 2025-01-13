# Use goreman to run `go get github.com/mattn/goreman`
#node1: ./build/raft-kv/raft-kv --id 1 --cluster=127.0.0.1:12379,127.0.0.1:22379 --port 63791
#node2: ./build/raft-kv/raft-kv --id 2 --cluster=127.0.0.1:12379,127.0.0.1:22379 --port 63792

node1: ./build/raft-kv/raft-kv --id 1 --cluster=127.0.0.1:12379,127.0.0.1:22379,127.0.0.1:32379 --port 63791
node2: ./build/raft-kv/raft-kv --id 2 --cluster=127.0.0.1:12379,127.0.0.1:22379,127.0.0.1:32379 --port 63792
node3: ./build/raft-kv/raft-kv --id 3 --cluster=127.0.0.1:12379,127.0.0.1:22379,127.0.0.1:32379 --port 63793


#node1: gdb --args ./build/raft-kv/raft-kv --id 1 --cluster=127.0.0.1:12379,127.0.0.1:22379 --port 63791
#node2: gdb --args ./build/raft-kv/raft-kv --id 2 --cluster=127.0.0.1:12379,127.0.0.1:22379 --port 63792


