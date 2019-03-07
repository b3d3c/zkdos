from kazoo.client import KazooClient
import kazoo
import sys
import logging


logging.basicConfig()


zk = KazooClient(hosts='127.0.0.1:2181')
try:
    zk.start()

except kazoo.exceptions.ConnectionLoss:
    print("Error trying to connect.\nStopping ZK connectoin and exiting...")
    zk.stop()
    sys.exit(1)


for i in range (1, 1000000):
    path = "/zookeeper/" + str(i)
    what = 1024 * 512 * 'a'
    what.encode()
    try:
        zk.create(path, what)
        print(path)

    except kazoo.exceptions.NodeExistsError:
        print("Error, node already exists. Exiting...")
        sys.exit(1)

    except kazoo.exceptions.ConnectionLoss:
        print("Error trying to create node. Exiting...")
        sys.exit(1)
