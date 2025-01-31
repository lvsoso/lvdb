
#### etcd
```shell
# install etcd
HOMEBREW_NO_AUTO_UPDATE=1 brew install etcd

# start etcd in background
etcd &> /tmp/etcd.log &

export ETCDCTL_API=3
etcdctl put foo "bar"
etcdctl get foo
```


#### run server
```shell
PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python  python master_server.py 
```