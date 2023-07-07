Consul伪分布式安装手册


1.环境准备
服务器IP	Consul类型	Node节点	http端口	Dns
端口	Serf_lan
端口	Serf_wan
端口	Server
端口
30.147.81.88	server	server-01	8501	8601	8001	8002	8000
30.147.81.88	server	server-02	8502	8602	8101	8102	8100
30.147.81.88	server	server-03	8503	8603	8201	8202	8200
30.147.81.88	client	client-01	8504	8604	8301	8302	8300

1.1安装
安装unzip命令
yum -y install unzip


#创建consul目录
sudo mkdir -p /usr/local/consul


附件下载 consul_1.9.5_linux_amd64.zip 上传到 /usr/local/consul目录
cd /usr/local/consul/




将安装包解压至/usr/local/consul目录


unzip consul_1.9.5_linux_amd64.zip -d /usr/local/consul/




1.2配置consul
创建各节点目录
sudo  mkdir -p node{1..3}/{bin,conf,data,log}
sudo  mkdir -p agent/{bin,conf,data,log}


将可执行文件复制到各节点的bin目录：
cd /usr/local/consul/
cp consul node1/bin/
cp consul node2/bin/
cp consul node3/bin/
cp consul agent/bin/


2.创建Consul-Server服务配置文件
2.1 server-1的配置文件
sudo vim /usr/local/consul/node1/conf/server.json
{
"bind_addr": "30.147.81.88",
"client_addr": "30.147.81.88",
"ports": {
"http": 8501,
"dns": 8601,
"serf_lan": 8001,
"serf_wan": 8002,
"server": 8000
},
"datacenter": "dc1",
"data_dir": "/usr/local/consul/node1/data",
"log_level": "INFO",
"log_file": "/usr/local/consul/node1/log/consul.log",
"node_name": "consul-server-1",
"disable_host_node_id": true,
"server": true,
"ui": true,
"bootstrap_expect": 3,
"rejoin_after_leave": true,
"retry_join": [
"30.147.81.88:8001",
"30.147.81.88:8101",
"30.147.81.88:8201"
]
}
2.2 server-2的配置文件
sudo vim /usr/local/consul/node2/conf/server.json
{
"bind_addr": "30.147.81.88",
"client_addr": "30.147.81.88",
"ports": {
"http": 8502,
"dns": 8602,
"serf_lan": 8101,
"serf_wan": 8102,
"server": 8100
},
"datacenter": "dc1",
"data_dir": "/usr/local/consul/node2/data",
"log_level": "INFO",
"log_file": "/usr/local/consul/node2/log/consul.log",
"node_name": "consul-server-2",
"disable_host_node_id": true,
"server": true,
"ui": true,
"bootstrap_expect": 3,
"rejoin_after_leave": true,
"retry_join": [
"30.147.81.88:8001",
"30.147.81.88:8101",
"30.147.81.88:8201"
]
}
2.3 server-3的配置文件
sudo vim /usr/local/consul/node3/conf/server.json
{
"bind_addr": "30.147.81.88",
"client_addr": "30.147.81.88",
"ports": {
"http": 8503,
"dns": 8603,
"serf_lan": 8201,
"serf_wan": 8202,
"server": 8200
},
"datacenter": "dc1",
"data_dir": "/usr/local/consul/node3/data",
"log_level": "INFO",
"log_file": "/usr/local/consul/node3/log/consul.log",
"node_name": "consul-server-3",
"disable_host_node_id": true,
"server": true,
"ui": true,
"bootstrap_expect": 3,
"rejoin_after_leave": true,
"retry_join": [
"30.147.81.88:8001",
"30.147.81.88:8101",
"30.147.81.88:8201"
]
}
2.4 client-1的配置文件
sudo vim /usr/local/consul/agent/conf/agent.json
{
"bind_addr": "30.147.81.88",
"client_addr": "30.147.81.88",
"ports": {
"http": 8504,
"dns": 8604,
"serf_lan": 8301,
"serf_wan": 8302,
"server": 8300
},
"datacenter": "dc1",
"data_dir": "/usr/local/consul/agent/data",
"log_level": "INFO",
"log_file": "/usr/local/consul/agent/log/consul.log",
"node_name": "consul-client-1",
"disable_host_node_id": true,
"server": false,
"ui": true,
"rejoin_after_leave": true,
"retry_join": [
"30.147.81.88:8001",
"30.147.81.88:8101",
"30.147.81.88:8201"
]
}




3.启动Consul节点
3.1启动server-1
sudo nohup ./consul agent -config-dir=/usr/local/consul/node1/conf &
3.2启动server-2
sudo nohup ./consul agent -config-dir=/usr/local/consul/node2/conf &
3.3启动server-3
sudo nohup ./consul agent -config-dir=/usr/local/consul/node3/conf &
3.4启动client-1
sudo nohup ./consul agent -config-dir=/usr/local/consul/agent/conf &


 
4.查看状态
4.1查看consul集群列表
sudo ./consul operator raft list-peers -http-addr=30.147.81.88:8501


4.2查看consul节点列表
sudo ./consul members -http-addr=30.147.81.88:8501


4.3验证
模拟一个Consul server端宕机
sudo  consul leave -http-addr=30.147.81.88:8502
验证发现Consul客户端节点不会丢失




模拟Consul client端宕机
sudo  consul leave -http-addr=30.147.81.88:8504
再重启服务
sudo  nohup ./consul agent -config-dir=/usr/local/consul/agent/conf &
验证发现consul挂掉 再重启，服务不会丢失

