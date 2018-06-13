package discovery

import (
	"encoding/json"
	"log"
	"time"

	"github.com/coreos/etcd/client"
	//"golang.org/x/net/context"
	"context"
)

type Master struct {
	members map[string]*Member
	// worker这边也跟master类似, 保存一个etcd KeysAPI, 通过它与etcd交互.然后用heartbeat来保持自己的状态.
	KeysAPI client.KeysAPI
}

// Member is a client machine
type Member struct {
	InGroup bool
	IP      string
	Name    string
	CPU     int
}

func NewMaster(endpoints []string) *Master {
	cfg := client.Config{
		Endpoints:               endpoints, // Endpoints是指etcd服务器们的地址,如http://192.168.0.1:2379等
		Transport:               client.DefaultTransport,
		HeaderTimeoutPerRequest: time.Second,
	}

	// 先建一个etcd的客户端
	etcdClient, err := client.New(cfg)
	if err != nil {
		log.Fatal("Error: cannot connec to etcd:", err)
	}

	master := &Master{
		members: make(map[string]*Member),
		// 把etcd客户端的KeysAPI放进master里面,这样我们以后只需要通过这个API来跟etcd进行交互
		KeysAPI: client.NewKeysAPI(etcdClient),
	}
	// go master.WatchWorkers() // 启动一个Go routine来监控节点的情况
	return master
}

func (m *Master) AddWorker(info *WorkerInfo) {
	member := &Member{
		InGroup: true,
		IP:      info.IP,
		Name:    info.Name,
		CPU:     info.CPU,
	}
	m.members[member.Name] = member
}

func (m *Master) UpdateWorker(info *WorkerInfo) {
	member := m.members[info.Name]
	member.InGroup = true
}

func NodeToWorkerInfo(node *client.Node) *WorkerInfo {
	log.Println(node.Value)
	info := &WorkerInfo{}
	err := json.Unmarshal([]byte(node.Value), info)
	if err != nil {
		log.Print(err)
	}
	return info
}

// 服务观察感觉是etcd的特色会观察key的行为，set del expire等
// 然后再做相应的动作，
/*
etcd是一种分布式存储，更强调的是各个节点之间的通信，同步，
确保各个节点上数据和事务的一致性，使得服务发现工作更稳定，本身单节点的写入能力并不强。

redis更像是内存型缓存，虽然也有cluster做主从同步和读写分离，
但节点间的一致性主要强调的是数据，并不在乎事务，因此读写能力很强，qps甚至可以达到10万+
*/
func (m *Master) WatchWorkers() {
	api := m.KeysAPI
	watcher := api.Watcher("workers/", &client.WatcherOptions{
		Recursive: true, // 递归的。指的是要监听这个文件夹下面所有节点的变化, 而不是这个文件夹的变化
	})
	for {
		res, err := watcher.Next(context.Background())
		if err != nil {
			log.Println("Error watch workers:", err)
			break
		}
		if res.Action == "expire" {
			info := NodeToWorkerInfo(res.PrevNode)
			log.Println("Expire worker ", info.Name)
			member, ok := m.members[info.Name]
			if ok {
				/*
				当返回expire的时候, 该节点不一定挂掉, 有可能只是网络状况不好,
				因此我们只将它暂时设置成不在集群里, 等当它返回update时在设置回来.
				只有返回delete才明确表示将它删除.
				*/
				member.InGroup = false
			}
		} else if res.Action == "set" {
			info := NodeToWorkerInfo(res.Node)
			if _, ok := m.members[info.Name]; ok {
				log.Println("Update worker ", info.Name)
				m.UpdateWorker(info)
			} else {
				log.Println("Add worker ", info.Name)
				m.AddWorker(info)
			}
		} else if res.Action == "delete" {
			info := NodeToWorkerInfo(res.Node)
			log.Println("Delete worker ", info.Name)
			delete(m.members, info.Name)
		}
	}
}
