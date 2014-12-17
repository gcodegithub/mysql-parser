package zk.client;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zk.utils.ZkConf;

/**
 * Created by hp on 14-12-12.
 */
public class ZkExecutor {

    private Logger logger = LoggerFactory.getLogger(ZkExecutor.class);
    private ZkConf conf;
    private ZooKeeper zk;

    public ZkExecutor (ZkConf cnf) {
        conf = cnf;
    }

    public void connect() throws Exception {
        zk = new ZooKeeper(conf.zkServers, conf.timeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                logger.info("watcher : " + event.getType());
            }
        });
    }

    public void close() throws Exception {
        zk.close();
    }

    public boolean exists(String path) throws Exception {
        if(zk.exists(path, false) == null) return false;
        else return true;
    }

    public void create(String path, String data) throws Exception {
        zk.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    public void set(String path, String data) throws Exception {
        zk.setData(path, data.getBytes(), -1);
    }

    public String get(String path) throws Exception {
        if(! exists(path)) {
            return null;//not exists return null
        }
        byte[] bytes = zk.getData(path, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                logger.info("get data watcher : " + event.getType());
            }
        },null);
        return new String(bytes);
    }

    public void delete(String path) throws Exception {
        if(exists(path)) {
            zk.delete(path, -1);
        }
    }
}
