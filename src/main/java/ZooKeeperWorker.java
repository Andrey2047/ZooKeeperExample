import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * @author andriiko
 * @since 12/11/2017
 */
public class ZooKeeperWorker implements Watcher {

    ZooKeeper zk;
    String serverId = String.valueOf(new Random().nextInt());

    Executor executor = Executors.newFixedThreadPool(2);
    List<String> onGoingTasks = new ArrayList<>();

    ZooKeeperWorker(String hostAndPort) throws IOException {
        this.zk = new ZooKeeper(hostAndPort, 15000, this);
        this.register();
        try {
            Thread.currentThread().sleep(600000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        ZooKeeperWorker worker = new ZooKeeperWorker("127.0.0.1:2181");
        worker.register();
    }

    void register() {
        zk.create("/workers/worker-" + serverId, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, createWorkerCallback, null);
        zk.create("/assign/worker-" + serverId, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, createWorkerCallback, null);
        getTasks();
    }

    AsyncCallback.StringCallback createWorkerCallback = new AsyncCallback.StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    register();
                    break;
                case OK:
                    System.out.println("Registered successfully: " + serverId);
                    break;
                case NODEEXISTS:
                    System.out.println("Already registered: " + serverId);
                    break;
                default:
                    System.out.println("Something went wrong: " + KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };

    Watcher newTaskWatcher = new Watcher() {
        public void process(WatchedEvent e) {
            if(e.getType() == Event.EventType.NodeChildrenChanged) {
                assert ("/assign/worker-" + serverId).equals( e.getPath() );
                getTasks();
            }
        }
    };

    void getTasks() {
        zk.getChildren("/assign/worker-" + serverId, newTaskWatcher, tasksGetChildrenCallback, null);
    }

    AsyncCallback.ChildrenCallback tasksGetChildrenCallback = new AsyncCallback.ChildrenCallback() {
        public void processResult(int rc, String path, Object ctx, List<String> children) {

            switch(KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    getTasks();
                    break;
                case OK:
                    if(children != null) {
                        executor.execute(new Runnable() {
                            List<String> children;
                            DataCallback cb;
                            public Runnable init(List<String> children, DataCallback cb) {
                                this.children = children;
                                this.cb = cb;
                                return this;
                            }

                            public void run() {
                                System.out.println("Looping into tasks");

                                synchronized(onGoingTasks) {
                                    for(String task : children) {
                                        if(!onGoingTasks.contains( task )) {
                                            System.out.println("New task: {}" + task);
                                            zk.getData("/assign/worker-" + serverId + "/" + task, false, cb, task);
                                            onGoingTasks.add( task );
                                        }
                                    }
                                }
                            }
                        }.init(children, taskDataCallback));
                    }
                    break;
                default:
                    System.out.println("getChildren failed: " + KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };

    AsyncCallback.DataCallback taskDataCallback = (i, s, o, bytes, stat) -> {};

    @Override
    public void process(WatchedEvent event) {

    }
}
