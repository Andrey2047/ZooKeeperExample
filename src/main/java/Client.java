import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.concurrent.ConcurrentHashMap;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

/**
 * @author andriiko
 * @since 12/11/2017
 */
public class Client implements Watcher {

    ZooKeeper zk;
    String hostPort;
    String clientName;

    ConcurrentHashMap<String, Object> ctxMap = new ConcurrentHashMap<String, Object>();

    Client(String hostPort) {
        this.hostPort = hostPort;
    }

    void startZK() throws Exception {
        zk = new ZooKeeper(hostPort, 150000, this);
    }

    public void process(WatchedEvent e) {
        System.out.println(e);
    }

    public static void main(String args[]) throws Exception {
        Client c = new Client("127.0.0.1:2181");
        c.startZK();
        TaskObject taskCtx = new TaskObject();
        taskCtx.setTask("task1");
        taskCtx.setTaskName("task1");
        c.submitTask("task1", taskCtx);
        System.out.println("Created " + c.clientName);

        Thread.sleep(60000);
    }

    void submitTask(String task, TaskObject taskCtx) {
        taskCtx.setTask(task);
        zk.create("/tasks/task-",
                task.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT_SEQUENTIAL,
                createTaskCallback,
                taskCtx);
    }


    AsyncCallback.StringCallback createTaskCallback = (rc, path, ctx, name) -> {
        switch (KeeperException.Code.get(rc)) {
            case CONNECTIONLOSS:
                submitTask(((TaskObject) ctx).getTask(),
                        (TaskObject) ctx);
                break;
            case OK:
                System.out.println("My created task name: " + name);
                ((TaskObject) ctx).setTaskName(name);
                watchStatus("/status/" + name.replace("/tasks/", ""),
                        ctx);
                break;
            default:
                System.out.println("Something went wrong" +
                        KeeperException.create(KeeperException.Code.get(rc), path));
        }
    };

    private void watchStatus(String path, Object ctx) {
        ctxMap.put(path, ctx);
        zk.exists(path,
                statusWatcher,
                existsCallback,
                ctx);
    }

    Watcher statusWatcher = new Watcher() {
        public void process(WatchedEvent e) {
            if(e.getType() == Event.EventType.NodeCreated) {
                assert e.getPath().contains("/status/task-");
                zk.getData(e.getPath(), false, getDataCallback, ctxMap.get(e.getPath()));
            }
        }
    };

    AsyncCallback.StatCallback existsCallback = new AsyncCallback.StatCallback() {
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    watchStatus(path, ctx);
                    break;
                case OK:
                    if(stat != null) {
                        zk.getData(path, false, getDataCallback, null);
                    }
                    break;
                case NONODE:
                    break;
                default:
                    System.out.println("Something went wrong when " +
                            "checking if the status node exists: " +
                            KeeperException.create(KeeperException.Code.get(rc), path));
                    break;
            }
        }
    };

    AsyncCallback.DataCallback getDataCallback = (i, s, o, bytes, stat) -> {

    };
}
