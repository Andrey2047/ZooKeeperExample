import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Random;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

/**
 * @author andriiko
 * @since 12/5/2017
 */
public class ZooKeeperMaster implements Watcher {

    private static final Logger LOG = LoggerFactory.getLogger("dddd");

    private MasterState masterState;

    private String serverId = "001x3444";

    ZooKeeper zk;

    String hostPort;

    List<String> availableWorkers;

    ZooKeeperMaster(String hostPort) {
        this.hostPort = hostPort;
    }

    void startZK() throws IOException {
        zk = new ZooKeeper(hostPort, 150000, this);
    }

    public void process(WatchedEvent e) {
        System.out.println(e);
    }

    public static void main(String args[]) throws Exception {
        ZooKeeperMaster m = new ZooKeeperMaster("127.0.0.1:2181");
        m.startZK();
        m.runForMaster();
        m.stopZK();
    }

    void runForMaster() {
        zk.create("/master", serverId.getBytes(), OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, masterCreateCallback, null);
    }

    private void stopZK() {
        try {
            Thread.currentThread().sleep(600000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    AsyncCallback.DataCallback masterCheckCallback = (rc, path, ctx, data, stat) -> {
        switch (KeeperException.Code.get(rc)) {
            case CONNECTIONLOSS:
                checkMaster();
                return;
            case NONODE:
                runForMaster();
                return;
            case NODEEXISTS:
                masterExists();
                return;
            default:
                masterState = MasterState.NOT_ELECTED;
        }
    };

    Watcher masterExistsWatcher = e -> {
        if (e.getType() == Event.EventType.NodeDeleted) {
            assert "/master".equals(e.getPath());
            runForMaster();
        }
    };

    AsyncCallback.StringCallback masterCreateCallback = (rc, path, ctx, name) -> {
        switch (KeeperException.Code.get(rc)) {
            case CONNECTIONLOSS:
                checkMaster();
                return;
            case OK:
                masterState = MasterState.ELECTED;
                takeLeadership();
                break;
            case NODEEXISTS:
                masterState = MasterState.NOT_ELECTED;
                masterExists();
                break;
            default:
                masterState = MasterState.NOT_ELECTED;
        }
    };

    AsyncCallback.StatCallback masterExistsCallback = new AsyncCallback.StatCallback() {
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    masterExists();
                    break;
                case OK:
                    if (stat == null) {
                        masterState = MasterState.RUNNING;
                        runForMaster();
                    }
                    break;
                default:
                    checkMaster();
                    break;
            }
        }
    };

    Watcher workersChangeWatcher = e -> {
        if(e.getType() == Event.EventType.NodeChildrenChanged) {
            assert "/workers".equals( e.getPath() );
            getWorkers();
        }
    };

    void getWorkers() {
        zk.getChildren("/workers", workersChangeWatcher, workersGetChildrenCallback, null);
    }

    Watcher tasksChangeWatcher = e -> {
        if(e.getType() == Event.EventType.NodeChildrenChanged) {
            assert "/tasks".equals( e.getPath() );
            getTasks();
        }
    };

    void getTasks() {
        zk.getChildren("/tasks", tasksChangeWatcher, tasksGetChildrenCallback, null);
    }

    AsyncCallback.ChildrenCallback tasksGetChildrenCallback = (rc, path, ctx, children) -> {
        switch(KeeperException.Code.get(rc)) {
            case CONNECTIONLOSS:
                getTasks();
                break;
            case OK:
                if(children != null) {
                    assignTasks(children);
                }
                break;
            default:
                LOG.error("getChildren failed.",
                        KeeperException.create(KeeperException.Code.get(rc), path));
        }
    };

    void assignTasks(List<String> tasks) {
        for(String task : tasks) {
            getTaskData(task);
        }
    }

    void getTaskData(String task) {
        zk.getData("/tasks/" + task, false, taskDataCallback, task);
    }

    AsyncCallback.DataCallback taskDataCallback = new AsyncCallback.DataCallback() {
        public void processResult(int rc,
                                  String path,
                                  Object ctx,
                                  byte[] data,
                                  Stat stat) {
            switch(KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    getTaskData((String) ctx);
                    break;
                case OK:
                    int worker = new Random().nextInt(availableWorkers.size());
                    String designatedWorker = availableWorkers.get(worker);
                    String assignmentPath = "/assign/" + designatedWorker + "/" +
                            (String) ctx;
                    createAssignment(assignmentPath, data);
                    break;
                default:
                    LOG.error("Error when trying to get task data.",
                            KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };
    void createAssignment(String path, byte[] data) {
        zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, assignTaskCallback, data);
    }

    AsyncCallback.StringCallback assignTaskCallback = (rc, path, ctx, name) -> {
        switch (KeeperException.Code.get(rc)) {
            case CONNECTIONLOSS:
                createAssignment(path, (byte[]) ctx);
                break;
            case OK:
                LOG.info("Task assigned correctly: " + name);
                deleteTask(name.substring(name.lastIndexOf("/") + 1));
                break;
            case NODEEXISTS:
                LOG.warn("Task already assigned");
                break;
            default:
                LOG.error("Error when trying to assign task.",
                        KeeperException.create(KeeperException.Code.get(rc), path));
        }
    };

    private void deleteTask(String substring) {
    }

    AsyncCallback.ChildrenCallback workersGetChildrenCallback = (rc, path, ctx, children) -> {
        switch (KeeperException.Code.get(rc)) {
            case CONNECTIONLOSS:
                getWorkers();
                break;
            case OK:
                LOG.info("Succesfully got a list of workers: "
                        + children.size()
                        + " workers");
                reassignAndSet(children);
                break;
            default:
                LOG.error("getChildren failed",
                        KeeperException.create(KeeperException.Code.get(rc), path));
        }
    };

    void reassignAndSet(List<String> children) {
        List<String> toProcess;
        if(availableWorkers == null) {
            availableWorkers = children;
            toProcess = null;
        } else {
            availableWorkers.removeAll(children);
            toProcess = availableWorkers;
            availableWorkers = children;
        }


        if(toProcess != null) {
            for(String worker : toProcess) {
                getAbsentWorkerTasks(worker);
            }
        }
    }

    private void getAbsentWorkerTasks(String worker) {

    }

    private void takeLeadership() {
        bootstrap();
        getTasks();
        getWorkers();
    }

    void masterExists() {
        zk.exists("/master", masterExistsWatcher, masterExistsCallback, null);
    }

    // returns true if there is a master
    void checkMaster() {
        zk.getData("/master", false, masterCheckCallback, null);
    }

    AsyncCallback.StringCallback createParentCallback = (rc, path, ctx, name) -> {
        switch (KeeperException.Code.get(rc)) {
            case CONNECTIONLOSS:
                createParent(path, (byte[]) ctx);
                break;
            case OK:
                LOG.info("Parent created");
                break;
            case NODEEXISTS:
                LOG.warn("Parent already registered: " + path);
                break;
            default:
                LOG.error("Something went wrong: ", KeeperException.create(KeeperException.Code.get(rc), path));
        }
    };

    public void bootstrap() {
        createParent("/workers", new byte[0]);
        createParent("/assign", new byte[0]);
        createParent("/tasks", new byte[0]);
        createParent("/status", new byte[0]);
    }

    void createParent(String path, byte[] data) {
        zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, createParentCallback, data);
    }

}
