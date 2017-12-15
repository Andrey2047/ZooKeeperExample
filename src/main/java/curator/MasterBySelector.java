package curator;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.RetryNTimes;

import java.io.IOException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * @author andriiko
 * @since 12/15/2017
 */
public class MasterBySelector {

    public static void main(String[] args) throws IOException, InterruptedException {
        CuratorFramework zkc =
                CuratorFrameworkFactory.newClient("127.0.0.1:2181", new RetryNTimes(5, 12));
        zkc.start();

        LeaderSelector leaderSelector = new LeaderSelector(zkc, "/master",  new LeaderSelectorListener() {

            @Override
            public void takeLeadership(CuratorFramework client) throws Exception {
                System.out.print(Thread.currentThread().getName());
            }

            @Override
            public void stateChanged(CuratorFramework client, ConnectionState newState) {
                System.out.println(newState);
            }
        });
        leaderSelector.setId("masterBySelector");
        leaderSelector.start();
        if(leaderSelector.hasLeadership()){
            System.out.println("has ld selector");
        }

        Thread.sleep(20000);

    }
}
