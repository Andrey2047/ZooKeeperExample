package curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.retry.RetryNTimes;

import java.io.IOException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * @author andriiko
 * @since 12/15/2017
 */
public class MasterByLatch {

    public static void main(String[] args) throws IOException, InterruptedException {
        CuratorFramework zkc =
                CuratorFrameworkFactory.newClient("127.0.0.1:2181", new RetryNTimes(5, 12));
        zkc.start();

        LeaderLatch leaderLatch = new LeaderLatch(zkc, "/master", "myServerId");
        leaderLatch.addListener(new LeaderLatchListener() {
            @Override
            public void isLeader() {
                System.out.println("isLeader");

            }

            @Override
            public void notLeader() {
                System.out.println("notLeader");
            }
        });
        boolean isLeader = false;
        try {
            leaderLatch.start();
            leaderLatch.await(2, TimeUnit.SECONDS);
            if(leaderLatch.hasLeadership()){
                isLeader = true;
                System.out.println(isLeader);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            leaderLatch.close();
        }

        Thread.sleep(20000);


    }
}
