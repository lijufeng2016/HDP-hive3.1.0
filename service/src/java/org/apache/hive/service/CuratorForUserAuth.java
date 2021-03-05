package org.apache.hive.service;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.auth.PasswdAuthenticationProvider;
import org.apache.zookeeper.data.Stat;

import javax.security.sasl.AuthenticationException;
import java.nio.charset.Charset;
import java.util.concurrent.TimeUnit;

public class CuratorForUserAuth {
    CuratorFramework curatorFramework;

    public CuratorForUserAuth() {
        this.curatorFramework = getZookeeperClient();
    }

    public void setUserPassword(String userName, String password) {
        curatorFramework.start();
        createNode(userName);
        try {
            curatorFramework.setData().forPath("/" + userName, password.getBytes());
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            curatorFramework.close();
        }
    }

    private void createNode(String nodeName){
        try {
            Stat stat = curatorFramework.checkExists().forPath("/" + nodeName);
            if(null == stat){
                curatorFramework.create().creatingParentsIfNeeded().forPath("/" + nodeName);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
    public String getUserPassword(String userName) {
        curatorFramework.start();
        byte[] bytes = new byte[0];
        try {
            bytes = curatorFramework.getData().forPath(  "/" + userName);
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            curatorFramework.close();
        }
        String password = new String(bytes, Charset.forName("UTF-8"));
        return password;
    }

    public boolean validatePassword(String userName, String password, PasswdAuthenticationProvider provider) {
        try {
            provider.Authenticate(userName,password);
        } catch (AuthenticationException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    private CuratorFramework getZookeeperClient() {
        HiveConf conf = new HiveConf();
        String namespace = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_ZOOKEEPER_USERPASSWORD_NAMESPACE);
        String zkEnsemble = getQuorumServers(conf);
        int sessionTimeout = (int) HiveConf.getTimeVar(conf,
                HiveConf.ConfVars.HIVE_ZOOKEEPER_SESSION_TIMEOUT, TimeUnit.MILLISECONDS);
        int connectionTimeout = (int) HiveConf.getTimeVar(conf,
                HiveConf.ConfVars.HIVE_ZOOKEEPER_CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS);
        int baseSleepTime = (int) HiveConf.getTimeVar(conf,
                HiveConf.ConfVars.HIVE_ZOOKEEPER_CONNECTION_BASESLEEPTIME, TimeUnit.MILLISECONDS);
        int maxRetries = HiveConf.getIntVar(conf, HiveConf.ConfVars.HIVE_ZOOKEEPER_CONNECTION_MAX_RETRIES);

        // Create a CuratorFramework instance to be used as the ZooKeeper client
        return CuratorFrameworkFactory.builder()
                .connectString(zkEnsemble)
                .sessionTimeoutMs(sessionTimeout)
                .connectionTimeoutMs(connectionTimeout)
                .namespace(namespace)
                .retryPolicy(new ExponentialBackoffRetry(baseSleepTime, maxRetries))
                .build();
    }

    private static String getQuorumServers(Configuration conf) {
        String[] hosts = conf.getTrimmedStrings(HiveConf.ConfVars.HIVE_ZOOKEEPER_QUORUM.varname);
        String port = conf.get(HiveConf.ConfVars.HIVE_ZOOKEEPER_CLIENT_PORT.varname,
                HiveConf.ConfVars.HIVE_ZOOKEEPER_CLIENT_PORT.getDefaultValue());
        StringBuilder quorum = new StringBuilder();
        for (int i = 0; i < hosts.length; i++) {
            quorum.append(hosts[i].trim());
            if (!hosts[i].contains(":")) {
                // if the hostname doesn't contain a port, add the configured port to hostname
                quorum.append(":");
                quorum.append(port);
            }

            if (i != hosts.length - 1) {
                quorum.append(",");
            }
        }
        return quorum.toString();
    }

}
