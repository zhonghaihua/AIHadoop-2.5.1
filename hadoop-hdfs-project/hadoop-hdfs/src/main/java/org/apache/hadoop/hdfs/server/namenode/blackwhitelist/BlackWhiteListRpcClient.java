package org.apache.hadoop.hdfs.server.namenode.blackwhitelist;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

import static org.apache.hadoop.security.blackwhitelist.BlackWhiteListConfigUtils.*;

/**
 * Created by iwardzhong on 2016/11/25.
 */
public class BlackWhiteListRpcClient {

  /**
   * get the black white list rpc service client
   * @param conf
   * @param namenode
   * @return
   * @throws IOException
   */
  public static BlackWhiteListProtocol getBlackWhiteListProxy(
          Configuration conf, String namenode) throws IOException {
    int port = conf.getInt(WHITE_LIST_SERVICE_PORT, WHITE_LIST_SERVICE_PORT_DEFAULT);
    InetSocketAddress addr = new InetSocketAddress(namenode, port);
    return RPC.waitForProxy(BlackWhiteListProtocol.class, BlackWhiteListProtocol.versionID,
            addr, conf, TIME_OUT);
  }

  /**
   * refresh the white list
   * @throws IOException
   */
  public static int refreshWhiteList() throws IOException {
    int exitCode = -1;
    Configuration conf = new Configuration();
    try {
      String hostname = BlackWhiteListUtils.execuShellCmd("hostname");
      List<String> namenodeHostnameList = BlackWhiteListUtils.getNamenodes(conf);
      if (namenodeHostnameList.contains(hostname)) {
        for (String nnHostname : namenodeHostnameList) {
          refreshWhiteListByNamenode(conf, nnHostname);
        }
        exitCode = 0;
      } else {
        String errmsg = "Refresh WhiteList Service in " +
                hostname + " Can't Refresh WhiteList Service in Client";
        LOG.error(errmsg);
      }
    } catch (IOException e) {
      throw e;
    }
    return exitCode;
  }

  /**
   * refresh white list by namenode
   * @param conf
   * @param hostname
   * @throws IOException
   */
  public static void refreshWhiteListByNamenode(
          Configuration conf, String hostname) throws IOException {
    BlackWhiteListProtocol proxy = getBlackWhiteListProxy(conf, hostname);
    proxy.refreshWhiteList();
    Text text = proxy.getWhiteList();
    if (N.equalsIgnoreCase(text.toString())) {
      LOG.info(hostname + " Black White List Service is Closed ");
    } else {
      LOG.info(hostname + " Black White List Service Refreshed WhiteList Success");
    }
    RPC.stopProxy(proxy);
  }

  /**
   * refresh the black list
   * @return
   * @throws IOException
   */
  public static int refreshBlackList() throws IOException {
    int exitCode = -1;
    Configuration conf = new Configuration();
    try {
      String hostname = BlackWhiteListUtils.execuShellCmd("hostname");
      List<String> namenodeHostnameList = BlackWhiteListUtils.getNamenodes(conf);
      if (namenodeHostnameList.contains(hostname)) {
        for (String nnHostname : namenodeHostnameList) {
          refreshBlackListByNamenode(conf, nnHostname);
        }
        exitCode = 0;
      } else {
        String errmsg = "Refresh BlackList Service in " +
                hostname + " Can't Refresh WhiteList Service in Client";
        LOG.error(errmsg);
      }

    } catch (IOException e) {
      throw e;
    }
    return exitCode;
  }

  /**
   * refresh black list
   * @param conf
   * @param hostname
   * @throws IOException
   */
  public static void refreshBlackListByNamenode(
          Configuration conf, String hostname) throws IOException {
    BlackWhiteListProtocol proxy = getBlackWhiteListProxy(conf, hostname);
    proxy.refreshBlackList();
    Text text = proxy.getWhiteList();
    if (N.equalsIgnoreCase(text.toString())) {
      LOG.info(hostname + " Black White List Service is Closed ");
    } else {
      LOG.info(hostname + " Black White List Service Refreshed BlackList Success");
    }
    RPC.stopProxy(proxy);
  }

  /**
   * refresh the namenode in white list
   * @return
   * @throws IOException
   */
  public static int refreshNameNode() throws IOException {
    int exitCode = -1;
    Configuration conf = new Configuration();
    try {
      String hostname = BlackWhiteListUtils.execuShellCmd("hostname");
      List<String> namenodeHostnameList = BlackWhiteListUtils.getNamenodes(conf);
      if (namenodeHostnameList.contains(hostname)) {
        for (String nnHostname : namenodeHostnameList) {
          refreshNameNodeByNameNode(conf, nnHostname);
        }
        exitCode = 0;
      } else {
        String errmsg = "Refresh Namenode Service in " +
                hostname + " Can't Refresh WhiteList Service in Client";
        LOG.error(errmsg);
      }
    } catch (IOException e) {
      throw e;
    }
    return exitCode;
  }
  /**
   * refresh namenode in white list
   * @param conf
   * @param hostname
   * @throws IOException
   */
  public static void refreshNameNodeByNameNode(
          Configuration conf, String hostname) throws IOException {
    BlackWhiteListProtocol proxy = getBlackWhiteListProxy(conf, hostname);
    proxy.refreshNameNode();
    Text text = proxy.getWhiteList();
    if (N.equalsIgnoreCase(text.toString())) {
      LOG.info(hostname + " Black White List Service is Closed ");
    } else {
      LOG.info(hostname + " Black White List Service Refreshed WhiteListNamenodes Success");
    }
    RPC.stopProxy(proxy);
  }

  /**
   * refresh the datanode in white list
   * @return
   * @throws IOException
   */
  public static int refreshDataNode() throws IOException {
    int exitCode = -1;
    Configuration conf = new Configuration();
    try {
      String hostname = BlackWhiteListUtils.execuShellCmd("hostname");
      List<String> namenodeHostnameList = BlackWhiteListUtils.getNamenodes(conf);
      if (namenodeHostnameList.contains(hostname)) {
        for (String nnHostname : namenodeHostnameList) {
          refreshDataNodeByNamenode(conf, nnHostname);
        }
        exitCode = 0;
      } else {
        String errmsg = "Refresh Datanode Service in " +
                hostname + " Can't Refresh WhiteList Service in Client";
        LOG.error(errmsg);
      }
    } catch (IOException e) {
      throw e;
    }
    return exitCode;
  }

  /**
   * refresh datanode in white list
   * @param conf
   * @param hostname
   * @throws IOException
   */
  public static void refreshDataNodeByNamenode(
          Configuration conf, String hostname) throws IOException {
    BlackWhiteListProtocol proxy = getBlackWhiteListProxy(conf, hostname);
    proxy.refreshDataNode();
    Text text = proxy.getWhiteList();
    if (N.equalsIgnoreCase(text.toString())) {
      LOG.info(hostname + " Black White List Service is Closed ");
    } else {
      LOG.info(hostname + " Black White List Service Refreshed WhiteListDatanodes Success");
    }
    RPC.stopProxy(proxy);
  }

  /**
   * refresh other cluster in white list
   * @return
   * @throws IOException
   */
  public static int refreshOtherCluster() throws IOException {
    int exitCode = -1;
    Configuration conf = new Configuration();
    try {
      String hostname = BlackWhiteListUtils.execuShellCmd("hostname");
      List<String> namenodeHostnameList = BlackWhiteListUtils.getNamenodes(conf);
      if (namenodeHostnameList.contains(hostname)) {
        for (String nnHostname : namenodeHostnameList) {
          refreshOtherClusterByNameNode(conf, nnHostname);
        }
        exitCode = 0;
      } else {
        String errmsg = "Refresh OtherCluster Service in " +
                hostname + " Can't Refresh WhiteList Service in Client";
        LOG.error(errmsg);
      }
    } catch (IOException e) {
      throw e;
    }
    return exitCode;
  }

  /**
   * refresh the other cluster in white list
   * @param conf
   * @param hostname
   * @throws IOException
   */
  public static void refreshOtherClusterByNameNode(
          Configuration conf, String hostname) throws IOException {
    BlackWhiteListProtocol proxy = getBlackWhiteListProxy(conf, hostname);
    proxy.refreshOtherCluster();
    Text text = proxy.getWhiteList();
    if (N.equalsIgnoreCase(text.toString())) {
      LOG.info(hostname + " Black White List Service is Closed ");
    } else {
      LOG.info(hostname + " Black White List Service Refreshed WhiteListOtherCluster Success");
    }
    RPC.stopProxy(proxy);
  }

  /**
   * refresh the secutiry dir
   * @return
   * @throws IOException
   */
  public static int refreshSecurityDir() throws IOException {
    int exitCode = -1;
    Configuration conf = new Configuration();
    try {
      String hostname = BlackWhiteListUtils.execuShellCmd("hostname");
      List<String> namenodeHostnameList = BlackWhiteListUtils.getNamenodes(conf);
      if (namenodeHostnameList.contains(hostname)) {
        for (String nnHostname : namenodeHostnameList) {
          refreshSecurityDirByNamenode(conf, nnHostname);
        }
        exitCode = 0;
      } else {
        String errmsg = "Refresh SecurityDir Service in " +
                hostname + " Can't Refresh WhiteList Service in Client";
        LOG.error(errmsg);
      }
    } catch (IOException e) {
      throw e;
    }
    return exitCode;
  }

  /**
   * refresh security dir
   * @param conf
   * @param hostname
   * @throws IOException
   */
  public static void refreshSecurityDirByNamenode(
          Configuration conf, String hostname) throws IOException {
    BlackWhiteListProtocol proxy = getBlackWhiteListProxy(conf, hostname);
    proxy.refreshSecurityDir();
    Text text = proxy.getWhiteList();
    if (N.equalsIgnoreCase(text.toString())) {
      LOG.info(hostname + " Black White List Service is Closed ");
    } else {
      LOG.info(hostname + " Black White List Service Refreshed SecurityDir Success");
    }
    RPC.stopProxy(proxy);
  }

  /**
   * print the white list
   * @return
   * @throws IOException
   */
  public static int printWhiteList() throws IOException {
    int exitCode = -1;
    Configuration conf = new Configuration();
    try {
      List<String> namenodeHostnameList = BlackWhiteListUtils.getNamenodes(conf);
        for (String nnHostname : namenodeHostnameList) {
          printWhiteListByNamenode(conf, nnHostname);
        }
        exitCode = 0;
    } catch (IOException e) {
      throw new IOException("print whitelist failed, details: " + e.getMessage());
    }
    return exitCode;
  }

  /**
   * print the white list by namenode
   * @param conf
   * @param hostname
   * @throws IOException
   */
  public static void printWhiteListByNamenode(
          Configuration conf, String hostname) throws IOException {
    BlackWhiteListProtocol proxy = getBlackWhiteListProxy(conf, hostname);
    Text text = proxy.getWhiteList();
    if (N.equalsIgnoreCase(text.toString())) {
      LOG.info(hostname + " Black White List Service is Closed ");
    } else {
      System.out.println("【namenode: " + hostname + "】" + "white list is: ");
      System.out.println(text);
    }
    RPC.stopProxy(proxy);
  }

  /**
   * print the black list
   * @return
   * @throws IOException
   */
  public static int printBlackList() throws IOException {
    int exitCode = -1;
    Configuration conf = new Configuration();
    try {
      List<String> namenodeHostnameList = BlackWhiteListUtils.getNamenodes(conf);
      for (String nnHostname : namenodeHostnameList) {
        printBlackListByNamenode(conf, nnHostname);
      }
      exitCode = 0;
    } catch (IOException e) {
      throw new IOException("print blacklist failed, details: " + e.getMessage());
    }
    return exitCode;
  }

  /**
   * print the black list by namenode
   * @param conf
   * @param hostname
   * @throws IOException
   */
  public static void printBlackListByNamenode(
          Configuration conf, String hostname) throws IOException {
    BlackWhiteListProtocol proxy = getBlackWhiteListProxy(conf, hostname);
    Text text = proxy.getBlackList();
    if (N.equalsIgnoreCase(text.toString())) {
      LOG.info(hostname + " Black White List Service is Closed ");
    } else {
      System.out.println("【namenode: " + hostname + "】" + " black list is: ");
      System.out.println(text);
    }
    RPC.stopProxy(proxy);
  }

  /**
   * print the namenode in white list
   * @return
   * @throws IOException
   */
  public static int printNamenode() throws IOException {
    int exitCode = -1;
    Configuration conf = new Configuration();
    try {
      List<String> namenodeHostnameList = BlackWhiteListUtils.getNamenodes(conf);
      for (String nnHostname : namenodeHostnameList) {
        printNamenodeByNamenode(conf, nnHostname);
      }
      exitCode = 0;
    } catch (IOException e) {
      throw new IOException("print namenode failed, details: " + e.getMessage());
    }
    return exitCode;
  }

  /**
   * print the namenode by namenode
   * @param conf
   * @param hostname
   * @throws IOException
   */
  public static void printNamenodeByNamenode(
          Configuration conf, String hostname) throws IOException {
    BlackWhiteListProtocol proxy = getBlackWhiteListProxy(conf, hostname);
    Text text = proxy.getNameNode();
    if (N.equalsIgnoreCase(text.toString())) {
      LOG.info(hostname + " Black White List Service is Closed ");
    } else {
      System.out.println("【namenode: " + hostname + "】" + " namenode list is: ");
      System.out.println(text);
    }
    RPC.stopProxy(proxy);
  }

  /**
   * print the datanode in white list
   * @return
   * @throws IOException
   */
  public static int printDatanode() throws IOException {
    int exitCode = -1;
    Configuration conf = new Configuration();
    try {
      List<String> namenodeHostnameList = BlackWhiteListUtils.getNamenodes(conf);
      for (String nnHostname : namenodeHostnameList) {
        printDatanodeByNamenode(conf, nnHostname);
      }
      exitCode = 0;
    } catch (IOException e) {
      throw new IOException("print datanode failed, details: " + e.getMessage());
    }
    return exitCode;
  }

  /**
   * print the datanode by namenode
   * @param conf
   * @param hostname
   * @throws IOException
   */
  public static void printDatanodeByNamenode(
          Configuration conf, String hostname) throws IOException {
    BlackWhiteListProtocol proxy = getBlackWhiteListProxy(conf, hostname);
    Text text = proxy.getDataNode();
    if (N.equalsIgnoreCase(text.toString())) {
      LOG.info(hostname + " Black White List Service is Closed ");
    } else {
      System.out.println("【namenode: " + hostname + "】" + " datanode list is: ");
      System.out.println(text);
    }
    RPC.stopProxy(proxy);
  }

  /**
   * print the otherCluster in white list
   * @return
   * @throws IOException
   */
  public static int printOtherCluster() throws IOException {
    int exitCode = -1;
    Configuration conf = new Configuration();
    try {
      List<String> namenodeHostnameList = BlackWhiteListUtils.getNamenodes(conf);
      for (String nnHostname : namenodeHostnameList) {
        printOtherClusterByNamenode(conf, nnHostname);
      }
      exitCode = 0;
    } catch (IOException e) {
      throw new IOException("print otherCluster failed, details: " + e.getMessage());
    }
    return exitCode;
  }

  /**
   * print the otherCluster by namenode
   * @param conf
   * @param hostname
   * @throws IOException
   */
  public static void printOtherClusterByNamenode(
          Configuration conf, String hostname) throws IOException {
    BlackWhiteListProtocol proxy = getBlackWhiteListProxy(conf, hostname);
    Text text = proxy.getOtherCluster();
    if (N.equalsIgnoreCase(text.toString())) {
      LOG.info(hostname + " Black White List Service is Closed ");
    } else {
      System.out.println("【namenode: " + hostname + "】" + " otherCluster list is: ");
      System.out.println(text);
    }
    RPC.stopProxy(proxy);
  }

  /**
   * print the security dir
   * @return
   * @throws IOException
   */
  public static int printSecurityDir() throws IOException {
    int exitCode = -1;
    Configuration conf = new Configuration();
    try {
      List<String> namenodeHostnameList = BlackWhiteListUtils.getNamenodes(conf);
      for (String nnHostname : namenodeHostnameList) {
        printSecurityDirByNamenode(conf, nnHostname);
      }
      exitCode = 0;
    } catch (IOException e) {
      throw new IOException("print securityDir failed, details: " + e.getMessage());
    }
    return exitCode;
  }

  /**
   * print the security dir by namenode
   * @param conf
   * @param hostname
   * @throws IOException
   */
  public static void printSecurityDirByNamenode(
          Configuration conf, String hostname) throws IOException {
    BlackWhiteListProtocol proxy = getBlackWhiteListProxy(conf, hostname);
    Text text = proxy.getSecurityDir();
    if (N.equalsIgnoreCase(text.toString())) {
      LOG.info(hostname + " Black White List Service is Closed ");
    } else {
      System.out.println("【namenode: " + hostname + "】" + " securityDir list is: ");
      System.out.println(text);
    }
    RPC.stopProxy(proxy);
  }

}
