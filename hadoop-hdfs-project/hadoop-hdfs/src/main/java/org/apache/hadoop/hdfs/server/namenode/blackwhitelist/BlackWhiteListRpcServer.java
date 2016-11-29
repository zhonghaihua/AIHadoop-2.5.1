package org.apache.hadoop.hdfs.server.namenode.blackwhitelist;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.blackwhitelist.BlackWhiteList;
import org.apache.hadoop.security.blackwhitelist.BlackWhiteListConfigUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.security.blackwhitelist.BlackWhiteListConfigUtils.*;

/**
 * Created by iwardzhong on 2016/11/24.
 */
public class BlackWhiteListRpcServer {

  /**
   * start the black white list service
   * @param conf
   */
  public static void startService(Configuration conf) {
    try {
      String hostname = InetAddress.getLocalHost().getHostAddress();
      int port = conf.getInt(WHITE_LIST_SERVICE_PORT, WHITE_LIST_SERVICE_PORT_DEFAULT);
      int numHandler =
              conf.getInt(WHITE_LIST_SERVICE_RPC_HANDLER, WHITE_LIST_SERVICE_RPC_HANDLER_DEFAULT);
      RPC.Server server = new RPC.Builder(conf).setProtocol(BlackWhiteListProtocol.class)
              .setInstance(new BlackWhiteListProtocolImpl()).setBindAddress(hostname).setPort(port)
              .setNumHandlers(numHandler).build();
      server.start();
      LOG.info("..................Black White List Service Listen in " + hostname + ":" + port);
    } catch (Exception e) {
      LOG.error("Black White List Service Start Service Failed" + e.getMessage());
    }
  }

  /**
   * init black white
   * @param conf
   * @throws IOException
   */
  public static void init(Configuration conf) throws IOException {
    boolean isEnable = conf.getBoolean(WHITE_LIST_ENABLE, false);
    if (isEnable) {
      try {
        initWhiteList(conf);
        initBlackList(conf);
        initNameNode(conf);
        initDataNode(conf);
        WHITE_LIST_ENABLE_VALUE = true;
      } catch (IOException e) {
        WHITE_LIST_ENABLE_VALUE = false;
        LOG.error("init black white list service failed, details: " + e.getMessage());
      }
      try {
        initOtherCluster(conf);
        initSecutiryDir(conf);
      } catch (IOException e) {
        LOG.error("init other cluster security dir failed, details: " + e.getMessage());
      }

    }
  }

  /**
   * init the white list
   * @param conf
   * @throws IOException
   */
  public static void initWhiteList(Configuration conf) throws IOException {
    WHITE_LIST_ENABLE_VALUE = conf.getBoolean(WHITE_LIST_ENABLE, false);
    if (WHITE_LIST_ENABLE_VALUE) {
      try {
        WHITE_LIST_ENABLE_VALUE = false;
        String whiteListPath = conf.get(WHITE_LIST_FILE_DIR, null);
        if (whiteListPath == null) {
          throw new IOException("whiteList service is open," +
                  "but white.list.file.dir is not in hdfs-site.xml");
        }
        Map<String, BlackWhiteList> blackWhiteListMap =
                BlackWhiteListConfigUtils.loadBlackWhiteList(whiteListPath);
        WHITELIST_MAP.clear();
        WHITELIST_MAP.putAll(blackWhiteListMap);
        WHITE_LIST_ENABLE_VALUE = true;
        LOG.info("white list init success");
      } catch (IOException e) {
        WHITE_LIST_ENABLE_VALUE = false;
        throw new IOException("white list init failed, details: " + e.getMessage());
      }
    }
  }

  /**
   * init the black list
   * @param conf
   * @throws IOException
   */
  public static void initBlackList(Configuration conf) throws IOException {
    WHITE_LIST_ENABLE_VALUE = conf.getBoolean(WHITE_LIST_ENABLE, false);
    if (WHITE_LIST_ENABLE_VALUE) {
      try {
        WHITE_LIST_ENABLE_VALUE = false;
        String blackListPath = conf.get(BLACK_LIST_FILE_DIR, null);
        if (blackListPath == null) {
          throw new IOException("whiteList service is open, " +
                  "but black.list.file.dir is not in hdfs-site.xml");
        }
        Map<String, BlackWhiteList> blackWhiteListMap =
                BlackWhiteListConfigUtils.loadBlackWhiteList(blackListPath);
        BLACKLIST_MAP.clear();
        BLACKLIST_MAP.putAll(blackWhiteListMap);
        WHITE_LIST_ENABLE_VALUE = true;
        LOG.info("black list init success");
      } catch (IOException e) {
        WHITE_LIST_ENABLE_VALUE = false;
        throw new IOException("black list init failed, details: " + e.getMessage());
      }
    }
  }

  /**
   * init the namenode info in black white list
   * @param conf
   * @throws IOException
   */
  public static void initNameNode(Configuration conf) throws IOException {
    WHITE_LIST_ENABLE_VALUE = conf.getBoolean(WHITE_LIST_ENABLE, false);
    if (WHITE_LIST_ENABLE_VALUE) {
      try {
        WHITE_LIST_ENABLE_VALUE = false;
        List<String> namodeList = BlackWhiteListUtils.getNamenodeList(conf);
        NAMENNODE_SET.clear();
        NAMENNODE_SET.addAll(namodeList);
        WHITE_LIST_ENABLE_VALUE = true;
        LOG.info("black white list init namenode success");
      } catch (IOException e) {
        WHITE_LIST_ENABLE_VALUE = false;
        throw new IOException("namenode info in white list init failed, " +
                "details: " + e.getMessage());
      }
    }
  }

  /**
   * init the datanode info in black white list
   * @param conf
   * @throws IOException
   */
  public static void initDataNode(Configuration conf) throws IOException {
    WHITE_LIST_ENABLE_VALUE = conf.getBoolean(WHITE_LIST_ENABLE, false);
    if (WHITE_LIST_ENABLE_VALUE) {
      try {
        WHITE_LIST_ENABLE_VALUE = false;
        List<String> datanodeList = BlackWhiteListUtils.getDatanodelist(conf);
        DATANODE_SET.clear();
        DATANODE_SET.addAll(datanodeList);
        WHITE_LIST_ENABLE_VALUE = true;
        LOG.info("black white list init datanode success");
      } catch (IOException e) {
        WHITE_LIST_ENABLE_VALUE = false;
        throw new IOException("datanode info in white list init failded, " +
                "details: " + e.getMessage());
      }
    }
  }

  public static void initOtherCluster(Configuration conf) throws IOException {
    WHITE_LIST_ENABLE_VALUE = conf.getBoolean(WHITE_LIST_ENABLE, false);
    if (WHITE_LIST_ENABLE_VALUE) {
      InputStream stdin = null;
      try {
        WHITE_LIST_ENABLE_VALUE = false;
        String otherClusterConfPath = conf.get(OTHER_CLUSTER_FILE_DIR, null);
        if (otherClusterConfPath == null) {
          WHITE_LIST_ENABLE_VALUE = true;
          throw new IOException("black white list is open, " +
                  "but other.cluster.file.dir is not in hdfs-sit.xml");
        }
        stdin = new FileInputStream(otherClusterConfPath);
        List<String> otherClusterList = IOUtils.readLines(stdin);
        OTHER_CLUSTER_SET.clear();
        OTHER_CLUSTER_SET.addAll(otherClusterList);
        WHITE_LIST_ENABLE_VALUE = true;
        LOG.info("all other cluster in black white list init success");
      } catch (IOException e) {
        WHITE_LIST_ENABLE_VALUE = false;
        throw new IOException("init the otherCluster info in black white list failed, " +
                "details: " + e.getMessage());
      }
    }
  }

  /**
   * init the security dir
   * @param conf
   * @throws IOException
   */
  public static void initSecutiryDir(Configuration conf) throws IOException {
    SECURITY_PATH_ENABLE_VALUE = conf.getBoolean(SECURITY_PATH_ENABLE, false);
    if (SECURITY_PATH_ENABLE_VALUE) {
      InputStream stdin = null;
      SECURITY_PATH_ENABLE_VALUE = false;
      try {
        String securiryConfPath = conf.get(SECURITY_PATH_FILE, null);
        if (securiryConfPath == null) {
          throw new IOException("security service is open, " +
                  "but security conf is not in hdfs-site.xml");
        }
        stdin = new FileInputStream(securiryConfPath);
        List<String> securityDir = IOUtils.readLines(stdin);
        SECURITY_PATH_SET.clear();
        SECURITY_PATH_SET.addAll(securityDir);
        SECURITY_PATH_ENABLE_VALUE = true;
        LOG.info("security dir init success");
      } catch (IOException e) {
        SECURITY_PATH_ENABLE_VALUE = false;
        throw new IOException("security dir init failed, details: " + e.getMessage());
      }
    }
  }

  /**
   * refresh the white list
   * @throws IOException
   */
  static void refreshWhiteList() throws IOException {
    Configuration conf = new Configuration();
    initNameNode(conf);
    initDataNode(conf);
    initWhiteList(conf);
  }

  /**
   * refresh the black list
   * @throws IOException
   */
  static void refreshBlackList() throws IOException {
    Configuration conf = new Configuration();
    initBlackList(conf);
  }

  /**
   * refresh the namenode
   * @throws IOException
   */
  static void refreshNameNode() throws IOException {
    Configuration conf = new Configuration();
    initNameNode(conf);
  }

  /**
   * refresh the datanode
   * @throws IOException
   */
  static void refreshDataNode() throws IOException {
    Configuration conf = new Configuration();
    initDataNode(conf);
  }

  /**
   * refresh the other cluster info
   * @throws IOException
   */
  static void refreshOtherCluster() throws IOException {
    Configuration conf = new Configuration();
    initOtherCluster(conf);
  }

  /**
   * refresh security dir
   * @throws IOException
   */
  static void refreshSecurityDir() throws IOException {
    Configuration conf = new Configuration();
    initSecutiryDir(conf);
  }

  /**
   * get the white list
   * @return
   */
  static Text getWhiteList() {
    StringBuilder sb = new StringBuilder("");
    for (String ip : WHITELIST_MAP.keySet()) {
      sb.append(ip).append(":").append(WHITELIST_MAP.get(ip).getUserList())
              .append(":").append("\n");
    }
    return new Text(sb.toString());
  }

  /**
   * get the black list
   * @return
   */
  static Text getBlackList() {
    StringBuilder sb = new StringBuilder("");
    for (String ip : BLACKLIST_MAP.keySet()) {
      sb.append(ip).append(":").append(BLACKLIST_MAP.get(ip).getUserList())
              .append(":").append("\n");
    }
    return new Text(sb.toString());
  }

  /**
   * get the namenode list
   * @return
   */
  static Text getNameNodeList() {
    StringBuilder sb = new StringBuilder("");
    for (String ip : NAMENNODE_SET) {
      sb.append(ip).append("\n");
    }
    return new Text(sb.toString());
  }

  /**
   * get the datanode list
   * @return
   */
  static Text getDataNodeList() {
    StringBuilder sb = new StringBuilder("");
    for (String ip : DATANODE_SET) {
      sb.append(ip).append("\n");
    }
    return new Text(sb.toString());
  }

  /**
   * get othercluster list
   * @return
   */
  static Text getOtherClusterList() {
    StringBuilder sb = new StringBuilder("");
    for (String ip : OTHER_CLUSTER_SET) {
      sb.append(ip).append("\n");
    }
    return new Text(sb.toString());
  }

  /**
   * get the security path list
   * @return
   */
  static Text getSecurityDir() {
    StringBuilder sb = new StringBuilder("");
    for (String path : SECURITY_PATH_SET) {
      sb.append(path).append("\n");
    }
    return new Text(sb.toString());
  }
}
