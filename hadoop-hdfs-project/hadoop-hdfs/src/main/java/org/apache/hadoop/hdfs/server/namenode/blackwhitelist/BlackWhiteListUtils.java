package org.apache.hadoop.hdfs.server.namenode.blackwhitelist;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.tools.HDFSConcat;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by iwardzhong on 2016/11/24.
 */
public class BlackWhiteListUtils {

  public static final Log LOG = LogFactory.getLog(BlackWhiteListUtils.class);

  public static synchronized List<String> getNamenodes(Configuration conf) throws IOException {
    List<String> namenodeList = new ArrayList<String>();
    try {
      Map<String, Map<String, InetSocketAddress>> nnMap = DFSUtil.getNNServiceRpcAddresses(conf);
      for (Map<String, InetSocketAddress> inetSocketAddressMap : nnMap.values()) {
        for (InetSocketAddress inetSocketAddress : inetSocketAddressMap.values()) {
          String nnHostname = inetSocketAddress.getHostName();
          namenodeList.add(nnHostname);
        }
      }
    } catch (IOException e) {
      throw new IOException("black white list get the namenode failed, " +
              "details: " + e.getMessage());
    }
    return namenodeList;
  }

  public static synchronized List<String> getNamenodeList(Configuration conf) throws IOException {
    List<String> namenodeList = new ArrayList<String>();
    try {
      List<String> namenodeListHostname = getNamenodes(conf);
      for (String nnHostname : namenodeListHostname) {
        String nnIp = getIpFromEtcHosts(nnHostname);
        namenodeList.add(nnIp);
      }
    } catch (IOException e) {
      throw new IOException("black white list get the namenode failed, " +
              "details: " + e.getMessage());
    }
    return namenodeList;
  }

  public static synchronized List<String> getDatanodelist(Configuration conf) throws IOException {
    List<String> datanodeList = new ArrayList<String>();
    try {
      DistributedFileSystem  dfs = (DistributedFileSystem) FileSystem.get(conf);
      DatanodeInfo[] datanodeInfos = dfs.getDataNodeStats(HdfsConstants.DatanodeReportType.ALL);
      if (datanodeInfos.length > 0) {
        for (DatanodeInfo datanodeInfo : datanodeInfos) {
          String datanodeIp = datanodeInfo.getIpAddr();
          datanodeList.add(datanodeIp);
        }
      }
    } catch (IOException e) {
      throw new IOException("black white list get the datanode failed, " +
              "details: " + e.getMessage());
    }
    return datanodeList;
  }

  public static String getIpFromEtcHosts(String hostname) throws IOException {
    String cmd = "cat /etc/hosts | grep \"" + hostname.trim() + "\"  | awk '{print $1}' | sort | uniq";
    try {
      String ip = execuShellCmd(cmd);
      return ip;
    } catch (IOException e) {
      LOG.info("get ip from /etc/hosts faild");
      throw new IOException("get ip from /etc/hosts faild, details: " + e.getMessage());
    }
  }

  public synchronized static String execuShellCmd(String cmd) throws IOException {
    Runtime rt = Runtime.getRuntime();
    InputStream stdin = null;
    String line = "";
    try {
      Process proc = rt.exec(new String[]{"/bin/sh", "-c", cmd}, null, null);
      stdin = proc.getInputStream();
      List<String> results = IOUtils.readLines(stdin);
      for (String result : results) {
        line = line + result;
      }
      return line.trim();
    } catch (Exception e) {
      LOG.error("execu shell command: " + cmd + " failed");
      throw new IOException("execu shell command: " + cmd + "failed");
    } finally {
      IOUtils.closeQuietly(stdin);
    }
  }
}
