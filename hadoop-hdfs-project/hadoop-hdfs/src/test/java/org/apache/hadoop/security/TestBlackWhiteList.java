package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.blackwhitelist.BlackWhiteListRpcServer;
import org.apache.hadoop.security.blackwhitelist.BlackWhiteListConfigUtils;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by iwardzhong on 2016/11/28.
 */
public class TestBlackWhiteList {

  @Test
  public void testInitWhiteList() throws IOException {
    Configuration conf = new Configuration();
    conf.set("white.list.file.dir", "F:/whiteList.xml");
    BlackWhiteListRpcServer.initWhiteList(conf);
    System.out.println(BlackWhiteListConfigUtils.WHITELIST_MAP.keySet());
  }
}
