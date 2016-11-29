package org.apache.hadoop.hdfs.server.namenode.blackwhitelist;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.retry.Idempotent;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.security.blackwhitelist.BlackWhiteListConfigUtils;

import java.io.IOException;

import static org.apache.hadoop.security.blackwhitelist.BlackWhiteListConfigUtils.*;

/**
 * Created by iwardzhong on 2016/11/24.
 */
public class BlackWhiteListProtocolImpl implements BlackWhiteListProtocol {
    @Override
    @Idempotent
    public Text getWhiteList() {
      if (!WHITE_LIST_ENABLE_VALUE) {
        return new Text(N);
      }
      return BlackWhiteListRpcServer.getWhiteList();
    }

    @Override
    @Idempotent
    public Text getBlackList() {
      if (!WHITE_LIST_ENABLE_VALUE) {
        return new Text(N);
      }
      return BlackWhiteListRpcServer.getBlackList();
    }

    @Override
    @Idempotent
    public Text getNameNode() {
      if (!WHITE_LIST_ENABLE_VALUE) {
        return new Text(N);
      }
      return BlackWhiteListRpcServer.getNameNodeList();
    }

    @Override
    @Idempotent
    public Text getDataNode() {
      if (!WHITE_LIST_ENABLE_VALUE) {
        return new Text(N);
      }
      return BlackWhiteListRpcServer.getDataNodeList();
    }

    @Override
    @Idempotent
    public Text getOtherCluster() {
      if (!WHITE_LIST_ENABLE_VALUE) {
        return new Text(N);
      }
      return BlackWhiteListRpcServer.getOtherClusterList();
    }

    @Override
    @Idempotent
    public Text getSecurityDir() {
      if (!WHITE_LIST_ENABLE_VALUE) {
        return new Text(N);
      }
      return BlackWhiteListRpcServer.getSecurityDir();
    }

    @Override
    @Idempotent
    public void refreshWhiteList() throws IOException {
      BlackWhiteListRpcServer.refreshWhiteList();
    }

    @Override
    @Idempotent
    public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
      return versionID;
    }

    @Override
    @Idempotent
    public void refreshBlackList() throws IOException {
      BlackWhiteListRpcServer.refreshBlackList();
    }

    @Override
    @Idempotent
    public void refreshNameNode() throws IOException {
      BlackWhiteListRpcServer.refreshNameNode();
    }

    @Override
    @Idempotent
    public void refreshDataNode() throws IOException {
      BlackWhiteListRpcServer.refreshDataNode();
    }

    @Override
    @Idempotent
    public void refreshOtherCluster() throws IOException {
      BlackWhiteListRpcServer.refreshOtherCluster();
    }

    @Override
    @Idempotent
    public ProtocolSignature getProtocolSignature(String p, long c, int i) throws IOException {
      return new ProtocolSignature(versionID, null);
    }

    @Override
    @Idempotent
    public void refreshSecurityDir() throws IOException {
      BlackWhiteListRpcServer.refreshSecurityDir();
    }
}
