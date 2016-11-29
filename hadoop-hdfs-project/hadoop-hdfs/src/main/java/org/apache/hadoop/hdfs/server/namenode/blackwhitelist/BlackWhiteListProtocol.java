package org.apache.hadoop.hdfs.server.namenode.blackwhitelist;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.retry.Idempotent;
import org.apache.hadoop.ipc.VersionedProtocol;

import java.io.IOException;

/**
 * Created by iwardzhong on 2016/11/23.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public interface BlackWhiteListProtocol extends VersionedProtocol{
    long versionID = 13811426717L;

    /**
     * get the white list
     * @return
     */
    @Idempotent
    Text getWhiteList();

    /**
     * get the black list
     * @return
     */
    @Idempotent
    Text getBlackList();

    /**
     * get the namenode info in the white list
     * @return
     */
    @Idempotent
    Text getNameNode();

    /**
     * get the datanode info in the white list
     * @return
     */
    @Idempotent
    Text getDataNode();

    /**
     * get the othercluster info in the white list
     * @return
     */
    @Idempotent
    Text getOtherCluster();

    /**
     * get the security dir, which can not be delete
     * @return
     */
    @Idempotent
    Text getSecurityDir();

    /**
     * refresh white list
     * @throws IOException
     */
    @Idempotent
    void refreshWhiteList() throws IOException;

    /**
     * refresh black list
     * @throws IOException
     */
    @Idempotent
    void refreshBlackList() throws IOException;

    /**
     * refresh the namenode info in the white list
     * @throws IOException
     */
    @Idempotent
    void refreshNameNode() throws IOException;

    /**
     * refresh the datanode info in the white list
     * @throws IOException
     */
    @Idempotent
    void refreshDataNode() throws IOException;

    /**
     * refresh the other cluster info in the white list
     * @throws IOException
     */
    @Idempotent
    void refreshOtherCluster() throws IOException;

    /**
     * refresh the security dir
     * @throws IOException
     */
    @Idempotent
    void refreshSecurityDir() throws IOException;


}
