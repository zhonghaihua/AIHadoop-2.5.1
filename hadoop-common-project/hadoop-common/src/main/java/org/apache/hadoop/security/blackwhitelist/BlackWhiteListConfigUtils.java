package org.apache.hadoop.security.blackwhitelist;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.w3c.dom.*;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Created by iwardzhong on 2016/11/24.
 */
public class BlackWhiteListConfigUtils {

  public static final Log LOG = LogFactory.getLog(BlackWhiteListConfigUtils.class);

  public static final Set<String> NAMENNODE_SET = new TreeSet<String>();//nn node
  public static final Set<String> DATANODE_SET = new TreeSet<String>();//dn node
  public static final Set<String> OTHER_CLUSTER_SET = new TreeSet<String>();//other cluster node

  public static final Map<String, BlackWhiteList> WHITELIST_MAP = new HashMap<String, BlackWhiteList>();//white list user map
  public static final Map<String, BlackWhiteList> BLACKLIST_MAP = new HashMap<String, BlackWhiteList>();//black list user map
  public static final String WHITE_LIST_ENABLE = "white.list.enable";  //white list or not
  public static volatile boolean WHITE_LIST_ENABLE_VALUE = false;      //white list or not

  public static final String WHITE_LIST_SERVICE_PORT = "white.list.service.port"; //white list listen port
  public static final int WHITE_LIST_SERVICE_PORT_DEFAULT = 26717;                //default port
  public static final String WHITE_LIST_SERVICE_RPC_HANDLER = "white.list.service.handler.count";
  public static final int WHITE_LIST_SERVICE_RPC_HANDLER_DEFAULT = 5;
  public static final String WHITE_LIST_FILE_DIR = "white.list.file.dir";        //white list path
  public static final String BLACK_LIST_FILE_DIR = "black.list.file.dir";        //black list path
  public static final String OTHER_CLUSTER_FILE_DIR = "other.cluster.file.dir";  //other clust node path

  public static final String ALL = "ALL";
  public static final String Y = "Y";
  public static final String N = "N";
  public static final int TIME_OUT = 10 * 1000;//10s

  public static final String SECURITY_PATH_ENABLE = "security.path.enable";  //security dir or not
  public static volatile boolean SECURITY_PATH_ENABLE_VALUE = false;      //security dir or not
  public static final Set<String> SECURITY_PATH_SET = new TreeSet<String>();//security dir
  public static final String SECURITY_PATH_FILE = "security.path.file.dir"; //security dir path

  /**
   * load and analysis the xml
   * @param whiteListPath
   * @return
   * @throws IOException
   */
  public static Map<String, BlackWhiteList> loadBlackWhiteList(
          String whiteListPath) throws IOException {
    Map<String, BlackWhiteList> whiteListMap = new HashMap<String, BlackWhiteList>();
    try {
      File whiteListFile = new File(whiteListPath);
      DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
      docBuilderFactory.setIgnoringComments(true);
      DocumentBuilder builder = docBuilderFactory.newDocumentBuilder();
      Document doc = builder.parse(whiteListFile);
      Element root = doc.getDocumentElement();
      if (!"configuration".equals(root.getTagName()))
        throw new ParserConfigurationException("Bad" + whiteListPath + "config " +
                "file: top-level element not <configuration>");
      NodeList elements = root.getElementsByTagName("property");

      for (int i = 0; i < elements.getLength(); i++) {
        Node node = elements.item(i);
        Element element = (Element) node;
        NodeList chridElements = element.getChildNodes();
        BlackWhiteList whiteList = new BlackWhiteList();
        String ip = "default";
        for (int j = 0; j < chridElements.getLength(); j++) {
          Node chridNode = chridElements.item(j);
          if (!(chridNode instanceof Element))
            continue;
          Element chridElement = (Element) chridNode;
          if ("ip".equals(chridElement.getTagName())) {
            ip = ((Text) chridElement.getFirstChild()).getData().trim();
            whiteList.setIp(ip);
          } else if ("user".equals(chridElement.getTagName())) {
            String[] userList = ((Text) chridElement.getFirstChild()).getData().trim().split(",");
            for (String user : userList) {
              whiteList.getUserList().add(user.trim());
            }
          } else if ("description".equals(chridElement.getTagName())) {
            String description = ((Text) chridElement.getFirstChild()).getData().trim();
            whiteList.setDescription(description);
            if (whiteListMap.containsKey(ip)) {
              whiteListMap.get(ip).getUserList().addAll(whiteList.getUserList());
            } else {
              whiteListMap.put(ip, whiteList);
            }
          } else {
            throw new IOException("Bad element in" + whiteListPath
                    + " file: " + chridElement.getTagName());
          }
        }
      }
    } catch (Exception e) {
      throw new IOException("WhiteList Service Load " + whiteListPath
              + " Failed : " + e.getMessage());
    }
    return whiteListMap;
  }

  /**
   * authorize the ip and user
   * @param ip
   * @param user
   * @throws AuthorizationException
   */
  public static void authorizeWhiteList(String ip, String user) throws AuthorizationException {
    if (user.toLowerCase().equals("hdfs") &&
            !NAMENNODE_SET.contains(ip) && !DATANODE_SET.contains(ip)) {
      throw new AuthorizationException("your ip don't have enough auth to use 【hdfs】 user");
    }
    if (BLACKLIST_MAP.keySet().contains(ip)) {
      List<String> userList = BLACKLIST_MAP.get(ip).getUserList();
      if (userList.contains(user)) {
        throw new AuthorizationException("your ip: 【" + ip +
                "】 and user: 【" + user + "】 is in black list");
      }
    } else {
      if (!WHITELIST_MAP.keySet().contains(ip) && !NAMENNODE_SET.contains(ip)
              && !DATANODE_SET.contains(ip) && !OTHER_CLUSTER_SET.contains(ip)) {
        throw new AuthorizationException("your ip 【" + ip + "】 is not in white list");
      } else if (WHITELIST_MAP.keySet().contains(ip)) {
        List<String> userList = WHITELIST_MAP.get(ip).getUserList();
        if (!userList.contains(user)) {
          throw new AuthorizationException("your ip 【" + ip + "】 is in white list, " +
                  "but your user 【" + user + "】 is not in white list");
        }
      }
    }
  }

}
