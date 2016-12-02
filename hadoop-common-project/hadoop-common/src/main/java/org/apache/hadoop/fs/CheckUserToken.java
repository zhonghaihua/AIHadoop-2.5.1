package org.apache.hadoop.fs;

import java.io.*;
import java.util.*;
import java.lang.*;
import java.net.*;

import org.codehaus.jettison.json.*;

public class CheckUserToken {
  public static String sendGet(String url, String param) {
    String result = "";
    BufferedReader in = null;

    try {
      String urlNameString = url + "?" + param;
      URL realUrl = new URL(urlNameString);

      //open a connection
      URLConnection connection = realUrl.openConnection();
      //set Attribute
      connection.setRequestProperty("accept", "*/*");
      connection.setRequestProperty("connection", "Keep-Alive");
      connection.setRequestProperty("user-agent", "Mozilla/4.0 (compatible; " +
              "MSIE 6.0; Windows NT 5.1;SV1)");
      //connect url
      connection.connect();
      //get Response head
      Map<String, List<String>> map = connection.getHeaderFields();

			/*for (String key : map.keySet()) {
                System.out.println(key + " : " + map.get(key));
			}*/

      in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
      String line;
      while ((line = in.readLine()) != null) {
        result += line;
      }
    } catch (Exception e) {
      System.out.println("request:get action error!" + e);
      e.printStackTrace();
    } finally {
      try {
        if (in != null) {
          in.close();
        }
      } catch (Exception e2) {
        e2.printStackTrace();
      }
    }

    return result;
  }

  public static String sendPost(String url, String param) {
    String result = "";
    PrintWriter out = null;
    BufferedReader in = null;

    try {
      URL realUrl = new URL(url);

      //open a connection
      URLConnection connection = realUrl.openConnection();
      //set Attribute
      connection.setRequestProperty("accept", "*/*");
      connection.setRequestProperty("connection", "Keep-Alive");
      connection.setRequestProperty("content-type", "JSON");
      connection.setRequestProperty("user-agent", "Mozilla/4.0 (compatible; " +
              "MSIE 6.0; Windows NT 5.1;SV1)");

      connection.setDoOutput(true);
      connection.setDoInput(true);
      //get connection outputStream
      out = new PrintWriter(connection.getOutputStream());
      //post param
      out.print(param);
      out.flush();
      //get connection inputStream and read contect
      in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
      String line;
      while ((line = in.readLine()) != null) {
        result += line;
      }
    } catch (Exception e) {
      System.out.println("request:post action error!" + e);
      e.printStackTrace();
    } finally {
      try {
        if (out != null) {
          out.close();
        }

        if (in != null) {
          in.close();
        }
      } catch (Exception e2) {
        e2.printStackTrace();
      }
    }

    return result;
  }

  public static boolean checkToken(String token, String business_name,
                                   String rtx_name, String action) throws Exception {
    String url = "http://10.151.15.155:8008/checkAuthority.php";
    JSONObject json = new JSONObject();
    json.put("token", token);
    json.put("business_name", business_name);
    json.put("rtx_name", rtx_name);
    json.put("action", action);

    String res = CheckUserToken.sendPost(url, json.toString());
    json = new JSONObject(res);

    res = json.getString("auth");

    if (res.equals("True")) {
      return true;
    } else {
      return false;
    }
  }

  public static String getRTXByToken(String token) throws Exception {
    String url = "http://10.151.15.155:8008/token2user.php";
    String param = "token=" + token;

    String res = CheckUserToken.sendGet(url, param);
    JSONObject json = new JSONObject(res);

    return json.getJSONObject("data").getString("rtx_name");
  }
}
