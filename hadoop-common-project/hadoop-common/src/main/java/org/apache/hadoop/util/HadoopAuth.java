package org.apache.hadoop.util;

import java.util.Arrays;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.RequestEntity;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.codehaus.jettison.json.JSONObject;

public class HadoopAuth {
  public HadoopAuth(String[] args) {
    m_args = args;
  }

  public boolean verify() throws Exception {
    if (m_args.length < 3) {
      return false;
    }

    String user = m_args[0];
    String business = m_args[1];
    String token = m_args[3];

    String url = "http://10.151.15.155:8008/checkAuthority.php";
    HttpClient client = new HttpClient();
    PostMethod method = new PostMethod(url);

    JSONObject authinfo = new JSONObject();
    authinfo.put("rtx_name", user);
    authinfo.put("business_name", business);
    authinfo.put("token", token);
    String authinfo_str = authinfo.toString();

    RequestEntity se = new StringRequestEntity(authinfo_str, "application/json", "UTF-8");
    method.setRequestEntity(se);

    client.executeMethod(method);

    String auth_result = "False";

    if (method.getStatusCode() == HttpStatus.SC_OK) {
      String result = method.getResponseBodyAsString();
      JSONObject jsonObj = new JSONObject(result);
      auth_result = jsonObj.get("auth").toString();
    }

		/*
//		System.out.println("user:" + user + ",business:" + business);

		CloseableHttpClient httpclient = HttpClients.createDefault();
		HttpPost httppost = new HttpPost("http://10.151.15.155:8008/checkAuthority.php");

		JSONObject authinfo  = new JSONObject();
		authinfo.put("rtx_name", user);
		authinfo.put("business_name", business);
		authinfo.put("token", token);
		String authinfo_str = authinfo.toString();
		//System.out.println(authinfo_str);
		
		String auth_result = "False";
		try
		{
			httppost.setEntity(new StringEntity(authinfo_str));
			//System.out.println("executing request " + httppost.getURI()); 
			CloseableHttpResponse response = httpclient.execute(httppost); 
			try {
				HttpEntity entity = response.getEntity();  
				if (entity != null) {
					String result = EntityUtils.toString(entity, "UTF-8");
					//System.out.println("Response content: " + result); 
					JSONObject jsonObj  = new JSONObject(result);
					auth_result = jsonObj.get("auth").toString();
					//System.out.println(auth_result);
					
				}
			} finally {
				response.close();
			}
		} catch (ClientProtocolException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e1) {  
			e1.printStackTrace();
		} catch (IOException e) {  
			 e.printStackTrace();
		} finally {
			try {
				httpclient.close(); 
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		*/
    if (auth_result.equals("True")) {
      return true;
    }

    System.out.println("Identify fail, may be you need check your rtx_name, " +
            "business_name and token");
    return false;
  }

  public String[] encrypt() throws Exception {
    String[] newArgs = Arrays.asList(m_args).subList(3, m_args.length).toArray(new String[0]);
//    for (int i = 0; i < newArgs.length; i++) {
//      if (newArgs[i].equals("-input")) {
//        String input_path = newArgs[i + 1];
//        //base64 encode
//        //String new_input_path = new String(Base64.encodeBase64(input_path.getBytes()));
//
////        String new_input_path = (new BASE64Encoder()).encode(input_path.getBytes("utf-8"));
//        String token_md5 = "/f136803ab9c241079ba0cc1b5d02ee77";
//        String new_input_path = input_path + token_md5;
//        newArgs[i + 1] = new_input_path;
//        break;
//      }
//    }

    return newArgs;
  }

  private String[] m_args;
}
