package edu.ucsf.rbvi.clusterJob.internal.io;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class HttpUtils {
	static boolean debug = true;
	public static Object getJSON(String url, Map<String, String> queryMap, Logger logger) {

		if (debug) {
			MockHttpServer server = MockHttpServer.getServer();
			return server.getJSON(url, queryMap, logger);
		}

		// Set up our connection
		CloseableHttpClient client = HttpClients.createDefault();
		String args = HttpUtils.getStringArguments(queryMap);
		HttpGet request = new HttpGet(url+"?"+args);
		// List<NameValuePair> nvps = HttpUtils.getArguments(queryMap);
		System.out.println("URL: "+url+"?"+args);
		Object jsonObject = null;

		// The underlying HTTP connection is still held by the response object
		// to allow the response content to be streamed directly from the network socket.
		// In order to ensure correct deallocation of system resources
		// the user MUST call CloseableHttpResponse#close() from a finally clause.
		// Please note that if response content is not fully consumed the underlying
		// connection cannot be safely re-used and will be shut down and discarded
		// by the connection manager. 
		CloseableHttpResponse response1 = null;
		try {
			// request.setEntity(new UrlEncodedFormEntity(nvps));
			response1 = client.execute(request);
			HttpEntity entity1 = response1.getEntity();
			InputStream entityStream = entity1.getContent();
			if (entity1.getContentLength() == 0 || entityStream.available() == 0)
				return null;
			BufferedReader reader = new BufferedReader(new InputStreamReader(entityStream));
			JSONParser parser = new JSONParser();
			jsonObject = parser.parse(reader);

			// and ensure it is fully consumed
			EntityUtils.consume(entity1);
		} catch (Exception e) {
			logger.error("Unable to parse JSON from server: "+e.getMessage());
			e.printStackTrace();
		} finally {
			try {
				response1.close();
			} catch(Exception e) {
				logger.error("Unable to send request: "+e.getMessage());
				e.printStackTrace();
			}
		}
		return jsonObject;
	}

	public static Object postJSON(String url, Map<String, String> queryMap, Logger logger) {
		if (debug) {
			MockHttpServer server = MockHttpServer.getServer();
			return server.postJSON(url, queryMap, logger);
		}

		// Set up our connection
		CloseableHttpClient client = HttpClients.createDefault();
		HttpPost request = new HttpPost(url);
		List<NameValuePair> nvps = HttpUtils.getArguments(queryMap);
		Object jsonObject = null;

		// The underlying HTTP connection is still held by the response object
		// to allow the response content to be streamed directly from the network socket.
		// In order to ensure correct deallocation of system resources
		// the user MUST call CloseableHttpResponse#close() from a finally clause.
		// Please note that if response content is not fully consumed the underlying
		// connection cannot be safely re-used and will be shut down and discarded
		// by the connection manager. 
		CloseableHttpResponse response1 = null;
		try {
			request.setEntity(new UrlEncodedFormEntity(nvps));
			response1 = client.execute(request);
			HttpEntity entity1 = response1.getEntity();
			InputStream entityStream = entity1.getContent();
			if (entity1.getContentLength() == 0)
				return null;
			BufferedReader reader = new BufferedReader(new InputStreamReader(entityStream));
			String lin;
			// while ((lin=reader.readLine()) != null) {
			// 	System.out.println(lin);
			// }
			JSONParser parser = new JSONParser();
			jsonObject = parser.parse(reader);

			// and ensure it is fully consumed
			EntityUtils.consume(entity1);
		} catch (Exception e) {
			logger.error("Unable to parse JSON from server: "+e.getMessage());
			e.printStackTrace();
			return null;
		} finally {
			try {
				response1.close();
			} catch(Exception e) {
				e.printStackTrace();
			}
		}
		return jsonObject;
	}

	public static String postText(String url, Map<String, String> queryMap, Logger logger) {
		if (debug) {
			MockHttpServer server = MockHttpServer.getServer();
			return server.postText(url, queryMap, logger);
		}

		CloseableHttpClient client = HttpClients.createDefault();
		HttpPost request = new HttpPost(url);
		List<NameValuePair> nvps = HttpUtils.getArguments(queryMap);
		CloseableHttpResponse response1 = null;
		StringBuilder builder = new StringBuilder();
		try {
			request.setEntity(new UrlEncodedFormEntity(nvps));
			response1 = client.execute(request);
			HttpEntity entity1 = response1.getEntity();
			BufferedReader reader = new BufferedReader(new InputStreamReader(entity1.getContent()));
			String line;
			while ((line = reader.readLine()) != null) {
				builder.append(line+"\n");
			}
			EntityUtils.consume(entity1);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				response1.close();
			} catch(Exception e) {
				e.printStackTrace();
			}
			return builder.toString();
		}
	}

	public static List<NameValuePair> getArguments(Map<String, String> args) {
		List<NameValuePair> nvps = new ArrayList<>();
		for (String key: args.keySet()) {
			nvps.add(new BasicNameValuePair(key, args.get(key)));
		}
		return nvps;
	}

	public static String getStringArguments(Map<String, String> args) {
		String s = null;
		try {
			for (String key: args.keySet()) {
				if (s != null) 
					s += "&"+key+"="+URLEncoder.encode(args.get(key));
				else
					s = key+"="+URLEncoder.encode(args.get(key));
			}
		} catch (Exception e) { e.printStackTrace(); }
		return s;
	}

}
