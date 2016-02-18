package edu.ucsf.rbvi.clusterJob.internal.io;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.log4j.Logger;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import org.cytoscape.jobs.CyJobStatus;
import org.cytoscape.jobs.CyJobStatus.Status;

/**
 * Create a mock HTTP service for a remote cluster service
 * that takes node data as input and returns a (currently random)
 * cluster result
 */
public class MockHttpServer {
	static MockHttpServer instance = null;
	static final String ERROR = "errorMessage";
	static final String JOBID = "jobId";
	static final String STATUS = "jobStatus";
	static final String STATUS_MESSAGE = "message";
	static int lastID = 0;
	final ConcurrentMap<String, CyJobStatus> statusMap;
	final ConcurrentMap<String, JSONObject> dataMap;
	Timer timer = null;
	
	static public MockHttpServer getServer() {
		if (instance == null)
			instance = new MockHttpServer();
		return instance;
	}

	protected MockHttpServer() {
		statusMap = new ConcurrentHashMap<>();
		dataMap = new ConcurrentHashMap<>();
	}

	public Object getJSON(String url, Map<String, String> queryMap, Logger logger) {
		return null;
	}

	public Object postJSON(String url, Map<String, String> queryMap, Logger logger) {
		// Get the command
		String command = queryMap.get("command");
		if (command.equals("submit")) {
			return mockSubmit(queryMap, logger);
		} else if (command.equals("fetch")) {
			return mockFetch(queryMap, logger);
		} else if (command.equals("cancel")) {
			return mockCancel(queryMap, logger);
		} else if (command.equals("check")) {
			return mockCheck(queryMap, logger);
		}
		return null;
	}

	public String postText(String url, Map<String, String> queryMap, Logger logger) {
		return null;
	}

	private Object mockSubmit(Map<String, String> queryMap, Logger logger) {
		// Do we have the data we need?
		if (!queryMap.containsKey("inputData"))
			return jsonStatus(Status.ERROR, "Call to mockSubmit without any input");

		String jobId = "JobId "+lastID;
		lastID++;

		// Parse the input
		JSONParser parser = new JSONParser();
		JSONObject obj;
		try {
			obj = (JSONObject) parser.parse(queryMap.get("inputData"));
		} catch (Exception e) {
			return jsonStatus(Status.ERROR, "Unable to parse network data: "+e.getMessage());
		}

		if (!obj.containsKey("network"))
			return jsonStatus(Status.ERROR, "No network in input!");

		JSONObject netObject = (JSONObject)obj.get("network");

		// Extract the nodes
		if (!netObject.containsKey("nodes"))
			return jsonStatus(Status.ERROR, "No nodes in network!");

		// Save the job
		dataMap.put(jobId, obj);
		statusMap.put(jobId, new CyJobStatus(Status.SUBMITTED, null));
		// Start our timer (if not already running)
		if (timer == null) {
			timer = new Timer("MockHttpServer timer");
			timer.schedule(new MockHttpTask(this), 10000);
		}
		JSONObject jsonReturn = (JSONObject)jsonStatus(statusMap.get(jobId).getStatus(), "Job "+jobId+" submitted");
		jsonReturn.put(JOBID, jobId);
		System.out.println("mockSubmit json: "+jsonReturn.toString());
		return jsonReturn;
	}

	private Object mockFetch(Map<String, String> queryMap, Logger logger) {
		if (!queryMap.containsKey(JOBID)) {
			return jsonStatus(Status.UNKNOWN, "Call to mockFetch without a jobID");
		}
		String jobId = queryMap.get(JOBID);
		if (!statusMap.containsKey(jobId)) {
			return jsonStatus(Status.UNKNOWN, "Call to mockFetch without an unknown jobID");
		}

		// Get the network data
		JSONObject json = dataMap.get(jobId);
		JSONObject jsonNetwork = (JSONObject)json.get("network");
		JSONArray nodes = (JSONArray)jsonNetwork.get("nodes");

		int nodeCount = nodes.size();
		int nClusters = 5;
		for (Object nodeObject: nodes) {
			JSONObject node = (JSONObject) nodeObject;
			double rand = Math.random();
			int cluster = ((int)(rand*nodeCount))%nClusters;
			node.put("ClusterNumber", Integer.valueOf(cluster));
		}
		
		// Remove it
		remove(jobId);
		return json;
	}

	private Object mockCancel(Map<String, String> queryMap, Logger logger) {
		if (!queryMap.containsKey(JOBID)) {
			return jsonStatus(Status.UNKNOWN, "Call to mockCancel without a jobID");
		}
		String jobId = queryMap.get(JOBID);
		if (!statusMap.containsKey(jobId)) {
			return jsonStatus(Status.UNKNOWN, "Call to mockCancel without an unknown jobID");
		}
		remove(jobId);

		return jsonStatus(Status.CANCELED, "Job ID "+jobId+" canceled by user");
	}

	private Object mockCheck(Map<String, String> queryMap, Logger logger) {
		if (!queryMap.containsKey(JOBID)) {
			return jsonStatus(Status.UNKNOWN, "Call to mockCheck without a jobID");
		}
		String jobId = queryMap.get(JOBID);
		System.out.println("mockCheck for job id: "+jobId);
		if (!statusMap.containsKey(jobId)) {
			return jsonStatus(Status.UNKNOWN, "Call to mockCheck without an unknown jobID");
		}
		CyJobStatus status = statusMap.get(jobId);
		if (status.getStatus().equals(Status.CANCELED)) {
			remove(jobId);
		}
		return jsonStatus(status.getStatus(), status.getMessage());
	}

	private Object jsonStatus(Status status, String message) {
		JSONObject obj = new JSONObject();
		obj.put(STATUS, status.toString());
		if (message != null)
			obj.put(STATUS_MESSAGE, message);
		return obj;
	}

	private void remove(String jobId) {
		System.out.println("Removing "+jobId);
		statusMap.remove(jobId);
		dataMap.remove(jobId);

		// If we've just removed the last job, reset our timer
		if (statusMap.size() == 0) {
			timer.cancel();
			timer = null;
		} else {
			for (String j: statusMap.keySet()) {
				System.out.println("Job: "+j+" has status "+statusMap.get(j));
			}
		}
	}

	class MockHttpTask extends TimerTask {
		final MockHttpServer server;

		MockHttpTask(MockHttpServer s) {
			server = s;
		}

		public void run() {
			timer.cancel();
			for (String jobId: statusMap.keySet()) {
				// More or less randomly progress the status
				double r = Math.random();
				System.out.println("r = "+r);
				if (r > 0.2) {
					CyJobStatus stat = statusMap.get(jobId);
					Status newStat = Status.UNKNOWN;
					System.out.println("Current status = "+stat.getStatus().toString());
					switch (stat.getStatus()) {
						case SUBMITTED:
							newStat = Status.QUEUED;
							break;
						case QUEUED:
							newStat = Status.RUNNING;
							break;
						case RUNNING:
							newStat = Status.FINISHED;
							break;
						case FINISHED:
							newStat = Status.FINISHED;
							break;
						case CANCELED:
							newStat = Status.CANCELED;
							break;
					}
					System.out.println("New status = "+newStat.toString());
					statusMap.put(jobId, new CyJobStatus(newStat, stat.getMessage()));
				}
			}
			timer = new Timer("MockHttpServer timer");
			timer.schedule(new MockHttpTask(server), 5000);
		}

	}

}
