package edu.ucsf.rbvi.clusterJob.internal.io;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.json.simple.JSONObject;

import org.cytoscape.application.CyUserLog;
import org.cytoscape.jobs.CyJob;
import org.cytoscape.jobs.CyJobData;
import org.cytoscape.jobs.CyJobDataService;
import org.cytoscape.jobs.CyJobStatus;
import org.cytoscape.jobs.CyJobStatus.Status;
import org.cytoscape.jobs.CyJobExecutionService;
import org.cytoscape.jobs.CyJobHandler;
import org.cytoscape.jobs.CyJobManager;
import org.cytoscape.service.util.CyServiceRegistrar;
import org.cytoscape.session.CySession;


import edu.ucsf.rbvi.clusterJob.internal.model.ClusterJob;
import edu.ucsf.rbvi.clusterJob.internal.model.ClusterJobDataService;
import edu.ucsf.rbvi.clusterJob.internal.handlers.ClusterJobHandler;

/**
 * The main interface to the RBVI network cluster REST service.  The
 * general URL format for the execution is:
 * 	http://www.rbvi.ucsf.edu/clusterService/submit?algorithm=alg&param1=param&param2=param&...
 * followed by a JSON data set with all of the edges:
 * {
 * 	edges: [
 * 		{ source: SUID, target: SUID, sourceName: name, targetName: name, attrName: value }
 * 	]
 * }
 * where attrName is the name of the attribute to be used for the cluster algorithm.  
 * This should immediately return with a job id.
 *
 * To check the status of a job:
 * 	http://www.rbvi.ucsf.edu/clusterService/check?jobId=jobId
 *
 * To cancel a job:
 * 	http://www.rbvi.ucsf.edu/clusterService/cancel?jobId=jobId
 *
 * To get the completed data:
 * 	http://www.rbvi.ucsf.edu/clusterService/fetch?jobId=jobId
 *
 * The return
 * value is a very simple JSON:
 * {
 * 	nodes: [
 * 		{ node: SUID, clusterNumber: value }
 * 	]
 * }
 */

public class ClusterJobExecutionService implements CyJobExecutionService {
	static final Logger logger = Logger.getLogger(CyUserLog.NAME);
	static final String COMMAND = "command";
	static final String ERROR = "errorMessage";
	static final String JOBID = "jobId";
	static final String STATUS = "jobStatus";
	static final String STATUS_MESSAGE = "message";
	static final String SUBMIT = "submit";
	final ClusterJobDataService dataService;
	final CyJobManager cyJobManager;
	final CyServiceRegistrar cyServiceRegistrar;

	public enum Command {
		CANCEL("cancel"),
		SUBMIT("submit"),
		FETCH("fetch"),
		CHECK("check");

		String text;
		Command(String text) {
			this.text = text;
		}
		public String toString() { return text; }
	}

	public ClusterJobExecutionService(CyJobManager manager, CyServiceRegistrar registrar) {
		cyJobManager = manager;
		cyServiceRegistrar = registrar;
		dataService = new ClusterJobDataService(cyServiceRegistrar);
	}

	@Override
	public CyJobDataService getDataService() { return dataService; }

	@Override
	public CyJob getCyJob(String name, String basePath) {
		return new ClusterJob(name, basePath, this, dataService, null);
	}

	@Override
	public String getServiceName() { return "ClusterJobExecutionService"; }

	@Override 
	public CyJobStatus cancelJob(CyJob job) {
		System.out.println("Canceling the job!");
		if (job instanceof ClusterJob) {
			JSONObject obj = handleCommand((ClusterJob)job, Command.CANCEL, null);
			return getStatus(obj, null);
		}
		return new CyJobStatus(Status.ERROR, "CyJob is not a ClusterJob");
	}

	@Override
	public CyJobStatus checkJobStatus(CyJob job) {
		if (job instanceof ClusterJob) {
			JSONObject result = handleCommand((ClusterJob)job, Command.CHECK, null);
			return getStatus(result, null);
		}
		return new CyJobStatus(Status.ERROR, "CyJob is not a ClusterJob");
	}

	@Override
	public CyJobStatus executeJob(CyJob job, String basePath, Map<String, Object> configuration,
	                              CyJobData inputData) {
		if (!(job instanceof ClusterJob))
			return new CyJobStatus(Status.ERROR, "CyJob is not a ClusterJob");

		ClusterJob clJob = (ClusterJob)job;
		Map<String, String> queryMap = convertConfiguration(configuration);

		Object serializedData = dataService.getSerializedData(inputData);
		queryMap.put("inputData", serializedData.toString());
		queryMap.put(COMMAND, Command.SUBMIT.toString());

		Object value = HttpUtils.postJSON(job.getPath(), queryMap, logger);
		// value is the JSONObject returned from the query.  For our purposes
		// the values we care about are the status and the jobID
		if (value == null)
			return new CyJobStatus(Status.ERROR, "Job submission failed!");
		JSONObject json = (JSONObject) value;
		if (!json.containsKey(JOBID)) {
			System.out.println("JSON returned: "+json.toString());
			return new CyJobStatus(Status.ERROR, "Server didn't return an ID!");
		}

		String jobId = json.get(JOBID).toString();
		job.setJobId(jobId);

		// return getStatus(json, "Job '"+jobId+"' submitted");
		return getStatus(json, null);
	}

	@Override
	public CyJobStatus fetchResults(CyJob job, CyJobData data) {
		if (job instanceof ClusterJob) {
			JSONObject result = handleCommand((ClusterJob)job, Command.FETCH, null);

			// Get the unserialized data
			CyJobData newData = dataService.unSerialize(result);

			// Merge it in
			for (String key: newData.keySet()) {
				data.put(key, newData.get(key));
			}
			CyJobStatus resultStatus = getStatus(result, null);
			if (resultStatus == null)
				return new CyJobStatus(Status.FINISHED, "Data fetched");
		}
		return new CyJobStatus(Status.ERROR, "CyJob is not a ClusterJob");
	}

	@Override
	public CyJob restoreJobFromSession(CySession session, File sessionFile) {
		CyJob job = null;
		try {
			FileReader reader = new FileReader(sessionFile);
			CyJobData sessionData = dataService.unSerialize(reader);
			job = getCyJob(sessionData.get("name").toString(), 
			               sessionData.get("path").toString());
			job.setJobId(sessionData.get("JobId").toString());
			job.setPollInterval((Integer)sessionData.get("pollInterval"));
			String handlerClass = sessionData.get("jobHandler").toString();
			if (!handlerClass.equals(ClusterJobHandler.class.getCanonicalName())) {
				cyJobManager.associateHandler(job, handlerClass, -1);
			}
		} catch (FileNotFoundException fnf) {
			logger.error("Unable to read session file!");
		}

		return job;
	}

	@Override
	public void saveJobInSession(CyJob job, File sessionFile) {
		// Create a JSON object for our ClusterJob
		CyJobData sessionData = dataService.getDataInstance();
		sessionData.put("name", job.getJobName());
		sessionData.put("JobId", job.getJobId());
		sessionData.put("path", job.getPath());
		sessionData.put("pollInterval", job.getPollInterval());
		sessionData.put("jobHandler", job.getJobHandler().getClass().getCanonicalName());
		String data = dataService.getSerializedData(sessionData).toString();
		try {
			FileWriter writer = new FileWriter(sessionFile);
			writer.write(data);
			writer.close();
		} catch (IOException ioe) {
			logger.error("Unable to save job "+job.getJobId()+" in session!");
		}
	}

	private CyJobStatus getStatus(JSONObject obj, String message) {
		if (obj.containsKey(STATUS)) {
			Status status = Status.valueOf((String)obj.get(STATUS));
			// Did we get any information about our status?
			if (obj.containsKey(STATUS_MESSAGE)) {
				if (message == null || message.length() == 0)
					message = (String)obj.get(STATUS_MESSAGE);
			}
			return new CyJobStatus(status, message);
		} else if (obj.containsKey(ERROR)) {
			return new CyJobStatus(Status.ERROR, (String)obj.get(ERROR));
		}
		return null;
	}

	private JSONObject handleCommand(ClusterJob job, Command command, Map<String, String> argMap) {
		if (argMap == null)
			argMap = new HashMap<>();

		argMap.put(COMMAND, command.toString());
		argMap.put(JOBID, job.getJobId());
		return (JSONObject)HttpUtils.postJSON(job.getPath(), argMap, logger);
	}

	private Map<String, String> convertConfiguration(Map<String, Object> config) {
		Map<String, String> map = new HashMap<>();
		if (config == null || config.size() == 0)
			return map;
		for (String key: config.keySet()) {
			map.put(key, config.get(key).toString());
		}
		return map;
	}
}

