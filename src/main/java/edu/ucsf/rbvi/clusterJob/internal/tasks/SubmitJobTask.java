package edu.ucsf.rbvi.clusterJob.internal.tasks;

import org.cytoscape.application.CyApplicationManager;
import org.cytoscape.jobs.CyJob;
import org.cytoscape.jobs.CyJobData;
import org.cytoscape.jobs.CyJobDataService;
import org.cytoscape.jobs.CyJobExecutionService;
import org.cytoscape.jobs.CyJobManager;
import org.cytoscape.jobs.CyJobStatus;
import org.cytoscape.jobs.SUIDUtil;
import org.cytoscape.model.CyNetwork;
import org.cytoscape.service.util.CyServiceRegistrar;
import org.cytoscape.task.AbstractNetworkTask;
import org.cytoscape.work.TaskIterator;
import org.cytoscape.work.TaskMonitor;

import edu.ucsf.rbvi.clusterJob.internal.handlers.ClusterJobHandler;

public class SubmitJobTask extends AbstractNetworkTask {
	final CyServiceRegistrar registrar;

	public SubmitJobTask(CyNetwork network, CyServiceRegistrar registrar) {
		super(network);
		this.registrar = registrar;
	}

	public void run(TaskMonitor monitor) {
		// Get the execution service
		CyJobExecutionService executionService = 
						registrar.getService(CyJobExecutionService.class, "(title=ClusterJobExecutor)");
		CyApplicationManager appManager = registrar.getService(CyApplicationManager.class);
		CyNetwork currentNetwork = appManager.getCurrentNetwork();

		// Get our initial job
		CyJob job = executionService.getCyJob("ClusterJob", "http://dev/null");
		// Get the data service
		CyJobDataService dataService = job.getJobDataService();
		// Add our data
		CyJobData jobData = dataService.addData(null, "network", currentNetwork, currentNetwork.getNodeList(), null, null);
		// Save our SUIDs in case we get saved and restored
		SUIDUtil.saveSUIDs(job, currentNetwork, currentNetwork.getNodeList());
		// Create our handler
		ClusterJobHandler jobHandler = new ClusterJobHandler(job, network);
		job.setJobHandler(jobHandler);
		// Submit the job
		CyJobStatus exStatus = executionService.executeJob(job, null, null, jobData);
		if (exStatus.getStatus().equals(CyJobStatus.Status.ERROR) ||
		    exStatus.getStatus().equals(CyJobStatus.Status.UNKNOWN)) {
			monitor.showMessage(TaskMonitor.Level.ERROR, exStatus.toString());
			return;
		}
		CyJobManager manager = registrar.getService(CyJobManager.class);
		manager.addJob(job, jobHandler, 5);
	}
}

