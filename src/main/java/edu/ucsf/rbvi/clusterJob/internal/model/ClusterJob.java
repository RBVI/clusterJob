package edu.ucsf.rbvi.clusterJob.internal.model;

import java.util.Map;

import org.cytoscape.jobs.AbstractCyJob;
import org.cytoscape.jobs.CyJobDataService;
import org.cytoscape.jobs.CyJobExecutionService;
import org.cytoscape.jobs.CyJobHandler;

public class ClusterJob extends AbstractCyJob {

	public ClusterJob(String name, String basePath, 
	                  CyJobExecutionService executionService, 
										CyJobDataService dataService, CyJobHandler jobHandler) {
		super(name, basePath, executionService, dataService, jobHandler);
	}
}
