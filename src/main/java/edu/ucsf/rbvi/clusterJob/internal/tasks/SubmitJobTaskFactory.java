package edu.ucsf.rbvi.clusterJob.internal.tasks;

import org.cytoscape.model.CyNetwork;
import org.cytoscape.service.util.CyServiceRegistrar;
import org.cytoscape.task.AbstractNetworkTaskFactory;
import org.cytoscape.work.TaskIterator;

public class SubmitJobTaskFactory extends AbstractNetworkTaskFactory {
	final CyServiceRegistrar registrar;
	public SubmitJobTaskFactory(CyServiceRegistrar registrar) {
		this.registrar = registrar;
	}

	public TaskIterator createTaskIterator(CyNetwork network) {
		return new TaskIterator(new SubmitJobTask(network, registrar));
	}
}

