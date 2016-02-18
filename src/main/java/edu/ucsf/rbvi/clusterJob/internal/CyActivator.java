package edu.ucsf.rbvi.clusterJob.internal;

import static org.cytoscape.work.ServiceProperties.COMMAND;
import static org.cytoscape.work.ServiceProperties.COMMAND_NAMESPACE;
import static org.cytoscape.work.ServiceProperties.IN_MENU_BAR;
import static org.cytoscape.work.ServiceProperties.MENU_GRAVITY;
import static org.cytoscape.work.ServiceProperties.PREFERRED_MENU;
import static org.cytoscape.work.ServiceProperties.TITLE;

import java.util.Properties;

import org.cytoscape.application.swing.CySwingApplication;
import org.cytoscape.jobs.CyJobExecutionService;
import org.cytoscape.jobs.CyJobManager;
import org.cytoscape.service.util.AbstractCyActivator;
import org.cytoscape.service.util.CyServiceRegistrar;
import org.cytoscape.session.events.SessionAboutToBeSavedListener;
import org.cytoscape.session.events.SessionLoadedListener;
import org.cytoscape.task.NetworkTaskFactory;
import org.cytoscape.work.TaskFactory;
import org.osgi.framework.BundleContext;


import edu.ucsf.rbvi.clusterJob.internal.io.ClusterJobExecutionService;
import edu.ucsf.rbvi.clusterJob.internal.tasks.SubmitJobTaskFactory;


public class CyActivator extends AbstractCyActivator {

	public CyActivator() {
		super();
	}

	public void start(BundleContext bc) throws Exception {
		// See if we have a graphics console or not
		CyServiceRegistrar registrar = getService(bc, CyServiceRegistrar.class);
		CyJobManager cyJobManager = getService(bc, CyJobManager.class);

		{
			Properties props = new Properties();
			ClusterJobExecutionService clusterJobService = 
							new ClusterJobExecutionService(cyJobManager, registrar);

			props.setProperty(TITLE, "ClusterJobExecutor");
			registerService(bc, clusterJobService, CyJobExecutionService.class, props);
		}

		{
			Properties props = new Properties();
			SubmitJobTaskFactory factory = new SubmitJobTaskFactory(registrar);
			props.setProperty(TITLE, "Submit cluster job");
			props.setProperty(PREFERRED_MENU, "Apps.TestRemote");
			registerService(bc, factory, NetworkTaskFactory.class, props);
		}

	}
}
