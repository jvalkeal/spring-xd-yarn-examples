/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.yarn.examples;

import java.util.concurrent.ScheduledFuture;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.springframework.util.Assert;
import org.springframework.yarn.am.AbstractEventingAppmaster;
import org.springframework.yarn.am.YarnAppmaster;
import org.springframework.yarn.am.allocate.AbstractAllocator;
import org.springframework.yarn.am.allocate.ContainerAllocator;
import org.springframework.yarn.examples.grid.ManagedContainerGroups;
import org.springframework.yarn.examples.grid.yarn.DefaultYarnContainerNode;
import org.springframework.yarn.examples.grid.yarn.YarnGroupsRebalanceData;
import org.springframework.yarn.examples.grid.yarn.YarnManagedContainerGroups;

/**
 * Implementation of application master which is utilizing concept
 * of {@link ManagedContainerGroups} order to manage running
 * containers.
 *
 * @author Janne Valkealahti
 *
 */
public abstract class AbstractManagedContainerGroupsAppmaster extends AbstractEventingAppmaster
		implements YarnAppmaster {

	private static final Log log = LogFactory.getLog(AbstractManagedContainerGroupsAppmaster.class);

	/** Container <-> Groups tracker */
	private YarnManagedContainerGroups managedGroups;

	/** Current running task if any */
	private volatile ScheduledFuture<?> runningTask;

	@Override
	public void submitApplication() {
		log.info("Submitting application");
		registerAppmaster();
		start();
		if(getAllocator() instanceof AbstractAllocator) {
			((AbstractAllocator)getAllocator()).setApplicationAttemptId(getApplicationAttemptId());
		}
	}

	@Override
	protected void onContainerAllocated(Container container) {
		log.info("XXX onContainerAllocated: " + container.getId() + " host=" + container.getNodeId().getHost());
		managedGroups.addContainerNode(new DefaultYarnContainerNode(container));
		getMonitor().addContainer(container);
		getLauncher().launchContainer(container, getCommands());
	}

	@Override
	protected void onContainerLaunched(Container container) {
		log.info("XXX onContainerLaunched: " + container.getId());
		getMonitor().reportContainer(container);
	}

	@Override
	protected void onContainerCompleted(ContainerStatus status) {
		log.info("XXX onContainerCompleted: " + status);
		super.onContainerCompleted(status);

		getMonitor().monitorContainer(status);

		int exitStatus = status.getExitStatus();
		ContainerId containerId = status.getContainerId();

		boolean handled = false;
		if (exitStatus > 0) {
			handled = onContainerFailed(containerId);
			if (!handled) {
				setFinalApplicationStatus(FinalApplicationStatus.FAILED);
				notifyCompleted();
			}
		} else {
			if (isComplete()) {
				notifyCompleted();
			}
		}
	}

	@Override
	protected void onInit() throws Exception {
		Assert.notNull(managedGroups, "managedGroups must be set");
		super.onInit();
	}

	@Override
	protected void doStart() {
		super.doStart();
		this.runningTask = getTaskScheduler().scheduleAtFixedRate(new ManagedGroupRunnable(), 5000);
	}

	@Override
	protected void doStop() {
		super.doStop();
		if (this.runningTask != null) {
			this.runningTask.cancel(true);
		}
		this.runningTask = null;
	}

	/**
	 * Sets the managed groups.
	 *
	 * @param managedGroups the new managed groups
	 */
	public void setManagedGroups(YarnManagedContainerGroups managedGroups) {
		this.managedGroups = managedGroups;
	}

	/**
	 * Gets the managed groups.
	 *
	 * @return the managed groups
	 */
	public YarnManagedContainerGroups getManagedGroups() {
		return managedGroups;
	}

	/**
	 * Called if completed container has failed. User
	 * may override this method to process failed container,
	 * i.e. making a request to re-allocate new container instead
	 * of failing the application.
	 *
	 * @param containerId the container id
	 * @return true, if container was handled.
	 */
	protected boolean onContainerFailed(ContainerId containerId) {
		return false;
	}

	protected void handleContainerFailed(ContainerId containerId) {
		if (!onContainerFailed(containerId)) {
			log.info("XXX removing failed member " + ConverterUtils.toString(containerId));
			managedGroups.removeContainerNode(ConverterUtils.toString(containerId));
		}
	}


	/**
	 * Returns state telling if application is considered
	 * as complete. Default implementation is delegating
	 * call to container monitor.
	 *
	 * @return True if application is complete
	 */
	protected boolean isComplete() {
		return getMonitor().hasRunning();
	}

	/**
	 * Runnable which is periodically used to handle allocation
	 * and release of containers based on state of managed groups.
	 */
	private class ManagedGroupRunnable implements Runnable {

		@Override
		public void run() {
			ContainerAllocator allocator = getAllocator();
//			for (ContainerId cid : managedGroups.getReleasedContainers()) {
//				log.info("XXX release cid=" + cid);
//				allocator.releaseContainer(cid);
//			}
//			ContainerAllocateData containerAllocateData = managedGroups.getContainerAllocateData();
//			log.info("XXX allocadata=" + containerAllocateData);
//			allocator.allocateContainers(containerAllocateData);
			YarnGroupsRebalanceData rebalanceData = managedGroups.getGroupsRebalanceData();
			for (ContainerId cid : rebalanceData.getContainers()) {
				log.info("XXX release cid=" + cid);
				allocator.releaseContainer(cid);
			}
			allocator.allocateContainers(rebalanceData.getAllocateData());
		}

	}

}
