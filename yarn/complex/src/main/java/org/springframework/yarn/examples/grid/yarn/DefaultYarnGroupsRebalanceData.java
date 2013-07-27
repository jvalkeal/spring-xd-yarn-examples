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
package org.springframework.yarn.examples.grid.yarn;

import java.util.List;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.springframework.yarn.am.allocate.ContainerAllocateData;

/**
 * Default implementation of {@link YarnGroupsRebalanceData}.
 *
 * @author Janne Valkealahti
 *
 */
public class DefaultYarnGroupsRebalanceData implements YarnGroupsRebalanceData {

	private List<ContainerId> containers;

	private ContainerAllocateData allocateData;

	/**
	 * Instantiates a new default yarn groups rebalance data.
	 */
	public DefaultYarnGroupsRebalanceData() {
	}

	/**
	 * Instantiates a new default yarn groups rebalance data.
	 *
	 * @param containers the containers
	 * @param allocateData the allocate data
	 */
	public DefaultYarnGroupsRebalanceData(List<ContainerId> containers, ContainerAllocateData allocateData) {
		this.containers = containers;
		this.allocateData = allocateData;
	}

	@Override
	public List<ContainerId> getContainers() {
		return containers;
	}

	/**
	 * Sets the containers.
	 *
	 * @param containers the new containers
	 */
	public void setContainers(List<ContainerId> containers) {
		this.containers = containers;
	}

	@Override
	public ContainerAllocateData getAllocateData() {
		return allocateData;
	}

	/**
	 * Sets the allocate data.
	 *
	 * @param allocateData the new allocate data
	 */
	public void setAllocateData(ContainerAllocateData allocateData) {
		this.allocateData = allocateData;
	}

}
