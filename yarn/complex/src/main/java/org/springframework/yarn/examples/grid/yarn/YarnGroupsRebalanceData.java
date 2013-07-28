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
import org.springframework.yarn.examples.grid.GroupsRebalanceData;

/**
 * Yarn specific extension of {@link GroupsRebalanceData}
 * adding info for {@link ContainerAllocateData} and a list
 * of {@link ContainerId}s.
 *
 * @author Janne Valkealahti
 *
 */
public interface YarnGroupsRebalanceData extends GroupsRebalanceData {

	/**
	 * Gets the container {@link ContainerAllocateData}
	 *
	 * @return Container allocate data
	 */
	ContainerAllocateData getAllocateData();

	/**
	 * Gets a list of {@link ContainerId}s.
	 *
	 * @return List of container id's
	 */
	List<ContainerId> getContainers();

}
