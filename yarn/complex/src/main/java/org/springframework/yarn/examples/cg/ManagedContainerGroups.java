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
package org.springframework.yarn.examples.cg;

import java.util.List;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.springframework.yarn.am.allocate.ContainerAllocateData;

/**
 * Interface for tracked and managed containers
 * with group association. Having a simple cluster where
 * all containers are treaded same will bring up a problem
 * when cluster size needs to be reduced. This implementation
 * is adding functionality to mark containers to belong to different
 * groups and later when cluster size is reduced user is able
 * to change size if individual groups.
 * <p>
 * Common use case is that for example two different tasks needs to
 * be distributed into hadoop where one task may stay relatively static
 * in terms of node count and other needs to be extended order to scale
 * up the performance. Later if cluster size is reduced user can change
 * the size of the nodes assigned to particular task.
 * <p>
 * Also other use cases for grouping is to place nodes into a group based
 * on container priority or locality.
 *
 * @author Janne Valkealahti
 *
 */
public interface ManagedContainerGroups {


	/**
	 * Adds the Yarn {@link Container} into managed container
	 * groups.
	 *
	 * @param container the container
	 */
	void addContainer(Container container);

	/**
	 * Sets the managed group projected size.
	 *
	 * @param group the group name
	 * @param count the count
	 */
	void setProjectedSize(String group, int count);

	/**
	 * Gets the container allocate data.
	 *
	 * @return the container allocate data
	 */
	ContainerAllocateData getContainerAllocateData();

	/**
	 * Gets the released containers.
	 *
	 * @return the released containers
	 */
	List<ContainerId> getReleasedContainers();

	/**
	 * Gets the group name.
	 *
	 * @param id the id
	 * @return the group name
	 */
	String getGroupName(String id);

}
