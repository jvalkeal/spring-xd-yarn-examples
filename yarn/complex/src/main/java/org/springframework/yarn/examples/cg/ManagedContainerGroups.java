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
	 * groups. Implementation is responsible for container.
	 * Generally this means that user of the implementation
	 * should pass {@link Container} into
	 * {@link ManagedContainerGroups} either after allocation
	 * or launch.
	 *
	 * @param container the container
	 */
	void addContainer(Container container);

	/**
	 * Removes the member managed container groups.
	 *
	 * @param id the member id
	 */
	void removeMember(String id);

	/**
	 * Sets the managed group projected size.
	 *
	 * @param group the group name
	 * @param count the count
	 */
	void setProjectedSize(String group, int count);

	/**
	 * Gets the container allocate data. This method
	 * needs to be called periodically to get notification
	 * from a {@link ManagedContainerGroups} what kind
	 * of {@link ContainerAllocateData} should be used
	 * to allocate new containers.
	 *
	 * @return the container allocate data
	 */
	ContainerAllocateData getContainerAllocateData();

	/**
	 * Gets the released containers. This method
	 * needs to be called periodically to get
	 * notification for {@link ContainerId}s to
	 * be released.
	 *
	 * @return the released containers
	 */
	List<ContainerId> getReleasedContainers();

	/**
	 * Gets the group name. Method should
	 * find a match for group name by using
	 * a given group member id.
	 *
	 * @param id the member id
	 * @return the group name
	 */
	String findGroupNameByMember(String id);

}
