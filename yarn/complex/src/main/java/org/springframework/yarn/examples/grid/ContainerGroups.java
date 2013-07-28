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
package org.springframework.yarn.examples.grid;

import java.util.Collection;

/**
 * Container groups is an extension on top of {@link ContainerGrid} to
 * introduce functionality of grouping of containers.
 * <p>
 * While this interface adds methods for groups the actual implementation
 * should handle group resolving when new Container nodes are added to
 * the grid using methods from a {@link ContainerGrid}.
 *
 * @author Janne Valkealahti
 *
 * @param <CN> the type of {@link ContainerNode}
 * @param <CG> the type of {@link ContainerGroup}
 * @see ContainerGrid
 */
public interface ContainerGroups<CN extends ContainerNode, CG extends ContainerGroup> extends ContainerGrid<CN> {

	/**
	 * Adds a new group.
	 *
	 * @param group Container group
	 */
	void addGroup(CG group);

	/**
	 * Removes a group by its id.
	 *
	 * @param id Container group identifier
	 */
	void removeGroup(String id);

	/**
	 * Gets a Container group by its identifier.
	 *
	 * @param id Container group identifier
	 * @return Container group
	 */
	CG getGroup(String id);

	/**
	 * Gets a collection of Container groups or empty collection of there
	 * are no groups defined.
	 *
	 * @return Collection of Container groups
	 */
	Collection<CG> getGroups();

	/**
	 * Gets a Container Group where Container node belongs to.
	 *
	 * @param id Container Node id
	 * @return Container Group or NULL if not match
	 */
	CG getGroupByMember(String id);

}
