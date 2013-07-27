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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.springframework.yarn.examples.grid.ContainerGroup;

/**
 * Yarn specific Container Group encapsulating enough
 * information order to work with grouping of Yarn
 * Containers.
 *
 * @author Janne Valkealahti
 *
 */
public class YarnContainerGroup implements ContainerGroup {

	private final String id;
	private Set<YarnContainerGroupMember> members = new HashSet<YarnContainerGroupMember>();
	private int projectedSize;
	private boolean dirty = true;
	private List<String> hosts;

	/**
	 * Instantiates a new yarn container group.
	 *
	 * @param id Group identifier
	 */
	public YarnContainerGroup(String id) {
		this.id = id;
	}

	@Override
	public String getId() {
		return id;
	}

	/**
	 * Gets the size.
	 *
	 * @return the size
	 */
	public int getSize() {
		return members.size();
	}

	/**
	 * Gets the members.
	 *
	 * @return the members
	 */
	public Set<YarnContainerGroupMember> getMembers() {
		return members;
	}

	/**
	 * Adds the member.
	 *
	 * @param member the member
	 */
	public void addMember(YarnContainerGroupMember member) {
		members.add(member);
	}

	/**
	 * Gets the projected size.
	 *
	 * @return the projected size
	 */
	public int getProjectedSize() {
		return projectedSize;
	}

	/**
	 * Sets the projected size.
	 *
	 * @param projectedSize the new projected size
	 */
	public void setProjectedSize(int projectedSize) {
		if (projectedSize != getSize()) {
			dirty = true;
		}
		this.projectedSize = projectedSize;
	}

	/**
	 * Checks if is dirty.
	 *
	 * @return true, if is dirty
	 */
	public boolean isDirty() {
		return dirty;
	}

	/**
	 * Sets the dirty.
	 *
	 * @param dirty the new dirty
	 */
	public void setDirty(boolean dirty) {
		this.dirty = dirty;
	}

	/**
	 * Checks if is full.
	 *
	 * @return true, if is full
	 */
	public boolean isFull() {
		return !(getSize() < projectedSize);
	}

	public void setHosts(List<String> hosts) {
		this.hosts = hosts;
	}

	/**
	 * Gets the hosts.
	 *
	 * @return the hosts
	 */
	public List<String> getHosts() {
		return hosts;
	}

	@Override
	public String toString() {
		return "Group [id=" + id + ", members=" + members + ", projectedSize=" + projectedSize + ", dirty=" + dirty
				+ "]";
	}

}
