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

import java.util.Collection;
import java.util.Hashtable;
import java.util.List;

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

	/** Group identifier, usually just name */
	private final String id;

	private Hashtable<String, YarnContainerNode> members = new Hashtable<String, YarnContainerNode>();

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

	public YarnContainerNode removeMember(String id) {
		return members.remove(id);
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
	public Collection<YarnContainerNode> getMembers() {
		return members.values();
	}

	public boolean hasMember(String id) {
		return members.containsKey(id);
	}

	public YarnContainerNode getMember(String id) {
		return members.get(id);
	}

	/**
	 * Adds the member.
	 *
	 * @param member the member
	 */
	public void addMember(YarnContainerNode member) {
		members.put(member.getId(), member);
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
