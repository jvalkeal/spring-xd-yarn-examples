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

	/** Mapping container id <-> container node */
	private Hashtable<String, YarnContainerNode> members = new Hashtable<String, YarnContainerNode>();

	/** Current projected size of this group */
	private int projectedSize;

	/** List of hosts this group requests allocation */
	private List<String> hosts;

	/**  */
	private boolean dirty = true;

	/**
	 * Instantiates a new yarn container group.
	 *
	 * @param id Group identifier
	 */
	public YarnContainerGroup(String id) {
		this.id = id;
	}

	/**
	 * Instantiates a new yarn container group.
	 *
	 * @param id the id
	 * @param projectedSize the projected size
	 */
	public YarnContainerGroup(String id, int projectedSize) {
		this.id = id;
		this.projectedSize = projectedSize;
	}

	@Override
	public String getId() {
		return id;
	}

	/**
	 * Removes the member.
	 *
	 * @param id the Container identifier
	 * @return Removed container node or <code>NULL</code> if key didn't have mapping
	 */
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

	/**
	 * Tests if a specific identifier is mapped for members.
	 *
	 * @param id the Container identifier
	 * @return true, if successful
	 */
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
		if (projectedSize < 0) {
			return false;
		} else {
			return !(getSize() < projectedSize);
		}
	}

	/**
	 * Sets the allocation hosts.
	 *
	 * @param hosts the new allocation hosts
	 */
	public void setHosts(List<String> hosts) {
		this.hosts = hosts;
	}

	/**
	 * Gets the allocation hosts.
	 *
	 * @return the allocation hosts
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
