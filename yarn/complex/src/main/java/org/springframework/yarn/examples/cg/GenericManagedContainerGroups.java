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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.springframework.util.StringUtils;
import org.springframework.yarn.am.allocate.ContainerAllocateData;

/**
 * Generic implementation of {@link ManagedContainerGroups}.
 * <p>
 *
 * @author Janne Valkealahti
 *
 */
public class GenericManagedContainerGroups implements ManagedContainerGroups {

	private final static Log log = LogFactory.getLog(GenericManagedContainerGroups.class);

	/** Name of the default group */
	public final static String DEFAULT_GROUP = "default";

	/** Name to Group mapping */
	private Hashtable<String, Group> managedGroups = new Hashtable<String, Group>();

	/** Container to group resolver if exists */
	private ContainerGroupResolver resolver;

	/**
	 * Instantiates a new managed container groups. One group
	 * named "default" is created automatically.
	 */
	public GenericManagedContainerGroups() {
		managedGroups.put(DEFAULT_GROUP, new Group(DEFAULT_GROUP));
	}

	/**
	 * Sets the container group resolver.
	 *
	 * @param resolver the new resolver
	 */
	public void setResolver(ContainerGroupResolver resolver) {
		this.resolver = resolver;
	}

	/**
	 * Adds the container.
	 *
	 * @param container the container
	 */
	@Override
	public void addContainer(Container container) {
		List<String> resolvesGroups = resolveGroupNamesInternal(container);
		log.info("XXX matching: " + StringUtils.collectionToCommaDelimitedString(resolvesGroups));

		for (String name : resolvesGroups) {
			if (!managedGroups.containsKey(name)) {
				managedGroups.put(name, new Group(name));
				log.info("XXX creating group: " + name);
			}
		}

		for (Entry<String, Group> entry : managedGroups.entrySet()) {
			Group g = managedGroups.get(entry.getKey());
			if(g != null && !g.isFull()) {
				g.addMember(new GroupMember(ConverterUtils.toString(container.getId())));
				log.info("XXX added " + ConverterUtils.toString(container.getId()) + " to " + g.getId());
				break;
			}
		}

	}

	@Override
	public void removeMember(String id) {
		for (Entry<String, Group> entry : managedGroups.entrySet()) {
			Group group = entry.getValue();
			GroupMember m = new GroupMember(id);
			log.info("XXX trying to remove member " + m + " from group " + group);
			if (group.getMembers().remove(m)) {
				log.info("XXX removed member " + m);
				break;
			}
		}
	}

	@Override
	public void setProjectedSize(String group, int count) {
		if (!managedGroups.containsKey(group)) {
			managedGroups.put(group, new Group(group));
			log.info("XXX creating group: " + group);
		}

		Group g = managedGroups.get(group);
		log.info("Setting projected size group=" + g.getId() + " oldsize=" +
				g.getProjectedSize() + " newsize=" + count);
		managedGroups.get(group).setProjectedSize(count);
	}

	@Override
	public ContainerAllocateData getContainerAllocateData() {
		ContainerAllocateData data = new ContainerAllocateData();
		for (Entry<String, Group> entry : managedGroups.entrySet()) {
			Group group = entry.getValue();
			if (group.isDirty()) {
				data.addAny(group.getProjectedSize()-group.getSize());
				group.setDirty(false);
			}
		}
		return data;
	}

	@Override
	public List<ContainerId> getReleasedContainers() {
		ArrayList<ContainerId> ids = new ArrayList<ContainerId>();
		for (Entry<String, Group> entry : managedGroups.entrySet()) {
			Group group = entry.getValue();
			int remove = Math.max(0, group.getSize()-group.getProjectedSize());
			for (int i = remove; i>0; i--) {
				String id = group.getMembers().iterator().next().getId();
				ids.add(ConverterUtils.toContainerId(id));
				group.getMembers().remove(new GroupMember(id));
			}
		}
		return ids;
	}

	@Override
	public String findGroupNameByMember(String id) {
		String found = null;
		for (Entry<String, Group> entry : managedGroups.entrySet()) {
			if (entry.getValue().getMembers().contains(new GroupMember(id))) {
				found = entry.getKey();
				break;
			}
		}
		return found;
	}

	/**
	 * Internal wrapper for handling null resolver.
	 *
	 * @param container the container
	 * @return the list of resolved group names
	 */
	private List<String> resolveGroupNamesInternal(Container container) {
		return resolver != null ? resolver.resolveGroupNames(container) : new ArrayList<String>();
	}

	/**
	 * Helper class for grouping
	 */
	private class Group {
		private final String id;
		private Set<GroupMember> members = new HashSet<GenericManagedContainerGroups.GroupMember>();
		private int projectedSize;
		private boolean dirty = true;

		public Group(String id) {
			this.id = id;
		}

		public int getSize() {
			return members.size();
		}

		public Set<GroupMember> getMembers() {
			return members;
		}

		public void addMember(GroupMember member) {
			members.add(member);
		}

		public int getProjectedSize() {
			return projectedSize;
		}

		public void setProjectedSize(int projectedSize) {
			if (projectedSize != getSize()) {
				dirty = true;
			}
			this.projectedSize = projectedSize;
		}

		public boolean isDirty() {
			return dirty;
		}

		public void setDirty(boolean dirty) {
			this.dirty = dirty;
		}

		public boolean isFull() {
			return !(getSize() < projectedSize);
		}

		public String getId() {
			return id;
		}

		@Override
		public String toString() {
			return "Group [id=" + id + ", members=" + members + ", projectedSize=" + projectedSize + ", dirty=" + dirty
					+ "]";
		}

	}

	/**
	 * Helper class for group members
	 */
	private class GroupMember {
		private final String id;

		public GroupMember(String id) {
			this.id = id;
		}

		public String getId() {
			return id;
		}

		@Override
		public int hashCode() {
			// needed order to remove by new GroupMember(id)
			final int prime = 31;
			int result = 1;
			result = prime * result + getOuterType().hashCode();
			result = prime * result + ((id == null) ? 0 : id.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			// needed order to remove by new GroupMember(id)
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			GroupMember other = (GroupMember) obj;
			if (!getOuterType().equals(other.getOuterType()))
				return false;
			if (id == null) {
				if (other.id != null)
					return false;
			} else if (!id.equals(other.id))
				return false;
			return true;
		}

		@Override
		public String toString() {
			return "GroupMember [id=" + id + "]";
		}

		private GenericManagedContainerGroups getOuterType() {
			return GenericManagedContainerGroups.this;
		}

	}

}
