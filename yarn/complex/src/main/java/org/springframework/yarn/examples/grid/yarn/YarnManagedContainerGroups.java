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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;
import org.springframework.yarn.am.allocate.ContainerAllocateData;
import org.springframework.yarn.examples.grid.CompositeContainerGridListener;
import org.springframework.yarn.examples.grid.CompositeContainerGroupsListener;
import org.springframework.yarn.examples.grid.ContainerGridListener;
import org.springframework.yarn.examples.grid.ContainerGroupsListener;
import org.springframework.yarn.examples.grid.ManagedContainerGroups;
import org.springframework.yarn.examples.grid.RebalancePolicy;

/**
 * Yarn specific implementation of {@link ManagedContainerGroups}.
 *
 * @author Janne Valkealahti
 *
 */
public class YarnManagedContainerGroups implements
		ManagedContainerGroups<YarnContainerNode, YarnContainerGroup, YarnGroupsRebalanceData> {

	private final static Log log = LogFactory.getLog(YarnManagedContainerGroups.class);

	/** We always have one reservation for default group name */
	public final static String DEFAULT_GROUP = "default";

	/** Structure for group id <-> group */
	private final Hashtable<String, YarnContainerGroup> managedGroups =
			new Hashtable<String, YarnContainerGroup>();

	/** Container to group resolver if exists */
	private ContainerGroupResolver resolver;

	/** Listener dispatcher for container grid events */
	private CompositeContainerGridListener<YarnContainerNode> containerGridListener =
			new CompositeContainerGridListener<YarnContainerNode>();

	/** Listener dispatcher for container group events */
	private CompositeContainerGroupsListener<YarnContainerGroup, YarnContainerNode> containerGroupsListener =
			new CompositeContainerGroupsListener<YarnContainerGroup, YarnContainerNode>();

	/** Current rebalance policy */
	private RebalancePolicy rebalancePolicy = RebalancePolicy.NONE;

	/**
	 * Instantiates a new yarn managed container groups.
	 */
	public YarnManagedContainerGroups() {
		this(false);
	}

	/**
	 * Instantiates a new yarn managed container groups.
	 *
	 * @param registerDefaultGroup the register default group
	 */
	public YarnManagedContainerGroups(boolean registerDefaultGroup) {
		this(registerDefaultGroup, DEFAULT_GROUP);
	}

	/**
	 * Instantiates a new yarn managed container groups.
	 *
	 * @param registerDefaultGroup the register default group
	 */
	public YarnManagedContainerGroups(boolean registerDefaultGroup, String groupName) {
		if (registerDefaultGroup) {
			managedGroups.put(groupName, new YarnContainerGroup(groupName));
		}
	}

	@Override
	public void addGroup(YarnContainerGroup group) {
		Assert.notNull(group, "Group must not be null");
		Assert.notNull(group.getId(), "Group id must not be null");
		managedGroups.put(group.getId(), group);
		containerGroupsListener.groupAdded(group);
	}

	@Override
	public void removeGroup(String id) {
		YarnContainerGroup group = managedGroups.remove(id);
		if (group != null) {
			containerGroupsListener.groupRemoved(group);
		}
	}

	@Override
	public YarnContainerGroup getGroup(String id) {
		return managedGroups.get(id);
	}

	@Override
	public Collection<YarnContainerGroup> getGroups() {
		return managedGroups.values();
	}

	@Override
	public YarnContainerGroup getGroupByMember(String id) {
		YarnContainerGroup g = null;
		for (Entry<String, YarnContainerGroup> entry : managedGroups.entrySet()) {
			if (entry.getValue().hasMember(id)) {
				g = entry.getValue();
				break;
			}
		}
		return g;
	}

	@Override
	public Collection<YarnContainerNode> getContainerNodes() {
		ArrayList<YarnContainerNode> ret = new ArrayList<YarnContainerNode>();
		for (YarnContainerGroup g : managedGroups.values()) {
			ret.addAll(g.getMembers());
		}
		return ret;
	}

	@Override
	public YarnContainerNode getContainerNode(String id) {
		YarnContainerNode n = null;
		for (YarnContainerGroup g : managedGroups.values()) {
			if ((n = g.getMember(id)) != null) {
				break;
			}
		}
		return n;
	}

	@Override
	public void addContainerNode(YarnContainerNode node) {
		Container container = node.getContainer();
		Assert.notNull(container, "Yarn Container must be set");

		List<String> resolvesGroups = resolveGroupNamesInternal(container);

		if (log.isDebugEnabled()) {
			log.debug("Matched node=" + node + " to groups " + StringUtils.collectionToCommaDelimitedString(resolvesGroups));
		}

		for (String name : resolvesGroups) {
			if (!managedGroups.containsKey(name)) {
				managedGroups.put(name, new YarnContainerGroup(name));
				if (log.isDebugEnabled()) {
					log.debug("Creating group " + name);
				}
			}
		}

		for (Entry<String, YarnContainerGroup> entry : managedGroups.entrySet()) {
			YarnContainerGroup g = managedGroups.get(entry.getKey());
			if(g != null && !g.isFull()) {
				g.addMember(new DefaultYarnContainerNode(container));
				if (log.isDebugEnabled()) {
					log.debug("Added " + ConverterUtils.toString(container.getId()) + " to " + g.getId());
				}
				break;
			}
		}

		containerGridListener.containerNodeAdded(node);

	}

	@Override
	public void removeContainerNode(String id) {
		YarnContainerNode node = null;
		for (Entry<String, YarnContainerGroup> entry : managedGroups.entrySet()) {
			YarnContainerGroup group = entry.getValue();
			if ((node = group.removeMember(id)) != null) {
				if (log.isDebugEnabled()) {
					log.debug("Removed member " + node);
				}
				break;
			}
		}
		if (node != null) {
			containerGridListener.containerNodeRemoved(node);
		}
	}

	@Override
	public void addContainerGridListener(ContainerGridListener<YarnContainerNode> listener) {
		containerGridListener.register(listener);
	}

	@Override
	public void addContainerGroupsListener(ContainerGroupsListener<YarnContainerGroup, YarnContainerNode> listener) {
		containerGroupsListener.register(listener);
	}

	@Override
	public boolean setProjectedGroupSize(String id, int size) {
		if (!managedGroups.containsKey(id)) {
			managedGroups.put(id, new YarnContainerGroup(id));
			if (log.isDebugEnabled()) {
				log.debug("Creating group: " + id);
			}
		}

		YarnContainerGroup g = managedGroups.get(id);
		log.info("Setting projected size group=" + g.getId() + " oldsize=" +
				g.getProjectedSize() + " newsize=" + size);
		managedGroups.get(id).setProjectedSize(size);
		return true;
	}

	@Override
	public void setRebalancePolicy(RebalancePolicy policy) {
		// not yet supported
		this.rebalancePolicy = policy;
	}

	@Override
	public YarnGroupsRebalanceData getGroupsRebalanceData() {
		DefaultYarnGroupsRebalanceData data = new DefaultYarnGroupsRebalanceData();

		ContainerAllocateData allocateData = new ContainerAllocateData();
		for (Entry<String, YarnContainerGroup> entry : managedGroups.entrySet()) {
			YarnContainerGroup group = entry.getValue();
			if (group.isDirty()) {
				allocateData.addAny(group.getProjectedSize()-group.getSize());
				group.setDirty(false);
			}
		}
		data.setAllocateData(allocateData);

		ArrayList<ContainerId> ids = new ArrayList<ContainerId>();
		for (Entry<String, YarnContainerGroup> entry : managedGroups.entrySet()) {
			YarnContainerGroup group = entry.getValue();
			int remove = Math.max(0, group.getSize()-group.getProjectedSize());
			for (int i = remove; i>0; i--) {
				String id = group.getMembers().iterator().next().getId();
				ids.add(ConverterUtils.toContainerId(id));
				group.removeMember(id);
			}
		}
		data.setContainers(ids);

		return data;
	}

	/**
	 * Sets the Container group resolver.
	 *
	 * @param resolver the new resolver
	 */
	public void setResolver(ContainerGroupResolver resolver) {
		this.resolver = resolver;
	}

	/**
	 * Sets the group hosts.
	 *
	 * @param groupHosts the group hosts
	 */
	public void setGroupHosts(Map<String, List<String>> groupHosts) {
		for (Entry<String, List<String>> entry : groupHosts.entrySet()) {
			if (log.isDebugEnabled()) {
				log.debug("setGroupHosts " + entry.getKey() + " " + StringUtils.collectionToCommaDelimitedString(entry.getValue()));
			}
			getMayCreateGroup(entry.getKey()).setHosts(entry.getValue());
		}
	}

	/**
	 * Sets the group sizes.
	 *
	 * @param groupSizes the group sizes
	 */
	public void setGroupSizes(Map<String, Integer> groupSizes) {
		for (Entry<String, Integer> entry : groupSizes.entrySet()) {
			if (log.isDebugEnabled()) {
				log.debug("setGroupSizes " + entry.getKey() + " " + entry.getValue());
			}
			getMayCreateGroup(entry.getKey()).setProjectedSize(entry.getValue());
		}
	}

	private YarnContainerGroup getMayCreateGroup(String name) {
		YarnContainerGroup group = managedGroups.get(name);
		if (group == null) {
			group = new YarnContainerGroup(name);
			managedGroups.put(name, group);
		}
		return group;
	}

	private List<String> resolveGroupNamesInternal(Container container) {
		return resolver != null ? resolver.resolveGroupNames(container) : new ArrayList<String>();
	}

}
