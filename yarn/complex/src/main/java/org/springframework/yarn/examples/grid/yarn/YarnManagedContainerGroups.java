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
import org.springframework.util.StringUtils;
import org.springframework.yarn.am.allocate.ContainerAllocateData;
import org.springframework.yarn.examples.grid.ContainerGridListener;
import org.springframework.yarn.examples.grid.ManagedContainerGroups;
import org.springframework.yarn.examples.grid.RebalancePolicy;

public class YarnManagedContainerGroups implements ManagedContainerGroups<YarnContainerNode, YarnContainerGroup, YarnGroupsRebalanceData> {

	private final static Log log = LogFactory.getLog(YarnManagedContainerGroups.class);

	public final static String DEFAULT_GROUP = "default";

	private final Hashtable<String, YarnContainerGroup> managedGroups = new Hashtable<String, YarnContainerGroup>();

	/** Container to group resolver if exists */
	private ContainerGroupResolver resolver;

	@Override
	public void addGroup(YarnContainerGroup group) {
		// not yet supported
	}

	@Override
	public void removeGroup(String id) {
		// not yet supported
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
	public Collection<YarnContainerNode> getContainerNodes() {
		return null;
	}

	@Override
	public YarnContainerNode getContainerNode(String id) {
		return null;
	}

	@Override
	public void addContainerNode(YarnContainerNode node) {

		Container container = node.getContainer();

		List<String> resolvesGroups = resolveGroupNamesInternal(container);
		log.info("XXX matching: " + StringUtils.collectionToCommaDelimitedString(resolvesGroups));

		for (String name : resolvesGroups) {
			if (!managedGroups.containsKey(name)) {
				managedGroups.put(name, new YarnContainerGroup(name));
				log.info("XXX creating group: " + name);
			}
		}

		for (Entry<String, YarnContainerGroup> entry : managedGroups.entrySet()) {
			YarnContainerGroup g = managedGroups.get(entry.getKey());
			if(g != null && !g.isFull()) {
				g.addMember(new YarnContainerGroupMember(ConverterUtils.toString(container.getId())));
				log.info("XXX added " + ConverterUtils.toString(container.getId()) + " to " + g.getId());
				break;
			}
		}

	}

	@Override
	public void removeContainerNode(String id) {
		for (Entry<String, YarnContainerGroup> entry : managedGroups.entrySet()) {
			YarnContainerGroup group = entry.getValue();
			YarnContainerGroupMember m = new YarnContainerGroupMember(id);
			log.info("XXX trying to remove member " + m + " from group " + group);
			if (group.getMembers().remove(m)) {
				log.info("XXX removed member " + m);
				break;
			}
		}

	}

	@Override
	public void addListener(ContainerGridListener listener) {
	}

	@Override
	public boolean setProjectedGroupSize(String id, int size) {
		if (!managedGroups.containsKey(id)) {
			managedGroups.put(id, new YarnContainerGroup(id));
			log.info("XXX creating group: " + id);
		}

		YarnContainerGroup g = managedGroups.get(id);
		log.info("Setting projected size group=" + g.getId() + " oldsize=" +
				g.getProjectedSize() + " newsize=" + size);
		managedGroups.get(id).setProjectedSize(size);
		return true;
	}

	@Override
	public void setRebalancePolicy(RebalancePolicy policy) {
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
				group.getMembers().remove(new YarnContainerGroupMember(id));
			}
		}
		data.setContainers(ids);

		return data;
	}

	public void setGroupHosts(Map<String, List<String>> groupHosts) {
		for (Entry<String, List<String>> entry : groupHosts.entrySet()) {
			log.info("XXX groupHost " + entry.getKey() + " " + StringUtils.collectionToCommaDelimitedString(entry.getValue()));
			getMayCreateGroup(entry.getKey()).setHosts(entry.getValue());
		}
	}

	public void setGroupSizes(Map<String, Integer> groupSizes) {
		for (Entry<String, Integer> entry : groupSizes.entrySet()) {
			log.info("XXX groupSize " + entry.getKey() + " " + entry.getValue());
			getMayCreateGroup(entry.getKey()).setProjectedSize(entry.getValue());
		}
	}

	public String findGroupNameByMember(String id) {
		String found = null;
		for (Entry<String, YarnContainerGroup> entry : managedGroups.entrySet()) {
			if (entry.getValue().getMembers().contains(new YarnContainerGroupMember(id))) {
				found = entry.getKey();
				break;
			}
		}
		return found;
	}

	public void setResolver(ContainerGroupResolver resolver) {
		this.resolver = resolver;
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
