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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;

import java.util.Arrays;
import java.util.Collection;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.junit.Test;

/**
 *
 * @author Janne Valkealahti
 *
 */
public class YarnManagedContainerGroupsTests {

	@Test
	public void testInitialEmptyState() {
		YarnManagedContainerGroups managedGroups = createDefault();

		Collection<YarnContainerGroup> groups = managedGroups.getGroups();
		assertThat(groups.size(), is(0));

		Collection<YarnContainerNode> nodes = managedGroups.getContainerNodes();
		assertThat(nodes.size(), is(0));

	}

	@Test
	public void testAddGroup() {
		YarnManagedContainerGroups managedGroups = createDefault();

		managedGroups.addGroup(new YarnContainerGroup("foo"));

		Collection<YarnContainerGroup> groups = managedGroups.getGroups();
		assertThat(groups.size(), is(1));
	}

	@Test
	public void testAddNode() {
		YarnManagedContainerGroups managedGroups = createWithOneGroup();

		managedGroups.addGroup(new YarnContainerGroup("foo"));

		Container container = mockContainer();
		DefaultYarnContainerNode node = new DefaultYarnContainerNode(container);
		managedGroups.addContainerNode(node);

		Collection<YarnContainerNode> nodes = managedGroups.getContainerNodes();
		assertThat(nodes.size(), is(1));

		YarnContainerGroup group = managedGroups.getGroupByMember("container_1375001068632_0001_01_000002");
		assertThat(group.getId(), is("default"));

	}

	private Container mockContainer() {
		Container container = Records.newRecord(Container.class);
		NodeId nodeId = Records.newRecord(NodeId.class);
		nodeId.setHost("myhost");
		container.setNodeId(nodeId);
		ContainerId containerId = ConverterUtils.toContainerId("container_1375001068632_0001_01_000002");
		container.setId(containerId);
		return container;
	}

	private YarnManagedContainerGroups createDefault() {
		GenericContainerGroupResolver groupResolver = new GenericContainerGroupResolver();
		Map<String, List<String>> resolves = new Hashtable<String, List<String>>();
		groupResolver.setResolves(resolves);
		YarnManagedContainerGroups managedGroups = new YarnManagedContainerGroups();
		managedGroups.setResolver(groupResolver);
		return managedGroups;
	}

	private YarnManagedContainerGroups createWithOneGroup() {
		GenericContainerGroupResolver groupResolver = new GenericContainerGroupResolver();

		// resolving default to all
		Map<String, List<String>> resolves = new Hashtable<String, List<String>>();
		resolves.put("default", Arrays.asList(new String[]{"*"}));
		groupResolver.setResolves(resolves);

		YarnManagedContainerGroups managedGroups = new YarnManagedContainerGroups(true);
		managedGroups.setResolver(groupResolver);

		// default group size to 1
		Map<String, Integer> groupSizes = new Hashtable<String, Integer>();
		groupSizes.put("default", 1);
		managedGroups.setGroupSizes(groupSizes);
		return managedGroups;
	}

}
