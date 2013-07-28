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
import org.springframework.yarn.examples.grid.ContainerGridListener;
import org.springframework.yarn.examples.grid.ContainerGroupsListener;

/**
 * Tests for {@link YarnManagedContainerGroups}.
 *
 * @author Janne Valkealahti
 *
 */
public class YarnManagedContainerGroupsTests {

	private final static String CID1 = "container_1375001068632_0001_01_000001";
	private final static String CID2 = "container_1375001068632_0001_01_000002";
	private final static String HOST1 = "hostname1";
	private final static String HOST2 = "hostname2";

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

		TestContainerGroupsListener listener = new TestContainerGroupsListener();
		managedGroups.addContainerGroupsListener(listener);
		managedGroups.addGroup(new YarnContainerGroup("foo"));
		assertThat(listener.groupAdded, is(1));

		Container container = mockContainer1();
		DefaultYarnContainerNode node = new DefaultYarnContainerNode(container);
		managedGroups.addContainerNode(node);

		Collection<YarnContainerNode> nodes = managedGroups.getContainerNodes();
		assertThat(nodes.size(), is(1));

		YarnContainerGroup group = managedGroups.getGroupByMember(CID1);
		assertThat(group.getId(), is("default"));

	}

	@Test
	public void testContainerGridListener() {
		YarnManagedContainerGroups managedGroups = createWithOneGroup();

		TestContainerGridListener listener = new TestContainerGridListener();
		managedGroups.addContainerGridListener(listener);

		Container container1 = mockContainer1();
		DefaultYarnContainerNode node1 = new DefaultYarnContainerNode(container1);
		managedGroups.addContainerNode(node1);

		Container container2 = mockContainer2();
		DefaultYarnContainerNode node2 = new DefaultYarnContainerNode(container2);
		managedGroups.addContainerNode(node2);

		assertThat(listener.added, is(2));
	}

	/**
	 * Mocks a yarn container with hostname myhost1 and container id {@link #CID1}
	 *
	 * @return the mocked Yarn Container
	 */
	private Container mockContainer1() {
		Container container = Records.newRecord(Container.class);
		NodeId nodeId = Records.newRecord(NodeId.class);
		nodeId.setHost(HOST1);
		container.setNodeId(nodeId);
		ContainerId containerId = ConverterUtils.toContainerId(CID1);
		container.setId(containerId);
		return container;
	}

	/**
	 * Mocks a yarn container with hostname myhost2 and container id {@link #CID2}
	 *
	 * @return the mocked Yarn Container
	 */
	private Container mockContainer2() {
		Container container = Records.newRecord(Container.class);
		NodeId nodeId = Records.newRecord(NodeId.class);
		nodeId.setHost(HOST2);
		container.setNodeId(nodeId);
		ContainerId containerId = ConverterUtils.toContainerId(CID2);
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

	/**
	 * Test implementation of {@link ContainerGridListener}.
	 */
	private static class TestContainerGridListener implements ContainerGridListener<YarnContainerNode> {
		public int added;
		public int removed;
		@Override
		public void containerNodeAdded(YarnContainerNode node) {
			added++;
		}
		@Override
		public void containerNodeRemoved(YarnContainerNode node) {
			removed++;
		}
	}

	/**
	 * Test implementation of {@link ContainerGroupsListener}.
	 */
	private static class TestContainerGroupsListener implements ContainerGroupsListener<YarnContainerGroup, YarnContainerNode> {
		public int groupAdded;
		public int groupRemoved;
		public int groupMemberAdded;
		public int groupMemberRemoved;
		@Override
		public void groupAdded(YarnContainerGroup group) {
			groupAdded++;
		}
		@Override
		public void groupRemoved(YarnContainerGroup group) {
			groupRemoved++;
		}
		@Override
		public void groupMemberAdded(YarnContainerGroup group, YarnContainerNode node) {
			groupMemberAdded++;
		}
		@Override
		public void groupMemberRemoved(YarnContainerGroup group, YarnContainerNode node) {
			groupMemberRemoved++;
		}

	}

}
