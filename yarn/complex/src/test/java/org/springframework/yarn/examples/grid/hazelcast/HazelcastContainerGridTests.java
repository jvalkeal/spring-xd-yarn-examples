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
package org.springframework.yarn.examples.grid.hazelcast;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;
import org.springframework.yarn.examples.grid.ContainerGridListener;
import org.springframework.yarn.examples.grid.ContainerNode;

/**
 * Tests for {@link HazelcastContainerGrid}.
 *
 * @author Janne Valkealahti
 *
 */
public class HazelcastContainerGridTests {

	@Test
	public void testSimpleGrid() throws InterruptedException {

		HazelcastContainerGrid grid1 = new HazelcastContainerGrid();
		TestContainerGridListener listener1 = new TestContainerGridListener();
		grid1.addContainerGridListener(listener1);
		HazelcastContainerGrid grid2 = new HazelcastContainerGrid();

		grid1.start();
		grid2.start();

		Thread.sleep(10000);

		assertThat(listener1.containerNodeAdded, is(1));

		grid2.stop();
		grid1.stop();

		assertThat(listener1.containerNodeRemoved, is(1));
	}

	/**
	 * Test implementation of {@link ContainerGridListener}.
	 */
	private static class TestContainerGridListener implements ContainerGridListener<ContainerNode> {
		public int containerNodeAdded;
		public int containerNodeRemoved;
		@Override
		public void containerNodeAdded(ContainerNode node) {
			containerNodeAdded++;
		}
		@Override
		public void containerNodeRemoved(ContainerNode node) {
			containerNodeRemoved++;
		}
	}

}
