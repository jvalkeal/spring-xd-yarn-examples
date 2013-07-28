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

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.util.ConverterUtils;

/**
 * Default implementation of {@link YarnContainerNode}.
 *
 * @author Janne Valkealahti
 *
 */
public class DefaultYarnContainerNode implements YarnContainerNode {

	private Container container;

	/**
	 * Instantiates a new default yarn container node.
	 *
	 * @param container the container
	 */
	public DefaultYarnContainerNode(Container container) {
		this.container = container;
	}

	@Override
	public Container getContainer() {
		return container;
	}

	@Override
	public String getId() {
		return container != null ? ConverterUtils.toString(container.getId()) : null;
	}

	/**
	 * Sets the Yarn container.
	 *
	 * @param container new Yarn container
	 */
	public void setContainer(Container container) {
		this.container = container;
	}

	@Override
	public String toString() {
		return "DefaultYarnContainerNode [container=" + container + "]";
	}

}
