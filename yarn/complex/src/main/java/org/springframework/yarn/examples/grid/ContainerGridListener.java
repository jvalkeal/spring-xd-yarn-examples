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
package org.springframework.yarn.examples.grid;


/**
 * Listener for grid Container Node events.
 *
 * @author Janne Valkealahti
 *
 * @param <CN> the type of {@link ContainerNode}
 */
public interface ContainerGridListener<CN extends ContainerNode> {

	/**
	 * Invoked when container node is added.
	 *
	 * @param node the {@link ContainerNode}
	 */
	void containerNodeAdded(CN node);

	/**
	 * Invoked when container node is removed.
	 *
	 * @param node the {@link ContainerNode}
	 */
	void containerNodeRemoved(CN node);

}
