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
 * Listener for grid Container Group events.
 *
 * @author Janne Valkealahti
 *
 */
public interface ContainerGroupsListener {

	void groupAdded(ContainerGroup group);

	void groupRemoved(ContainerGroup group);

	void groupMemberAdded(ContainerGroup group, ContainerNode node);

	void groupMemberRemoved(ContainerGroup group, ContainerNode node);

}
