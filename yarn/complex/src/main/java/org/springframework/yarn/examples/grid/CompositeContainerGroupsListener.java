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

import java.util.Iterator;

import org.springframework.yarn.listener.AbstractCompositeListener;

/**
 * Composite listener for handling Container Group events.
 *
 * @author Janne Valkealahti
 *
 * @param <CG> the type of {@link ContainerGroup}
 * @param <CN> the type of {@link ContainerNode}
 */
public class CompositeContainerGroupsListener<CG extends ContainerGroup, CN extends ContainerNode> extends
		AbstractCompositeListener<ContainerGroupsListener<CG,CN>> implements ContainerGroupsListener<CG,CN> {

	@Override
	public void groupAdded(CG group) {
		for (Iterator<ContainerGroupsListener<CG,CN>> iterator = getListeners().reverse(); iterator.hasNext();) {
			iterator.next().groupAdded(group);
		}
	}

	@Override
	public void groupRemoved(CG group) {
		for (Iterator<ContainerGroupsListener<CG,CN>> iterator = getListeners().reverse(); iterator.hasNext();) {
			iterator.next().groupRemoved(group);
		}
	}

	@Override
	public void groupMemberAdded(CG group, CN node) {
		for (Iterator<ContainerGroupsListener<CG,CN>> iterator = getListeners().reverse(); iterator.hasNext();) {
			iterator.next().groupMemberAdded(group, node);
		}
	}

	@Override
	public void groupMemberRemoved(CG group, CN node) {
		for (Iterator<ContainerGroupsListener<CG,CN>> iterator = getListeners().reverse(); iterator.hasNext();) {
			iterator.next().groupMemberRemoved(group, node);
		}
	}

}
