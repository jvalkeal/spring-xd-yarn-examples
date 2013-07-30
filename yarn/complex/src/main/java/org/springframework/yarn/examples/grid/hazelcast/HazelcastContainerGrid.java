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

import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.SmartLifecycle;
import org.springframework.yarn.examples.grid.CompositeContainerGridListener;
import org.springframework.yarn.examples.grid.ContainerGrid;
import org.springframework.yarn.examples.grid.ContainerGridListener;
import org.springframework.yarn.examples.grid.ContainerNode;

import com.hazelcast.config.Config;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;

/**
 * Container grid implementation on top of Hazelcast.
 *
 * @author Janne Valkealahti
 *
 */
public class HazelcastContainerGrid implements ContainerGrid<ContainerNode>, InitializingBean, SmartLifecycle {

	private final static Log log = LogFactory.getLog(HazelcastContainerGrid.class);

	private Config config = new Config();
	private HazelcastInstance hazelcastInstance;
	private final ReentrantLock lifecycleLock = new ReentrantLock();
	private volatile boolean autoStartup = true;
	private volatile int phase = 0;
	private volatile boolean running;
	private HashMap<String, ContainerNode> nodes = new HashMap<String, ContainerNode>();
	private CompositeContainerGridListener<ContainerNode> containerGridListener =
			new CompositeContainerGridListener<ContainerNode>();

	@Override
	public Collection<ContainerNode> getContainerNodes() {
		return nodes.values();
	}

	@Override
	public ContainerNode getContainerNode(String id) {
		return nodes.get(id);
	}

	@Override
	public void addContainerNode(ContainerNode node) {
		nodes.put(node.getId(), node);
		containerGridListener.containerNodeAdded(node);
	}

	@Override
	public void removeContainerNode(String id) {
		ContainerNode node = nodes.remove(id);
		containerGridListener.containerNodeRemoved(node);
	}

	@Override
	public void addContainerGridListener(ContainerGridListener<ContainerNode> listener) {
		containerGridListener.register(listener);
	}

	@Override
	public void start() {
		lifecycleLock.lock();
		try {
			if (!running) {
				running = true;
				init();
				if (log.isInfoEnabled()) {
					log.info("started " + this);
				} else {
					if(log.isDebugEnabled()) {
						log.debug("already started " + this);
					}
				}
			}
		} finally {
			lifecycleLock.unlock();
		}


	}

	@Override
	public void stop() {
		lifecycleLock.lock();
		try {
			if (running) {
				hazelcastInstance.getLifecycleService().shutdown();
				running = false;
				if (log.isInfoEnabled()) {
					log.info("stopped " + this);
				}
			} else {
				if (log.isDebugEnabled()) {
					log.debug("already stopped " + this);
				}
			}
		} finally {
			lifecycleLock.unlock();
		}
	}

	@Override
	public boolean isRunning() {
		lifecycleLock.lock();
		try {
			return running;
		} finally {
			lifecycleLock.unlock();
		}
	}

	@Override
	public void afterPropertiesSet() throws Exception {

	}

	@Override
	public int getPhase() {
		return phase;
	}

	@Override
	public boolean isAutoStartup() {
		return autoStartup;
	}

	@Override
	public void stop(Runnable callback) {
		lifecycleLock.lock();
		try {
			stop();
			callback.run();
		} finally {
			lifecycleLock.unlock();
		}
	}

	public void setPhase(int phase) {
		this.phase = phase;
	}

	public void setAutoStartup(boolean autoStartup) {
		this.autoStartup = autoStartup;
	}

	public void setHazelcastConfig(Config config) {
		this.config = config;
	}

	private void init() {
		hazelcastInstance = Hazelcast.newHazelcastInstance(config);
		Cluster cluster = hazelcastInstance.getCluster();
		cluster.addMembershipListener(new MembershipListener(){
		    public void memberAdded(MembershipEvent membersipEvent) {
		        log.info("MemberAdded " + membersipEvent);
		        addContainerNode(new HazelcastContainerNode(membersipEvent.getMember().getUuid()));
		    }

		    public void memberRemoved(MembershipEvent membersipEvent) {
		    	log.info("MemberRemoved " + membersipEvent);
		    	removeContainerNode(membersipEvent.getMember().getUuid());
		    }
		});
	}

	public static class HazelcastContainerNode implements ContainerNode {

		private final String id;

		public HazelcastContainerNode(String id) {
			this.id = id;
		}

		@Override
		public String getId() {
			return id;
		}

	}

}
