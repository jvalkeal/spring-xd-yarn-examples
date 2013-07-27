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
package org.springframework.yarn.examples;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.Assert;
import org.springframework.yarn.examples.gen.XdAdmin;
import org.springframework.yarn.thrift.ThriftAppmasterService;

/**
 * Thrift based service exposing api for external
 * clients to be able to interact with Application
 * Master controlling XD system running in Hadoop.
 *
 * @author Janne Valkealahti
 *
 */
public class XdAppmasterService extends ThriftAppmasterService implements XdAdmin.Iface {

	private final static Log log = LogFactory.getLog(XdAppmasterService.class);

	@Autowired
	private XdAppmaster xdAppmaster;

	@Override
	protected void onInit() throws Exception {
		Assert.notNull(xdAppmaster, "xdAppmaster must be set");
		super.onInit();
	}

	@Override
	protected TProcessor getProcessor() {
		return new XdAdmin.Processor<XdAppmasterService>(this);
	}

	/**
	 * Thrift XdAdmin service endpoint setting up a new count
	 * of running containers on a default group.
	 *
	 * @see org.springframework.yarn.examples.gen.XdAdmin.Iface#setRunningCount(int)
	 */
	@Override
	public boolean setRunningCount(int count) throws TException {
		log.info("New container running count " + count + " requested");
		xdAppmaster.setRunningXdContainerCount(count);
		return true;
	}

	/**
	 * Thrift XdAdmin service endpoint setting up a new count
	 * of running containers on a group.
	 *
	 * @see org.springframework.yarn.examples.gen.XdAdmin.Iface#setGroupRunningCount(int, String)
	 */
	@Override
	public boolean setGroupRunningCount(int count, String group) throws TException {
		log.info("New container running count " + count + " for group " + group + " requested");
		xdAppmaster.setRunningXdContainerCount(count, group);
		return true;
	}

	/**
	 * Thrift XdAdmin service endpoint initiating an application
	 * shutdown command. This method will do a graceful
	 * shutdown of a whole running application.
	 *
	 * @see org.springframework.yarn.examples.gen.XdAdmin.Iface#shutdown()
	 */
	@Override
	public boolean shutdown() throws TException {
		log.info("Application shutdown requested");
		xdAppmaster.shutdownXdSystem();
		return true;
	}

}
