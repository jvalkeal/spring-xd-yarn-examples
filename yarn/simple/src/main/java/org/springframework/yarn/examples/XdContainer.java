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
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.xd.dirt.core.Container;
import org.springframework.xd.dirt.launcher.ContainerLauncher;
import org.springframework.xd.dirt.server.options.AbstractOptions;
import org.springframework.xd.dirt.server.options.ContainerOptions;
import org.springframework.xd.dirt.server.options.Transport;
import org.springframework.yarn.container.AbstractYarnContainer;

/**
 * Custom Yarn container handling XD container launch operation.
 * 
 * @author Janne Valkealahti
 *
 */
public class XdContainer extends AbstractYarnContainer implements ApplicationContextAware {

	private static final Log log = LogFactory.getLog(XdContainer.class);

	private ApplicationContext context;

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.context = applicationContext;
	}
	
	@Override
	protected void runInternal() {
		log.info("XdContainer internal run starting, setting up XD container launcher");		
		log.info("Using xd.home=" + getEnvironment("xd.home"));
		
		// setup needed options
		ContainerOptions options = new ContainerOptions();
		AbstractOptions.setXDHome(getEnvironment("xd.home"));
		AbstractOptions.setXDTransport(Transport.redis);
		
		// do the xd internal container launch
		log.info("XdContainer launch");
		ContainerLauncher launcher = context.getBean(ContainerLauncher.class);
		Container container = launcher.launch(options);
		log.info("XdContainer launched id=" + container.getId() + " jvm=" + container.getJvmName());

		// wait for container exit
		try {
			log.info("XdContainer entering wait/sleep for 120 secs");
			Thread.sleep(120000);
		} catch (InterruptedException e) {
		}
		log.info("XdContainer: I'm done baby");
	}

}
