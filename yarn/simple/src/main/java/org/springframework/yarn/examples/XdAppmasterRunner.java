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
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.web.context.support.XmlWebApplicationContext;
import org.springframework.xd.dirt.container.DefaultContainer;
import org.springframework.xd.dirt.stream.StreamServer;
import org.springframework.yarn.am.CommandLineAppmasterRunner;
import org.springframework.yarn.am.track.UrlAppmasterTrackService;
import org.springframework.yarn.support.YarnContextUtils;

/**
 * Custom command line runner for XD Yarn Application Master.
 * This is needed order to manually setup Web Application
 * Context needed my XD Admin build-in mvc layer.
 * 
 * @author Janne Valkealahti
 *
 */
public class XdAppmasterRunner extends CommandLineAppmasterRunner {

	private final static Log log = LogFactory.getLog(XdAppmasterRunner.class);

	@Override
	protected ConfigurableApplicationContext getChildApplicationContext(
			String configLocation, ConfigurableApplicationContext parent) {

		log.info("Using xd.transport=" + System.getProperty("xd.transport"));
		log.info("Using xd.store=" + System.getProperty("xd.store"));
		log.info("Using xd.home=" + System.getProperty("xd.home"));

		XmlWebApplicationContext context = new XmlWebApplicationContext();
		context.setConfigLocation("classpath:" + DefaultContainer.XD_INTERNAL_CONFIG_ROOT + "admin-server.xml");

		final StreamServer server = new StreamServer(context, 8282);
		server.afterPropertiesSet();
		server.start();

		// when we get fix for 0-port trick in spring-xd
		//log.info("Streamserver tomcat port=" + server.getLocalPort());

		// context is already refreshed so just register singleton
		// not really a proper way to do it but serves the demo purpose
		// if streamserver could choose free port then below would
		// make much more sense
		parent.getBeanFactory().registerSingleton(
				YarnContextUtils.TASK_AMTRACKSERVER_BEAN_NAME,
				new UrlAppmasterTrackService("http://localhost:" + server.getPort()));

		context.addApplicationListener(new ApplicationListener<ContextClosedEvent>() {
			@Override
			public void onApplicationEvent(ContextClosedEvent event) {
				server.stop();
			}
		});

		return context;
	}

	public static void main(String[] args) {
		new XdAppmasterRunner().doMain(args);
	}
	
}
