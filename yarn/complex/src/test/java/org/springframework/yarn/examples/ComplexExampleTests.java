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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.PutMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.junit.Test;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.test.annotation.Timed;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.yarn.examples.gen.XdAdmin;
import org.springframework.yarn.examples.gen.XdAdmin.Client;
import org.springframework.yarn.test.context.MiniYarnCluster;
import org.springframework.yarn.test.context.YarnDelegatingSmartContextLoader;
import org.springframework.yarn.test.junit.AbstractYarnClusterTests;
import org.springframework.yarn.thrift.ThriftCallback;
import org.springframework.yarn.thrift.ThriftTemplate;

/**
 * Tests for Spring XD Yarn Complex example.
 *
 * @author Janne Valkealahti
 *
 */
@ContextConfiguration(loader=YarnDelegatingSmartContextLoader.class)
@MiniYarnCluster(nodes=2)
public class ComplexExampleTests extends AbstractYarnClusterTests {

	@Test
	@Timed(millis=240000)
	public void testAppSubmission() throws Exception {
		// submit and wait running state
		ApplicationId applicationId = submitApplication();
		assertNotNull(applicationId);
		YarnApplicationState state = waitState(applicationId, 120, TimeUnit.SECONDS, YarnApplicationState.RUNNING);
		assertNotNull(state);
		assertTrue(state.equals(YarnApplicationState.RUNNING));


		ApplicationReport applicationReport = yarnClient.getApplicationReport(applicationId);
		String rpcHost = applicationReport.getHost();
		int rpcPort = applicationReport.getRpcPort();

		assertThat(rpcHost, notNullValue());
		assertThat(rpcPort, greaterThan(0));

		doSetContainerCountViaThrift(2, "default", rpcHost, rpcPort);
		doSetContainerCountViaThrift(2, "xdgroup", rpcHost, rpcPort);

		// wait and do ticktock put
		Thread.sleep(40000);
		assertTrue("Ticktock request failed", doTickTockTimeLogPut(applicationId));

		// wait a bit for spring-xd containers to log something
		Thread.sleep(20000);

		doSetContainerCountViaThrift(1, "default", rpcHost, rpcPort);
		doSetContainerCountViaThrift(1, "xdgroup", rpcHost, rpcPort);
		Thread.sleep(30000);

		// long running app, kill it and check that state is KILLED
		killApplication(applicationId);
		state = getState(applicationId);
		assertTrue(state.equals(YarnApplicationState.KILLED));

		// get log files
		File workDir = getYarnCluster().getYarnWorkDir();
		PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
		String locationPattern = "file:" + workDir.getAbsolutePath() + "/**/*.std*";
		Resource[] resources = resolver.getResources(locationPattern);

		// appmaster and 1 container should make it 4 log files
		assertThat(resources, notNullValue());
		assertThat(resources.length, is(10));

		// do some checks for log file content
		int found = 0;
		for (Resource res : resources) {
			File file = res.getFile();
			if (file.getName().endsWith("stdout")) {
				// there has to be some content in stdout file
				assertThat(file.length(), greaterThan(0l));
				if (file.getName().equals("Container.stdout")) {
					Scanner scanner = new Scanner(file);
					String content = scanner.useDelimiter("\\A").next();
					scanner.close();
					// this is what xd container should log
					if (content.contains("LoggingHandler")) {
						found++;
					}
				}
			}
		}
		assertThat(found, is(1));
	}

	private void doSetContainerCountViaThrift(final int count, final String group, String host, int port) throws Exception {
		TTransport transport = new TFramedTransport(new TSocket(host, port, 2000));
		transport.open();
		TBinaryProtocol protocol = new TBinaryProtocol(transport);

		ThriftTemplate<XdAdmin.Client> template =
				new ThriftTemplate<XdAdmin.Client>(XdAdmin.class, protocol);
		template.afterPropertiesSet();

		template.executeClient(new ThriftCallback<Boolean, XdAdmin.Client>() {
			@Override
			public Boolean doInThrift(Client proxy) throws TException{
				return proxy.setGroupRunningCount(count, group);
			}
		});
	}

	private boolean doTickTockTimeLogPut(ApplicationId applicationId) throws Exception {
		HttpClient httpclient = new HttpClient();
		PutMethod put = new PutMethod(findTickTockUrl(applicationId));
		StringRequestEntity entity = new StringRequestEntity("time | log", "text/plain", "UTF-8");
		put.setRequestEntity(entity);
		int executeMethod = httpclient.executeMethod(put);
		return executeMethod == HttpStatus.SC_CREATED;
	}

	private String findTickTockUrl(ApplicationId applicationId) {
		// returned track url is "<rm manager>:xxxx//<node>:xxxx"
		// we need the last part
		return "http://" +
				getYarnClient().getApplicationReport(applicationId).getTrackingUrl().split("//")[1] +
				"/streams/ticktock";
	}

}
