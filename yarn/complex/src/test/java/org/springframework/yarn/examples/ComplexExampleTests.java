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
import java.net.URI;
import java.util.concurrent.TimeUnit;

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
import org.springframework.test.annotation.Timed;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.xd.rest.client.SpringXDOperations;
import org.springframework.xd.rest.client.StreamOperations;
import org.springframework.xd.rest.client.domain.StreamDefinitionResource;
import org.springframework.xd.rest.client.impl.SpringXDTemplate;
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

	/** Pattern matching container's stdout files*/
	private final String PAT_C_STDOUT = "/**/Container.stdout";

	/** Pattern matching all log files*/
	private final String PAT_ALL = "/**/*.std*";

	@Test
	@Timed(millis=300000)
	public void testAppSubmission() throws Exception {
		// submit and wait running state
		ApplicationId applicationId = submitApplication();
		assertNotNull(applicationId);
		YarnApplicationState state = waitState(applicationId, 120, TimeUnit.SECONDS, YarnApplicationState.RUNNING);
		assertNotNull(state);
		assertTrue(state.equals(YarnApplicationState.RUNNING));

		// check registered rpc port
		ApplicationReport applicationReport = yarnClient.getApplicationReport(applicationId);
		String rpcHost = applicationReport.getHost();
		int rpcPort = applicationReport.getRpcPort();
		assertThat(rpcHost, notNullValue());
		assertThat(rpcPort, greaterThan(0));

		File baseDir = getYarnCluster().getYarnWorkDir();
		String baseUrl = findXdBaseUrl(applicationId);

		// we're starting for zero, launch and wait
		setContainerCountViaThrift(1, "default", rpcHost, rpcPort);
		setContainerCountViaThrift(1, "xdgroup", rpcHost, rpcPort);
		int count = ApplicationTestUtils.waitResourcesMatchCount(baseDir, PAT_C_STDOUT, 2, true, null);
		assertThat(count, is(2));

		// do ticktock request and wait logging message
		StreamDefinitionResource stream = createTickTockStream(baseUrl);
		// for some reason stream name is null, bug in xd?
		//assertThat(stream.getName(), is("ticktock"));
		count = ApplicationTestUtils.waitResourcesMatchCount(baseDir, PAT_C_STDOUT, 1, true, "LoggingHandler");
		assertThat(count, is(1));

		// resize and wait
		setContainerCountViaThrift(2, "default", rpcHost, rpcPort);
		setContainerCountViaThrift(2, "xdgroup", rpcHost, rpcPort);
		count = ApplicationTestUtils.waitResourcesMatchCount(baseDir, PAT_C_STDOUT, 4, true, null);
		assertThat(count, is(4));

		deleteTickTockStream(baseUrl);

		// long running app, kill it and check that state is KILLED
		killApplication(applicationId);
		state = waitState(applicationId, 30, TimeUnit.SECONDS, YarnApplicationState.KILLED);
		assertThat(state, is(YarnApplicationState.KILLED));

		// appmaster and 4 container should make it 10 log files
		Resource[] resources = ApplicationTestUtils.matchResources(baseDir, PAT_ALL);
		assertThat(resources, notNullValue());
		assertThat(resources.length, is(10));
	}

	private void setContainerCountViaThrift(final int count, final String group, String host, int port) throws Exception {
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

	private StreamDefinitionResource createTickTockStream(String url) throws Exception {
		SpringXDOperations springXDOperations = new SpringXDTemplate(URI.create(url));
		StreamOperations streamOperations = springXDOperations.streamOperations();
		try {
			streamOperations.destroyStream("ticktock");
		} catch (Exception e) {
		}
		StreamDefinitionResource stream = streamOperations.createStream("ticktock", "time | log", true);
		return stream;
	}

	private void deleteTickTockStream(String url) throws Exception {
		SpringXDOperations springXDOperations = new SpringXDTemplate(URI.create(url));
		StreamOperations streamOperations = springXDOperations.streamOperations();
		try {
			streamOperations.destroyStream("ticktock");
		} catch (Exception e) {
		}
	}

	private String findXdBaseUrl(ApplicationId applicationId) {
		// returned track url is "<rm manager>:xxxx//<node>:xxxx", we need the last part
		return "http://" +	getYarnClient().getApplicationReport(applicationId).getTrackingUrl().split("//")[1];
	}

}
