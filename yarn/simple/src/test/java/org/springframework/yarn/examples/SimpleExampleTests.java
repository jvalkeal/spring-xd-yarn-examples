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

import static org.hamcrest.CoreMatchers.containsString;
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
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.junit.Test;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.test.annotation.Timed;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.yarn.test.context.MiniYarnCluster;
import org.springframework.yarn.test.context.YarnDelegatingSmartContextLoader;
import org.springframework.yarn.test.junit.AbstractYarnClusterTests;

/**
 * Tests for Spring XD Yarn Simple example.
 *
 * @author Janne Valkealahti
 *
 */
@ContextConfiguration(loader=YarnDelegatingSmartContextLoader.class)
@MiniYarnCluster
public class SimpleExampleTests extends AbstractYarnClusterTests {

	@Test
	@Timed(millis=240000)
	public void testAppSubmission() throws Exception {
		// submit and wait running state
		ApplicationId applicationId = submitApplication();
		assertNotNull(applicationId);
		YarnApplicationState state = waitState(applicationId, 120, TimeUnit.SECONDS, YarnApplicationState.RUNNING);
		assertNotNull(state);
		assertTrue(state.equals(YarnApplicationState.RUNNING));

		// wait and do ticktock put
		Thread.sleep(20000);
		assertTrue("Ticktock request failed", doTickTockTimeLogPut(applicationId));

		// wait a bit for spring-xd containers to log something
		Thread.sleep(10000);

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
		assertThat(resources.length, is(4));

		// do some checks for log file content
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
					assertThat(content, containsString("LoggingHandler"));
				}
			}
		}
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
