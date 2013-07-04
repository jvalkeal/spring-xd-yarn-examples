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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.concurrent.TimeUnit;

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
	@Timed(millis=160000)
	public void testAppSubmission() throws Exception {
		YarnApplicationState state = submitApplicationAndWait(120, TimeUnit.SECONDS);
		assertNotNull(state);
		assertTrue(state.equals(YarnApplicationState.FINISHED));
		
		File workDir = getYarnCluster().getYarnWorkDir();
		
		PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
		String locationPattern = "file:" + workDir.getAbsolutePath() + "/**/*.std*";
		Resource[] resources = resolver.getResources(locationPattern);
		
		// appmaster and 1 containers should
		// make it 4 log files
		assertThat(resources, notNullValue());
		assertThat(resources.length, is(4));
		
//		for (Resource res : resources) {
//			File file = res.getFile();		
//			if (file.getName().endsWith("stdout")) {
//				// there has to be some content in stdout file
//				assertThat(file.length(), greaterThan(0l));
//				if (file.getName().equals("Container.stdout")) {
//					Scanner scanner = new Scanner(file);
//					String content = scanner.useDelimiter("\\A").next();
//					scanner.close();
//					// this is what container will log in stdout
//					assertThat(content, containsString("Hello from MultiContextBeanExample"));
//				}
//			} else if (file.getName().endsWith("stderr")) {
//				// can't have anything in stderr files
//				assertThat(file.length(), is(0l));
//			}
//		}		
	}

}
