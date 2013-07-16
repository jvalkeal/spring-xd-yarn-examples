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
package org.springframework.yarn.examples.cg;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.util.RackResolver;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 *
 *
 * @author Janne Valkealahti
 *
 */
public class GenericContainerGroupResolver implements ContainerGroupResolver, InitializingBean {

	private final static Log log = LogFactory.getLog(GenericContainerGroupResolver.class);

	private Map<String, List<String>> resolves = new Hashtable<String, List<String>>();

	private Configuration configuration;

	private boolean resolveRacks;

	@Override
	public List<String> resolveGroupNames(Container container) {
		String containerHost = container.getNodeId().getHost();
		String rack = resolveRacks ? RackResolver.resolve(containerHost).getNetworkLocation() : null;
		ArrayList<String> found = new ArrayList<String>();
		for (Entry<String, List<String>> entry : resolves.entrySet()) {
			for (String host : entry.getValue()) {
				if (safeMatch(containerHost, host) || safeMatch(rack, host)) {
					found.add(entry.getKey());
					break;
				}
			}
		}
		return found;
	}

	private boolean safeMatch(String string, String pattern) {
		if (!StringUtils.hasText(string) || !StringUtils.hasText(pattern)) {
			return false;
		}
		if (pattern.equals("*")) {
			return true;
		}
		try {
			return string.matches(pattern);
		} catch (Exception e) {
			log.warn("Can't use pattern " + pattern);
		}
		return false;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		if (resolveRacks) {
			Assert.notNull(configuration, "Rack resolving enabled, Hadoop configuration must be set");
		}
	}

	public void setResolveRacks(boolean resolveRacks) {
		this.resolveRacks = resolveRacks;
	}

	public void addResolve(String groupName, String... hosts) {
		resolves.put(groupName, Arrays.asList(hosts));
	}

	public void setResolves(Map<String, List<String>> resolves) {
		this.resolves = resolves;
	}

	public void setConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}

}
