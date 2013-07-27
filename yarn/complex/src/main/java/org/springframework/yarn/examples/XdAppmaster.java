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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.yarn.YarnSystemConstants;
import org.springframework.yarn.am.AppmasterService;
import org.springframework.yarn.am.ContainerLauncherInterceptor;
import org.springframework.yarn.am.container.AbstractLauncher;
import org.springframework.yarn.examples.grid.yarn.YarnManagedContainerGroups;
import org.springframework.yarn.thrift.hb.HeartbeatAppmasterService;
import org.springframework.yarn.thrift.hb.HeartbeatMasterClientAdapter;
import org.springframework.yarn.thrift.hb.HeartbeatNode;
import org.springframework.yarn.thrift.hb.HeartbeatNode.NodeState;
import org.springframework.yarn.thrift.hb.gen.CommandMessageType;
import org.springframework.yarn.thrift.hb.gen.HeartbeatCommandMessage;

/**
 * Application Master for XD system running on Hadoop.
 * <p>
 * XdAppmaster itself is embedding XD's Admin component which
 * is defined in a context configuration. Its other main purpose
 * is to launch and monitor Yarn managed containers which are
 * used as XD containers.
 *
 * @author Janne Valkealahti
 *
 */
public class XdAppmaster extends AbstractManagedContainerGroupsAppmaster
		implements ContainerLauncherInterceptor {

	private final static Log log = LogFactory.getLog(XdAppmaster.class);

	private String sessionId;

	/** Container <-> Groups tracker */
	@Autowired
	private YarnManagedContainerGroups managedGroups;

	@Autowired
	private HeartbeatAppmasterService heartbeatAppmasterService;

	/**
	 * Sets a new container count this application
	 * should keep up and running.
	 *
	 * @param count the new container count
	 */
	public void setRunningXdContainerCount(int count) {
		setRunningXdContainerCount(count, "default");
	}

	/**
	 * Sets a new container count for a group this application
	 * should keep up and running.
	 *
	 * @param count the new container count
	 * @param group the group name
	 */
	public void setRunningXdContainerCount(int count, String group) {
		log.info("XXX incoming " + group + " " + count);
		getManagedGroups().setProjectedGroupSize(group, count);
	}

	/**
	 * Shutdowns the XD system managed by this
	 * Application Master.
	 */
	public void shutdownXdSystem() {
		notifyCompleted();
	}

	@Override
	protected void onInit() throws Exception {

		sessionId = UUID.randomUUID().toString();

		for (Entry<String, String> entry : System.getenv().entrySet()) {
			log.info("XXXenv: " + entry.getKey() + " " + entry.getValue());
		}

		// for now no ref for appmaster in xml,
		// set managedGroups here
		setManagedGroups(managedGroups);

		super.onInit();
		if(getLauncher() instanceof AbstractLauncher) {
			((AbstractLauncher)getLauncher()).addInterceptor(this);
		}
		((HeartbeatAppmasterService) getAppmasterService()).addHeartbeatMasterClientListener(new HbMasterClient());
//		((HeartbeatAppmasterService) getAppmasterService()).setSessionId(sessionId);
		heartbeatAppmasterService.setSessionId(sessionId);
	}

	@Override
	public ContainerLaunchContext preLaunch(ContainerLaunchContext context) {
		if (log.isDebugEnabled()) {
			log.debug("preLaunch: " + context);
		}

		AppmasterService service = getAppmasterService();
		if (service != null) {
			int port = service.getPort();
			String address = service.getHost();
			Map<String, String> env = new HashMap<String, String>(context.getEnvironment());
			env.put(YarnSystemConstants.AMSERVICE_PORT, Integer.toString(port));
			env.put(YarnSystemConstants.AMSERVICE_HOST, address);
			env.put(YarnSystemConstants.SYARN_CONTAINER_ID, ConverterUtils.toString(context.getContainerId()));
			String xdGroup = getManagedGroups().findGroupNameByMember(ConverterUtils.toString(context.getContainerId()));
			env.put("syarn.cg.group", xdGroup != null ? xdGroup : "");
			context.setEnvironment(env);

			// testing
			try {
				Credentials credentials = new Credentials();
				credentials.addSecretKey(new Text(YarnSystemConstants.SYARN_SEC_SESSIONID), sessionId.getBytes());
				DataOutputBuffer dob = new DataOutputBuffer();
				credentials.writeTokenStorageToStream(dob);
				ByteBuffer containerToken  = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
				context.setContainerTokens(containerToken);
			} catch (IOException e) {
				log.error("XXX error setContainerTokens", e);
			}


			return context;
		} else {
			return context;
		}
	}

	@Override
	protected boolean onContainerFailed(ContainerId containerId) {
		log.info("onContainerFailed: " + containerId);
		return false;
	}

	/**
	 * Helper class listening heartbeat notifications.
	 */
	private class HbMasterClient extends HeartbeatMasterClientAdapter {
		@Override
		public void nodeUp(HeartbeatNode node, NodeState state) {
			log.info("XXX nodeUp nodeId=" + node.getId());
			HeartbeatCommandMessage m = new HeartbeatCommandMessage();
			m.setCommandMessageType(CommandMessageType.GENERIC);
			m.setJsonData("fakedata");
			heartbeatAppmasterService.sendCommand(m);
//			((HeartbeatAppmasterService) getAppmasterService()).sendCommand(m);
		}

		@Override
		public void nodeDead(HeartbeatNode node, NodeState state) {
			log.info("XXX nodeDead nodeId=" + node.getId());
			XdAppmaster.this.handleContainerFailed(ConverterUtils.toContainerId(node.getId()));
		}
	}

}
