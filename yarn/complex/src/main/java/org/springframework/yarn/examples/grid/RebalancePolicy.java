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
package org.springframework.yarn.examples.grid;

/**
 * Enum storing possible options for supported
 * rebalance modes.
 *
 * @author Janne Valkealahti
 *
 */
public enum RebalancePolicy {

	/**
	 * No defined rebalance logic.
	 */
	NONE,

	/**
	 * Rebalance logic where group balancing is
	 * strick. This logic means that group sizes
	 * are kept in exact numbers set by the user.
	 */
	STRICK

}
