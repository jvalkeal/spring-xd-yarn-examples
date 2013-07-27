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

import java.io.File;
import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.util.StringUtils;

/**
 * Various utilities helping to test
 * a running application.
 *
 * @author Janne Valkealahti
 *
 */
public abstract class ApplicationTestUtils {

	private final static long DEFAULT_TIMEOUT = 120;
	private final static long DEFAULT_POLL = 2000;

	/**
	 * Match resources.
	 *
	 * @param baseDir the base directory to scan
	 * @param pattern the pattern finding files
	 * @return the resource[]
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public static Resource[] matchResources(File baseDir, String pattern) throws IOException {
		PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
		String locationPattern = "file:" + baseDir.getAbsolutePath() + pattern;
		return resolver.getResources(locationPattern);
	}

	/**
	 * Wait match count.
	 *
	 * @param baseDir the base directory to scan
	 * @param pattern the pattern finding files
	 * @param count the expected resource count
	 * @param needsContent if matched resource needs content
	 * @param contentContains string to be matched from a resource
	 * @return the count of matched resources
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @see #waitResourcesMatchCount(File, String, int, long, TimeUnit, long, boolean, String)
	 */
	public static int waitResourcesMatchCount(File baseDir, String pattern, int count) throws IOException {
		return waitResourcesMatchCount(baseDir, pattern, count, DEFAULT_TIMEOUT, TimeUnit.SECONDS, DEFAULT_POLL, false, null);
	}

	/**
	 * Wait match count.
	 *
	 * @param baseDir the base directory to scan
	 * @param pattern the pattern finding files
	 * @param count the expected resource count
	 * @param needsContent if matched resource needs content
	 * @param contentContains string to be matched from a resource
	 * @return the count of matched resources
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @see #waitResourcesMatchCount(File, String, int, long, TimeUnit, long, boolean, String)
	 */
	public static int waitResourcesMatchCount(File baseDir, String pattern, int count,
			boolean needsContent, String contentContains) throws IOException {
		return waitResourcesMatchCount(baseDir, pattern, count, DEFAULT_TIMEOUT, TimeUnit.SECONDS, DEFAULT_POLL, needsContent, contentContains);
	}

	/**
	 * Wait match count.
	 *
	 * @param baseDir the base directory to scan
	 * @param pattern the pattern finding files
	 * @param count the expected resource count
	 * @param timeout the timeout
	 * @param unit the time unit
	 * @param poll the poll cycle length
	 * @param needsContent if matched resource needs content
	 * @param contentContains string to be matched from a resource
	 * @return the count of matched resources
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public static int waitResourcesMatchCount(File baseDir, String pattern, int count, long timeout,
			TimeUnit unit, long poll, boolean needsContent, String contentContains) throws IOException {
		int ret = 0;
		long end = System.currentTimeMillis() + unit.toMillis(timeout);
		do {
			ret = 0;
			Resource[] resources = matchResources(baseDir, pattern);
			for (Resource res : resources) {
				if (needsContent) {
					if (res.getFile().length() > 0) {
						if (StringUtils.hasText(contentContains)) {
							Scanner scanner = new Scanner(res.getFile());
							String content = "";
							try {
								content = scanner.useDelimiter("\\A").next();
								scanner.close();
							} catch (Exception e) {
							}
							if (content.contains(contentContains)) {
								ret++;
							}
						} else {
							ret++;
						}
					}
				} else {
					ret++;
				}
			}
			if (ret >= count) {
				break;
			}
			try {
				Thread.sleep(poll);
			} catch (InterruptedException e) {
			}
		} while (System.currentTimeMillis() < end);
		return ret;
	}


}
