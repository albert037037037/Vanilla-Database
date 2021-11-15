/*******************************************************************************
 * Copyright 2016, 2017 elasql.org contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.elasql.bench.util;

import org.vanilladb.core.util.PropertiesLoader;

public class ElasqlBenchProperties extends PropertiesLoader {
	
	private static ElasqlBenchProperties loader;

	public static ElasqlBenchProperties getLoader() {
		// Singleton
		if (loader == null)
			loader = new ElasqlBenchProperties();
		return loader;
	}

	protected ElasqlBenchProperties() {
		super();
	}

	@Override
	protected String getConfigFilePath() {
		return System.getProperty("org.elasql.bench.config.file");
	}

}
