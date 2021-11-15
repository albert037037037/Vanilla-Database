/*******************************************************************************
 * Copyright 2016, 2017 vanilladb.org contributors
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
package org.vanilladb.core;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;
import org.vanilladb.core.storage.buffer.BufferConcurrencyTest;
import org.vanilladb.core.storage.buffer.BufferPoolConcurrencyTest;
import org.vanilladb.core.storage.buffer.BufferTest;
import org.vanilladb.core.storage.buffer.LastLSNTest;
import org.vanilladb.core.storage.file.FileMgrConcurrencyTest;
import org.vanilladb.core.storage.file.FileTest;
import org.vanilladb.core.storage.file.PageConcurrencyTest;

@RunWith(Suite.class)
@SuiteClasses({
	// storage.file
	FileTest.class, PageConcurrencyTest.class,
	FileMgrConcurrencyTest.class,
	
	// storage.buffer
	BufferTest.class, BufferConcurrencyTest.class,
	BufferPoolConcurrencyTest.class,
	LastLSNTest.class,
})
public class Assignment4TestSuite {
	
}
