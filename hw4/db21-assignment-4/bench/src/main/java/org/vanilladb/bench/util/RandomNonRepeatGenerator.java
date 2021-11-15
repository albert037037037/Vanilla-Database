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
package org.vanilladb.bench.util;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class RandomNonRepeatGenerator {
	private int size;
	private static Random rg = new Random();
	
	private Set<Integer> numberSet = new HashSet<Integer>();

	public RandomNonRepeatGenerator(int size) {
		this.size = size;
	}

	/**
	 * Not repeat, randomly choose a number from 1 to the initinalized size
	 */
	public int next() {
		int number;
		do{
			number = rg.nextInt(size) + 1;
		} while(!numberSet.add(number));
		
		return number;
	}

	public void reset() {
		numberSet.clear();
	}
}
