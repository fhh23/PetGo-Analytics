/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.rnowling.bps.datagenerator.framework.pdfs;

import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;

public class DiscretePDF<T> implements ProbabilityDensityFunction<T>
{
	private final ImmutableMap<T, Double> probabilities;
	
	public DiscretePDF(Map<T, Double> probabilities)
	{
		this.probabilities = ImmutableMap.copyOf(probabilities);
	}
	
	public Set<T> getData()
	{
		return probabilities.keySet();
	}
	
	public double probability(T value)
	{
		if(probabilities.containsKey(value))
		{
			return probabilities.get(value);
		}
		
		return 0.0;
	}
}