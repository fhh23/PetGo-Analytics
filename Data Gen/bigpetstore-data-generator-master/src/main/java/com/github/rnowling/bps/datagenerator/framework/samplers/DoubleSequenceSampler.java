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
package com.github.rnowling.bps.datagenerator.framework.samplers;

public class DoubleSequenceSampler implements Sampler<Double>
{
	Double start;
	Double end;
	Double step;
	Double next;
	
	public DoubleSequenceSampler()
	{
		start = 0.0;
		end = null;
		step = 1.0;
		next = start;
	}
	
	public DoubleSequenceSampler(Double start)
	{
		this.start = start;
		end = null;
		step = 1.0;
		next = start;
	}
	
	public DoubleSequenceSampler(Double start, Double end)
	{
		this.start = start;
		this.end = end;
		step = 1.0;
		next = start;
	}
	
	public DoubleSequenceSampler(Double start, Double end, Double step)
	{
		this.start = start;
		this.end = end;
		this.step = step;
		next = start;
	}
	
	public Double sample() throws Exception
	{
		if(end == null || next < end)
		{
			Double current = next;
			next = current + step;
			return current;
		}
		
		throw new Exception("All values have been sampled");
	}
	
	
}
