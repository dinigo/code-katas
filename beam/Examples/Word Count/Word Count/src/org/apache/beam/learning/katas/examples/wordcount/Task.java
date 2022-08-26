/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.beam.learning.katas.examples.wordcount;

// beam-playground:
//   name: WordCount
//   description: Task from katas to create a pipeline that counts the number of words.
//   multifile: false
//   context_line: 43
//   categories:
//     - Combiners

import org.apache.beam.learning.katas.util.Log;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.Arrays;

import static org.apache.beam.sdk.values.TypeDescriptors.strings;

public class Task {

	public static void main(String[] args) {
		String[] lines = {
			"apple orange grape banana apple banana",
			"banana orange banana papaya"
		};

		PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
		Pipeline pipeline = Pipeline.create(options);

		PCollection<String> wordCounts =
			pipeline.apply(Create.of(Arrays.asList(lines)));

		PCollection<String> output = applyTransform(wordCounts);

		output.apply(Log.ofElements());

		pipeline.run();
	}

	static PCollection<String> applyTransform(PCollection<String> input) {
		return input
			.apply("Lines to words", FlatMapElements
				.into(strings())
				.via(sentence -> Arrays.asList(sentence.split(" "))))
			.apply("Add keys to words", WithKeys
				.of((String el) -> el)
				.withKeyType(strings()))
      .apply("Count keys", Count.perKey())
      .apply("Format to word:count", MapElements
        .into(strings())
        .via(kvPair -> kvPair.getKey() + ":" + kvPair.getValue()));
	}
}