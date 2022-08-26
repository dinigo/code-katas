package org.apache.beam.learning.katas.coretransforms.composite;

// beam-playground:
//   name: CompositeTransform
//   description: Task from katas to implement a composite transform "ExtractAndMultiplyNumbers"
//     that extracts numbers from comma separated line and then multiplies each number by 10.
//   multifile: false
//   context_line: 46
//   categories:
//     - Combiners
//     - Flatten
//     - Core Transforms

import org.apache.beam.learning.katas.util.Log;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;

import java.util.Arrays;

import static org.apache.beam.sdk.values.TypeDescriptors.integers;
import static org.apache.beam.sdk.values.TypeDescriptors.strings;

public class Task {

	public static void main(String[] args) {
		PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
		Pipeline pipeline = Pipeline.create(options);

		pipeline
						.apply(Create.of("1,2,3,4,5", "6,7,8,9,10"))
						.apply(new ExtractAndMultiplyNumbers())
						.apply(Log.ofElements());

		pipeline.run();
	}

	/**
	 * Extracts numbers from comma separated line and then multiplies each number by 10
	 */
	public static class ExtractAndMultiplyNumbers extends PTransform<PCollection<String>, PCollection<Integer>> {
		@Override
		public PCollection<Integer> expand(PCollection<String> numList) {
			return numList
							.apply("Split list of nums", FlatMapElements.into(strings()).via((sentence) -> Arrays.asList(sentence.split(","))))
							.apply("String numbers to ints", MapElements.into(integers()).via(Integer::parseInt))
							.apply("Multiply nums x10", MapElements.into(integers()).via((num) -> num * 10));
		}
	}
}