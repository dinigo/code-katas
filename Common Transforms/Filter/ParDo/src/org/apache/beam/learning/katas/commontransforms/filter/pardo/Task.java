package org.apache.beam.learning.katas.commontransforms.filter.pardo;

// beam-playground:
//   name: FilterParDo
//   description: Task from katas to implement a filter function that filters out the even numbers by using DoFn.
//   multifile: false
//   context_line: 38
//   categories:
//     - Filtering

import org.apache.beam.learning.katas.util.Log;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class Task {

	public static void main(String[] args) {
		PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
		Pipeline pipeline = Pipeline.create(options);

		PCollection<Integer> numbers = pipeline.apply(Create.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));

		PCollection<Integer> output = applyTransform(numbers);

		output.apply(Log.ofElements());

		pipeline.run();
	}

	static PCollection<Integer> applyTransform(PCollection<Integer> input) {
		return input.apply(ParDo.of(new DoFn<Integer, Integer>() {
			@ProcessElement
			public void filterOutEvenNumbers(@Element Integer num, OutputReceiver<Integer> emitter) {
				if (num % 2 == 1) {
					emitter.output(num);
				}
			}
		}));
	}
}
