package org.apache.beam.learning.katas.triggers.eventtimetriggers;

// beam-playground:
//   name: EventTimeTriggers
//   description: Task from katas to count events with event time triggers
//   multifile: true
//   context_line: 42
//   categories:
//     - Streaming

import org.apache.beam.learning.katas.util.Log;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

public class Task {

	public static void main(String[] args) {
		PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
		Pipeline pipeline = Pipeline.create(options);

		PCollection<String> events =
			pipeline.apply(GenerateEvent.everySecond());

		PCollection<Long> output = applyTransform(events);
		output.apply(Log.ofElements());
		pipeline.run();
	}

	static PCollection<Long> applyTransform(PCollection<String> events) {
		return events
			.apply(Window
				.<String>into(FixedWindows.of(Duration.standardSeconds(5)))
				.triggering(AfterWatermark.pastEndOfWindow())
				.withAllowedLateness(Duration.ZERO)
				.discardingFiredPanes()
			)
			.apply(Combine.globally(Count.<String>combineFn()).withoutDefaults());
	}
}
