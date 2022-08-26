package org.apache.beam.learning.katas.windowing.addingtimestamp.pardo;

// beam-playground:
//   name: WindowingAddTimestamp
//   description: Task from katas to assign each element a timestamp based on the `Event.timestamp`.
//   multifile: true
//   context_line: 39
//   categories:
//     - Streaming

import org.apache.beam.learning.katas.util.Log;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.DateTime;
import org.joda.time.Instant;

public class Task {

	public static void main(String[] args) {
		PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
		Pipeline pipeline = Pipeline.create(options);

		PCollection<Event> events =
			pipeline.apply(
				Create.of(
					new Event("1", "book-order", DateTime.parse("2019-06-01T00:00:00+00:00")),
					new Event("2", "pencil-order", DateTime.parse("2019-06-02T00:00:00+00:00")),
					new Event("3", "paper-order", DateTime.parse("2019-06-03T00:00:00+00:00")),
					new Event("4", "pencil-order", DateTime.parse("2019-06-04T00:00:00+00:00")),
					new Event("5", "book-order", DateTime.parse("2019-06-05T00:00:00+00:00"))
				)
			);

		PCollection<Event> output = applyTransform(events);

		output.apply(Log.ofElements());

		pipeline.run();
	}

	static PCollection<Event> applyTransform(PCollection<Event> events) {
		return events.apply(ParDo.of(new DoFn<Event, Event>() {
			@ProcessElement
			public void setTimestamp(@Element Event event, OutputReceiver<Event> emitter) {
				emitter.outputWithTimestamp(event, new Instant(event.getDate()));
			}
		}));
	}
}
