package org.apache.beam.learning.katas.windowing.fixedwindow;

// beam-playground:
//   name: FixedTimeWindow
//   description: Task from katas to count the number of events that happened based on fixed window with 1-day duration.
//   multifile: false
//   context_line: 44
//   categories:
//     - Combiners
//     - Streaming

import org.apache.beam.learning.katas.util.Log;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;

public class Task {

	public static void main(String[] args) {
		PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
		Pipeline pipeline = Pipeline.create(options);

		PCollection<String> events =
			pipeline.apply(
				Create.timestamped(
					TimestampedValue.of("event", Instant.parse("2019-06-01T00:00:00+00:00")),
					TimestampedValue.of("event", Instant.parse("2019-06-01T00:00:00+00:00")),
					TimestampedValue.of("event", Instant.parse("2019-06-01T00:00:00+00:00")),
					TimestampedValue.of("event", Instant.parse("2019-06-01T00:00:00+00:00")),
					TimestampedValue.of("event", Instant.parse("2019-06-05T00:00:00+00:00")),
					TimestampedValue.of("event", Instant.parse("2019-06-05T00:00:00+00:00")),
					TimestampedValue.of("event", Instant.parse("2019-06-08T00:00:00+00:00")),
					TimestampedValue.of("event", Instant.parse("2019-06-08T00:00:00+00:00")),
					TimestampedValue.of("event", Instant.parse("2019-06-08T00:00:00+00:00")),
					TimestampedValue.of("event", Instant.parse("2019-06-10T00:00:00+00:00"))
				)
			);

		PCollection<KV<String, Long>> output = applyTransform(events);
		output.apply(Log.ofElements());
		pipeline.run();
	}

	static PCollection<KV<String, Long>> applyTransform(PCollection<String> events) {
		return events
      .apply(Window.into(FixedWindows.of(Duration.standardDays(1))))
      .apply(Count.perElement());
	}

}