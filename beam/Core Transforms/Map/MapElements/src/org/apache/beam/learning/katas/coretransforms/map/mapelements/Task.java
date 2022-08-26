package org.apache.beam.learning.katas.coretransforms.map.mapelements;

// beam-playground:
//   name: Map
//   description: Task from katas to implement a simple map function that multiplies all input elements by 5.
//   multifile: false
//   context_line: 38
//   categories:
//     - Core Transforms

import org.apache.beam.learning.katas.util.Log;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

public class Task {

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<Integer> numbers =
                pipeline.apply(Create.of(10, 20, 30, 40, 50));

        PCollection<Integer> output = applyTransform(numbers);

        output.apply(Log.ofElements());

        pipeline.run();
    }

    static PCollection<Integer> applyTransform(PCollection<Integer> input) {
        return input.apply(MapElements
                .into(TypeDescriptors.integers())
                .via((Integer num) -> num * 5)
        );
    }

}