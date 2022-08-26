package org.apache.beam.learning.katas.coretransforms.map.pardo;

// beam-playground:
//   name: MapPardo
//   description: Task from katas that maps the input element by multiplying it by 10.
//   multifile: false
//   context_line: 38
//   categories:
//     - Core Transforms

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

        PCollection<Integer> numbers =
                pipeline.apply(Create.of(1, 2, 3, 4, 5));

        PCollection<Integer> output = applyTransform(numbers);

        output.apply(Log.ofElements());

        pipeline.run();
    }

    static PCollection<Integer> applyTransform(PCollection<Integer> numbers) {
        DoFn<Integer, Integer> multiplyNumbers = new DoFn<Integer, Integer>() {
            @ProcessElement
            public void processElements(@Element Integer number, OutputReceiver<Integer> emitter) {
                emitter.output(number * 10);
            }
        };
        return numbers.apply(ParDo.of(multiplyNumbers));

    }
}