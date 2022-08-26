package org.apache.beam.learning.katas.coretransforms.combine.combinefn;

// beam-playground:
//   name: CombineFn
//   description: Task from katas averaging.
//   multifile: false
//   context_line: 41
//   categories:
//     - Combiners
//     - Core Transforms

import org.apache.beam.learning.katas.util.Log;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;

import java.io.Serializable;

public class Task {

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<Integer> numbers = pipeline.apply(Create.of(10, 20, 50, 70, 90));

        PCollection<Double> output = applyTransform(numbers);

        output.apply(Log.ofElements());

        pipeline.run();
    }

    static PCollection<Double> applyTransform(PCollection<Integer> input) {
        return input.apply(Combine.globally(new AverageFn()));
    }

    static class AverageFn extends CombineFn<Integer, AverageFn.Accum, Double> {

        public static class Accum implements Serializable {
            int sum = 0;
            int count = 0;
        }

        @Override
        public Accum createAccumulator() {
            return new Accum();
        }

        @Override
        public Accum addInput(AverageFn.Accum mutableAccumulator, Integer input) {
            mutableAccumulator.sum += input;
            mutableAccumulator.count++;
            return mutableAccumulator;
        }

        @Override
        public Accum mergeAccumulators(Iterable<AverageFn.Accum> accumulators) {
            Accum merged = createAccumulator();
            for (Accum accum : accumulators) {
                merged.sum += accum.sum;
                merged.count += accum.count;
            }
            return merged;
        }

        @Override
        public Double extractOutput(AverageFn.Accum accumulator) {
            return ((double) accumulator.sum) / accumulator.count;
        }
    }
}