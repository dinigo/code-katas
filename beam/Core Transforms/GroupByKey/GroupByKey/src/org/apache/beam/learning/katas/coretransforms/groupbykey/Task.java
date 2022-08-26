package org.apache.beam.learning.katas.coretransforms.groupbykey;

// beam-playground:
//   name: GroupByKey
//   description: Task from katas that groups words by its first letter.
//   multifile: false
//   context_line: 43
//   categories:
//     - Combiners
//     - Core Transforms

import org.apache.beam.learning.katas.util.Log;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import static org.apache.beam.sdk.values.TypeDescriptors.strings;

public class Task {

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> words =
                pipeline.apply(
                        Create.of("apple", "ball", "car", "bear", "cheetah", "ant")
                );

        PCollection<KV<String, Iterable<String>>> output = applyTransform(words);

        output.apply(Log.ofElements());

        pipeline.run();
    }

    static PCollection<KV<String, Iterable<String>>> applyTransform(PCollection<String> input) {
        return input
                .apply(WithKeys.of((String word) -> word.substring(0,1)).withKeyType(strings()))
                .apply(GroupByKey.create());
    }
}