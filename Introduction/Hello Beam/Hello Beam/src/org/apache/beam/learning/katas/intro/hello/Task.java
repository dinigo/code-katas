package org.apache.beam.learning.katas.intro.hello;

import org.apache.beam.learning.katas.util.Log;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;

public class Task {

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> output = setupPipeline(pipeline);

        output.apply(Log.ofElements());

        pipeline.run();
    }

    static PCollection<String> setupPipeline(Pipeline pipeline) {
        return pipeline.apply(Create.of("Hello Beam"));
    }
}