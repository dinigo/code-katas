package org.apache.beam.learning.katas.coretransforms.map.pardoonetomany;

// beam-playground:
//   name: MapParDoOneToMany
//   description: Task from katas that maps each input sentence into words split by whitespace (" ").
//   multifile: false
//   context_line: 39
//   categories:
//     - Flatten
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

    PCollection<String> sentences =
        pipeline.apply(Create.of("Hello Beam", "It is awesome"));

    PCollection<String> output = applyTransform(sentences);

    output.apply(Log.ofElements());

    pipeline.run();
  }

  static PCollection<String> applyTransform(PCollection<String> input) {
    DoFn<String, String> splitString = new DoFn<String, String>() {
      @ProcessElement
      public void processElement(@Element String sentence, OutputReceiver<String> emitter){
        String[] words = sentence.split(" ");
        for (String word: words) {
          emitter.output(word);
        }
      }
    };

    return input.apply(ParDo.of(splitString));
  }

}