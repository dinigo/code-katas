package org.apache.beam.learning.katas.coretransforms.cogroupbykey;

import static org.apache.beam.sdk.values.TypeDescriptors.kvs;
import static org.apache.beam.sdk.values.TypeDescriptors.strings;

import org.apache.beam.learning.katas.util.Log;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;

public class Task {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> fruits =
                pipeline.apply("Fruits",
                        Create.of("apple", "banana", "cherry")
                );

        PCollection<String> countries =
                pipeline.apply("Countries",
                        Create.of("australia", "brazil", "canada")
                );

        PCollection<String> output = applyTransform(fruits, countries);

        output.apply(Log.ofElements());

        pipeline.run();
    }


    static PCollection<String> applyTransform(PCollection<String> fruits, PCollection<String> countries) {
        WithKeys<String, String> firstLetterKey = WithKeys
                .of((String word) -> word.substring(0, 1))
                .withKeyType(strings());

        PCollection<KV<String, String>> keyedCountries = countries.apply("Create keys for countries", firstLetterKey);
        PCollection<KV<String, String>> keyedFruits = fruits.apply("Create keys for fruits", firstLetterKey);
        TupleTag<String> countriesTag = new TupleTag<>();
        TupleTag<String> fruitsTag = new TupleTag<>();

        // ParDo new cannot infer the type from the variable declaration. It must be
        // explicitly declared to in the new
        DoFn<KV<String, CoGbkResult>, String> stringifyResults = new DoFn<KV<String, CoGbkResult>, String>() {
            @ProcessElement
            public void processElement(@Element KV<String, CoGbkResult> record, OutputReceiver<String> emitter) {
                WordsAlphabet wa = new WordsAlphabet(
                        record.getKey(),
                        record.getValue().getOnly(fruitsTag),
                        record.getValue().getOnly(countriesTag)
                );
                emitter.output(wa.toString());
            }
        };

        KeyedPCollectionTuple<String> keyedPCollection = KeyedPCollectionTuple
                .of(countriesTag, keyedCountries)
                .and(fruitsTag, keyedFruits);

        PCollection<KV<String, CoGbkResult>> joinedPCollection = keyedPCollection
                .apply("Group by initial letter", CoGroupByKey.create());

        PCollection<String> joinedDict = joinedPCollection
                .apply("Map to string each group result", ParDo.of(stringifyResults));

        return joinedDict;
    }
}

