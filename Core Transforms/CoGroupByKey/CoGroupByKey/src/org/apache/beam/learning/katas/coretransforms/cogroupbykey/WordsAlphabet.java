package org.apache.beam.learning.katas.coretransforms.cogroupbykey;

public class WordsAlphabet {

    private String alphabet;
    private String fruit;
    private String country;

    public WordsAlphabet(String alphabet, String fruit, String country) {
        this.alphabet = alphabet;
        this.fruit = fruit;
        this.country = country;
    }

    @Override
    public String toString() {
        return "WordsAlphabet{" +
                "alphabet='" + alphabet + '\'' +
                ", fruit='" + fruit + '\'' +
                ", country='" + country + '\'' +
                '}';
    }

}
