package cz.chrismati.blogprojects.lucene;

import cz.chrismati.blogprojects.lucene.serde.JsonSerdeable;

public record User(String name, String email) implements JsonSerdeable {
}
