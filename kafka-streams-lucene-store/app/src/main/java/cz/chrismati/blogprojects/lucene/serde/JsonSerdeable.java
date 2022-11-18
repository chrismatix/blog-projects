package cz.chrismati.blogprojects.lucene.serde;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import cz.chrismati.blogprojects.lucene.User;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = User.class, name = "user"),
})
public interface JsonSerdeable {}
