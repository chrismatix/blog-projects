package cz.chrismati.blogprojects.lucene.store;

import cz.chrismati.blogprojects.lucene.User;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StringField;

public class DocumentMapper {
    public static Document from(String key, User user) {
        final Document document = new Document();
        document.add(new StringField("id", key, StringField.Store.YES));
        document.add(new StringField("name", user.name(), org.apache.lucene.document.Field.Store.YES));
        document.add(new StringField("email", user.email(), org.apache.lucene.document.Field.Store.YES));
        return document;
    };
}
