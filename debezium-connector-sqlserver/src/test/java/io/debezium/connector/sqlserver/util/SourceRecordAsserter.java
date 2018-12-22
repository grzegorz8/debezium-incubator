package io.debezium.connector.sqlserver.util;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.fest.assertions.Assertions;


public class SourceRecordAsserter {

    public static SourceRecordAsserter assertThat(SourceRecord sourceRecord) {
        return new SourceRecordAsserter(sourceRecord);
    }

    private final SourceRecord record;

    private SourceRecordAsserter(SourceRecord record) {
        this.record = record;
    }

    public SourceRecordAsserter valueAfterFieldIsEqualTo(Struct expectedValue) {
        Struct value = (Struct) record.value();
        Struct afterValue = (Struct) value.get("after");
        Assertions.assertThat(afterValue).isEqualTo(expectedValue);
        return this;
    }

    public SourceRecordAsserter valueAfterFieldSchemaIsEqualTo(Schema expectedSchema) {
        Schema valueSchema = record.valueSchema();
        Schema afterFieldSchema = valueSchema.field("after").schema();
        Assertions.assertThat(afterFieldSchema).isEqualTo(expectedSchema);
        return this;
    }
}
