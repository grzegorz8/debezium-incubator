package io.debezium.connector.sqlserver;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

import io.debezium.data.Envelope;
import io.debezium.data.SchemaUtil;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnId;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;
import io.debezium.relational.TableSchemaBuilder;
import io.debezium.relational.ValueConverter;
import io.debezium.relational.ValueConverterProvider;
import io.debezium.relational.mapping.ColumnMapper;
import io.debezium.relational.mapping.ColumnMappers;
import io.debezium.util.SchemaNameAdjuster;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SqlServerTableSchemaBuilder extends TableSchemaBuilder {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerTableSchemaBuilder.class);

    private final SchemaNameAdjuster schemaNameAdjuster;
    private final ValueConverterProvider valueConverterProvider;
    private final Schema sourceInfoSchema;

    /**
     * Create a new instance of the builder.
     *
     * @param valueConverterProvider the provider for obtaining {@link ValueConverter}s and {@link SchemaBuilder}s; may not be
     * null
     * @param schemaNameAdjuster the adjuster for schema names; may not be null
     */
    public SqlServerTableSchemaBuilder(
            ValueConverterProvider valueConverterProvider,
            SchemaNameAdjuster schemaNameAdjuster,
            Schema sourceInfoSchema) {
        super(valueConverterProvider, schemaNameAdjuster, sourceInfoSchema);
        this.schemaNameAdjuster = schemaNameAdjuster;
        this.valueConverterProvider = valueConverterProvider;
        this.sourceInfoSchema = sourceInfoSchema;
    }

    @Override
    public TableSchema create(String schemaPrefix, String envelopSchemaName, Table table, Predicate<ColumnId> filter, ColumnMappers mappers) {
        if (schemaPrefix == null) schemaPrefix = "";
        // Build the schemas ...
        final TableId tableId = table.id();
        final String tableIdStr = tableId.toString();
        final String schemaNamePrefix = schemaPrefix + tableIdStr;
        LOGGER.debug("Mapping table '{}' to schemas under '{}'", tableId, schemaNamePrefix);
        SchemaBuilder valSchemaBuilder = SchemaBuilder.struct().name(schemaNameAdjuster.adjust(schemaNamePrefix + ".Value"));
        SchemaBuilder keySchemaBuilder = SchemaBuilder.struct().name(schemaNameAdjuster.adjust(schemaNamePrefix + ".Key"));
        AtomicBoolean hasPrimaryKey = new AtomicBoolean(false);
        table.columns().forEach(column -> {
            if (table.isPrimaryKeyColumn(column.name())) {
                // The column is part of the primary key, so ALWAYS add it to the PK schema ...
                addField(keySchemaBuilder, column, null);
                hasPrimaryKey.set(true);
            }
            if (filter == null || filter.test(new ColumnId(tableId, column.name()))) {
                // Add the column to the value schema only if the column has not been filtered ...
                ColumnMapper mapper = mappers == null ? null : mappers.mapperFor(tableId, column);
                addField(valSchemaBuilder, column, mapper);
            }
        });
        Schema valSchema = valSchemaBuilder.optional().build();
        Schema keySchema = hasPrimaryKey.get() ? keySchemaBuilder.build() : null;

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Mapped primary key for table '{}' to schema: {}", tableId, SchemaUtil.asDetailedString(keySchema));
            LOGGER.debug("Mapped columns for table '{}' to schema: {}", tableId, SchemaUtil.asDetailedString(valSchema));
        }

        Envelope envelope = Envelope.defineSchema()
                .withName(schemaNameAdjuster.adjust(envelopSchemaName))
                .withRecord(valSchema)
                .withSource(sourceInfoSchema)
                .build();

        Map<Column, Integer> columnIndexes = columnsToIndexes(table.columns());
        // Create the generators ...
        Function<Object[], Object> keyGenerator = createKeyGenerator(keySchema, tableId, table.primaryKeyColumns(), columnIndexes);
        Function<Object[], Struct> valueGenerator = createValueGenerator(valSchema, tableId, table.columns(), columnIndexes, filter, mappers);

        // And the table schema ...
        return new TableSchema(tableId, keySchema, keyGenerator, envelope, valSchema, valueGenerator);
    }

    private Function<Object[], Object> createKeyGenerator(Schema schema, TableId columnSetName,
            List<Column> columns, Map<Column, Integer> columnIndexes) {
        if (schema != null) {
            // Assumption: row contains only whitelisted columns. The order of values in row is the same.
            Map<Column, ValueConverter> converters = convertersForColumns(schema, columnSetName, columns, null);
            Map<Column, Field> fields = fieldMapForColumns(schema, columns);

            return (row) -> {
                Struct result = new Struct(schema);
                for (Column column : columns) {
                    Object value = row[columnIndexes.get(column)];
                    ValueConverter converter = converters.get(column);
                    if (converter != null) {
                        value = value == null ? value : converter.convert(value);
                        try {
                            result.put(fields.get(column), value);
                        } catch (DataException e) {
                            LOGGER.error("Failed to properly convert key value for '{}.{}' of type {} for row {}:",
                                    columnSetName, column.name(), column.typeName(), row, e);
                        }
                    }
                }
                return result;
            };
        }
        return null;
    }

    private Function<Object[], Struct> createValueGenerator(Schema schema, TableId tableId,
            List<Column> columns, Map<Column, Integer> columnIndexes, Predicate<ColumnId> filter, ColumnMappers mappers) {
        if (schema != null) {
            // Assumption: row contains only whitelisted columns. The order of values in row is the same.
            List<Column> whitelistedColumns = filterColumns(columns, tableId, filter);
            Map<Column, ValueConverter> converters = convertersForColumns(schema, tableId, whitelistedColumns, mappers);
            Map<Column, Field> fields = fieldMapForColumns(schema, whitelistedColumns);

            return (row) -> {
                Struct result = new Struct(schema);
                for (Column column : whitelistedColumns) {
                    Object value = row[columnIndexes.get(column)];
                    ValueConverter converter = converters.get(column);
                    if (converter != null) {
                        try {
                            value = converter.convert(value);
                            result.put(fields.get(column), value);
                        } catch (final Exception e) {
                            LOGGER.error("Failed to properly convert data value for '{}.{}' of type {} for row {}:",
                                    tableId, column.name(), column.typeName(), row, e);
                        }
                    }
                }
                return result;
            };
        }
        return null;
    }

    private List<Column> filterColumns(List<Column> columns, TableId tableId,
            Predicate<ColumnId> filter) {
        return filter == null
                ? null
                : columns.stream()
                .filter(column -> filter.test(new ColumnId(tableId, column.name())))
                .collect(Collectors.toList());
    }

    private Map<Column, Integer> columnsToIndexes(List<Column> columns) {
        return IntStream.range(0, columns.size()).boxed().collect(toMap(columns::get, identity()));
    }


    private Map<Column, Field> fieldMapForColumns(Schema schema, List<Column> columns) {
        return columns.stream().collect(toMap(identity(), column -> schema.field(column.name())));
    }

    private Map<Column, ValueConverter> convertersForColumns(Schema schema, TableId tableId,
            List<Column> columns, ColumnMappers mappers) {

        Map<Column, ValueConverter> converters = new HashMap<>(columns.size());

        for (Column column : columns) {
            ValueConverter converter = createValueConverterFor(column, schema.field(column.name()));
            converter = wrapInMappingConverterIfNeeded(mappers, tableId, column, converter);

            if (converter == null) {
                LOGGER.warn(
                        "No converter found for column {}.{} of type {}. The column will not be part of change events for that table.",
                        tableId, column.name(), column.typeName());
            }

            // may be null if no converter found
            converters.put(column, converter);
        }

        return converters;
    }


    private ValueConverter wrapInMappingConverterIfNeeded(ColumnMappers mappers, TableId tableId,
            Column column, ValueConverter converter) {
        if (mappers == null || converter == null) {
            return converter;
        }

        ValueConverter mappingConverter = mappers.mappingConverterFor(tableId, column);
        if (mappingConverter == null) {
            return converter;
        }

        return (value) -> {
            if (value != null) {
                value = converter.convert(value);
            }

            return mappingConverter.convert(value);
        };
    }

}
