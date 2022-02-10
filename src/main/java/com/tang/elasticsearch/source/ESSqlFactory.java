package com.tang.elasticsearch.source;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.kafka.table.KafkaOptions;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

import java.util.HashSet;
import java.util.Set;

/**
 * @Description: 工厂类
 * @author tang
 * @date 2021/11/14 22:05
 */
public class ESSqlFactory implements DynamicTableSourceFactory {

    public static final ConfigOption<String> HOSTS= ConfigOptions.key("hosts").stringType().noDefaultValue();
    public static final ConfigOption<String> USERNAME = ConfigOptions.key("username").stringType().noDefaultValue();
    public static final ConfigOption<String> PASSWORD = ConfigOptions.key("password").stringType().noDefaultValue();
    public static final ConfigOption<String> INDEX = ConfigOptions.key("index").stringType().noDefaultValue();
    public static final ConfigOption<String> DOCUMENT_TYPE = ConfigOptions.key("document-type").stringType().noDefaultValue();
    public static final ConfigOption<String> FORMAT = ConfigOptions.key("format").stringType().noDefaultValue();
    public static final ConfigOption<Integer> FETCH_SIZE = ConfigOptions.key("fetch_size").intType().noDefaultValue();

    /**
     * @Description: 连接器名称
     * @author tang
     * @date 2021/11/1 23:15
     */
    @Override
    public String factoryIdentifier() {
        return "elasticsearch-source";
    }

    /**
     * @Description: 必须参数
     * @author tang
     * @date 2021/11/14 22:05
     */
    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(HOSTS);
        options.add(INDEX);
        options.add(USERNAME);
        options.add(PASSWORD);
        options.add(FactoryUtil.FORMAT);
        // use pre-defined option for format
        return options;
    }

    /**
     * @Description: 可选参数
     * @author tang
     * @date 2021/11/14 22:04
     */
    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(FORMAT);
        options.add(FETCH_SIZE);
        options.add(DOCUMENT_TYPE);
        return options;
    }

    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        // 获取解码器
        final DecodingFormat<DeserializationSchema<RowData>> valueFormat =
                (DecodingFormat)helper.discoverOptionalDecodingFormat(
                        DeserializationFormatFactory.class, FactoryUtil.FORMAT).orElseGet(() -> {
                    return helper.discoverDecodingFormat(DeserializationFormatFactory.class, KafkaOptions.VALUE_FORMAT);
                });

        final DecodingFormat<DeserializationSchema<RowData>> decodingFormat = helper.discoverDecodingFormat(
                DeserializationFormatFactory.class, FactoryUtil.FORMAT);

        helper.validate();

        final ReadableConfig options = helper.getOptions();
        final String hosts = options.get(HOSTS);
        final String username = options.get(USERNAME);
        final String password = options.get(PASSWORD);
        final String index = options.get(INDEX);
        final String document_type = options.get(DOCUMENT_TYPE);
        Integer fetch_size = options.get(FETCH_SIZE);
        //...

        final DataType producedDataType = context.getCatalogTable().getSchema().toPersistedRowDataType();

        return new ESDynamicTableSource(hosts, username, password,index,document_type,fetch_size,valueFormat, producedDataType);
    }
}

