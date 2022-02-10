package com.tang.elasticsearch.source;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

/**
 * @Description: 动态table source
 * @author tang
 * @date 2021/11/14 22:06
 */
public class ESDynamicTableSource implements ScanTableSource {

    private final String hosts;
    private final String username;
    private final String password;
    private final String index;
    private final String document_type;
    private Integer fetch_size;
    //...
    private final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;
    private final DataType producedDataType;

    public ESDynamicTableSource(String hosts,
                                String username,
                                String password,
                                String index,
                                String document_type,
                                Integer fetch_size,
                                DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
                                DataType producedDataType) {
        this.hosts = hosts;
        this.username = username;
        this.password = password;
        this.index = index;
        this.document_type = document_type;
        //...
        this.decodingFormat = decodingFormat;
        this.producedDataType = producedDataType;
        this.fetch_size = fetch_size;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return decodingFormat.getChangelogMode();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        final DeserializationSchema<RowData> deserializer = decodingFormat.createRuntimeDecoder(
                runtimeProviderContext,
                producedDataType);

        final SourceFunction<RowData> sourceFunction = new ESSourceFunction(
                hosts, username, password,index,document_type,fetch_size,deserializer);

        return SourceFunctionProvider.of(sourceFunction, false);
    }

    @Override
    public DynamicTableSource copy() {
        return new ESDynamicTableSource(hosts, username, password,index,document_type,fetch_size,decodingFormat,producedDataType);
    }

    @Override
    public String asSummaryString() {
        return "elastic Table Source";
    }
}

