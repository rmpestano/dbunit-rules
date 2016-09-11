package com.github.dbunit.rules.api.expoter;

import com.github.dbunit.rules.api.dataset.DataSetFormat;

/**
 * Created by pestano on 09/09/16.
 */
public class DataSetExportConfig {

    private DataSetFormat dataSetFormat = DataSetFormat.YML;
    private String outputName;
    private String[] includeTables;
    private boolean dependentTables = true;


    public DataSetExportConfig dataSetFormat(DataSetFormat dataSetFormat) {
        this.dataSetFormat = dataSetFormat;
        return this;
    }

    public DataSetExportConfig outputName(String outputName) {
        this.outputName = outputName;
        return this;
    }

    public DataSetExportConfig includeTables(String[] includeTables) {
        this.includeTables = includeTables;
        return this;
    }



    public DataSetFormat getDataSetFormat() {
        return dataSetFormat;
    }

    public String getOutputName() {
        return outputName;
    }

    public String[] getIncludeTables() {
        return includeTables;
    }

    public boolean isDependentTables() {
        return dependentTables;
    }

    public DataSetExportConfig dependentTables(boolean dependentTables) {
        this.dependentTables = dependentTables;
        return this;
    }
}
