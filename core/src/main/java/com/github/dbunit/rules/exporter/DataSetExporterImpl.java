package com.github.dbunit.rules.exporter;

import com.github.dbunit.rules.api.expoter.DataSetExportConfig;
import com.github.dbunit.rules.api.expoter.DataSetExporter;
import org.dbunit.DatabaseUnitException;
import org.dbunit.database.*;
import org.dbunit.database.search.TablesDependencyHelper;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.filter.ITableFilter;

import java.io.OutputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by pestano on 09/09/16.
 */
public class DataSetExporterImpl implements DataSetExporter {



    @Override
    public OutputStream export(Connection connection, DataSetExportConfig dataSetExportConfig) throws SQLException, DatabaseUnitException {

        if(connection == null || connection.isClosed()){
            throw new RuntimeException("Provide a valid connection to export datasets");
        }

        if(dataSetExportConfig == null){
            dataSetExportConfig = new DataSetExportConfig();
        }

        String outputName = dataSetExportConfig.getOutputName();
        if(outputName == null || "".equals(outputName.trim())){
            outputName = "dbunit-export."+dataSetExportConfig.getDataSetFormat().name().toLowerCase();
        }

        boolean hasIncludes = dataSetExportConfig.getIncludeTables() == null || dataSetExportConfig.getIncludeTables().length > 0;

        IDatabaseConnection dbunitConnection = new DatabaseConnection(connection);
        Set<String> targetTables = new HashSet<>();

        if(hasIncludes){
            targetTables.addAll(Arrays.asList(dataSetExportConfig.getIncludeTables()));
        }

        if(dataSetExportConfig.isDependentTables()){
            String[] dependentTables = TablesDependencyHelper.getAllDependentTables(dbunitConnection, dataSetExportConfig.getIncludeTables());
            if(dependentTables != null && dependentTables.length > 0){
                targetTables.addAll(Arrays.asList(dependentTables));
            }
        }

        IDataSet outputDataSet = null;
        ITableFilter filter = null;
        DatabaseConfig config = dbunitConnection.getConfig();
        if(!targetTables.isEmpty()){
            filter = new DatabaseSequenceFilter(dbunitConnection,targetTables.toArray(new String[targetTables.size()]));
        }else {
            filter = new DatabaseSequenceFilter(dbunitConnection);
        }
      /*  JSONDataSet j;
        j.
        FlatXmlWriter
        YamlDataSet yamlDataSet = new YamlDataSet().*/
        config.setProperty(DatabaseConfig.PROPERTY_RESULTSET_TABLE_FACTORY, new ForwardOnlyResultSetTableFactory());


        return null;
    }
}
