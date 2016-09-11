package com.github.dbunit.rules.exporter;

import com.github.dbunit.rules.api.expoter.DataSetExportConfig;
import com.github.dbunit.rules.api.expoter.DataSetExporter;
import org.dbunit.DatabaseUnitException;
import org.dbunit.database.*;
import org.dbunit.database.search.TablesDependencyHelper;
import org.dbunit.dataset.FilteredDataSet;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.filter.ITableFilter;
import org.dbunit.dataset.xml.FlatXmlDataSet;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by pestano on 09/09/16.
 * <p/>
 * based on: http://archive.oreilly.com/pub/post/dbunit_made_easy.html
 */
public class DataSetExporterImpl implements DataSetExporter {

    /**
     * A regular expression that is used to get the table name
     * from a SQL 'select' statement.
     * This  pattern matches a string that starts with any characters,
     * followed by the case-insensitive word 'from',
     * followed by a table name of the form 'foo' or 'schema.foo',
     * followed by any number of remaining characters.
     */
    private static final Pattern TABLE_MATCH_PATTERN = Pattern.compile(".*\\s+from\\s+(\\w+(\\.\\w+)?).*",
            Pattern.CASE_INSENSITIVE);

    private static Logger log = Logger.getLogger(DataSetExporterImpl.class.getName());


    private static DataSetExporter instance;

    private DataSetExporterImpl(){}

    public static DataSetExporter getInstance(){
        if(instance == null){
            instance = new DataSetExporterImpl();
        }
        return instance;
    }

    @Override
    public OutputStream export(Connection connection, DataSetExportConfig dataSetExportConfig, String outputFile) throws SQLException, DatabaseUnitException {

        if (connection == null || connection.isClosed()) {
            throw new RuntimeException("Provide a valid connection to export datasets");
        }

        if (dataSetExportConfig == null) {
            dataSetExportConfig = new DataSetExportConfig();
        }

        if(outputFile == null || "".equals(outputFile)){
            throw new RuntimeException("Provide output file name to export dataset.");
        }

        if(!outputFile.contains(".")){
            outputFile = outputFile +"."+dataSetExportConfig.getDataSetFormat().name().toLowerCase();
        }

        boolean hasIncludes = dataSetExportConfig.getIncludeTables() != null && dataSetExportConfig.getIncludeTables().length > 0;

        IDatabaseConnection dbunitConnection = new DatabaseConnection(connection);

        DatabaseConfig config = dbunitConnection.getConfig();
        config.setProperty(DatabaseConfig.PROPERTY_RESULTSET_TABLE_FACTORY, new ForwardOnlyResultSetTableFactory());

        Set<String> targetTables = new HashSet<>();

        if (hasIncludes) {
            targetTables.addAll(Arrays.asList(dataSetExportConfig.getIncludeTables()));
            if (dataSetExportConfig.isDependentTables()) {
                String[] dependentTables = TablesDependencyHelper.getAllDependentTables(dbunitConnection, dataSetExportConfig.getIncludeTables());
                if (dependentTables != null && dependentTables.length > 0) {
                    targetTables.addAll(Arrays.asList(dependentTables));
                }
            }
        }


        QueryDataSet queryDataSet = null;
        if (dataSetExportConfig.getQueryList() != null && dataSetExportConfig.getQueryList().length > 0) {
            queryDataSet = new QueryDataSet(dbunitConnection);
            addQueries(queryDataSet, dataSetExportConfig.getQueryList(), targetTables);
        }


        IDataSet dataset = null;
        ITableFilter filter = null;
        //sequenceFiltering
        if (!targetTables.isEmpty()) {
            filter = new DatabaseSequenceFilter(dbunitConnection, targetTables.toArray(new String[targetTables.size()]));
        } else {
            //if no tables are included then use seq filtering on all tables
            filter = new DatabaseSequenceFilter(dbunitConnection);
        }
        if (queryDataSet != null) {
            dataset = new FilteredDataSet(filter, queryDataSet);
        } else {
            dataset = new FilteredDataSet(filter, dbunitConnection.createDataSet());
        }
        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(outputFile);
            switch (dataSetExportConfig.getDataSetFormat()) {
                case XML: {
                    FlatXmlDataSet.write(dataset, fos);
                    log.info("DataSet exported successfully at "+ Paths.get(outputFile).toAbsolutePath().toString());
                }
            }
        } catch (Exception e) {
            log.log(Level.SEVERE, "Could not export dataset.", e);
            throw new RuntimeException("Could not export dataset.", e);
        }
        finally {
            if(fos != null){
                try {
                    fos.close();
                } catch (IOException e) {
                    log.log(Level.SEVERE, "Could not close file output stream.", e);
                }
            }
        }

        return null;
    }

    private void addQueries(QueryDataSet dataSet, String[] queryList, Set<String> targetTables) {
        try {
            for (String targetTable : targetTables) {
                dataSet.addTable(targetTable);
            }
            for (String query : queryList) {
                Matcher m = TABLE_MATCH_PATTERN.matcher(query);
                if (!m.matches()) {
                    log.warning("Unable to parse query. Ignoring '" + query + "'.");
                } else {
                    String table = m.group(1);
                    if (targetTables.contains(table)) {
                        //already in includes
                        log.warning(String.format("Ignoring query %s because its table is already in includedTables.", query));
                        continue;
                    } else {
                        dataSet.addTable(table, query);
                    }
                }
            }
        } catch (Exception e) {
            log.log(Level.SEVERE, String.format("Could not add query due to following error:" + e.getMessage(), e));
        }

    }
}
