package com.gihub.dbunit.rules.dataset;

/**
 * Created by rafael-pestano on 22/07/2015.
 */

import org.dbunit.dataset.*;
import org.dbunit.dataset.datatype.DataType;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class YamlDataSet implements IDataSet {

  private Map<String, MyTable> tables = new HashMap<String, MyTable>();

  public YamlDataSet(InputStream source) {
    @SuppressWarnings("unchecked")
    Map<String, List<Map<String, Object>>> data = (Map<String, List<Map<String, Object>>>) new Yaml().load(source);
    for (Map.Entry<String, List<Map<String, Object>>> ent : data.entrySet()) {
      String tableName = ent.getKey();
      List<Map<String, Object>> rows = ent.getValue();
      createTable(tableName, rows);
    }
  }

  class MyTable implements ITable {
    String                    name;

    List<Map<String, Object>> data;

    ITableMetaData            meta;

    MyTable(String name, List<String> columnNames) {
      this.name = name;
      this.data = new ArrayList<Map<String, Object>>();
      meta = createMeta(name, columnNames);
    }

    ITableMetaData createMeta(String name, List<String> columnNames) {
      Column[] columns = null;
      if (columnNames != null) {
        columns = new Column[columnNames.size()];
        for (int i = 0; i < columnNames.size(); i++)
          columns[i] = new Column(columnNames.get(i), DataType.UNKNOWN);
      }
      return new DefaultTableMetaData(name, columns);
    }

    public int getRowCount() {
      return data.size();
    }

    public ITableMetaData getTableMetaData() {
      return meta;
    }

    public Object getValue(int row, String column) throws DataSetException {
      if (data.size() <= row)
        throw new RowOutOfBoundsException("" + row);
      return data.get(row).get(column.toUpperCase());
    }

    public void addRow(Map<String, Object> values) {
      data.add(convertMap(values));
    }

    Map<String, Object> convertMap(Map<String, Object> values) {
      Map<String, Object> ret = new HashMap<String, Object>();
      for (Map.Entry<String, Object> ent : values.entrySet()) {
        ret.put(ent.getKey().toUpperCase(), ent.getValue());
      }
      return ret;
    }

  }

  MyTable createTable(String name, List<Map<String, Object>> rows) {
    MyTable table = new MyTable(name, rows.size() > 0 ? new ArrayList<String>(rows.get(0).keySet()) : null);
    for (Map<String, Object> values : rows)
      table.addRow(values);
    tables.put(name, table);
    return table;
  }

  public ITable getTable(String tableName) throws DataSetException {
    return tables.get(tableName);
  }

  public ITableMetaData getTableMetaData(String tableName) throws DataSetException {
    return tables.get(tableName).getTableMetaData();
  }

  public String[] getTableNames() throws DataSetException {
    return (String[]) tables.keySet().toArray(new String[tables.size()]);
  }

  public ITable[] getTables() throws DataSetException {
    return (ITable[]) tables.values().toArray(new ITable[tables.size()]);
  }

  public ITableIterator iterator() throws DataSetException {
    return new DefaultTableIterator(getTables());
  }

  public ITableIterator reverseIterator() throws DataSetException {
    return new DefaultTableIterator(getTables(), true);
  }

  public boolean isCaseSensitiveTableNames() {
    return false;
  }

}
