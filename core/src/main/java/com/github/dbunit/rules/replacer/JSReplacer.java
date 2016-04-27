package com.github.dbunit.rules.replacer;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.dbunit.dataset.*;

import com.github.dbunit.rules.api.replacer.*;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

/**
 * based on: http://marcin-michalski.pl/2012/10/22/decorating-dbunit-datasets-power-of-replacementdataset/
 */
public class JSReplacer {

    private static JSReplacer instance;

    private static Logger log = Logger.getLogger(JSReplacer.class.getName());

    private ScriptEngine engine;

    private JSReplacer() {
        engine = new ScriptEngineManager().getEngineByName("js");
    }

    public static IDataSet replace(IDataSet dataset) {
        if (instance == null) {
            instance = new JSReplacer();
        }
        ReplacementDataSet replacementDataSet = new ReplacementDataSet(dataset);
        try{
            instance.replaceScripts(replacementDataSet);
        }catch (DataSetException e){
            log.log(Level.WARNING, "Could not replace dataset: " + dataset, e);
        }
        return replacementDataSet;
    }

    private void replaceScripts(ReplacementDataSet dataSet) throws DataSetException {
        ITableIterator iterator = dataSet.iterator();
        while (iterator.next()) {
            ITable table = iterator.getTable();
            for (Column column : table.getTableMetaData().getColumns()) {
                for (int i = 0; i < table.getRowCount(); i++) {
                    String script = table.getValue(i, column.getColumnName()).toString();
                    if(script.startsWith("js:")){
                      Object scriptResult = getScriptResult(script, dataSet);
                      if(scriptResult != null){
                        dataSet.addReplacementObject(script, scriptResult);;
                      }else{
                        throw new RuntimeException(String.format("Could not perform script replacement for table '%s', column '%s'.",table.getTableMetaData().getTableName(),column.getColumnName()));
                      }
                    }
                }
            }
         }
    }

    private Object getScriptResult(String script, ReplacementDataSet replacementDataSet) {
       /* format is 'js:script to execute', ex:
        - id: "2"
        date: "js:var date=new Date(); date.toString();"*/
        String scriptToExecute = script.trim().substring(3);
        String scriptResult = null;
        try{
          return engine.eval(scriptToExecute);
        }catch (Exception e){
          log.log(Level.WARNING, "Could not perform replacement for script: " + script, e);
          return null;
        }
    }




}
