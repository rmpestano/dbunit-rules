package com.github.dbunit.rules;

import com.github.dbunit.rules.api.dataset.DataSet;
import com.github.dbunit.rules.api.dataset.DataSetFormat;
import com.github.dbunit.rules.api.expoter.ExportDataSet;
import com.github.dbunit.rules.util.EntityManagerProvider;
import org.junit.AfterClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.contentOf;

/**
 * Created by pestano on 11/09/16.
 */
@RunWith(JUnit4.class)
public class ExportDataSetIt {

    @Rule
    public EntityManagerProvider emProvider = EntityManagerProvider.instance("rules-it"); //<1>

    @Rule
    public DBUnitRule dbUnitRule = DBUnitRule.instance(emProvider.connection());


    @Test
    @DataSet("datasets/yml/user.yml")
    @ExportDataSet(format = DataSetFormat.XML,outputName="target/exported/generated.xml")
    public void shouldExportDataSetAfterTestExecution() {
    }

    @Test
    @DataSet("datasets/yml/user.yml")
    @ExportDataSet(format = DataSetFormat.XML, queryList = {"select * from USER u where u.ID = 1"}, outputName="target/exported/filtered.xml")
    public void shouldExportDataSetUsingQueryToFilterRows() {

    }

    @Test
    @DataSet("datasets/yml/user.yml")
    @ExportDataSet(format = DataSetFormat.XML, includeTables = "USER", outputName="target/exported/includes.xml")
    public void shouldExportDataSetUsingIncludes() {

    }

    @Test
    @DataSet("datasets/yml/user.yml")
    @ExportDataSet(format = DataSetFormat.XML, includeTables = "USER", dependentTables = true, outputName="target/exported/dependentTables.xml")
    public void shouldExportDataSetUsingIncludesWithDependentTables() {

    }

    /**
     * full dataset
     * <?xml version='1.0' encoding='UTF-8'?>
     <dataset>
     <USER ID="1" NAME="@realpestano"/>
     <USER ID="2" NAME="@dbunit"/>
     <FOLLOWER/>
     <SEQUENCE SEQ_NAME="SEQ_GEN" SEQ_COUNT="0"/>
     <TWEET/>
     </dataset>
     */
    @AfterClass
    public static void assertGeneratedDataSets(){
        File generatedXmlDataSet = new File("target/exported/generated.xml");
        assertThat(generatedXmlDataSet).exists();
        assertThat(contentOf(generatedXmlDataSet)).contains("<USER ID=\"1\" NAME=\"@realpestano\"/>");
        assertThat(contentOf(generatedXmlDataSet)).contains("<TWEET/>");
        assertThat(contentOf(generatedXmlDataSet)).contains("<USER ID=\"2\" NAME=\"@dbunit\"/>");
        assertThat(contentOf(generatedXmlDataSet)).contains("<FOLLOWER/>");

        generatedXmlDataSet.delete();

        File filterdXmlDataSet = new File("target/exported/filtered.xml");
        assertThat(filterdXmlDataSet).exists();
        assertThat(contentOf(filterdXmlDataSet)).contains("<USER ID=\"1\" NAME=\"@realpestano\"/>");
        assertThat(contentOf(filterdXmlDataSet)).doesNotContain("<USER ID=\"2\" NAME=\"@dbunit\"/>");
        assertThat(contentOf(filterdXmlDataSet)).doesNotContain("<TWEET/>");
        assertThat(contentOf(filterdXmlDataSet)).doesNotContain("<FOLLOWER/>");
        filterdXmlDataSet.delete();

        File includesXmlDataSet = new File("target/exported/includes.xml");
        assertThat(includesXmlDataSet).exists();
        assertThat(contentOf(includesXmlDataSet)).contains("<USER ID=\"1\" NAME=\"@realpestano\"/>");
        assertThat(contentOf(includesXmlDataSet)).contains("<USER ID=\"2\" NAME=\"@dbunit\"/>");
        assertThat(contentOf(includesXmlDataSet)).doesNotContain("<TWEET/>");
        assertThat(contentOf(includesXmlDataSet)).doesNotContain("<FOLLOWER/>");
        includesXmlDataSet.delete();

        File dependentTablesXmlDataSet = new File("target/exported/dependentTables.xml");
        assertThat(dependentTablesXmlDataSet).exists();
        assertThat(contentOf(dependentTablesXmlDataSet)).contains("<USER ID=\"1\" NAME=\"@realpestano\"/>");
        assertThat(contentOf(dependentTablesXmlDataSet)).contains("<USER ID=\"2\" NAME=\"@dbunit\"/>");
        assertThat(contentOf(dependentTablesXmlDataSet)).contains("<TWEET/>");
        assertThat(contentOf(dependentTablesXmlDataSet)).contains("<FOLLOWER/>");
        assertThat(contentOf(dependentTablesXmlDataSet)).contains("<FOLLOWER/>");

        dependentTablesXmlDataSet.delete();

    }
}
