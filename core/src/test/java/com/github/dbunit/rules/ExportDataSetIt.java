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
    @ExportDataSet(format = DataSetFormat.XML,outputName="target/exported.xml")
    public void shouldExportDataSetAfterTestExecution() {
    }


    /**
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
        File xmlDataSet = new File("target/exported.xml");
        assertThat(xmlDataSet).exists();
        assertThat(contentOf(xmlDataSet)).contains("<USER ID=\"1\" NAME=\"@realpestano\"/>");
        assertThat(contentOf(xmlDataSet)).contains("<USER ID=\"2\" NAME=\"@dbunit\"/>");
    }
}
