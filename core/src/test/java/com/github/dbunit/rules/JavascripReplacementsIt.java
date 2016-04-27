package com.github.dbunit.rules;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Calendar;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.github.dbunit.rules.api.dataset.DataSet;
import com.github.dbunit.rules.model.Tweet;

/**
 * Created by pestano on 15/02/16.
 */
public class JavascripReplacementsIt {

    Calendar now;

    @Rule
    public EntityManagerProvider emProvider = EntityManagerProvider.instance("scripts-it");

    @Rule
    public DBUnitRule dbUnitRule = DBUnitRule.instance("scripts-it",emProvider.getConnection());

    @Before
    public void setup(){
        now = Calendar.getInstance();
    }

    @Test
    @DataSet(value = "datasets/yml/js-with-date-replacements.yml",disableConstraints = true, executorId = "scripts-it")
    public void shouldReplaceDateUsingJavaScriptInDataset() {
        Tweet tweet = (Tweet) emProvider.em().createQuery("select t from Tweet t where t.id = '1'").getSingleResult();
        assertThat(tweet).isNotNull();
        assertThat(tweet.getDate().get(Calendar.DAY_OF_MONTH)).isEqualTo(now.get(Calendar.DAY_OF_MONTH));
        assertThat(tweet.getDate().get(Calendar.HOUR_OF_DAY)).isEqualTo(now.get(Calendar.HOUR_OF_DAY));
    }

    @Test
    @DataSet(value = "datasets/yml/js-with-calc-replacements.yml",disableConstraints = true, executorId = "scripts-it")
    public void shouldReplaceLikesUsingJavaScriptInDataset() {
        Tweet tweet = (Tweet) emProvider.em().createQuery("select t from Tweet t where t.id = '1'").getSingleResult();
        assertThat(tweet).isNotNull();
        assertThat(tweet.getLikes()).isEqualTo(50);
    }


}
