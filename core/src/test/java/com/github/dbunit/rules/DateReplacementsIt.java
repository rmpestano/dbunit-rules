package com.github.dbunit.rules;

import com.github.dbunit.rules.api.dataset.DataSet;
import com.github.dbunit.rules.model.Tweet;
import com.github.dbunit.rules.model.User;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Calendar;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by pestano on 15/02/16.
 */
public class DateReplacementsIt {

    Calendar now;

    @Rule
    public EntityManagerProvider emProvider = EntityManagerProvider.instance("rules-it");

    @Rule
    public DBUnitRule dbUnitRule = DBUnitRule.instance(emProvider.getConnection());

    @Before
    public void setup(){
        now = Calendar.getInstance();
    }

    @Test
    @DataSet(value = "datasets/yml/date-replacements.yml",disableConstraints = true)
    public void shouldReplaceDateWithNowPlaceHolder() {
        Tweet tweet = (Tweet) emProvider.em().createQuery("select t from Tweet t where t.id = '1'").getSingleResult();
        assertThat(tweet).isNotNull();
        assertThat(tweet.getDate().get(Calendar.DAY_OF_MONTH)).isEqualTo(now.get(Calendar.DAY_OF_MONTH));
        assertThat(tweet.getDate().get(Calendar.HOUR_OF_DAY)).isEqualTo(now.get(Calendar.HOUR_OF_DAY));
    }

    @Test
    @DataSet(value = "datasets/yml/date-replacements.yml",disableConstraints = true)
    public void shouldReplaceDateWithYesterdayPlaceHolder() {
        Tweet tweet = (Tweet) emProvider.em().createQuery("select t from Tweet t where t.id = '2'").getSingleResult();
        assertThat(tweet).isNotNull();
        assertThat(tweet.getDate().get(Calendar.DAY_OF_MONTH)+1).isEqualTo(now.get(Calendar.DAY_OF_MONTH));
    }

    @Test
    @DataSet(value = "datasets/yml/date-replacements.yml",disableConstraints = true)
    public void shouldReplaceDateWithTomorrowPlaceHolder() {
        Tweet tweet = (Tweet) emProvider.em().createQuery("select t from Tweet t where t.id = '3'").getSingleResult();
        assertThat(tweet).isNotNull();
        assertThat(tweet.getDate().get(Calendar.DAY_OF_MONTH)-1).isEqualTo(now.get(Calendar.DAY_OF_MONTH));
    }

    @Test
    @DataSet(value = "datasets/yml/date-replacements.yml",disableConstraints = true)
    public void shouldReplaceDateWithYearAfterPlaceHolder() {
        Tweet tweet = (Tweet) emProvider.em().createQuery("select t from Tweet t where t.id = '4'").getSingleResult();
        assertThat(tweet).isNotNull();
        assertThat(tweet.getDate().get(Calendar.YEAR)-1).isEqualTo(now.get(Calendar.YEAR));
    }

    @Test
    @DataSet(value = "datasets/yml/date-replacements.yml",disableConstraints = true)
    public void shouldReplaceDateWithYearBeforePlaceHolder() {
        Tweet tweet = (Tweet) emProvider.em().createQuery("select t from Tweet t where t.id = '5'").getSingleResult();
        assertThat(tweet).isNotNull();
        assertThat(tweet.getDate().get(Calendar.YEAR)+1).isEqualTo(now.get(Calendar.YEAR));
    }

    @Test
    @DataSet(value = "datasets/yml/date-replacements.yml",disableConstraints = true)
    public void shouldReplaceDateWithHourPlaceHolder() {
        Tweet tweet = (Tweet) emProvider.em().createQuery("select t from Tweet t where t.id = '6'").getSingleResult();
        assertThat(tweet).isNotNull();
        tweet.getDate().add(Calendar.HOUR_OF_DAY,-1);
        assertThat(tweet.getDate().get(Calendar.HOUR_OF_DAY)).isEqualTo(now.get(Calendar.HOUR_OF_DAY));
    }
}
