package de.tuberlin.dbpra.mapreduce;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;


public class Aufgabe1Test {

    @Test
    public void testA1() throws Exception {
        String input = "src/test/resources/input/btest.tbl";
        String output = "src/test/resources/output/aufgabe1.tbl";
        String expectedResult = "src/test/resources/expected_results/aufgabe1.tbl";

        File file = new File(output);
        if (file.exists()) {
            FileUtils.forceDelete(file);
        }
        Aufgabe1.main(new String[]{input, output});

        Assert.assertTrue(FileUtils.contentEquals(new File(output), new File(expectedResult)));
    }
}