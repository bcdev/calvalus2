package bc.com.calvalus.ui.client;

import java.util.Date;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author ubits on 2/15/2017.
 */
public class JobTableViewTest {
    @Test
    public void testCool() throws Exception {


        Date date = new Date("2014/12/04");
        Date date1 = new Date("2014/12/4");

        System.out.println("date1.compareTo(date) = " + date1.compareTo(date));
    }
}