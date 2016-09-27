package com.bc.calvalus.wps.cmd;

import org.junit.*;

import java.util.ArrayList;
import java.util.List;

/**
 * @author hans
 */
public class LdapHelperTest {

    public static final String ALLOWABLE_GROUP = "calwps";

    @Test
    public void testRegex() throws Exception {
        String response = "uid=10223(tep_test123) gid=10118(calwps) groups=10118(calwps) ";
        List<String> outputList = new ArrayList<>();
        outputList.add(response);

        LdapHelper ldapHelper = new LdapHelper();
        List<String> groupList = ldapHelper.parseLdapIdResponse(outputList);

        for (String group : groupList) {
            System.out.println("group.equals(\"calwps\") = " + group.equals("calwps"));
            System.out.println("\"calwps\".equals(group) = " + ALLOWABLE_GROUP.equalsIgnoreCase(group));
            System.out.println("group = " + group);
        }
    }
}