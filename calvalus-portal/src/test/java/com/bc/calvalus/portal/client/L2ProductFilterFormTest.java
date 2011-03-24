package com.bc.calvalus.portal.client;

import com.google.gwt.junit.client.GWTTestCase;

import java.util.Date;


public class L2ProductFilterFormTest extends GWTTestCase {
    public String getModuleName() {
        return "com.bc.calvalus.portal.CalvalusPortalJUnit";
    }

    public void testMaxDaysValidation() {
        L2ProductFilterForm l2ProductFilterForm = new L2ProductFilterForm();

        try {
            l2ProductFilterForm.validateForm(3);
        } catch (ValidationException e) {
        }

        Date date1, date2;

        date1 = new Date();
        date2 = new Date(date1.getTime() + 3 * L2ProductFilterForm.MILLIS_PER_DAY);
        l2ProductFilterForm.getMinDate().setValue(date1);
        l2ProductFilterForm.getMaxDate().setValue(date2);
        try {
            l2ProductFilterForm.validateForm(3);
        } catch (ValidationException e) {
            fail("ValidationException not expected: " + e.getMessage());
        }

        date1 = new Date();
        date2 = new Date(date1.getTime() + 5 * L2ProductFilterForm.MILLIS_PER_DAY + 1);
        l2ProductFilterForm.getMinDate().setValue(date1);
        l2ProductFilterForm.getMaxDate().setValue(date2);
        try {
            l2ProductFilterForm.validateForm(3);
            fail("ValidationException expected");
        } catch (ValidationException e) {
            // expected
        }
    }

    public void testDateMinMaxValidation() {
        L2ProductFilterForm l2ProductFilterForm = new L2ProductFilterForm();

        Date date1, date2;

        date1 = new Date();
        date2 = new Date(date1.getTime() - L2ProductFilterForm.MILLIS_PER_DAY);
        l2ProductFilterForm.getMinDate().setValue(date1);
        l2ProductFilterForm.getMaxDate().setValue(date2);
        try {
            l2ProductFilterForm.validateForm(3);
            fail("ValidationException expected");
        } catch (ValidationException e) {
            // expected
        }
    }

}
