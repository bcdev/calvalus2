package com.bc.calvalus.processing.utils;

import org.joda.time.DateTime;
import org.junit.Test;
import org.opensaml.saml2.core.Assertion;
import org.opensaml.saml2.core.Response;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class SamlUtilTest
{
    static String UNENCRYPTED_ASSERTION =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
        "<saml2:Assertion Version=\"2.0\" xmlns:saml2=\"urn:oasis:names:tc:SAML:2.0:assertion\">\n" +
        "    <saml2:Issuer>cas</saml2:Issuer>\n" +
        "    <saml2:Subject>\n" +
        "        <saml2:NameID>cd_calvalus</saml2:NameID>\n" +
        "    </saml2:Subject>\n" +
        "    <saml2:Conditions NotBefore=\"2017-06-23T10:27:05.354Z\" NotOnOrAfter=\"2017-06-24T10:27:05.354Z\"/>\n" +
        "    <saml2:AuthnStatement>\n" +
        "        <saml2:AuthnContext>\n" +
        "            <saml2:AuthnContextClassRef>urn:oasis:names:tc:SAML:2.0:ac:classes:Password</saml2:AuthnContextClassRef>\n" +
        "        </saml2:AuthnContext>\n" +
        "    </saml2:AuthnStatement>\n" +
        "    <saml2:AttributeStatement>\n" +
        "        <saml2:Attribute Name=\"groups\">\n" +
        "            <saml2:AttributeValue\n" +
        "                xmlns:xs=\"http://www.w3.org/2001/XMLSchema\"\n" +
        "                xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"xs:string\">calvalus,testproject</saml2:AttributeValue>\n" +
        "        </saml2:Attribute>\n" +
        "        <saml2:Attribute Name=\"email\">\n" +
        "            <saml2:AttributeValue\n" +
        "                xmlns:xs=\"http://www.w3.org/2001/XMLSchema\"\n" +
        "                xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"xs:string\">calvalustest@code.de</saml2:AttributeValue>\n" +
        "        </saml2:Attribute>\n" +
        "    </saml2:AttributeStatement>\n" +
        "</saml2:Assertion>\n";

    @Test
    public void createUnencryptedSamlToken() throws Exception {
        String issuer = "cas";
        String subject = "cd_calvalus";
        Map<String,String> attributes = new HashMap<>();
        attributes.put("groups", "calvalus,testproject");
        attributes.put("email", "calvalustest@code.de");
        int timeoutSeconds = 60*60*24;

        SamlUtil util = new SamlUtil();
        Response response = util.build(issuer, subject, attributes, new DateTime("2017-06-23T10:27:05.354Z"), timeoutSeconds);
        String formattedAssertion = util.pp(response);

        assertEquals("SAML token differs", UNENCRYPTED_ASSERTION, formattedAssertion);
    }
}
