<?xml version="1.0"?>

<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                version="2.0">

    <xsl:output method="html" encoding="UTF-8" indent="no"/>

    <xsl:template match="listing">
        <html>
            <head>
                <title>Calvalus Staging</title>
                <link type="text/css" rel="stylesheet" href="/calvalus/calvalus.css"/>
                <meta http-equiv="content-type" content="text/html; charset=UTF-8"/>
                <style>
                    body {
                    font-family : "Lato", "Helvetica Neue", Helvetica, Arial, sans-serif;
                    color : black;
                    background-color : white;
                    margin: 0;
                    }
                    b {
                    color : white;
                    background-color : #0086b2;
                    }
                    a {
                    color : black;
                    }
                    .header-panel {
                    background-color: #373B50;
                    height: 80px;
                    display: flex;
                    flex-direction: row;
                    }

                    .header-logo {
                    flex: 0 0 200px;
                    margin: 15px 0 0 0;
                    text-align: center;
                    }

                    .header-title {
                    width: 100%;
                    padding-left: 15px;
                    color: white;
                    font-size: 32pt;
                    font-weight: bold;
                    margin: auto 0;
                    }

                    .footer {
                    background-color: #333;
                    display: flex;
                    flex-direction: row;
                    color: white;
                    height: 40px;
                    padding: 0 10px;
                    margin-top: 20px;
                    }

                    .footer-copyright-legal-privacy {
                    flex: 0 0 50%;
                    display: flex;
                    }

                    .footer-copyright {
                    margin: auto;
                    flex: 0 0 230px;
                    }

                    .footer-legal {
                    margin: auto;
                    text-align: center;
                    flex: 0 0 100px;
                    }

                    .footer-privacy {
                    margin: auto;
                    text-align: center;
                    flex: 0 0 130px;
                    }

                    .footer-empty {
                    width: 100%;
                    }

                    .copyright-text {
                    font-family: "Lato", "Helvetica Neue", Helvetica, Arial, sans-serif;
                    font-size: 14px;
                    font-weight: 700;
                    color: white;
                    }

                    .legal-privacy-text {
                    font-family: "Lato", "Helvetica Neue", Helvetica, Arial, sans-serif;
                    font-size: 14px;
                    font-weight: 700;
                    color: white;
                    }

                    .legal-privacy-text:hover {
                    color: #7ac6cf;
                    }

                    .directory-name {
                    font-weight: 700;
                    }

                    .staging-area {
                    margin: 10px;
                    }
                </style>
            </head>
            <body>
                <div class="header-panel">
                    <div class="header-logo">
                        <img src="../../../images/0_maaamet_vapp_eng_78px_resize.png" alt="Maamet logo"/>
                    </div>
                    <div class="header-title">
                        Processing Service - staging area
                    </div>
                </div>

                <p class="staging-area">Staging area
                    <span class="directory-name">
                        <xsl:value-of select="@directory"/>
                    </span>
                </p>

                <hr/>
                <table cellspacing="0"
                       width="100%"
                       cellpadding="5"
                       align="center"
                       padding="6px"
                >
                    <tr>
                        <th align="left">Filename</th>
                        <th align="center">Size</th>
                        <th align="right">Last Modified</th>
                    </tr>
                    <xsl:apply-templates select="entries"/>
                </table>
                <xsl:apply-templates select="readme"/>
                <div class="footer">
                    <div class="footer-copyright">
                        <span class="copyright-text">Copyright &#169; 2018 ESTHub</span>
                    </div>
                    <div class="footer-legal">
                        <a href="https://esthub.maamet.ee/en/legal_notice" target="_blank" style="text-decoration:none;">
                            <span class="legal-privacy-text">Legal Notice</span>
                        </a>
                    </div>
                    <div class="footer-privacy">
                        <a href="https://cesthub.maamet.ee/en/data_protection" target="_blank" style="text-decoration:none;">
                            <span class="legal-privacy-text">Privacy Statement</span>
                        </a>
                    </div>
                    <div class="footer-empty"/>
                </div>
            </body>
        </html>
    </xsl:template>


    <xsl:template match="entries">
        <xsl:apply-templates select="entry"/>
    </xsl:template>

    <xsl:template match="readme">
        <hr size="1"/>
        <pre>
            <xsl:apply-templates/>
        </pre>
    </xsl:template>

    <xsl:template match="entry">
        <tr>
            <td align="left">
                <xsl:variable name="urlPath" select="@urlPath"/>
                <a href="{$urlPath}">
                    <tt>
                        <xsl:apply-templates/>
                    </tt>
                </a>
            </td>
            <td align="right">
                <tt>
                    <xsl:value-of select="@size"/>
                </tt>
            </td>
            <td align="right">
                <tt>
                    <xsl:value-of select="@date"/>
                </tt>
            </td>
        </tr>
    </xsl:template>

</xsl:stylesheet>
