<?xml version="1.0"?>

<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
  version="1.0">

  <xsl:output method="html" encoding="iso-8859-1" indent="no"/>

  <xsl:template match="listing">
   <html>
    <head>
      <title>
	Genie Job Results
       <xsl:value-of select="@directory"/>
      </title>
      <style>
        h3{color : black;background-color : white;}
        body{font-family : sans-serif,Arial,Tahoma;
             color : black;background-color : white;}
	h4{font-family : sans-serif,Arial,Tahoma;
	     color : white;background-color : #B9090B;}
      </style>
    </head>
    <body>
      <h4><img src="/img/nf_logo.png" alt="Netflix" border="0"/></h4>
      <h3>Genie Job Results: <xsl:value-of select="@directory"/></h3>
      <hr/>
      <table cellspacing="0"
                  width="100%"
            cellpadding="5"
                  align="center">
        <tr>
          <th align="left">Filename</th>
          <th align="right">Size</th>
          <th align="right">Last Modified</th>
        </tr>
        <xsl:apply-templates select="entries"/>
        </table>
      <xsl:apply-templates select="readme"/>
      <hr/>
    </body>
   </html>
  </xsl:template>


  <xsl:template match="entries">
    <xsl:apply-templates select="entry"/>
  </xsl:template>

  <xsl:template match="readme">
    <hr size="1" />
    <pre><xsl:apply-templates/></pre>
  </xsl:template>

  <xsl:template match="entry">
    <tr>
      <td align="left">
        <xsl:variable name="urlPath" select="@urlPath"/>
        <a href="{$urlPath}">
          <tt><xsl:apply-templates/></tt>
        </a>
      </td>
      <td align="right">
        <tt><xsl:value-of select="@size"/></tt>
      </td>
      <td align="right">
        <tt><xsl:value-of select="@date"/></tt>
      </td>
    </tr>
  </xsl:template>

</xsl:stylesheet>

