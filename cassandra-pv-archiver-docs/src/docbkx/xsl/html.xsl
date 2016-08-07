<?xml version="1.0" encoding="utf-8" ?>
<xsl:stylesheet
  xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
  xmlns:xslthl="http://xslthl.sf.net"
  exclude-result-prefixes="xslthl"
  version="1.0">

  <xsl:import href="urn:docbkx:stylesheet"/>
  <xsl:import href="urn:docbkx:stylesheet/highlight.xsl"/>

  <xsl:template match="xslthl:*" mode="xslthl">
    <span>
      <xsl:attribute name="class">
        <xsl:text>hl-</xsl:text>
        <xsl:value-of select="local-name()"/>
      </xsl:attribute>
      <xsl:apply-templates mode="xslthl"/>
    </span>
  </xsl:template>

</xsl:stylesheet>
