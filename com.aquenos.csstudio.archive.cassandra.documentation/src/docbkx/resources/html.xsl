<xsl:stylesheet 
    xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
    xmlns:xslthl="http://xslthl.sf.net"
    exclude-result-prefixes="xslthl"
    version="1.0">

  <xsl:import href="urn:docbkx:stylesheet"/>
  <xsl:import href="urn:docbkx:stylesheet/highlight.xsl"/>
  
  <xsl:template match="xslthl:*" mode="xslthl">
    <span class="hl-{local-name()}"><xsl:apply-templates/></span>
  </xsl:template>
  
</xsl:stylesheet>
