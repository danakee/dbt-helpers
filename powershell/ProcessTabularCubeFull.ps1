Import-Module SqlServer

$server = "sql.odsitar.app.dev.flightsafety.com"

$xmla = @"
<Execute xmlns="urn:schemas-microsoft-com:xml-analysis">
  <Command>
    <Process xmlns="http://schemas.microsoft.com/analysisservices/2014/engine">
      <Object>
        <DatabaseID>SimulationsAnalytics</DatabaseID>
      </Object>
      <Type>ProcessFull</Type>
    </Process>
  </Command>
</Execute>
"@

Invoke-ASCmd -Server $server -Query $xmla -Verbose

