$server = "sql.odsitar.app.dev.flightsafety.com"
$database = "SimulationsAnalytics"

$xmla = @"
<Batch xmlns="http://schemas.microsoft.com/analysisservices/2003/engine">
  <Process>
    <Object>
      <DatabaseID>$database</DatabaseID>
    </Object>
    <Type>ProcessFull</Type>
    <WriteBackTableCreation>UseExisting</WriteBackTableCreation>
  </Process>
</Batch>
"@

Invoke-ASCmd -Server $server -Query $xmla
