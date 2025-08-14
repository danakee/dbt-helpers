$serverA = "ProdServer";   $db = "OperationsAnalyticsStage"
$serverB = "QAserver"
$serverC = "DevServer"

$base    = "C:\Diffs"
$filter  = "C:\Filters\ProcsOnly.scpf"  # saved from GUI to include only stored procedures

& "C:\Program Files (x86)\Red Gate\SQL Compare 15\sqlcompare.exe" `
  /server1:$serverA /db1:$db /server2:$serverB /db2:$db `
  /filter:$filter /report:"$base\A_vs_B.html" /reportType:Html `
  /Options:IgnoreComments,IgnoreWhitespace

& "C:\Program Files (x86)\Red Gate\SQL Compare 15\sqlcompare.exe" `
  /server1:$serverA /db1:$db /server2:$serverC /db2:$db `
  /filter:$filter /report:"$base\A_vs_C.html" /reportType:Html `
  /Options:IgnoreComments,IgnoreWhitespace

& "C:\Program Files (x86)\Red Gate\SQL Compare 15\sqlcompare.exe" `
  /server1:$serverB /db1:$db /server2:$serverC /db2:$db `
  /filter:$filter /report:"$base\B_vs_C.html" /reportType:Html `
  /Options:IgnoreComments,IgnoreWhitespace