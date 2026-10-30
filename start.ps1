param([int]$Port = 18080)
$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $PSScriptRoot
$java = if ($env:JAVA_HOME -and (Test-Path -LiteralPath (Join-Path $env:JAVA_HOME 'bin\java.exe'))) {
    Join-Path $env:JAVA_HOME 'bin\java.exe'
} else {
    (Get-Command java -ErrorAction Stop).Source
}
if (-not (Test-Path -LiteralPath 'target\recoverable-pipeline-0.1.0.jar')) {
    & mvn -B -ntp -gs settings-local.xml -s settings-local.xml '-DskipTests=true' package
    if ($LASTEXITCODE -ne 0) { throw 'Build failed. Check JDK 17+, Maven and network settings.' }
}
Write-Host "Open http://127.0.0.1:$Port . Press Ctrl+C to stop."
& $java -jar target/recoverable-pipeline-0.1.0.jar "--server.port=$Port"
