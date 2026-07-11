[CmdletBinding()]
param(
    [switch]$KeepTemp,
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$PytestArgs
)

$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$Python = if ($env:MERLIN_PYTHON) {
    $env:MERLIN_PYTHON
} else {
    "C:\Users\mt\Desktop\Strategy\S_Python\.venv\Scripts\python.exe"
}

$TempRoot = Join-Path $RepoRoot ".pytest_tmp"
$RunTemp = Join-Path $TempRoot ("run_{0}" -f $PID)
$exitCode = 1

New-Item -ItemType Directory -Force -Path $RunTemp | Out-Null

try {
    & $Python -m pytest --basetemp $RunTemp @PytestArgs
    $exitCode = $LASTEXITCODE
} finally {
    if (-not $KeepTemp -and (Test-Path -LiteralPath $RunTemp)) {
        Remove-Item -LiteralPath $RunTemp -Recurse -Force
    }
}

exit $exitCode
