param([switch]$EnableTestControls)

$ErrorActionPreference = "Stop"
$root = $PSScriptRoot
$python = Join-Path $root ".venv\Scripts\python.exe"
if (-not (Test-Path $python)) {
    throw "Crie o ambiente conforme o README antes de iniciar."
}
foreach ($port in 50051..50054) {
    if (Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue) {
        throw "Porta $port ocupada. Encerre o cluster existente antes de iniciar outro."
    }
}
$config = Join-Path $root "config.json"
$data = Join-Path $root "data"
foreach ($id in 1..4) {
    $arguments = "-u `"$root\node.py`" --id $id --config `"$config`" --data-dir `"$data`""
    if ($EnableTestControls) {
        $arguments += " --enable-test-controls"
    }
    $process = Start-Process -FilePath $python -ArgumentList $arguments -WorkingDirectory $root -PassThru
    Write-Host "No $id iniciado (PID=$($process.Id)). Feche a janela para simular queda."
}
Write-Host "Quatro nos iniciados. Use status no cliente; aguarde a eleicao."
& $python "$root\client.py" --config $config
