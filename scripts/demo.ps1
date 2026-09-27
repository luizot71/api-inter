$ErrorActionPreference = 'Stop'
$base = 'http://localhost:8080'
$noteId = [guid]::NewGuid().ToString()
$body = @{ id = $noteId; content = 'Preparar entrevista Inter' } | ConvertTo-Json
$created = Invoke-WebRequest -Method Post -Uri "$base/notes" -ContentType 'application/json' -Body $body
Write-Host "Criacao: $($created.StatusCode) (esperado 201)"
Invoke-RestMethod -Uri "$base/notes/$noteId" | Format-List
$replay = Invoke-WebRequest -Method Post -Uri "$base/notes" -ContentType 'application/json' -Body $body
Write-Host "Repeticao: $($replay.StatusCode) (esperado 200)"
Write-Host "ID para consultas: $noteId"
