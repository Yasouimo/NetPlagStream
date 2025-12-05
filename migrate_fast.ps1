# Script de migration ULTRA-RAPIDE via Docker avec volume monte
Write-Host "=== MIGRATION ULTRA-RAPIDE VERS HDFS ===" -ForegroundColor Cyan

$LOCAL_DATA = "D:\work_and_study\NetPlag\data"
$LOCAL_STORAGE = "D:\work_and_study\NetPlag\storage"

# Verifier que Docker est en cours d'execution
$containers = docker ps --filter "name=hadoop-namenode" --format "{{.Names}}"
if (-not $containers) {
    Write-Host "[ERROR] HDFS n'est pas demarre. Lancez: docker-compose up -d" -ForegroundColor Red
    exit 1
}

Write-Host "[OK] HDFS est demarre" -ForegroundColor Green

# Methode ULTRA-RAPIDE: utiliser un conteneur temporaire avec volume monte
Write-Host "`n1. Migration ULTRA-RAPIDE du corpus initial..." -ForegroundColor Yellow

# Compter les fichiers
$corpusFiles = Get-ChildItem -Path "$LOCAL_DATA\corpus_initial\*.txt"
$total = $corpusFiles.Count
Write-Host "  Total fichiers a migrer: $total" -ForegroundColor Gray

# Methode OPTIMALE: utiliser Docker Desktop volume mount
# Docker Desktop monte automatiquement les volumes Windows
# Convertir le chemin Windows en chemin Docker
$winPath = $LOCAL_DATA.Replace('\', '/')
# Pour Docker Desktop sur Windows, utiliser le format /d/path
if ($winPath -match '^([A-Z]):') {
    $drive = $matches[1].ToLower()
    $dockerPath = "/$drive" + ($winPath -replace '^[A-Z]:', '')
} else {
    $dockerPath = $winPath
}

Write-Host "  Utilisation du volume monte pour transfert direct..." -ForegroundColor Gray
Write-Host "  Chemin source: $dockerPath/corpus_initial" -ForegroundColor Gray

# Utiliser un conteneur temporaire avec le volume monte et transfert direct
# Cette methode est BEAUCOUP plus rapide car elle evite docker cp
docker run --rm `
    --network netplag_hadoop-net `
    -v "${LOCAL_DATA}:/data:ro" `
    bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8 `
    bash -c "hdfs dfs -copyFromLocal /data/corpus_initial/*.txt /netplag/data/corpus_initial/" 2>&1 | Out-Null

if ($LASTEXITCODE -eq 0) {
    Write-Host "[OK] Transfert termine!" -ForegroundColor Green
} else {
    Write-Host "[WARN] Erreur lors du transfert direct, utilisation de la methode batch..." -ForegroundColor Yellow
    
    # Methode alternative: batch avec tar (plus rapide que fichier par fichier)
    $batchSize = 500  # Augmenter la taille du batch
    $count = 0
    $batchNum = 0
    $originalLocation = Get-Location
    
    try {
        Set-Location "$LOCAL_DATA\corpus_initial"
        
        for ($i = 0; $i -lt $total; $i += $batchSize) {
            $batchNum++
            $batch = $corpusFiles[$i..([Math]::Min($i + $batchSize - 1, $total - 1))]
            Write-Host "  Batch $batchNum/$([Math]::Ceiling($total / $batchSize)): $($batch.Count) fichiers..." -ForegroundColor Gray
            
            # Creer archive tar
            $tempTar = "$env:TEMP\batch_$batchNum.tar"
            $batchNames = $batch | ForEach-Object { $_.Name }
            & tar -cf $tempTar $batchNames 2>&1 | Out-Null
            
            # Copier et extraire dans le conteneur
            docker cp $tempTar hadoop-namenode:/tmp/batch.tar 2>&1 | Out-Null
            docker exec hadoop-namenode bash -c "mkdir -p /tmp/batch_extract && tar -xf /tmp/batch.tar -C /tmp/batch_extract && hdfs dfs -put /tmp/batch_extract/*.txt /netplag/data/corpus_initial/ && rm -rf /tmp/batch.tar /tmp/batch_extract" 2>&1 | Out-Null
            
            Remove-Item $tempTar -ErrorAction SilentlyContinue
            $count += $batch.Count
            Write-Host "  [OK] $count/$total fichiers migres" -ForegroundColor Green
        }
    } finally {
        Set-Location $originalLocation
    }
}

# Verification
Write-Host "`n=== VERIFICATION ===" -ForegroundColor Cyan
$result = docker exec hadoop-namenode hdfs dfs -count /netplag/data/corpus_initial 2>&1
Write-Host "Resultat: $result" -ForegroundColor Green

$fileCount = docker exec hadoop-namenode hdfs dfs -ls /netplag/data/corpus_initial 2>&1 | Select-String ".txt" | Measure-Object | Select-Object -ExpandProperty Count
Write-Host "Fichiers migres: $fileCount/$total" -ForegroundColor Green

Write-Host "`n[OK] Migration terminee!" -ForegroundColor Green
