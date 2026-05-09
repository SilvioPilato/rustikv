# Measure disk usage per payload for SPARSE_INDEX_INTERVAL=1
# Runs write-only phase and measures SSTable footprint

$rustikv = ".\target\release\rustikv.exe"
$kvbench  = ".\target\release\kvbench.exe"

$payloads = @(
    @{ label="100B";   count=10000; vsize=100      },
    @{ label="1KB";    count=10000; vsize=1024     },
    @{ label="10KB";   count=2000;  vsize=10240    },
    @{ label="100KB";  count=1000;  vsize=102400   },
    @{ label="1MB";    count=500;   vsize=1048576  }
)

foreach ($p in $payloads) {
    $dbDir = "C:\Temp\bench-si1-$($p.label)"
    if (Test-Path $dbDir) { Remove-Item -Recurse -Force $dbDir }
    New-Item -ItemType Directory -Path $dbDir | Out-Null

    # Start server
    $proc = Start-Process -FilePath $rustikv -ArgumentList "$dbDir --engine lsm --fsync-interval never -bc lz77" -PassThru -WindowStyle Hidden
    Start-Sleep -Seconds 1.5

    # Write-only: run kvbench, capture output
    $out = & $kvbench -n $p.count -s $p.vsize 2>&1

    # Disk size
    $bytes = (Get-ChildItem -Recurse -File $dbDir | Measure-Object -Property Length -Sum).Sum
    if ($null -eq $bytes) { $bytes = 0 }

    Write-Host "$($p.label): disk=$bytes"

    # Kill server
    Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue
    Start-Sleep -Seconds 2
}
