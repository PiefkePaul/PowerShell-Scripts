<#
.SYNOPSIS
    Sicheres SCVMM-Wartungsskript für Hyper-V-Hosts in Clustern (SCVMM 2022)
.DESCRIPTION
    Führt Wartung auf Hyper-V Hosts in einer SCVMM Umgebung durch, behandelt Cluster parallel und Hosts nacheinander.
    Stellt sicher, dass VMs migriert wurden, der Host keine Rollen mehr hat und korrekt rebootet ist.
#>

# Konfiguration: Hostgruppenfilterung
$IncludeHostGroups = @("HG-Produktiv", "West")
$ExcludeHostGroups = @("Alt", "Test")

$IncludeClusters   = @("CL-", "Cluster")
$ExcludeClusters   = @("TEST", "Archiv")

# SCVMM Verbindung
function GetVMMServerByUserDomain ($domain){
    Try
    {
        Switch ($domain) {
        "KNE." { $VMMServer = "CLL01." + $domain }
        "QS." { $VMMServer = "CLL01." + $domain }
        "PROD." { $VMMServer = "CLL01." + $domain }
        "ENTW." { $VMMServer = "CLL01." + $domain }
        } 
    }
    Catch
    {
        $ErrorMessage = $_.Exception.Message
        Write-Host "$ErrorMessage" -ForegroundColor Red
        Exit  
    }
    return $VMMServer
}

#Auslesen der Userdomäne
$domain = $env:USERDNSDOMAIN

#Aufruf der Funktion welche den SCVMM Server basierend auf der Domäne auswählt
$VMMServer = GetVMMServerByUserDomain $domain

# Ziel-Hosts aus SCVMM abrufen
$AllHosts = Get-SCVMHost -VMMServer $VMMServer | Where-Object {
    $_.HostCluster -ne $null -and
    ($IncludeHostGroups.Count -eq 0 -or ($IncludeHostGroups | Where-Object { $_ -and $_ -ne "" -and $_.ToLower() -in $_.HostGroupPath.ToLower() }) -ne $null) -and
    ($ExcludeHostGroups.Count -eq 0 -or ($ExcludeHostGroups | Where-Object { $_ -and $_ -ne "" -and $_.ToLower() -in $_.HostGroupPath.ToLower() }) -eq $null) -and
    ($IncludeClusters.Count -eq 0 -or ($IncludeClusters | Where-Object { $_ -and $_ -ne "" -and $_.ToLower() -in $_.HostCluster.Name.ToLower() }) -ne $null) -and
    ($ExcludeClusters.Count -eq 0 -or ($ExcludeClusters | Where-Object { $_ -and $_ -ne "" -and $_.ToLower() -in $_.HostCluster.Name.ToLower() }) -eq $null)
}

# Ziel-Hosts gruppieren nach Cluster
$GroupedClusters = $AllHosts | Group-Object { $_.HostCluster.Name } | Sort-Object Name

# Hilfsfunktion (innerhalb Job): Wartung für einen Host ausführen
$MaintenanceBlock = {
    param($ClusterName, $HostName, $Reboot)

    Import-Module FailoverClusters
    Import-Module VirtualMachineManager
    Import-Module Hyper-V

    function Check-VMStates {
        param ($NodeName)
        $vms = Get-VM -ComputerName $NodeName -ErrorAction Stop
        $allowed = 'Running','Off','Saved','Paused'
        foreach ($vm in $vms) {
            if ($allowed -notcontains $vm.State.ToString()) {
                throw "VM '$($vm.Name)' hat ungültigen Zustand: $($vm.State)"
            }
        }
    }

    $Cluster = Get-Cluster -Name $ClusterName -ErrorAction Stop
    $Nodes = $Cluster | Get-ClusterNode | Select-Object -ExpandProperty Name
    $ThisHost = $HostName
    $OtherNode = ($Nodes | Where-Object { $_ -ne $ThisHost })

    # 2-Knoten-Cluster prüfen
    if ($Nodes.Count -eq 2) {
        $OCNode = Get-ClusterNode -Name $OtherNode
        if ($OCNode.State -ne 'Paused') {
            throw "Anderer Knoten '$OtherNode' ist im Cluster nicht pausiert."
        }
        $OCHost = Get-SCVMHost -ComputerName $OtherNode
        if (-not $OCHost.MaintenanceMode) {
            throw "Anderer Knoten '$OtherNode' ist nicht im SCVMM-Wartungsmodus."
        }
    }

    # Vorprüfung VMs
    Check-VMStates -NodeName $ThisHost

    # Wartungsmodus aktivieren
    $VMHost = Get-SCVMHost -ComputerName $ThisHost -ErrorAction Stop
    Disable-SCVMHost -VMHost $VMHost -MoveWithinCluster -RunAsynchronously -JobVariable job -ErrorAction Stop
    while ((Get-SCJob -ID $job).Status -ne 'Completed') {
        Start-Sleep -Seconds 5
    }

    # Clusterrollen prüfen
    $ClusterNode = Get-ClusterNode -Name $ThisHost
    if ($ClusterNode | Get-ClusterGroup) {
        throw "Host '$ThisHost' hält noch Clustergruppen!"
    }
    if ((Get-ClusterSharedVolume -Cluster $ClusterName | Where-Object { $_.OwnerNode.Name -eq $ThisHost })) {
        throw "Host '$ThisHost' ist noch CSV-Owner!"
    }

    # VMs nach Wartungsmodus
    Check-VMStates -NodeName $ThisHost

    # Hauptaufgabe: Neustart (optional)
    if ($Reboot) {
        Restart-Computer -ComputerName $ThisHost -Force -ErrorAction Stop
        $timeout = 600; $elapsed = 0
        while ($elapsed -lt $timeout) {
            if (Test-Connection -ComputerName $ThisHost -Count 1 -Quiet) {
                break
            }
            Start-Sleep -Seconds 10
            $elapsed += 10
        }
        if ($elapsed -ge $timeout) {
            throw "Host '$ThisHost' nach 10 Minuten nicht erreichbar!"
        }
    }

    # Wartungsmodus beenden
    Enable-SCVMHost -VMHost $VMHost -ErrorAction Stop

    # Abschlussprüfung
    Check-VMStates -NodeName $ThisHost
    Write-Host "✔ Host '$ThisHost' im Cluster '$ClusterName' fertig."
}

# Jobs pro Cluster starten
$Jobs = @()
foreach ($group in $GroupedClusters) {
    $ClusterName = $group.Name
    $Hosts = $group.Group | Sort-Object Name

    $Jobs += Start-Job -ScriptBlock {
        param($ClusterName, $Hosts, $Block)
        foreach ($vhost in $Hosts) {
            & $Block.Invoke($ClusterName, $vhost.Name, $true)
        }
    } -ArgumentList $ClusterName, $Hosts, $MaintenanceBlock
}

# Warten auf Jobs
Write-Host "Warte auf Abschluss der Cluster-Jobs..."
while ($Jobs.State -contains 'Running') {
    Start-Sleep -Seconds 10
}

# Ausgabe Ergebnisse
foreach ($job in $Jobs) {
    Receive-Job -Job $job
}
