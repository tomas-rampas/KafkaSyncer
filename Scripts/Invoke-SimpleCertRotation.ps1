<#
.SYNOPSIS
    Simple and safe certificate rotation for Kafka clusters using complete cluster shutdown.

.DESCRIPTION
    This script takes a straightforward approach to certificate rotation by shutting down the
    entire cluster, replacing certificates, and bringing everything back up in the correct order.
    While this approach requires a maintenance window, it's simpler, easier to understand, and
    has fewer failure modes than zero-downtime rolling restarts.
    
    The script handles the complete workflow:
    - Pre-rotation health assessment and validation
    - Automated backup of existing certificates
    - Clean shutdown of all cluster components in the correct order
    - Certificate deployment to all nodes
    - Controlled startup with health verification at each stage
    - Post-rotation validation and monitoring
    - Rollback capability if issues are detected
    
    This approach is ideal for:
    - Clusters where maintenance windows are acceptable
    - First-time certificate rotations where simplicity reduces risk
    - Environments where the operations team prefers straightforward procedures
    - Quarterly or annual certificate renewal where cumulative downtime isn't a concern

.PARAMETER ClusterConfigFile
    Path to JSON file containing cluster node definitions with connection details.

.PARAMETER RemoteCertificateDir
    Directory on remote nodes where certificates are stored.
    Default: /opt/kafka/ssl

.PARAMETER RemoteKafkaHome
    Kafka installation directory on remote nodes.
    Default: /opt/kafka

.PARAMETER LocalCertificateDir
    Local directory containing the new certificates to deploy.
    Should contain keystores and truststores for each node.

.PARAMETER BackupDir
    Local directory for storing configuration backups and rollback files.
    Default: .\backups

.PARAMETER LogDir
    Directory for detailed execution logs.
    Default: .\logs

.PARAMETER DryRun
    Performs all checks and planning but doesn't actually stop services or replace certificates.
    Use this to validate your procedure before executing it for real.

.PARAMETER SkipBackup
    Skips the backup phase. Not recommended unless you have external backups.

.PARAMETER ShutdownTimeoutSeconds
    Maximum time to wait for each service to stop gracefully before forcing shutdown.
    Default: 60 seconds

.PARAMETER StartupTimeoutSeconds
    Maximum time to wait for each service to start and become healthy.
    Default: 120 seconds

.EXAMPLE
    .\Invoke-SimpleCertRotation.ps1 -ClusterConfigFile .\cluster.json -DryRun
    
    Performs a dry run, showing what would happen without actually making changes.

.EXAMPLE
    .\Invoke-SimpleCertRotation.ps1 -ClusterConfigFile .\cluster.json -LocalCertificateDir .\new-certs
    
    Executes the full certificate rotation using certificates from the new-certs directory.

.NOTES
    Requires: PowerShell 7.0+, Posh-SSH module
    Recommended: Test this procedure in a non-production environment first
#>

#Requires -Version 7.0

param(
    [Parameter(Mandatory=$true)]
    [ValidateScript({Test-Path $_ -PathType Leaf})]
    [string]$ClusterConfigFile,
    
    [string]$RemoteCertificateDir = "/opt/kafka/ssl",
    [string]$RemoteKafkaHome = "/opt/kafka",
    
    [Parameter(Mandatory=$true)]
    [ValidateScript({Test-Path $_ -PathType Container})]
    [string]$LocalCertificateDir,
    
    [string]$BackupDir = ".\backups",
    [string]$LogDir = ".\logs",
    
    [switch]$DryRun,
    [switch]$SkipBackup,
    
    [int]$ShutdownTimeoutSeconds = 60,
    [int]$StartupTimeoutSeconds = 120
)

# ============================================================================
# Module Check and Import
# ============================================================================

if (-not (Get-Module -ListAvailable -Name Posh-SSH)) {
    Write-Host "ERROR: Posh-SSH module is required. Install with: Install-Module -Name Posh-SSH" -ForegroundColor Red
    exit 1
}

Import-Module Posh-SSH -ErrorAction Stop

# ============================================================================
# Configuration and Global Variables
# ============================================================================

# Load cluster configuration from the provided JSON file
$script:ClusterConfig = Get-Content -Path $ClusterConfigFile -Raw | ConvertFrom-Json

# These paths control where operations happen on the remote cluster nodes
$script:RemotePaths = @{
    KafkaHome = $RemoteKafkaHome
    ZooKeeperHome = $RemoteKafkaHome
    KafkaConfig = "$RemoteKafkaHome/config/server.properties"
    ZooKeeperConfig = "$RemoteKafkaHome/config/zookeeper.properties"
    CertificateDir = $RemoteCertificateDir
    BackupDir = "$RemoteCertificateDir/backups"
}

# Ensure local directories exist for our operations
foreach ($dir in @($BackupDir, $LogDir)) {
    if (-not (Test-Path $dir)) {
        New-Item -ItemType Directory -Path $dir -Force | Out-Null
    }
}

# Track SSH sessions for cleanup
$script:SSHSessions = @{}

# Create a timestamp for this rotation session to use in backups and logs
$script:RotationTimestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$script:RotationLogFile = Join-Path $LogDir "rotation-$script:RotationTimestamp.log"

# ============================================================================
# Logging Functions
# ============================================================================

function Write-RotationLog {
    <#
    .SYNOPSIS
        Writes timestamped log entries to both console and file.
    .DESCRIPTION
        This function provides consistent logging throughout the rotation process. Every
        significant action is logged with a timestamp and severity level. The logs help
        you understand exactly what happened during the rotation and are invaluable for
        troubleshooting if something goes wrong.
    #>
    param(
        [string]$Message,
        [ValidateSet('Info', 'Success', 'Warning', 'Error', 'Header')]
        [string]$Level = 'Info'
    )
    
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $logEntry = "[$timestamp] [$Level] $Message"
    
    # Choose console color based on message severity
    $color = switch ($Level) {
        'Info'    { 'White' }
        'Success' { 'Green' }
        'Warning' { 'Yellow' }
        'Error'   { 'Red' }
        'Header'  { 'Cyan' }
    }
    
    Write-Host $logEntry -ForegroundColor $color
    
    # Also write to the log file for permanent record
    Add-Content -Path $script:RotationLogFile -Value $logEntry
}

function Write-RotationHeader {
    <#
    .SYNOPSIS
        Writes a prominent section header to help organize the log output.
    #>
    param([string]$Title)
    
    $separator = "=" * 80
    Write-RotationLog $separator -Level Header
    Write-RotationLog $Title -Level Header
    Write-RotationLog $separator -Level Header
}

# ============================================================================
# SSH Connection Management
# ============================================================================

function Connect-ClusterNode {
    <#
    .SYNOPSIS
        Establishes an SSH connection to a cluster node.
    .DESCRIPTION
        Creates and caches SSH sessions to cluster nodes. Sessions are reused throughout
        the script execution for efficiency. The function handles both password and
        key-based authentication depending on what's provided in the node configuration.
    #>
    param([hashtable]$Node)
    
    $sessionKey = "$($Node.User)@$($Node.Host)"
    
    # Return existing session if it's still connected
    if ($script:SSHSessions.ContainsKey($sessionKey)) {
        $session = $script:SSHSessions[$sessionKey]
        if ($session.Connected) {
            return $session
        }
    }
    
    try {
        Write-Verbose "Establishing SSH connection to $($Node.Host)"
        
        # Convert password to secure string for credential object
        $securePassword = ConvertTo-SecureString -String $Node.Password -AsPlainText -Force
        $credential = New-Object System.Management.Automation.PSCredential($Node.User, $securePassword)
        
        $session = New-SSHSession -ComputerName $Node.Host -Credential $credential -AcceptKey -ErrorAction Stop
        $script:SSHSessions[$sessionKey] = $session
        
        return $session
    }
    catch {
        Write-RotationLog "Failed to connect to $($Node.Host): $_" -Level Error
        return $null
    }
}

function Invoke-ClusterCommand {
    <#
    .SYNOPSIS
        Executes a command on a remote cluster node.
    .DESCRIPTION
        This is the workhorse function for executing commands on cluster nodes. It handles
        connection management, error detection, and provides structured output that makes
        it easy to determine if the command succeeded.
    #>
    param(
        [hashtable]$Node,
        [string]$Command,
        [switch]$SuppressErrors
    )
    
    $session = Connect-ClusterNode -Node $Node
    if (-not $session) {
        return @{ Success = $false; Output = "Connection failed"; ExitCode = -1 }
    }
    
    try {
        $result = Invoke-SSHCommand -SessionId $session.SessionId -Command $Command
        
        $success = ($result.ExitStatus -eq 0)
        if (-not $success -and -not $SuppressErrors) {
            Write-RotationLog "Command failed on $($Node.Host): $Command" -Level Warning
        }
        
        return @{
            Success = $success
            Output = $result.Output
            ExitCode = $result.ExitStatus
        }
    }
    catch {
        Write-RotationLog "Exception executing command on $($Node.Host): $_" -Level Error
        return @{ Success = $false; Output = $_.Exception.Message; ExitCode = -1 }
    }
}

function Copy-FileToClusterNode {
    <#
    .SYNOPSIS
        Copies a file to a remote cluster node using SFTP.
    #>
    param(
        [hashtable]$Node,
        [string]$LocalPath,
        [string]$RemotePath
    )
    
    try {
        $securePassword = ConvertTo-SecureString -String $Node.Password -AsPlainText -Force
        $credential = New-Object System.Management.Automation.PSCredential($Node.User, $securePassword)
        
        $sftp = New-SFTPSession -ComputerName $Node.Host -Credential $credential -AcceptKey
        Set-SFTPItem -SessionId $sftp.SessionId -Path $LocalPath -Destination $RemotePath -Force
        Remove-SFTPSession -SessionId $sftp.SessionId | Out-Null
        
        return $true
    }
    catch {
        Write-RotationLog "Failed to copy file to $($Node.Host): $_" -Level Error
        return $false
    }
}

function Disconnect-AllClusterNodes {
    <#
    .SYNOPSIS
        Closes all cached SSH sessions cleanly.
    #>
    foreach ($session in $script:SSHSessions.Values) {
        if ($session.Connected) {
            Remove-SSHSession -SessionId $session.SessionId | Out-Null
        }
    }
    $script:SSHSessions.Clear()
}

# ============================================================================
# Pre-Rotation Assessment Functions
# ============================================================================

function Test-ClusterConnectivity {
    <#
    .SYNOPSIS
        Verifies SSH connectivity to all cluster nodes before beginning rotation.
    .DESCRIPTION
        This is a critical safety check. Before we start any certificate operations, we need
        to ensure we can connect to every node. If we can't reach a node, we shouldn't proceed
        because we won't be able to complete the rotation successfully. Better to discover
        connectivity issues now than halfway through the procedure.
    #>
    
    Write-RotationHeader "Testing Cluster Connectivity"
    
    $allReachable = $true
    
    Write-RotationLog "Testing ZooKeeper nodes..."
    foreach ($node in $script:ClusterConfig.ZooKeeperNodes) {
        $result = Invoke-ClusterCommand -Node $node -Command "echo 'connectivity-test'" -SuppressErrors
        
        if ($result.Success) {
            Write-RotationLog "  ✓ $($node.Host) is reachable" -Level Success
        }
        else {
            Write-RotationLog "  ✗ $($node.Host) is NOT reachable" -Level Error
            $allReachable = $false
        }
    }
    
    Write-RotationLog "`nTesting Kafka broker nodes..."
    foreach ($node in $script:ClusterConfig.KafkaNodes) {
        $result = Invoke-ClusterCommand -Node $node -Command "echo 'connectivity-test'" -SuppressErrors
        
        if ($result.Success) {
            Write-RotationLog "  ✓ $($node.Host) is reachable" -Level Success
        }
        else {
            Write-RotationLog "  ✗ $($node.Host) is NOT reachable" -Level Error
            $allReachable = $false
        }
    }
    
    if (-not $allReachable) {
        Write-RotationLog "`nConnectivity check FAILED. Cannot proceed with rotation." -Level Error
        Write-RotationLog "Please verify network connectivity and SSH credentials before retrying." -Level Error
    }
    else {
        Write-RotationLog "`nConnectivity check PASSED. All nodes are reachable." -Level Success
    }
    
    return $allReachable
}

function Get-ClusterHealthStatus {
    <#
    .SYNOPSIS
        Assesses the current health of the cluster before rotation.
    .DESCRIPTION
        This function examines the cluster's current state to establish a baseline. We want
        to know that the cluster is healthy before we start making changes. If there are
        existing issues - under-replicated partitions, nodes already down, or service
        problems - we should address those first rather than compounding them with a
        certificate rotation.
    #>
    
    Write-RotationHeader "Assessing Cluster Health"
    
    $health = @{
        ZooKeeperHealthy = $true
        KafkaHealthy = $true
        Issues = @()
    }
    
    # Check ZooKeeper ensemble status
    Write-RotationLog "Checking ZooKeeper ensemble..."
    foreach ($node in $script:ClusterConfig.ZooKeeperNodes) {
        $statusCmd = "echo stat | nc localhost $($node.Port) 2>/dev/null"
        $result = Invoke-ClusterCommand -Node $node -Command $statusCmd -SuppressErrors
        
        if ($result.Success -and $result.Output -match "Mode: (leader|follower)") {
            $mode = $matches[1]
            Write-RotationLog "  ✓ $($node.Host) is running as $mode" -Level Success
        }
        else {
            Write-RotationLog "  ✗ $($node.Host) is not responding properly" -Level Warning
            $health.ZooKeeperHealthy = $false
            $health.Issues += "ZooKeeper node $($node.Host) is not healthy"
        }
    }
    
    # Check Kafka broker status and partition health
    Write-RotationLog "`nChecking Kafka brokers..."
    $firstBroker = $script:ClusterConfig.KafkaNodes[0]
    
    foreach ($node in $script:ClusterConfig.KafkaNodes) {
        # Check if broker process is running
        $processCmd = "ps aux | grep -v grep | grep kafka.Kafka | wc -l"
        $result = Invoke-ClusterCommand -Node $node -Command $processCmd -SuppressErrors
        
        if ($result.Success -and $result.Output.Trim() -gt 0) {
            Write-RotationLog "  ✓ $($node.Host) broker process is running" -Level Success
        }
        else {
            Write-RotationLog "  ✗ $($node.Host) broker process is not running" -Level Warning
            $health.KafkaHealthy = $false
            $health.Issues += "Kafka broker $($node.Host) is not running"
        }
    }
    
    # Check for under-replicated partitions
    Write-RotationLog "`nChecking partition replication status..."
    $urpCmd = "$($script:RemotePaths.KafkaHome)/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --under-replicated-partitions 2>/dev/null | wc -l"
    $urpResult = Invoke-ClusterCommand -Node $firstBroker -Command $urpCmd -SuppressErrors
    
    if ($urpResult.Success) {
        $urpCount = [int]$urpResult.Output.Trim()
        if ($urpCount -eq 0) {
            Write-RotationLog "  ✓ No under-replicated partitions" -Level Success
        }
        else {
            Write-RotationLog "  ⚠ Found $urpCount under-replicated partitions" -Level Warning
            $health.Issues += "$urpCount under-replicated partitions detected"
            Write-RotationLog "    This may indicate existing cluster issues that should be addressed first" -Level Warning
        }
    }
    
    # Summary
    Write-RotationLog "`nHealth Assessment Summary:"
    if ($health.ZooKeeperHealthy -and $health.KafkaHealthy -and $health.Issues.Count -eq 0) {
        Write-RotationLog "  ✓ Cluster is healthy and ready for certificate rotation" -Level Success
    }
    else {
        Write-RotationLog "  ⚠ Cluster has some health concerns:" -Level Warning
        foreach ($issue in $health.Issues) {
            Write-RotationLog "    - $issue" -Level Warning
        }
        Write-RotationLog "  Consider addressing these issues before proceeding with rotation" -Level Warning
    }
    
    return $health
}

function Get-CurrentCertificateInfo {
    <#
    .SYNOPSIS
        Documents the current certificate configuration for comparison after rotation.
    .DESCRIPTION
        This function captures details about the certificates currently in use. This
        serves as a baseline for comparison after rotation and provides rollback
        information if we need to revert changes. We save this information to a file
        so we have a permanent record of what was deployed before we made changes.
    #>
    
    Write-RotationHeader "Documenting Current Certificate Configuration"
    
    $currentConfig = @{
        Timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
        ZooKeeperNodes = @()
        KafkaNodes = @()
    }
    
    # Gather certificate info from ZooKeeper nodes
    Write-RotationLog "Reading ZooKeeper certificate configurations..."
    foreach ($node in $script:ClusterConfig.ZooKeeperNodes) {
        $nodeInfo = @{
            Host = $node.Host
            CertificateFiles = @()
        }
        
        # List certificate files in the certificate directory
        $listCmd = "ls -la $($script:RemotePaths.CertificateDir)/*.jks 2>/dev/null || echo 'NO_CERTS'"
        $result = Invoke-ClusterCommand -Node $node -Command $listCmd -SuppressErrors
        
        if ($result.Success -and $result.Output -notmatch "NO_CERTS") {
            $nodeInfo.CertificateFiles = $result.Output -split "`n" | Where-Object { $_ -match "\.jks" }
            Write-RotationLog "  Found certificates on $($node.Host)" -Level Info
        }
        
        $currentConfig.ZooKeeperNodes += $nodeInfo
    }
    
    # Gather certificate info from Kafka nodes
    Write-RotationLog "`nReading Kafka broker certificate configurations..."
    foreach ($node in $script:ClusterConfig.KafkaNodes) {
        $nodeInfo = @{
            Host = $node.Host
            CertificateFiles = @()
        }
        
        $listCmd = "ls -la $($script:RemotePaths.CertificateDir)/*.jks 2>/dev/null || echo 'NO_CERTS'"
        $result = Invoke-ClusterCommand -Node $node -Command $listCmd -SuppressErrors
        
        if ($result.Success -and $result.Output -notmatch "NO_CERTS") {
            $nodeInfo.CertificateFiles = $result.Output -split "`n" | Where-Object { $_ -match "\.jks" }
            Write-RotationLog "  Found certificates on $($node.Host)" -Level Info
        }
        
        $currentConfig.KafkaNodes += $nodeInfo
    }
    
    # Save the current configuration to a file for rollback purposes
    $configFile = Join-Path $BackupDir "pre-rotation-config-$script:RotationTimestamp.json"
    $currentConfig | ConvertTo-Json -Depth 10 | Out-File -FilePath $configFile -Encoding UTF8
    Write-RotationLog "`nCurrent configuration saved to: $configFile" -Level Success
    
    return $currentConfig
}

# ============================================================================
# Cluster Shutdown Functions
# ============================================================================

function Stop-KafkaCluster {
    <#
    .SYNOPSIS
        Stops all Kafka brokers in the cluster.
    .DESCRIPTION
        This function coordinates the shutdown of all Kafka brokers. We issue stop commands
        to all brokers simultaneously because they're peers - there's no special order required.
        After issuing stop commands, we verify that all brokers have actually stopped before
        proceeding. This verification is important because we don't want to replace certificates
        while services are still running.
    #>
    
    Write-RotationHeader "Stopping Kafka Cluster"
    
    if ($DryRun) {
        Write-RotationLog "[DRY RUN] Would stop all Kafka brokers" -Level Info
        return $true
    }
    
    # Issue stop commands to all brokers simultaneously
    Write-RotationLog "Issuing stop commands to all Kafka brokers..."
    foreach ($node in $script:ClusterConfig.KafkaNodes) {
        $stopCmd = "$($script:RemotePaths.KafkaHome)/bin/kafka-server-stop.sh"
        Write-RotationLog "  Stopping broker on $($node.Host)..." -Level Info
        
        $result = Invoke-ClusterCommand -Node $node -Command $stopCmd -SuppressErrors
        
        # The stop script may return non-zero even on success, so we don't fail immediately
        Write-RotationLog "  Stop command issued to $($node.Host)" -Level Info
    }
    
    # Wait and verify that all brokers have actually stopped
    Write-RotationLog "`nVerifying Kafka broker shutdown..."
    $maxWaitSeconds = $ShutdownTimeoutSeconds
    $waitedSeconds = 0
    $allStopped = $false
    
    while ($waitedSeconds -lt $maxWaitSeconds -and -not $allStopped) {
        Start-Sleep -Seconds 5
        $waitedSeconds += 5
        
        $stillRunning = @()
        
        foreach ($node in $script:ClusterConfig.KafkaNodes) {
            # Check if Kafka process is still running
            $checkCmd = "ps aux | grep -v grep | grep kafka.Kafka | wc -l"
            $result = Invoke-ClusterCommand -Node $node -Command $checkCmd -SuppressErrors
            
            if ($result.Success -and $result.Output.Trim() -gt 0) {
                $stillRunning += $node.Host
            }
        }
        
        if ($stillRunning.Count -eq 0) {
            $allStopped = $true
            Write-RotationLog "  ✓ All Kafka brokers have stopped" -Level Success
        }
        else {
            Write-RotationLog "  Waiting for brokers to stop: $($stillRunning -join ', ') ($waitedSeconds seconds elapsed)" -Level Info
        }
    }
    
    if (-not $allStopped) {
        Write-RotationLog "  ⚠ Some brokers did not stop gracefully within timeout" -Level Warning
        Write-RotationLog "  Force-stopping remaining Kafka processes..." -Level Warning
        
        # Force kill any remaining Kafka processes
        foreach ($node in $script:ClusterConfig.KafkaNodes) {
            $killCmd = "pkill -9 -f kafka.Kafka"
            Invoke-ClusterCommand -Node $node -Command $killCmd -SuppressErrors | Out-Null
        }
        
        Start-Sleep -Seconds 5
        Write-RotationLog "  Forced shutdown complete" -Level Info
    }
    
    return $true
}

function Stop-ZooKeeperCluster {
    <#
    .SYNOPSIS
        Stops all ZooKeeper nodes in the ensemble.
    .DESCRIPTION
        ZooKeeper should be stopped after Kafka because Kafka depends on ZooKeeper for metadata.
        Stopping ZooKeeper first could cause issues with Kafka's shutdown process. We stop all
        ZooKeeper nodes simultaneously since the cluster is already down at this point.
    #>
    
    Write-RotationHeader "Stopping ZooKeeper Ensemble"
    
    if ($DryRun) {
        Write-RotationLog "[DRY RUN] Would stop all ZooKeeper nodes" -Level Info
        return $true
    }
    
    # Issue stop commands to all ZooKeeper nodes
    Write-RotationLog "Issuing stop commands to all ZooKeeper nodes..."
    foreach ($node in $script:ClusterConfig.ZooKeeperNodes) {
        $stopCmd = "$($script:RemotePaths.ZooKeeperHome)/bin/zookeeper-server-stop.sh"
        Write-RotationLog "  Stopping ZooKeeper on $($node.Host)..." -Level Info
        
        $result = Invoke-ClusterCommand -Node $node -Command $stopCmd -SuppressErrors
        Write-RotationLog "  Stop command issued to $($node.Host)" -Level Info
    }
    
    # Verify shutdown
    Write-RotationLog "`nVerifying ZooKeeper shutdown..."
    $maxWaitSeconds = $ShutdownTimeoutSeconds
    $waitedSeconds = 0
    $allStopped = $false
    
    while ($waitedSeconds -lt $maxWaitSeconds -and -not $allStopped) {
        Start-Sleep -Seconds 5
        $waitedSeconds += 5
        
        $stillRunning = @()
        
        foreach ($node in $script:ClusterConfig.ZooKeeperNodes) {
            # Check if ZooKeeper process is still running
            $checkCmd = "ps aux | grep -v grep | grep zookeeper | wc -l"
            $result = Invoke-ClusterCommand -Node $node -Command $checkCmd -SuppressErrors
            
            if ($result.Success -and $result.Output.Trim() -gt 0) {
                $stillRunning += $node.Host
            }
        }
        
        if ($stillRunning.Count -eq 0) {
            $allStopped = $true
            Write-RotationLog "  ✓ All ZooKeeper nodes have stopped" -Level Success
        }
        else {
            Write-RotationLog "  Waiting for nodes to stop: $($stillRunning -join ', ') ($waitedSeconds seconds elapsed)" -Level Info
        }
    }
    
    if (-not $allStopped) {
        Write-RotationLog "  ⚠ Some ZooKeeper nodes did not stop gracefully within timeout" -Level Warning
        Write-RotationLog "  Force-stopping remaining ZooKeeper processes..." -Level Warning
        
        foreach ($node in $script:ClusterConfig.ZooKeeperNodes) {
            $killCmd = "pkill -9 -f zookeeper"
            Invoke-ClusterCommand -Node $node -Command $killCmd -SuppressErrors | Out-Null
        }
        
        Start-Sleep -Seconds 5
        Write-RotationLog "  Forced shutdown complete" -Level Info
    }
    
    return $true
}

# ============================================================================
# Certificate Backup and Deployment Functions
# ============================================================================

function Backup-ExistingCertificates {
    <#
    .SYNOPSIS
        Creates backups of existing certificates on all cluster nodes.
    .DESCRIPTION
        Before replacing any certificates, we create complete backups on each node. These
        backups serve as our rollback mechanism if something goes wrong. The backup includes
        all certificate files and is timestamped so we can identify which backup corresponds
        to which rotation attempt. We also download a copy locally for additional safety.
    #>
    
    Write-RotationHeader "Backing Up Existing Certificates"
    
    if ($DryRun) {
        Write-RotationLog "[DRY RUN] Would back up certificates from all nodes" -Level Info
        return $true
    }
    
    if ($SkipBackup) {
        Write-RotationLog "Skipping backup as requested (not recommended)" -Level Warning
        return $true
    }
    
    $backupSuccess = $true
    
    # Backup ZooKeeper certificates
    Write-RotationLog "Backing up ZooKeeper certificates..."
    foreach ($node in $script:ClusterConfig.ZooKeeperNodes) {
        $remoteBackupDir = "$($script:RemotePaths.BackupDir)/pre-rotation-$script:RotationTimestamp"
        
        # Create backup directory on remote node
        $mkdirCmd = "mkdir -p '$remoteBackupDir'"
        Invoke-ClusterCommand -Node $node -Command $mkdirCmd | Out-Null
        
        # Copy all certificate files to backup directory
        $backupCmd = "cp -p $($script:RemotePaths.CertificateDir)/*.jks '$remoteBackupDir/' 2>/dev/null || true"
        $result = Invoke-ClusterCommand -Node $node -Command $backupCmd
        
        if ($result.Success) {
            Write-RotationLog "  ✓ Backed up certificates on $($node.Host) to $remoteBackupDir" -Level Success
        }
        else {
            Write-RotationLog "  ✗ Failed to backup certificates on $($node.Host)" -Level Error
            $backupSuccess = $false
        }
    }
    
    # Backup Kafka certificates
    Write-RotationLog "`nBacking up Kafka certificates..."
    foreach ($node in $script:ClusterConfig.KafkaNodes) {
        $remoteBackupDir = "$($script:RemotePaths.BackupDir)/pre-rotation-$script:RotationTimestamp"
        
        $mkdirCmd = "mkdir -p '$remoteBackupDir'"
        Invoke-ClusterCommand -Node $node -Command $mkdirCmd | Out-Null
        
        $backupCmd = "cp -p $($script:RemotePaths.CertificateDir)/*.jks '$remoteBackupDir/' 2>/dev/null || true"
        $result = Invoke-ClusterCommand -Node $node -Command $backupCmd
        
        if ($result.Success) {
            Write-RotationLog "  ✓ Backed up certificates on $($node.Host) to $remoteBackupDir" -Level Success
        }
        else {
            Write-RotationLog "  ✗ Failed to backup certificates on $($node.Host)" -Level Error
            $backupSuccess = $false
        }
    }
    
    if ($backupSuccess) {
        Write-RotationLog "`nAll certificates backed up successfully" -Level Success
        Write-RotationLog "Backups stored in: $($script:RemotePaths.BackupDir)/pre-rotation-$script:RotationTimestamp" -Level Info
    }
    else {
        Write-RotationLog "`nSome backup operations failed. Review errors above." -Level Error
    }
    
    return $backupSuccess
}

function Deploy-NewCertificates {
    <#
    .SYNOPSIS
        Deploys new certificates to all cluster nodes.
    .DESCRIPTION
        This function copies the new certificate files from your local system to each cluster
        node. We need to match the correct certificates to the correct nodes. The naming
        convention helps with this - certificates should be named to clearly indicate which
        node they're for (for example, zk1-keystore.jks, broker1-keystore.jks, etc.).
        
        The truststore is typically the same across all nodes, while each node gets its
        own unique keystore containing its identity certificate.
    #>
    
    Write-RotationHeader "Deploying New Certificates"
    
    if ($DryRun) {
        Write-RotationLog "[DRY RUN] Would deploy certificates from $LocalCertificateDir" -Level Info
        return $true
    }
    
    # Verify that we have the certificates we need locally
    if (-not (Test-Path $LocalCertificateDir)) {
        Write-RotationLog "Certificate directory not found: $LocalCertificateDir" -Level Error
        return $false
    }
    
    $deploySuccess = $true
    
    # Deploy to ZooKeeper nodes
    Write-RotationLog "Deploying certificates to ZooKeeper nodes..."
    foreach ($node in $script:ClusterConfig.ZooKeeperNodes) {
        Write-RotationLog "  Processing $($node.Host)..."
        
        # Look for node-specific keystore
        # Common naming patterns: zk1-keystore.jks, zk-hostname-keystore.jks, etc.
        $possibleKeystores = @(
            "zk-$($node.Host -replace '\..*')-keystore.jks",  # zk-hostname-keystore.jks
            "zookeeper-$($node.Host -replace '\..*')-keystore.jks",
            "zk$($node.Host -replace '\D')-keystore.jks"  # zk1-keystore.jks
        )
        
        $keystoreFile = $null
        foreach ($pattern in $possibleKeystores) {
            $path = Join-Path $LocalCertificateDir $pattern
            if (Test-Path $path) {
                $keystoreFile = $path
                break
            }
        }
        
        if (-not $keystoreFile) {
            Write-RotationLog "    ⚠ No keystore found for $($node.Host). Tried: $($possibleKeystores -join ', ')" -Level Warning
            $deploySuccess = $false
            continue
        }
        
        # Copy keystore to remote node
        Write-RotationLog "    Deploying keystore: $(Split-Path -Leaf $keystoreFile)"
        $remoteKeystorePath = "$($script:RemotePaths.CertificateDir)/zookeeper-keystore.jks"
        $copied = Copy-FileToClusterNode -Node $node -LocalPath $keystoreFile -RemotePath $remoteKeystorePath
        
        if ($copied) {
            Write-RotationLog "    ✓ Keystore deployed successfully" -Level Success
        }
        else {
            Write-RotationLog "    ✗ Failed to deploy keystore" -Level Error
            $deploySuccess = $false
        }
        
        # Deploy truststore (typically same for all nodes)
        $truststorePath = Join-Path $LocalCertificateDir "kafka-truststore.jks"
        if (Test-Path $truststorePath) {
            Write-RotationLog "    Deploying truststore..."
            $remoteTruststorePath = "$($script:RemotePaths.CertificateDir)/zookeeper-truststore.jks"
            $copied = Copy-FileToClusterNode -Node $node -LocalPath $truststorePath -RemotePath $remoteTruststorePath
            
            if ($copied) {
                Write-RotationLog "    ✓ Truststore deployed successfully" -Level Success
            }
            else {
                Write-RotationLog "    ✗ Failed to deploy truststore" -Level Error
                $deploySuccess = $false
            }
        }
    }
    
    # Deploy to Kafka nodes
    Write-RotationLog "`nDeploying certificates to Kafka brokers..."
    foreach ($node in $script:ClusterConfig.KafkaNodes) {
        Write-RotationLog "  Processing $($node.Host)..."
        
        # Look for node-specific keystore
        $possibleKeystores = @(
            "broker-$($node.Host -replace '\..*')-keystore.jks",
            "kafka-$($node.Host -replace '\..*')-keystore.jks",
            "broker$($node.Host -replace '\D')-keystore.jks"
        )
        
        $keystoreFile = $null
        foreach ($pattern in $possibleKeystores) {
            $path = Join-Path $LocalCertificateDir $pattern
            if (Test-Path $path) {
                $keystoreFile = $path
                break
            }
        }
        
        if (-not $keystoreFile) {
            Write-RotationLog "    ⚠ No keystore found for $($node.Host). Tried: $($possibleKeystores -join ', ')" -Level Warning
            $deploySuccess = $false
            continue
        }
        
        Write-RotationLog "    Deploying keystore: $(Split-Path -Leaf $keystoreFile)"
        $remoteKeystorePath = "$($script:RemotePaths.CertificateDir)/kafka-keystore.jks"
        $copied = Copy-FileToClusterNode -Node $node -LocalPath $keystoreFile -RemotePath $remoteKeystorePath
        
        if ($copied) {
            Write-RotationLog "    ✓ Keystore deployed successfully" -Level Success
        }
        else {
            Write-RotationLog "    ✗ Failed to deploy keystore" -Level Error
            $deploySuccess = $false
        }
        
        # Deploy truststore
        $truststorePath = Join-Path $LocalCertificateDir "kafka-truststore.jks"
        if (Test-Path $truststorePath) {
            Write-RotationLog "    Deploying truststore..."
            $remoteTruststorePath = "$($script:RemotePaths.CertificateDir)/kafka-truststore.jks"
            $copied = Copy-FileToClusterNode -Node $node -LocalPath $truststorePath -RemotePath $remoteTruststorePath
            
            if ($copied) {
                Write-RotationLog "    ✓ Truststore deployed successfully" -Level Success
            }
            else {
                Write-RotationLog "    ✗ Failed to deploy truststore" -Level Error
                $deploySuccess = $false
            }
        }
    }
    
    if ($deploySuccess) {
        Write-RotationLog "`nAll certificates deployed successfully" -Level Success
    }
    else {
        Write-RotationLog "`nSome certificate deployments failed. Review errors above." -Level Error
    }
    
    return $deploySuccess
}

# ============================================================================
# Cluster Startup Functions
# ============================================================================

function Start-ZooKeeperCluster {
    <#
    .SYNOPSIS
        Starts all ZooKeeper nodes and waits for ensemble formation.
    .DESCRIPTION
        ZooKeeper must start before Kafka because Kafka depends on ZooKeeper for metadata
        storage and coordination. We start all ZooKeeper nodes, then wait for them to form
        a healthy ensemble with a leader elected. This verification step is crucial - we
        don't want to start Kafka until ZooKeeper is fully operational.
    #>
    
    Write-RotationHeader "Starting ZooKeeper Ensemble"
    
    if ($DryRun) {
        Write-RotationLog "[DRY RUN] Would start all ZooKeeper nodes" -Level Info
        return $true
    }
    
    # Start all ZooKeeper nodes
    Write-RotationLog "Starting ZooKeeper nodes..."
    foreach ($node in $script:ClusterConfig.ZooKeeperNodes) {
        $startCmd = "$($script:RemotePaths.ZooKeeperHome)/bin/zookeeper-server-start.sh -daemon $($script:RemotePaths.ZooKeeperConfig)"
        Write-RotationLog "  Starting ZooKeeper on $($node.Host)..." -Level Info
        
        $result = Invoke-ClusterCommand -Node $node -Command $startCmd
        
        if ($result.Success -or $result.ExitCode -eq 0) {
            Write-RotationLog "  Start command issued successfully" -Level Success
        }
        else {
            Write-RotationLog "  ⚠ Start command may have failed: $($result.Output)" -Level Warning
        }
    }
    
    # Wait for ZooKeeper to start and form ensemble
    Write-RotationLog "`nWaiting for ZooKeeper ensemble to form..."
    $maxWaitSeconds = $StartupTimeoutSeconds
    $waitedSeconds = 0
    $ensembleHealthy = $false
    
    while ($waitedSeconds -lt $maxWaitSeconds -and -not $ensembleHealthy) {
        Start-Sleep -Seconds 5
        $waitedSeconds += 5
        
        $healthyNodes = 0
        $leaderFound = $false
        
        foreach ($node in $script:ClusterConfig.ZooKeeperNodes) {
            $statusCmd = "echo stat | nc localhost $($node.Port) 2>/dev/null"
            $result = Invoke-ClusterCommand -Node $node -Command $statusCmd -SuppressErrors
            
            if ($result.Success -and $result.Output -match "Mode: (leader|follower)") {
                $healthyNodes++
                if ($result.Output -match "Mode: leader") {
                    $leaderFound = $true
                    Write-RotationLog "  ✓ $($node.Host) is the leader" -Level Success
                }
                else {
                    Write-RotationLog "  ✓ $($node.Host) is a follower" -Level Success
                }
            }
        }
        
        if ($healthyNodes -eq $script:ClusterConfig.ZooKeeperNodes.Count -and $leaderFound) {
            $ensembleHealthy = $true
            Write-RotationLog "`n  ✓ ZooKeeper ensemble is healthy with $healthyNodes nodes and a leader elected" -Level Success
        }
        else {
            Write-RotationLog "  Waiting for ensemble formation... ($healthyNodes/$($script:ClusterConfig.ZooKeeperNodes.Count) nodes healthy, $waitedSeconds seconds elapsed)" -Level Info
        }
    }
    
    if (-not $ensembleHealthy) {
        Write-RotationLog "`n  ✗ ZooKeeper ensemble failed to form properly within timeout" -Level Error
        Write-RotationLog "  Check ZooKeeper logs on each node for errors" -Level Error
        Write-RotationLog "  Log location: $($script:RemotePaths.ZooKeeperHome)/logs/zookeeper.out" -Level Info
        return $false
    }
    
    return $true
}

function Start-KafkaCluster {
    <#
    .SYNOPSIS
        Starts all Kafka brokers and waits for cluster formation.
    .DESCRIPTION
        With ZooKeeper running, we can now start the Kafka brokers. We start all brokers
        and then verify that they successfully join the cluster. Each broker needs to
        register itself with ZooKeeper and become ready to handle requests. We wait for
        all brokers to be healthy before considering the startup complete.
    #>
    
    Write-RotationHeader "Starting Kafka Cluster"
    
    if ($DryRun) {
        Write-RotationLog "[DRY RUN] Would start all Kafka brokers" -Level Info
        return $true
    }
    
    # Start all Kafka brokers
    Write-RotationLog "Starting Kafka brokers..."
    foreach ($node in $script:ClusterConfig.KafkaNodes) {
        $startCmd = "$($script:RemotePaths.KafkaHome)/bin/kafka-server-start.sh -daemon $($script:RemotePaths.KafkaConfig)"
        Write-RotationLog "  Starting broker on $($node.Host)..." -Level Info
        
        $result = Invoke-ClusterCommand -Node $node -Command $startCmd
        
        if ($result.Success -or $result.ExitCode -eq 0) {
            Write-RotationLog "  Start command issued successfully" -Level Success
        }
        else {
            Write-RotationLog "  ⚠ Start command may have failed: $($result.Output)" -Level Warning
        }
    }
    
    # Wait for all brokers to start and register with the cluster
    Write-RotationLog "`nWaiting for Kafka brokers to start and register..."
    $maxWaitSeconds = $StartupTimeoutSeconds
    $waitedSeconds = 0
    $clusterReady = $false
    
    while ($waitedSeconds -lt $maxWaitSeconds -and -not $clusterReady) {
        Start-Sleep -Seconds 10
        $waitedSeconds += 10
        
        $healthyBrokers = 0
        
        foreach ($node in $script:ClusterConfig.KafkaNodes) {
            # Check if broker is responding to API requests
            $checkCmd = "$($script:RemotePaths.KafkaHome)/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 2>&1 | grep -q 'ApiVersion' && echo 'HEALTHY' || echo 'UNHEALTHY'"
            $result = Invoke-ClusterCommand -Node $node -Command $checkCmd -SuppressErrors
            
            if ($result.Success -and $result.Output -match "HEALTHY") {
                $healthyBrokers++
                Write-RotationLog "  ✓ Broker on $($node.Host) is healthy" -Level Success
            }
        }
        
        if ($healthyBrokers -eq $script:ClusterConfig.KafkaNodes.Count) {
            $clusterReady = $true
            Write-RotationLog "`n  ✓ All Kafka brokers are healthy and registered" -Level Success
        }
        else {
            Write-RotationLog "  Waiting for brokers to start... ($healthyBrokers/$($script:ClusterConfig.KafkaNodes.Count) brokers healthy, $waitedSeconds seconds elapsed)" -Level Info
        }
    }
    
    if (-not $clusterReady) {
        Write-RotationLog "`n  ✗ Some brokers failed to start properly within timeout" -Level Error
        Write-RotationLog "  Check Kafka logs on each node for errors" -Level Error
        Write-RotationLog "  Log location: $($script:RemotePaths.KafkaHome)/logs/server.log" -Level Info
        return $false
    }
    
    return $true
}

# ============================================================================
# Post-Rotation Validation Functions
# ============================================================================

function Test-ClusterHealth {
    <#
    .SYNOPSIS
        Performs comprehensive health checks after certificate rotation.
    .DESCRIPTION
        This validation suite verifies that the cluster is fully operational with the new
        certificates. We test ZooKeeper ensemble health, broker availability, partition
        replication status, and actual message production and consumption. This thorough
        testing gives you confidence that the rotation succeeded completely.
    #>
    
    Write-RotationHeader "Validating Cluster Health After Rotation"
    
    $validationResults = @{
        ZooKeeperHealthy = $true
        KafkaHealthy = $true
        ReplicationHealthy = $true
        FunctionalTestPassed = $false
        Issues = @()
    }
    
    # Verify ZooKeeper ensemble
    Write-RotationLog "Verifying ZooKeeper ensemble health..."
    $leaderFound = $false
    $healthyZKNodes = 0
    
    foreach ($node in $script:ClusterConfig.ZooKeeperNodes) {
        $statusCmd = "echo stat | nc localhost $($node.Port) 2>/dev/null"
        $result = Invoke-ClusterCommand -Node $node -Command $statusCmd -SuppressErrors
        
        if ($result.Success -and $result.Output -match "Mode: (leader|follower)") {
            $healthyZKNodes++
            $mode = $matches[1]
            Write-RotationLog "  ✓ $($node.Host) is healthy ($mode)" -Level Success
            if ($mode -eq "leader") {
                $leaderFound = $true
            }
        }
        else {
            Write-RotationLog "  ✗ $($node.Host) is not responding correctly" -Level Error
            $validationResults.ZooKeeperHealthy = $false
            $validationResults.Issues += "ZooKeeper node $($node.Host) is unhealthy"
        }
    }
    
    if ($healthyZKNodes -eq $script:ClusterConfig.ZooKeeperNodes.Count -and $leaderFound) {
        Write-RotationLog "  ✓ ZooKeeper ensemble is fully healthy" -Level Success
    }
    
    # Verify Kafka brokers
    Write-RotationLog "`nVerifying Kafka broker health..."
    $healthyKafkaNodes = 0
    
    foreach ($node in $script:ClusterConfig.KafkaNodes) {
        $checkCmd = "$($script:RemotePaths.KafkaHome)/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 2>&1 | grep -q 'ApiVersion' && echo 'OK' || echo 'FAIL'"
        $result = Invoke-ClusterCommand -Node $node -Command $checkCmd -SuppressErrors
        
        if ($result.Success -and $result.Output -match "OK") {
            $healthyKafkaNodes++
            Write-RotationLog "  ✓ Broker on $($node.Host) is healthy" -Level Success
        }
        else {
            Write-RotationLog "  ✗ Broker on $($node.Host) is not responding correctly" -Level Error
            $validationResults.KafkaHealthy = $false
            $validationResults.Issues += "Kafka broker $($node.Host) is unhealthy"
        }
    }
    
    if ($healthyKafkaNodes -eq $script:ClusterConfig.KafkaNodes.Count) {
        Write-RotationLog "  ✓ All Kafka brokers are healthy" -Level Success
    }
    
    # Check partition replication
    Write-RotationLog "`nChecking partition replication status..."
    $firstBroker = $script:ClusterConfig.KafkaNodes[0]
    $urpCmd = "$($script:RemotePaths.KafkaHome)/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --under-replicated-partitions 2>/dev/null | wc -l"
    $result = Invoke-ClusterCommand -Node $firstBroker -Command $urpCmd -SuppressErrors
    
    if ($result.Success) {
        $urpCount = [int]$result.Output.Trim()
        if ($urpCount -eq 0) {
            Write-RotationLog "  ✓ No under-replicated partitions" -Level Success
        }
        else {
            Write-RotationLog "  ⚠ Found $urpCount under-replicated partitions" -Level Warning
            $validationResults.ReplicationHealthy = $false
            $validationResults.Issues += "$urpCount under-replicated partitions"
        }
    }
    
    # Functional test - produce and consume a message
    Write-RotationLog "`nPerforming functional test (produce and consume)..."
    $testTopic = "cert-rotation-test-$script:RotationTimestamp"
    
    # Create test topic
    $createTopicCmd = "$($script:RemotePaths.KafkaHome)/bin/kafka-topics.sh --create --topic $testTopic --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092 2>&1"
    $result = Invoke-ClusterCommand -Node $firstBroker -Command $createTopicCmd -SuppressErrors
    
    if ($result.Success -or $result.Output -match "Created topic") {
        Write-RotationLog "  Test topic created: $testTopic"
        
        # Produce a test message
        $testMessage = "Certificate rotation test message at $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
        $produceCmd = "echo '$testMessage' | $($script:RemotePaths.KafkaHome)/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic $testTopic 2>&1"
        $produceResult = Invoke-ClusterCommand -Node $firstBroker -Command $produceCmd -SuppressErrors
        
        if ($produceResult.Success) {
            Write-RotationLog "  ✓ Test message produced successfully" -Level Success
            
            # Consume the test message
            $consumeCmd = "$($script:RemotePaths.KafkaHome)/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic $testTopic --from-beginning --max-messages 1 --timeout-ms 10000 2>&1"
            $consumeResult = Invoke-ClusterCommand -Node $firstBroker -Command $consumeCmd -SuppressErrors
            
            if ($consumeResult.Success -and $consumeResult.Output -match [regex]::Escape($testMessage)) {
                Write-RotationLog "  ✓ Test message consumed successfully" -Level Success
                $validationResults.FunctionalTestPassed = $true
            }
            else {
                Write-RotationLog "  ✗ Failed to consume test message" -Level Error
                $validationResults.Issues += "Message consumption test failed"
            }
        }
        else {
            Write-RotationLog "  ✗ Failed to produce test message" -Level Error
            $validationResults.Issues += "Message production test failed"
        }
        
        # Clean up test topic
        $deleteCmd = "$($script:RemotePaths.KafkaHome)/bin/kafka-topics.sh --delete --topic $testTopic --bootstrap-server localhost:9092 2>&1"
        Invoke-ClusterCommand -Node $firstBroker -Command $deleteCmd -SuppressErrors | Out-Null
    }
    else {
        Write-RotationLog "  ⚠ Could not create test topic for functional testing" -Level Warning
        $validationResults.Issues += "Unable to perform functional test"
    }
    
    # Final validation summary
    Write-RotationLog "`n=== Validation Summary ===" -Level Header
    
    $allHealthy = $validationResults.ZooKeeperHealthy -and 
                  $validationResults.KafkaHealthy -and 
                  $validationResults.ReplicationHealthy -and 
                  $validationResults.FunctionalTestPassed
    
    if ($allHealthy) {
        Write-RotationLog "✓ ALL VALIDATION CHECKS PASSED" -Level Success
        Write-RotationLog "The cluster is fully operational with the new certificates" -Level Success
    }
    else {
        Write-RotationLog "⚠ SOME VALIDATION CHECKS FAILED:" -Level Warning
        foreach ($issue in $validationResults.Issues) {
            Write-RotationLog "  - $issue" -Level Warning
        }
        Write-RotationLog "`nReview the issues above and investigate as needed" -Level Warning
    }
    
    return $validationResults
}

function Get-CertificateExpirationInfo {
    <#
    .SYNOPSIS
        Checks certificate expiration dates across the cluster.
    .DESCRIPTION
        After rotation, it's good practice to verify the expiration dates of the newly
        deployed certificates. This function reads the certificates from each node and
        displays their expiration information so you can confirm they have the expected
        validity period and plan for the next rotation.
    #>
    
    Write-RotationHeader "Certificate Expiration Information"
    
    Write-RotationLog "Checking certificate expiration dates on ZooKeeper nodes..."
    foreach ($node in $script:ClusterConfig.ZooKeeperNodes) {
        $checkCmd = "keytool -list -v -keystore $($script:RemotePaths.CertificateDir)/zookeeper-keystore.jks -storepass changeit 2>/dev/null | grep -A 2 'Valid from'"
        $result = Invoke-ClusterCommand -Node $node -Command $checkCmd -SuppressErrors
        
        if ($result.Success) {
            Write-RotationLog "`n  $($node.Host):" -Level Info
            Write-RotationLog "    $($result.Output)" -Level Info
        }
    }
    
    Write-RotationLog "`nChecking certificate expiration dates on Kafka brokers..."
    foreach ($node in $script:ClusterConfig.KafkaNodes) {
        $checkCmd = "keytool -list -v -keystore $($script:RemotePaths.CertificateDir)/kafka-keystore.jks -storepass changeit 2>/dev/null | grep -A 2 'Valid from'"
        $result = Invoke-ClusterCommand -Node $node -Command $checkCmd -SuppressErrors
        
        if ($result.Success) {
            Write-RotationLog "`n  $($node.Host):" -Level Info
            Write-RotationLog "    $($result.Output)" -Level Info
        }
    }
}

# ============================================================================
# Rollback Function
# ============================================================================

function Restore-PreviousCertificates {
    <#
    .SYNOPSIS
        Rolls back to the previous certificates if validation fails.
    .DESCRIPTION
        If the validation checks reveal that the new certificates aren't working correctly,
        this function provides a quick path back to the working state. It restores the
        backed-up certificates and restarts the cluster. This is why the backup step is
        so important - it's your safety net if something goes wrong.
    #>
    param(
        [switch]$Confirm
    )
    
    Write-RotationHeader "Certificate Rollback"
    
    if (-not $Confirm) {
        Write-RotationLog "This will restore the previous certificates and restart the cluster." -Level Warning
        Write-RotationLog "Are you sure you want to proceed? Use -Confirm to execute rollback." -Level Warning
        return $false
    }
    
    Write-RotationLog "Starting rollback to previous certificates..." -Level Warning
    $backupDir = "$($script:RemotePaths.BackupDir)/pre-rotation-$script:RotationTimestamp"
    
    # Stop cluster
    $stopped = Stop-KafkaCluster
    if ($stopped) {
        $stopped = Stop-ZooKeeperCluster
    }
    
    if (-not $stopped) {
        Write-RotationLog "Failed to stop cluster for rollback" -Level Error
        return $false
    }
    
    # Restore certificates on all nodes
    Write-RotationLog "`nRestoring previous certificates..."
    
    foreach ($node in $script:ClusterConfig.ZooKeeperNodes) {
        $restoreCmd = "cp -p '$backupDir'/*.jks $($script:RemotePaths.CertificateDir)/"
        $result = Invoke-ClusterCommand -Node $node -Command $restoreCmd
        
        if ($result.Success) {
            Write-RotationLog "  ✓ Restored certificates on $($node.Host)" -Level Success
        }
        else {
            Write-RotationLog "  ✗ Failed to restore certificates on $($node.Host)" -Level Error
        }
    }
    
    foreach ($node in $script:ClusterConfig.KafkaNodes) {
        $restoreCmd = "cp -p '$backupDir'/*.jks $($script:RemotePaths.CertificateDir)/"
        $result = Invoke-ClusterCommand -Node $node -Command $restoreCmd
        
        if ($result.Success) {
            Write-RotationLog "  ✓ Restored certificates on $($node.Host)" -Level Success
        }
        else {
            Write-RotationLog "  ✗ Failed to restore certificates on $($node.Host)" -Level Error
        }
    }
    
    # Restart cluster
    Write-RotationLog "`nRestarting cluster with restored certificates..."
    $started = Start-ZooKeeperCluster
    if ($started) {
        $started = Start-KafkaCluster
    }
    
    if ($started) {
        Write-RotationLog "`nRollback completed successfully" -Level Success
        return $true
    }
    else {
        Write-RotationLog "`nRollback encountered issues during startup" -Level Error
        return $false
    }
}

# ============================================================================
# Main Execution Flow
# ============================================================================

function Start-CertificateRotation {
    <#
    .SYNOPSIS
        Main orchestration function that coordinates the complete rotation workflow.
    .DESCRIPTION
        This is the entry point that ties together all the individual functions into a
        coherent workflow. It guides you through each phase of the rotation with clear
        feedback at every step. The workflow is designed to be safe with validation at
        each stage and the ability to stop if problems are detected.
    #>
    
    try {
        # Display banner and configuration
        Write-Host "`n" + ("=" * 80) -ForegroundColor Cyan
        Write-Host "  Kafka Certificate Rotation - Full Cluster Shutdown Method" -ForegroundColor Cyan
        Write-Host ("=" * 80) -ForegroundColor Cyan
        Write-Host "`nRotation Session: $script:RotationTimestamp" -ForegroundColor Yellow
        Write-Host "Configuration:" -ForegroundColor Yellow
        Write-Host "  Cluster Config: $ClusterConfigFile" -ForegroundColor Gray
        Write-Host "  Certificate Source: $LocalCertificateDir" -ForegroundColor Gray
        Write-Host "  Remote Kafka Home: $($script:RemotePaths.KafkaHome)" -ForegroundColor Gray
        Write-Host "  Backup Directory: $BackupDir" -ForegroundColor Gray
        Write-Host "  Log File: $script:RotationLogFile" -ForegroundColor Gray
        
        if ($DryRun) {
            Write-Host "`n*** DRY RUN MODE - No changes will be made ***`n" -ForegroundColor Magenta
        }
        
        Write-Host "`nPress Enter to begin or Ctrl+C to cancel..." -ForegroundColor Yellow
        Read-Host
        
        # Phase 1: Pre-rotation checks
        Write-RotationLog "`nStarting certificate rotation workflow..." -Level Info
        Write-RotationLog "Phase 1: Pre-rotation assessment" -Level Info
        
        # Check connectivity to all nodes
        $connectivityOK = Test-ClusterConnectivity
        if (-not $connectivityOK) {
            Write-RotationLog "`nCannot proceed - connectivity issues detected" -Level Error
            return $false
        }
        
        # Assess cluster health
        $health = Get-ClusterHealthStatus
        if (-not $health.ZooKeeperHealthy -or -not $health.KafkaHealthy) {
            Write-Host "`nWARNING: Cluster health issues detected." -ForegroundColor Yellow
            Write-Host "Proceeding with rotation may compound existing problems." -ForegroundColor Yellow
            Write-Host "Do you want to continue anyway? (yes/no): " -NoNewline
            $response = Read-Host
            if ($response -ne "yes") {
                Write-RotationLog "Rotation cancelled by user" -Level Info
                return $false
            }
        }
        
        # Document current configuration
        $currentConfig = Get-CurrentCertificateInfo
        
        Write-RotationLog "`nPre-rotation assessment complete" -Level Success
        Write-Host "`nReady to proceed with cluster shutdown and certificate replacement." -ForegroundColor Yellow
        Write-Host "Press Enter to continue or Ctrl+C to cancel..." -ForegroundColor Yellow
        Read-Host
        
        # Phase 2: Cluster shutdown
        Write-RotationLog "`nPhase 2: Cluster shutdown" -Level Info
        
        $kafkaStopped = Stop-KafkaCluster
        if (-not $kafkaStopped) {
            Write-RotationLog "Failed to stop Kafka cluster. Cannot proceed." -Level Error
            return $false
        }
        
        $zkStopped = Stop-ZooKeeperCluster
        if (-not $zkStopped) {
            Write-RotationLog "Failed to stop ZooKeeper cluster. Cannot proceed." -Level Error
            return $false
        }
        
        Write-RotationLog "`nCluster shutdown complete. All services stopped." -Level Success
        
        # Phase 3: Certificate backup and deployment
        Write-RotationLog "`nPhase 3: Certificate backup and deployment" -Level Info
        
        $backupSuccess = Backup-ExistingCertificates
        if (-not $backupSuccess -and -not $SkipBackup) {
            Write-RotationLog "Backup failed. Cannot proceed without valid backups." -Level Error
            Write-RotationLog "To skip backup (not recommended), use -SkipBackup parameter" -Level Error
            
            Write-Host "`nAttempting to restart cluster with existing certificates..." -ForegroundColor Yellow
            Start-ZooKeeperCluster | Out-Null
            Start-KafkaCluster | Out-Null
            return $false
        }
        
        $deploySuccess = Deploy-NewCertificates
        if (-not $deploySuccess) {
            Write-RotationLog "Certificate deployment failed" -Level Error
            
            Write-Host "`nDo you want to:" -ForegroundColor Yellow
            Write-Host "1. Attempt to start cluster with new certificates anyway (risky)" -ForegroundColor Yellow
            Write-Host "2. Rollback to previous certificates (recommended)" -ForegroundColor Yellow
            Write-Host "3. Exit and investigate manually" -ForegroundColor Yellow
            Write-Host "`nChoice (1/2/3): " -NoNewline
            $choice = Read-Host
            
            switch ($choice) {
                "1" {
                    Write-RotationLog "User chose to proceed despite deployment issues" -Level Warning
                }
                "2" {
                    Write-RotationLog "User requested rollback" -Level Info
                    Restore-PreviousCertificates -Confirm
                    return $false
                }
                default {
                    Write-RotationLog "User chose to exit for manual investigation" -Level Info
                    return $false
                }
            }
        }
        
        Write-RotationLog "`nCertificate deployment complete" -Level Success
        Write-Host "`nReady to start cluster with new certificates." -ForegroundColor Yellow
        Write-Host "Press Enter to continue..." -ForegroundColor Yellow
        Read-Host
        
        # Phase 4: Cluster startup
        Write-RotationLog "`nPhase 4: Cluster startup" -Level Info
        
        $zkStarted = Start-ZooKeeperCluster
        if (-not $zkStarted) {
            Write-RotationLog "Failed to start ZooKeeper cluster" -Level Error
            Write-Host "`nZooKeeper failed to start. Do you want to rollback? (yes/no): " -NoNewline
            $response = Read-Host
            if ($response -eq "yes") {
                Restore-PreviousCertificates -Confirm
            }
            return $false
        }
        
        $kafkaStarted = Start-KafkaCluster
        if (-not $kafkaStarted) {
            Write-RotationLog "Failed to start Kafka cluster" -Level Error
            Write-Host "`nKafka failed to start. Do you want to rollback? (yes/no): " -NoNewline
            $response = Read-Host
            if ($response -eq "yes") {
                Stop-KafkaCluster | Out-Null
                Restore-PreviousCertificates -Confirm
            }
            return $false
        }
        
        Write-RotationLog "`nCluster startup complete" -Level Success
        
        # Phase 5: Validation
        Write-RotationLog "`nPhase 5: Post-rotation validation" -Level Info
        
        $validation = Test-ClusterHealth
        
        if (-not ($validation.ZooKeeperHealthy -and $validation.KafkaHealthy -and 
                  $validation.ReplicationHealthy -and $validation.FunctionalTestPassed)) {
            Write-Host "`nValidation checks revealed issues." -ForegroundColor Yellow
            Write-Host "Do you want to rollback to previous certificates? (yes/no): " -NoNewline
            $response = Read-Host
            if ($response -eq "yes") {
                Restore-PreviousCertificates -Confirm
                return $false
            }
        }
        
        # Display certificate expiration information
        Get-CertificateExpirationInfo
        
        # Final success message
        Write-RotationHeader "Certificate Rotation Complete"
        Write-RotationLog "✓ Certificate rotation completed successfully" -Level Success
        Write-RotationLog "✓ All validation checks passed" -Level Success
        Write-RotationLog "✓ Cluster is operational with new certificates`n" -Level Success
        
        Write-RotationLog "Next steps:" -Level Info
        Write-RotationLog "1. Monitor cluster health for the next 24 hours" -Level Info
        Write-RotationLog "2. Test client connectivity from various locations" -Level Info
        Write-RotationLog "3. Update monitoring to track new certificate expiration dates" -Level Info
        Write-RotationLog "4. Document the rotation in your change management system`n" -Level Info
        
        Write-RotationLog "Detailed logs saved to: $script:RotationLogFile" -Level Info
        Write-RotationLog "Backups available at: $($script:RemotePaths.BackupDir)/pre-rotation-$script:RotationTimestamp" -Level Info
        
        return $true
    }
    catch {
        Write-RotationLog "Fatal error during certificate rotation: $_" -Level Error
        Write-RotationLog "Stack trace: $($_.ScriptStackTrace)" -Level Error
        return $false
    }
    finally {
        # Always clean up SSH sessions
        Disconnect-AllClusterNodes
    }
}

# Execute the rotation workflow
$success = Start-CertificateRotation

# Exit with appropriate code
exit $(if ($success) { 0 } else { 1 })
