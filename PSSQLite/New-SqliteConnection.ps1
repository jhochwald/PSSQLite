function New-SQLiteConnection
{
   <#
         .SYNOPSIS
         Creates a SQLiteConnection to a SQLite data source
   
         .DESCRIPTION
         Creates a SQLiteConnection to a SQLite data source
   
         .PARAMETER DataSource
         SQLite Data Source to connect to.
   
         .PARAMETER Password
         Specifies A Secure String password to use in the SQLite connection string.
      
         SECURITY NOTE: If you use the -Debug switch, the connectionstring including plain text password will be sent to the debug stream.
   
         .PARAMETER ReadOnly
         If specified, open SQLite data source as read only
   
         .PARAMETER Open
         We open the connection by default.  You can use this parameter to create a connection without opening it.
   
         .PARAMETER Additional
         A description of the Additional parameter.
   
         .EXAMPLE
         $Connection = New-SQLiteConnection -DataSource C:\NAMES.SQLite
         Invoke-SQLiteQuery -SQLiteConnection $Connection -query $Query
      
         # Connect to C:\NAMES.SQLite, invoke a query against it
   
         .EXAMPLE
         $Connection = New-SQLiteConnection -DataSource :MEMORY:
         Invoke-SqliteQuery -SQLiteConnection $Connection -Query "CREATE TABLE OrdersToNames (OrderID INT PRIMARY KEY, fullname TEXT);"
         Invoke-SqliteQuery -SQLiteConnection $Connection -Query "INSERT INTO OrdersToNames (OrderID, fullname) VALUES (1,'Cookie Monster');"
         Invoke-SqliteQuery -SQLiteConnection $Connection -Query "PRAGMA STATS"
      
         # Create a connection to a SQLite data source in memory
         # Create a table in the memory based datasource, verify it exists with PRAGMA STATS
      
         $Connection.Close()
         $Connection.Open()
         Invoke-SqliteQuery -SQLiteConnection $Connection -Query "PRAGMA STATS"
      
         #Close the connection, open it back up, verify that the ephemeral data no longer exists
   
         .OUTPUTS
         System.Data.SQLite.SQLiteConnection
   
         .NOTES
         Additional information about the function.
   
         .LINK
         Remove-SqliteConnection

         .LINK
         https://github.com/RamblingCookieMonster/Invoke-SQLiteQuery
   
         .LINK
         Invoke-SQLiteQuery
   
         .FUNCTIONALITY
         SQL
   #>
   [CmdletBinding(ConfirmImpact = 'None')]
   [OutputType([Data.SQLite.SQLiteConnection])]
   param
   (
      [Parameter(Mandatory,
            ValueFromPipeline,
            ValueFromPipelineByPropertyName,
            ValueFromRemainingArguments = $false,
            Position = 0,
      HelpMessage = 'SQL Server Instance required...')]
      [ValidateNotNullOrEmpty()]
      [Alias('Instance', 'Instances', 'ServerInstance', 'Server', 'Servers', 'cn', 'Path', 'File', 'FullName', 'Database')]
      [string[]]
      $DataSource,
      [Parameter(ValueFromPipeline,
            ValueFromPipelineByPropertyName,
            ValueFromRemainingArguments = $false,
      Position = 2)]
      [AllowNull()]
      [securestring]
      $Password,
      [Parameter(ValueFromPipeline,
            ValueFromPipelineByPropertyName,
            ValueFromRemainingArguments = $false,
      Position = 3)]
      [Switch]
      $ReadOnly,
      [Parameter(ValueFromPipeline,
            ValueFromPipelineByPropertyName,
            ValueFromRemainingArguments = $false,
      Position = 4)]
      [ValidateNotNullOrEmpty()]
      [bool]
      $Open = $True,
      [Parameter(ValueFromPipeline,
            ValueFromPipelineByPropertyName,
            ValueFromRemainingArguments = $false,
      Position = 5)]
      [AllowNull()]
      [Alias('AdditionalConnectionParams', 'ConnectionParams')]
      [string]
      $Additional
   )
   
   process
   {
      foreach ($DataSRC in $DataSource)
      {
         if ($DataSRC -match ':MEMORY:')
         {
            $Database = $DataSRC
         }
         else
         {
            $Database = ($ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath($DataSRC))
         }
         
         Write-Verbose -Message ("Querying Data Source '{0}'" -f $Database)
         
         # The basic connection string we would like to use
         [string]$ConnectionString = ('Data Source={0}; Version = 3;' -f $Database)
         
         if ($Password)
         {
            $BSTR = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($Password)
            $PlainPassword = [Runtime.InteropServices.Marshal]::PtrToStringAuto($BSTR)
            $ConnectionString += ('Password={0};' -f $PlainPassword)
         }
         
         if ($ReadOnly)
         {
            $ConnectionString += ' Read Only=True;'
         }
         
         if ($Additional)
         {
            $ConnectionString += $Additional
         }
         
         # Append the default we would like to use
         $ConnectionString += ' ForeignKeys = true; LegacyFormat = false; FailIfMissing = false;'
         
         $conn = (New-Object -TypeName System.Data.SQLite.SQLiteConnection -ArgumentList $ConnectionString)
         
         # Allow UNC paths, thanks to Ray Alex!
         $conn.ParseViaFramework = $True
         
         Write-Debug -Message ('ConnectionString {0}' -f $ConnectionString)
         
         if ($Open)
         {
            try
            {
               $conn.Open()
            }
            catch
            {
               Write-Error -Message $_
               continue
            }
         }
         
         Write-Verbose -Message ("Created SQLiteConnection:`n{0}" -f ($conn | Out-String))
         
         $conn
      }
   }
}
