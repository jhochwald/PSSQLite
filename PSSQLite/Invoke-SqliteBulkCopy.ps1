function Invoke-SQLiteBulkCopy
{
   <#
         .SYNOPSIS
         Use a SQLite transaction to quickly insert data
   
         .DESCRIPTION
         Use a SQLite transaction to quickly insert data.  If we run into any errors, we roll back the transaction.
      
         The data source is not limited to SQL Server; any data source can be used, as long as the data can be loaded to a DataTable instance or read with a IDataReader instance.
   
         .PARAMETER DataTable
         Add help message for user
   
         .PARAMETER DataSource
         Path to one ore more SQLite data sources to query
   
         .PARAMETER SQLiteConnection
         An existing SQLiteConnection to use.  We do not close this connection upon completed query.
   
         .PARAMETER Table
         Add help message for user
   
         .PARAMETER ConflictClause
         The conflict clause to use in case a conflict occurs during insert. Valid values: Rollback, Abort, Fail, Ignore, Replace
      
         See https://www.sqlite.org/lang_conflict.html for more details
   
         .PARAMETER NotifyAfter
         The number of rows to fire the notification event after transferring.  0 means don't notify.  Notifications hit the verbose stream (use -verbose to see them)
   
         .PARAMETER Force
         If specified, skip the confirm prompt
   
         .PARAMETER QueryTimeout
         Specifies the number of seconds before the queries time out.
   
         .EXAMPLE
         #
         #Create a table
         Invoke-SqliteQuery -DataSource "C:\Names.SQLite" -Query "CREATE TABLE NAMES (
         fullname VARCHAR(20) PRIMARY KEY,
         surname TEXT,
         givenname TEXT,
         BirthDate DATETIME)"
      
         #Build up some fake data to bulk insert, convert it to a datatable
         $DataTable = 1..10000 | %{
         [pscustomobject]@{
         fullname = "Name $_"
         surname = "Name"
         givenname = "$_"
         BirthDate = (Get-Date).Adddays(-$_)
         }
         } | Out-DataTable
      
         #Copy the data in within a single transaction (SQLite is faster this way)
         Invoke-SQLiteBulkCopy -DataTable $DataTable -DataSource $Database -Table Names -NotifyAfter 1000 -ConflictClause Ignore -Verbose
   
         .OUTPUTS
         None
         Produces no output
   
         .NOTES
         This function borrows from:
         Chad Miller's Write-Datatable
         jbs534's Invoke-SQLBulkCopy
         Mike Shepard's Invoke-BulkCopy from SQLPSX
   
         .INPUTS
         System.Data.DataTable
   
         .LINK
         https://github.com/RamblingCookieMonster/Invoke-SQLiteQuery
   
         .LINK
         New-SQLiteConnection
   
         .LINK
         Invoke-SQLiteBulkCopy
   
         .LINK
         Out-DataTable
   
         .FUNCTIONALITY
         SQL
   #>
   
   [CmdletBinding(DefaultParameterSetName = 'Datasource',
         ConfirmImpact = 'Medium',
   SupportsShouldProcess)]
   param
   (
      [Parameter(Mandatory,
            ValueFromPipeline,
            ValueFromPipelineByPropertyName,
            ValueFromRemainingArguments = $true,
            Position = 0,
      HelpMessage = 'Add help message for user')]
      [ValidateNotNullOrEmpty()]
      [Data.DataTable]
      $DataTable,
      [Parameter(ParameterSetName = 'Datasource',
            Mandatory,
            ValueFromPipeline,
            ValueFromPipelineByPropertyName,
            ValueFromRemainingArguments = $false,
            Position = 1,
      HelpMessage = 'SQLite Data Source required...')]
      [ValidateScript({
               #This should match memory, or the parent path should exist
               if ($_ -match ':MEMORY:' -or (Test-Path -Path $_))
               {
                  $true
               }
               else
               {
                  throw ("Invalid datasource '{0}'.`nThis must match :MEMORY:, or must exist" -f $_)
               }
      })]
      [ValidateNotNullOrEmpty()]
      [Alias('Path', 'File', 'FullName', 'Database')]
      [string]
      $DataSource,
      [Parameter(ParameterSetName = 'Connection',
            Mandatory,
            ValueFromPipeline,
            ValueFromPipelineByPropertyName,
            ValueFromRemainingArguments = $false,
            Position = 1,
      HelpMessage = 'Add help message for user')]
      [ValidateNotNullOrEmpty()]
      [Alias('Connection', 'Conn')]
      [Data.SQLite.SQLiteConnection]
      $SQLiteConnection,
      [Parameter(Mandatory,
            ValueFromPipeline,
            ValueFromPipelineByPropertyName,
            ValueFromRemainingArguments = $true,
            Position = 2,
      HelpMessage = 'Add help message for user')]
      [ValidateNotNullOrEmpty()]
      [string]
      $Table,
      [Parameter(ValueFromPipeline,
            ValueFromPipelineByPropertyName,
            ValueFromRemainingArguments = $false,
      Position = 3)]
      [ValidateSet('Rollback', 'Abort', 'Fail', 'Ignore', 'Replace')]
      [AllowNull()]
      [string]
      $ConflictClause,
      [int]
      $NotifyAfter = 0,
      [switch]
      $Force,
      [int]
      $QueryTimeout = 600
   )
   
   begin
   {
      Write-Verbose -Message ("Running Invoke-SQLiteBulkCopy with ParameterSet '{0}'." -f $PSCmdlet.ParameterSetName)
      
      function CleanUp
      {
         <#
               .SYNOPSIS
               Describe purpose of "CleanUp" in 1-2 sentences.
   
               .DESCRIPTION
               Add a more complete description of what the function does.
   
               .PARAMETER conn
               Describe parameter -conn.
   
               .PARAMETER com
               Describe parameter -com.
   
               .PARAMETER BoundParams
               Describe parameter -BoundParams.
   
               .EXAMPLE
               CleanUp -conn Value -com Value -BoundParams Value
               Describe what this call does
   
               .OUTPUTS
               List of output types produced by this function.
   
               .NOTES
               Place additional notes here.
   
               .LINK
               URLs to related sites
               The first link is opened by Get-Help -Online CleanUp
   
               .INPUTS
               List of input types that are accepted by this function.
         #>
         [CmdletBinding(ConfirmImpact = 'None')]
         param
         (
            [Parameter(ValueFromPipeline,
                  ValueFromPipelineByPropertyName,
            ValueFromRemainingArguments = $true)]
            $conn,
            [Parameter(ValueFromPipeline,
                  ValueFromPipelineByPropertyName,
            ValueFromRemainingArguments = $true)]
            $com,
            $BoundParams
         )
         
         process
         {
            # Only dispose of the connection if we created it
            if ($BoundParams.Keys -notcontains 'SQLiteConnection')
            {
               $conn.Close()
               $conn.Dispose()
               
               Write-Verbose -Message 'Closed connection'
            }
         }
         
         end
         {
            $com.Dispose()
         }
      }
      
      function Get-ParameterName
      {
         <#
               .SYNOPSIS
               Describe purpose of "Get-ParameterName" in 1-2 sentences.
   
               .DESCRIPTION
               Add a more complete description of what the function does.
   
               .PARAMETER InputObject
               Describe parameter -InputObject.
   
               .PARAMETER Regex
               Describe parameter -Regex.
   
               .PARAMETER Separator
               Describe parameter -Separator.
   
               .EXAMPLE
               Get-ParameterName -InputObject Value -Regex Value -Separator Value
               Describe what this call does
   
               .OUTPUTS
               List of output types produced by this function.
   
               .NOTES
               Place additional notes here.
   
               .LINK
               URLs to related sites
               The first link is opened by Get-Help -Online Get-ParameterName
   
               .INPUTS
               List of input types that are accepted by this function.
         #>
         
         [CmdletBinding(ConfirmImpact = 'None')]
         param
         (
            [Parameter(Mandatory,
                  ValueFromPipeline,
                  ValueFromPipelineByPropertyName,
                  ValueFromRemainingArguments = $true,
            HelpMessage = 'Add help message for user')]
            [ValidateNotNullOrEmpty()]
            [string[]]
            $InputObject,
            [Parameter(ValueFromPipeline,
                  ValueFromPipelineByPropertyName,
            ValueFromRemainingArguments = $true)]
            [AllowNull()]
            [string]
            $Regex = '(\W+)',
            [Parameter(ValueFromPipeline,
                  ValueFromPipelineByPropertyName,
            ValueFromRemainingArguments = $true)]
            [AllowNull()]
            [string]
            $Separator = '_'
         )
         
         process
         {
            $InputObject | ForEach-Object -Process {
               if ($_ -match $Regex)
               {
                  $Groups = @($_ -split $Regex | Where-Object -FilterScript {
                        $_
                  })
                  
                  for ($i = 0; $i -lt $Groups.Count; $i++)
                  {
                     if ($Groups[$i] -match $Regex)
                     {
                        $Groups[$i] = ($Groups[$i].ToCharArray() | ForEach-Object -Process {
                              [string][int]$_
                        }) -join $Separator
                     }
                  }
                  
                  $Groups -join $Separator
               }
               else
               {
                  $_
               }
            }
         }
      }
      
      function New-SqliteBulkQuery
      {
         <#
               .SYNOPSIS
               Describe purpose of "New-SqliteBulkQuery" in 1-2 sentences.
   
               .DESCRIPTION
               Add a more complete description of what the function does.
   
               .PARAMETER Table
               Describe parameter -Table.
   
               .PARAMETER Columns
               Describe parameter -Columns.
   
               .PARAMETER Parameters
               Describe parameter -Parameters.
   
               .PARAMETER ConflictClause
               Describe parameter -ConflictClause.
   
               .EXAMPLE
               New-SqliteBulkQuery -Table Value -Columns Value -Parameters Value -ConflictClause Value
               Describe what this call does
   
               .OUTPUTS
               List of output types produced by this function.
   
               .NOTES
               Place additional notes here.
   
               .LINK
               URLs to related sites
               The first link is opened by Get-Help -Online New-SqliteBulkQuery
   
               .INPUTS
               List of input types that are accepted by this function.
         #>
         
         [CmdletBinding(ConfirmImpact = 'None')]
         param
         (
            [Parameter(Mandatory,
                  ValueFromPipeline,
                  ValueFromPipelineByPropertyName,
                  ValueFromRemainingArguments = $true,
            HelpMessage = 'Add help message for user')]
            [ValidateNotNullOrEmpty()]
            [string]
            $Table,
            [Parameter(Mandatory,
                  ValueFromPipeline,
                  ValueFromPipelineByPropertyName,
                  ValueFromRemainingArguments = $true,
            HelpMessage = 'Add help message for user')]
            [ValidateNotNullOrEmpty()]
            [string[]]
            $Columns,
            [Parameter(Mandatory,
                  ValueFromPipeline,
                  ValueFromPipelineByPropertyName,
                  ValueFromRemainingArguments = $true,
            HelpMessage = 'Add help message for user')]
            [ValidateNotNullOrEmpty()]
            [string[]]
            $Parameters,
            [Parameter(ValueFromPipeline,
                  ValueFromPipelineByPropertyName,
            ValueFromRemainingArguments = $true)]
            [AllowNull()]
            [string]
            $ConflictClause = ''
         )
         
         begin
         {
            $EscapeSingleQuote = "'", "''"
            $Delimeter = ', '
            $QueryTemplate = 'INSERT{0} INTO {1} ({2}) VALUES ({3})'
         }
         
         process
         {
            $fmtConflictClause = if ($ConflictClause)
            {
               " OR $ConflictClause"
            }
            
            $fmtTable = "'{0}'" -f ($Table -replace $EscapeSingleQuote)
            $fmtColumns = ($Columns | ForEach-Object -Process {
                  "'{0}'" -f ($_ -replace $EscapeSingleQuote)
            }) -join $Delimeter
            $fmtParameters = ($Parameters | ForEach-Object -Process {
                  "@$_"
            }) -join $Delimeter
            $QueryTemplate -f $fmtConflictClause, $fmtTable, $fmtColumns, $fmtParameters
         }
      }
   }
   
   process
   {
      # Connections
      if ($PSBoundParameters.Keys -notcontains 'SQLiteConnection')
      {
         if ($DataSource -match ':MEMORY:')
         {
            $Database = $DataSource
         }
         else
         {
            $Database = $ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath($DataSource)
         }
         
         $ConnectionString = 'Data Source={0}' -f $Database
         $SQLiteConnection = (New-Object -TypeName System.Data.SQLite.SQLiteConnection -ArgumentList $ConnectionString)
         $SQLiteConnection.ParseViaFramework = $true #Allow UNC paths, thanks to Ray Alex!
      }
      
      Write-Debug -Message ('ConnectionString {0}' -f $SQLiteConnection.ConnectionString)
      
      try
      {
         if ($SQLiteConnection.State -notlike 'Open')
         {
            $SQLiteConnection.Open()
         }
         $Command = $SQLiteConnection.CreateCommand()
         $CommandTimeout = $QueryTimeout
         $Transaction = $SQLiteConnection.BeginTransaction()
      }
      catch
      {
         throw $_
      }
      
      Write-Verbose -Message ('DATATABLE IS {0} with value {1}' -f $DataTable.gettype().fullname, ($DataTable | Out-String))
      
      $RowCount = $DataTable.Rows.Count
      
      Write-Verbose -Message ('Processing datatable with {0} rows' -f $RowCount)
      
      if ($Force -or $PSCmdlet.ShouldProcess("$($DataTable.Rows.Count) rows, with BoundParameters $($PSBoundParameters | Out-String)", 'SQL Bulk Copy'))
      {
         #Get column info...
         [array]$Columns = ($DataTable.Columns | Select-Object -ExpandProperty ColumnName)
         $ColumnTypeHash = @{
         }
         $ColumnToParamHash = @{
         }
         $Index = 0
         
         foreach ($Col in $DataTable.Columns)
         {
            $Type = switch -regex ($Col.DataType.FullName)
            {
               # I figure we create a hashtable, can act upon expected data when doing insert
               # Might be a better way to handle this...
               '^(|\ASystem\.)Boolean$'
               {
                  'BOOLEAN'
               } #I know they're fake...
               '^(|\ASystem\.)Byte\[\]'
               {
                  'BLOB'
               }
               '^(|\ASystem\.)Byte$'
               {
                  'BLOB'
               }
               '^(|\ASystem\.)Datetime$'
               {
                  'DATETIME'
               }
               '^(|\ASystem\.)Decimal$'
               {
                  'REAL'
               }
               '^(|\ASystem\.)Double$'
               {
                  'REAL'
               }
               '^(|\ASystem\.)Guid$'
               {
                  'TEXT'
               }
               '^(|\ASystem\.)Int16$'
               {
                  'INTEGER'
               }
               '^(|\ASystem\.)Int32$'
               {
                  'INTEGER'
               }
               '^(|\ASystem\.)Int64$'
               {
                  'INTEGER'
               }
               '^(|\ASystem\.)UInt16$'
               {
                  'INTEGER'
               }
               '^(|\ASystem\.)UInt32$'
               {
                  'INTEGER'
               }
               '^(|\ASystem\.)UInt64$'
               {
                  'INTEGER'
               }
               '^(|\ASystem\.)Single$'
               {
                  'REAL'
               }
               '^(|\ASystem\.)String$'
               {
                  'TEXT'
               }
               Default
               {
                  'BLOB'
               } #Let SQLite handle the rest...
            }
            
            #We ref columns by their index, so add that...
            $ColumnTypeHash.Add($Index, $Type)
            
            # Parameter names can only be alphanumeric: https://www.sqlite.org/c3ref/bind_blob.html
            # So we have to replace all non-alphanumeric chars in column name to use it as parameter later.
            # This builds hashtable to correlate column name with parameter name.
            $ColumnToParamHash.Add($Col.ColumnName, (Get-ParameterName -InputObject $Col.ColumnName))
            
            $Index++
         }
         
         #Build up the query
         if ($PSBoundParameters.ContainsKey('ConflictClause'))
         {
            $Command.CommandText = (New-SqliteBulkQuery -Table $Table -Columns $ColumnToParamHash.Keys -Parameters $ColumnToParamHash.Values -ConflictClause $ConflictClause)
         }
         else
         {
            $Command.CommandText = (New-SqliteBulkQuery -Table $Table -Columns $ColumnToParamHash.Keys -Parameters $ColumnToParamHash.Values)
         }
         
         foreach ($Column in $Columns)
         {
            $param = (New-Object -TypeName System.Data.SQLite.SqLiteParameter -ArgumentList $ColumnToParamHash[$Column])
            $null = $Command.Parameters.Add($param)
         }
         
         for ($RowNumber = 0; $RowNumber -lt $RowCount; $RowNumber++)
         {
            $row = $DataTable.Rows[$RowNumber]
            
            for ($Col = 0; $Col -lt $Columns.count; $Col++)
            {
               # Depending on the type of thid column, quote it
               # For dates, convert it to a string SQLite will recognize
               switch ($ColumnTypeHash[$Col])
               {
                  'BOOLEAN'
                  {
                     $Command.Parameters[$ColumnToParamHash[$Columns[$Col]]].Value = [int][bool]$row[$Col]
                  }
                  'DATETIME'
                  {
                     try
                     {
                        $Command.Parameters[$ColumnToParamHash[$Columns[$Col]]].Value = $row[$Col].ToString('yyyy-MM-dd HH:mm:ss')
                     }
                     catch
                     {
                        $Command.Parameters[$ColumnToParamHash[$Columns[$Col]]].Value = $row[$Col]
                     }
                  }
                  Default
                  {
                     $Command.Parameters[$ColumnToParamHash[$Columns[$Col]]].Value = $row[$Col]
                  }
               }
            }
            
            # We have the query, execute!
            try
            {
               $null = $Command.ExecuteNonQuery()
            }
            catch
            {
               # Minimal testing for this rollback...
               Write-Verbose -Message ("Rolling back due to error:`n{0}" -f $_)
               
               $Transaction.Rollback()
               
               #Clean up and throw an error
               CleanUp -conn $SQLiteConnection -com $Command -BoundParams $PSBoundParameters
               
               throw ("Rolled back due to error:`n{0}" -f $_)
            }
            
            if ($NotifyAfter -gt 0 -and $($RowNumber % $NotifyAfter) -eq 0)
            {
               Write-Verbose -Message "Processed $($RowNumber + 1) records"
            }
         }
      }
   }
   
   end
   {
      #Commit the transaction and clean up the connection
      $Transaction.Commit()
      CleanUp -conn $SQLiteConnection -com $Command -BoundParams $PSBoundParameters
   }
}
