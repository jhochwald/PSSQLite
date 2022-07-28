function Invoke-SqliteQuery
{
   <#
         .SYNOPSIS
         Runs a SQL script against a SQLite database.

         .DESCRIPTION
         Runs a SQL script against a SQLite database.

         Paramaterized queries are supported.

         Help details below borrowed from Invoke-Sqlcmd, may be inaccurate here.

         .PARAMETER DataSource
         Path to one or more SQLite data sources to query

         .PARAMETER Query
         Specifies a query to be run.

         .PARAMETER InputFile
         Specifies a file to be used as the query input to Invoke-SqliteQuery. Specify the full path to the file.

         .PARAMETER QueryTimeout
         Specifies the number of seconds before the queries time out.

         .PARAMETER As
         Specifies output type - DataSet, DataTable, array of DataRow, PSObject or Single Value

         PSObject output introduces overhead but adds flexibility for working with results: http://powershell.org/wp/forums/topic/dealing-with-dbnull/

         .PARAMETER SqlParameters
         Hashtable of parameters for parameterized SQL queries.  http://blog.codinghorror.com/give-me-parameterized-sql-or-give-me-death/

         Limited support for conversions to SQLite friendly formats is supported.
         For example, if you pass in a .NET DateTime, we convert it to a string that SQLite will recognize as a datetime

         Example:
         -Query "SELECT ServerName FROM tblServerInfo WHERE ServerName LIKE @ServerName"
         -SqlParameters @{"ServerName = "c-is-hyperv-1"}

         .PARAMETER SQLiteConnection
         An existing SQLiteConnection to use.  We do not close this connection upon completed query.

         .PARAMETER AppendDataSource
         If specified, append the SQLite data source path to PSObject or DataRow output

         .INPUTS
         DataSource
         You can pipe DataSource paths to Invoke-SQLiteQuery.  The query will execute against each Data Source.

         .OUTPUTS
         As PSObject:     System.Management.Automation.PSCustomObject
         As DataRow:      System.Data.DataRow
         As DataTable:    System.Data.DataTable
         As DataSet:      System.Data.DataTableCollectionSystem.Data.DataSet
         As SingleValue:  Dependent on data type in first column.

         .EXAMPLE

         #
         # First, we create a database and a table
         $Query = "CREATE TABLE NAMES (fullname VARCHAR(20) PRIMARY KEY, surname TEXT, givenname TEXT, BirthDate DATETIME)"
         $Database = "C:\Names.SQLite"

         Invoke-SqliteQuery -Query $Query -DataSource $Database

         # We have a database, and a table, let's view the table info
         Invoke-SqliteQuery -DataSource $Database -Query "PRAGMA table_info(NAMES)"

         cid name      type         notnull dflt_value pk
         --- ----      ----         ------- ---------- --
         0 fullname  VARCHAR(20)        0             1
         1 surname   TEXT               0             0
         2 givenname TEXT               0             0
         3 BirthDate DATETIME           0             0

         # Insert some data, use parameters for the fullname and birthdate
         $query = "INSERT INTO NAMES (fullname, surname, givenname, birthdate) VALUES (@full, 'Cookie', 'Monster', @BD)"
         Invoke-SqliteQuery -DataSource $Database -Query $query -SqlParameters @{
         full = "Cookie Monster"
         BD   = (get-date).addyears(-3)
         }

         # Check to see if we inserted the data:
         Invoke-SqliteQuery -DataSource $Database -Query "SELECT * FROM NAMES"

         fullname       surname givenname BirthDate
         --------       ------- --------- ---------
         Cookie Monster Cookie  Monster   3/14/2012 12:27:13 PM

         # Insert another entry with too many characters in the fullname.
         # Illustrate that SQLite data types may be misleading:
         Invoke-SqliteQuery -DataSource $Database -Query $query -SqlParameters @{
         full = "Cookie Monster$('!' * 20)"
         BD   = (get-date).addyears(-3)
         }

         Invoke-SqliteQuery -DataSource $Database -Query "SELECT * FROM NAMES"

         fullname              surname givenname BirthDate
         --------              ------- --------- ---------
         Cookie Monster        Cookie  Monster   3/14/2012 12:27:13 PM
         Cookie Monster![...]! Cookie  Monster   3/14/2012 12:29:32 PM

         .EXAMPLE
         Invoke-SqliteQuery -DataSource C:\NAMES.SQLite -Query "SELECT * FROM NAMES" -AppendDataSource

         fullname       surname givenname BirthDate             Database
         --------       ------- --------- ---------             --------
         Cookie Monster Cookie  Monster   3/14/2012 12:55:55 PM C:\Names.SQLite

         # Append Database column (path) to each result

         .EXAMPLE
         Invoke-SqliteQuery -DataSource C:\Names.SQLite -InputFile C:\Query.sql

         # Invoke SQL from an input file

         .EXAMPLE
         $Connection = New-SQLiteConnection -DataSource :MEMORY:
         Invoke-SqliteQuery -SQLiteConnection $Connection -Query "CREATE TABLE OrdersToNames (OrderID INT PRIMARY KEY, fullname TEXT);"
         Invoke-SqliteQuery -SQLiteConnection $Connection -Query "INSERT INTO OrdersToNames (OrderID, fullname) VALUES (1,'Cookie Monster');"
         Invoke-SqliteQuery -SQLiteConnection $Connection -Query "PRAGMA STATS"

         # Execute a query against an existing SQLiteConnection
         # Create a connection to a SQLite data source in memory
         # Create a table in the memory based datasource, verify it exists with PRAGMA STATS

         .EXAMPLE
         $Connection = New-SQLiteConnection -DataSource :MEMORY:
         Invoke-SqliteQuery -SQLiteConnection $Connection -Query "CREATE TABLE OrdersToNames (OrderID INT PRIMARY KEY, fullname TEXT);"
         Invoke-SqliteQuery -SQLiteConnection $Connection -Query "INSERT INTO OrdersToNames (OrderID, fullname) VALUES (1,'Cookie Monster');"
         Invoke-SqliteQuery -SQLiteConnection $Connection -Query "INSERT INTO OrdersToNames (OrderID) VALUES (2);"

         # We now have two entries, only one has a fullname.  Despite this, the following command returns both; very un-PowerShell!
         Invoke-SqliteQuery -SQLiteConnection $Connection -Query "SELECT * FROM OrdersToNames" -As DataRow | Where{$_.fullname}

         OrderID fullname
         ------- --------
         1 Cookie Monster
         2

         # Using the default -As PSObject, we can get PowerShell-esque behavior:
         Invoke-SqliteQuery -SQLiteConnection $Connection -Query "SELECT * FROM OrdersToNames" | Where{$_.fullname}

         OrderID fullname
         ------- --------
         1 Cookie Monster

         .LINK
         https://github.com/RamblingCookieMonster/Invoke-SQLiteQuery

         .LINK
         New-SQLiteConnection

         .LINK
         Invoke-SQLiteBulkCopy

         .LINK
         Out-DataTable

         .LINK
         https://www.sqlite.org/datatype3.html

         .LINK
         https://www.sqlite.org/lang.html

         .LINK
         http://www.sqlite.org/pragma.html

         .FUNCTIONALITY
         SQL
   #>
   
   [CmdletBinding(DefaultParameterSetName = 'Src-Que')]
   [OutputType([Management.Automation.PSCustomObject], [Data.DataRow], [Data.DataTable], [Data.DataTableCollection], [Data.DataSet])]
   param (
      [Parameter(ParameterSetName = 'Src-Que',
                 Position = 0,
                 Mandatory,
                 ValueFromPipeline,
                 ValueFromPipelineByPropertyName,
                 ValueFromRemainingArguments = $false,
                 HelpMessage = 'SQLite Data Source required...')]
      [Parameter(ParameterSetName = 'Src-Fil',
                 Position = 0,
                 Mandatory,
                 ValueFromPipeline,
                 ValueFromPipelineByPropertyName,
                 ValueFromRemainingArguments = $false,
                 HelpMessage = 'SQLite Data Source required...')]
      [Alias('Path', 'File', 'FullName', 'Database')]
      [validatescript({
            #This should match memory, or the parent path should exist
            $Parent = Split-Path -Path $_ -Parent
            if (
               $_ -match ':MEMORY:|^WHAT$' -or
               ($Parent -and (Test-Path -Path $Parent))
            )
            {
               $True
            }
            else
            {
               throw ("Invalid datasource '{0}'.`nThis must match :MEMORY:, or '{1}' must exist" -f $_, $Parent)
            }
         })]
      [string[]]
      $DataSource,
      [Parameter(ParameterSetName = 'Src-Que', HelpMessage = 'Add help message for user',
                 Position = 1,
                 Mandatory,
                 ValueFromPipelineByPropertyName,
                 ValueFromRemainingArguments = $false)]
      [Parameter(ParameterSetName = 'Con-Que',
                 Position = 1,
                 Mandatory,
                 ValueFromPipelineByPropertyName,
                 ValueFromRemainingArguments = $false)]
      [string]
      $Query,
      [Parameter(ParameterSetName = 'Src-Fil', HelpMessage = 'Add help message for user',
                 Position = 1,
                 Mandatory,
                 ValueFromPipelineByPropertyName,
                 ValueFromRemainingArguments = $false)]
      [Parameter(ParameterSetName = 'Con-Fil',
                 Position = 1,
                 Mandatory,
                 ValueFromPipelineByPropertyName,
                 ValueFromRemainingArguments = $false)]
      [ValidateScript({
            Test-Path -Path $_
         })]
      [string]
      $InputFile,
      [Parameter(Position = 2,
                 ValueFromPipelineByPropertyName,
                 ValueFromRemainingArguments = $false)]
      [int]
      $QueryTimeout = 600,
      [Parameter(Position = 3,
                 ValueFromPipelineByPropertyName,
                 ValueFromRemainingArguments = $false)]
      [ValidateSet('DataSet', 'DataTable', 'DataRow', 'PSObject', 'SingleValue')]
      [string]
      $As = 'PSObject',
      [Parameter(Position = 4,
                 ValueFromPipelineByPropertyName,
                 ValueFromRemainingArguments = $false)]
      [Collections.IDictionary]
      $SqlParameters,
      [Parameter(Position = 5)]
      [switch]
      $AppendDataSource,
      [Parameter(Position = 6)]
      [validatescript({
            Test-Path -Path $_
         })]
      [string]
      $AssemblyPath = $SQLiteAssembly,
      [Parameter(ParameterSetName = 'Con-Que', HelpMessage = 'Add help message for user',
                 Position = 7,
                 Mandatory,
                 ValueFromPipelineByPropertyName,
                 ValueFromRemainingArguments = $false)]
      [Parameter(ParameterSetName = 'Con-Fil',
                 Position = 7,
                 Mandatory,
                 ValueFromPipelineByPropertyName,
                 ValueFromRemainingArguments = $false)]
      [Alias('Connection', 'Conn')]
      [Data.SQLite.SQLiteConnection]
      $SQLiteConnection
   )
   
   begin
   {
      # Assembly, should already be covered by psm1
      try
      {
         $null = [Data.SQLite.SQLiteConnection]
      }
      catch
      {
         if (-not ($Library = Add-Type -Path $SQLiteAssembly -PassThru -ErrorAction Stop))
         {
            throw "This module requires the ADO.NET driver for SQLite:`n`thttp://system.data.sqlite.org/index.html/doc/trunk/www/downloads.wiki"
         }
      }
      
      if ($PSBoundParameters.ContainsKey('InputFile'))
      {
         $filePath = ($(Resolve-Path -Path $InputFile).path)
         $Query = [IO.File]::ReadAllText("$filePath")
         
         Write-Verbose -Message ('Extracted query from [{0}]' -f $InputFile)
      }
      
      Write-Verbose -Message ("Running Invoke-SQLiteQuery with ParameterSet '{0}'.  Performing query '{1}'" -f $PSCmdlet.ParameterSetName, $Query)
      
      if ($As -eq 'PSObject')
      {
         #This code scrubs DBNulls.  Props to Dave Wyatt
         $cSharp = @'
                using System;
                using System.Data;
                using System.Management.Automation;

                public class DBNullScrubber
                {
                    public static PSObject DataRowToPSObject(DataRow row)
                    {
                        PSObject psObject = new PSObject();

                        if (row != null && (row.RowState & DataRowState.Detached) != DataRowState.Detached)
                        {
                            foreach (DataColumn column in row.Table.Columns)
                            {
                                Object value = null;
                                if (!row.IsNull(column))
                                {
                                    value = row[column];
                                }

                                psObject.Properties.Add(new PSNoteProperty(column.ColumnName, value));
                            }
                        }

                        return psObject;
                    }
                }
'@
         
         try
         {
            if ($PSEdition -eq 'Core')
            {
               # Core doesn't auto-load these assemblies unlike desktop?
               # Not csharp coder, unsure why
               # by fffnite
               $Ref = @(
                  'System.Data.Common'
                  'System.Management.Automation'
                  'System.ComponentModel.TypeConverter'
               )
            }
            else
            {
               $Ref = @(
                  'System.Data'
                  'System.Xml'
               )
            }
            
            $null = (Add-Type -TypeDefinition $cSharp -ReferencedAssemblies $Ref -ErrorAction Stop)
         }
         catch
         {
            if (-not $_.ToString() -like "*The type name 'DBNullScrubber' already exists*")
            {
               Write-Warning -Message ('Could not load DBNullScrubber.  Defaulting to DataRow output: {0}' -f $_)
               
               $As = 'Datarow'
            }
         }
      }
      
      # Handle existing connections
      if ($PSBoundParameters.Keys -contains 'SQLiteConnection')
      {
         if ($SQLiteConnection.State -notlike 'Open')
         {
            try
            {
               $SQLiteConnection.Open()
            }
            catch
            {
               throw $_
            }
         }
         
         if ($SQLiteConnection.state -notlike 'Open')
         {
            throw ("SQLiteConnection is not open:`n{0}" -f ($SQLiteConnection | Out-String))
         }
         
         $DataSource = @('WHAT')
      }
   }
   
   process
   {
      foreach ($DB in $DataSource)
      {
         if ($PSBoundParameters.Keys -contains 'SQLiteConnection')
         {
            $Conn = $SQLiteConnection
         }
         else
         {
            # Resolve the path entered for the database to a proper path name.
            # This accounts for a variaty of possible ways to provide a path, but
            # in the end the connection string needs a fully qualified file path.
            if ($DB -match ':MEMORY:')
            {
               $Database = $DB
            }
            else
            {
               $Database = $ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath($DB)
            }
            
            if (Test-Path -Path $Database)
            {
               Write-Verbose -Message ("Querying existing Data Source '{0}'" -f $Database)
            }
            else
            {
               Write-Verbose -Message ("Creating andn querying Data Source '{0}'" -f $Database)
            }
            
            $ConnectionString = 'Data Source={0}' -f $Database
            
            $Conn = (New-Object -TypeName System.Data.SQLite.SQLiteConnection -ArgumentList $ConnectionString)
            # Allow UNC paths, thanks to Ray Alex!
            $Conn.ParseViaFramework = $True
            
            Write-Debug -Message ('ConnectionString {0}' -f $ConnectionString)
            
            try
            {
               $Conn.Open()
            }
            catch
            {
               Write-Error -Message $_ -ErrorAction Continue
               continue
            }
         }
         
         $cmd = $Conn.CreateCommand()
         $cmd.CommandText = $Query
         $cmd.CommandTimeout = $QueryTimeout
         
         if ($SqlParameters -ne $null)
         {
            $SqlParameters.GetEnumerator() | ForEach-Object -Process {
               if ($_.Value -ne $null)
               {
                  if ($_.Value -is [datetime])
                  {
                     $_.Value = $_.Value.ToString('yyyy-MM-dd HH:mm:ss')
                  }
                  $cmd.Parameters.AddWithValue("@$($_.Key)", $_.Value)
               }
               else
               {
                  $cmd.Parameters.AddWithValue("@$($_.Key)", [DBNull]::Value)
               }
            } > $null
         }
         
         $ds = (New-Object -TypeName system.Data.DataSet)
         $da = (New-Object -TypeName System.Data.SQLite.SQLiteDataAdapter -ArgumentList ($cmd))
         
         try
         {
            $null = $da.fill($ds)
            
            if ($PSBoundParameters.Keys -notcontains 'SQLiteConnection')
            {
               $Conn.Close()
            }
            
            $cmd.Dispose()
         }
         catch
         {
            $Err = $_
            
            if ($PSBoundParameters.Keys -notcontains 'SQLiteConnection')
            {
               $Conn.Close()
            }
            
            switch ($ErrorActionPreference.tostring())
            {
               {
                  'SilentlyContinue', 'Ignore' -contains $_
               }
               {
                  
               }
               'Stop'
               {
                  throw $Err
               }
               'Continue'
               {
                  Write-Error -Message $Err
               }
               Default
               {
                  Write-Error -Message $Err
               }
            }
         }
         
         if ($AppendDataSource)
         {
            # Basics from Chad Miller
            $Column = (New-Object -TypeName Data.DataColumn)
            $Column.ColumnName = 'Datasource'
            $ds.Tables[0].Columns.Add($Column)
            
            try
            {
               # Someone better at regular expression, feel free to tackle this
               $Conn.ConnectionString -match 'Data Source=(?<DataSource>.*);'
               $Datasrc = $Matches.DataSource.split(';')[0]
            }
            catch
            {
               $Datasrc = $DB
            }
            
            foreach ($row in $ds.Tables[0])
            {
               $row.Datasource = $Datasrc
            }
         }
         
         switch ($As)
         {
            'DataSet'
            {
               $ds
            }
            'DataTable'
            {
               $ds.Tables
            }
            'DataRow'
            {
               $ds.Tables[0]
            }
            'PSObject'
            {
               #Scrub DBNulls - Provides convenient results you can use comparisons with
               #Introduces overhead (e.g. ~2000 rows w/ ~80 columns went from .15 Seconds to .65 Seconds - depending on your data could be much more!)
               foreach ($row in $ds.Tables[0].Rows)
               {
                  [DBNullScrubber]::DataRowToPSObject($row)
               }
            }
            'SingleValue'
            {
               ($ds.Tables[0] | Select-Object -ExpandProperty $ds.Tables[0].Columns[0].ColumnName)
            }
         }
      }
   }
}
