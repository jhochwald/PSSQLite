function Out-DataTable1
{
   <#
         .SYNOPSIS
         Creates a DataTable for an object
   
         .DESCRIPTION
         Creates a DataTable based on an object's properties.
   
         .PARAMETER InputObject
         One or more objects to convert into a DataTable
   
         .PARAMETER NonNullable
         A list of columns to set disable AllowDBNull on
   
         .EXAMPLE
         $dt = Get-psdrive | Out-DataTable
      
         # This example creates a DataTable from the properties of Get-psdrive and assigns output to $dt variable
   
         .EXAMPLE
         Get-Process | Select Name, CPU | Out-DataTable | Invoke-SQLBulkCopy -ServerInstance $SQLInstance -Database $Database -Table $SQLTable -force -verbose
      
         # Get a list of processes and their CPU, create a datatable, bulk import that data
   
         .OUTPUTS
         System.Data.DataTable
   
         .NOTES
         Adapted from script by Marc van Orsouw and function from Chad Miller
         Version History
         v1.0  - Chad Miller - Initial Release
         v1.1  - Chad Miller - Fixed Issue with Properties
         v1.2  - Chad Miller - Added setting column datatype by property as suggested by emp0
         v1.3  - Chad Miller - Corrected issue with setting datatype on empty properties
         v1.4  - Chad Miller - Corrected issue with DBNull
         v1.5  - Chad Miller - Updated example
         v1.6  - Chad Miller - Added column datatype logic with default to string
         v1.7  - Chad Miller - Fixed issue with IsArray
         v1.8  - ramblingcookiemonster - Removed if($Value) logic.  This would not catch empty strings, zero, $false and other non-null items
         - Added perhaps pointless error handling
   
         .INPUTS
         Object
         Any object can be piped to Out-DataTable
   
         .LINK
         https://github.com/RamblingCookieMonster/PowerShell
   
         .LINK
         Invoke-SQLBulkCopy
   
         .LINK
         Invoke-Sqlcmd2
   
         .LINK
         New-SQLConnection
   
         .FUNCTIONALITY
         SQL
   #>
   
   [CmdletBinding(ConfirmImpact = 'None')]
   [OutputType([Data.DataTable])]
   param
   (
      [Parameter(Mandatory,
                 ValueFromPipeline,
                 ValueFromPipelineByPropertyName,
                 ValueFromRemainingArguments = $true,
                 Position = 0,
                 HelpMessage = 'Add help message for user')]
      [ValidateNotNullOrEmpty()]
      [PSObject[]]
      $InputObject,
      [Parameter(ValueFromPipeline,
                 ValueFromPipelineByPropertyName,
                 ValueFromRemainingArguments = $true)]
      [AllowEmptyCollection()]
      [AllowEmptyString()]
      [AllowNull()]
      [string[]]
      $NonNullable = @()
   )
   
   begin
   {
      $dt = (New-Object -TypeName Data.datatable)
      $First = $true
      
      function Get-ODTType
      {
         [CmdletBinding(ConfirmImpact = 'None')]
         param ($type)
         
         $types = @(
            'System.Boolean',
            'System.Byte[]',
            'System.Byte',
            'System.Char',
            'System.Datetime',
            'System.Decimal',
            'System.Double',
            'System.Guid',
            'System.Int16',
            'System.Int32',
            'System.Int64',
            'System.Single',
            'System.UInt16',
            'System.UInt32',
            'System.UInt64')
         
         if ($types -contains $type)
         {
            Write-Output -InputObject ('{0}' -f $type)
         }
         else
         {
            Write-Output -InputObject 'System.String'
         }
      }
   }
   
   process
   {
      foreach ($Object in $InputObject)
      {
         $DR = $dt.NewRow()
         
         foreach ($Property in $Object.PsObject.Properties)
         {
            $Name = $Property.Name
            $Value = $Property.Value
            
            # RCM: what if the first property is not reflective of all the properties?  Unlikely, but...
            if ($First)
            {
               $Col = (New-Object -TypeName Data.DataColumn)
               $Col.ColumnName = $Name
               
               # If it's not DBNull or Null, get the type
               if ($Value -isnot [DBNull] -and $Value -ne $null)
               {
                  $Col.DataType = [type]::GetType($(Get-ODTType -type $Property.TypeNameOfValue))
               }
               
               # Set it to nonnullable if specified
               if ($NonNullable -contains $Name)
               {
                  $Col.AllowDBNull = $false
               }
               
               try
               {
                  $dt.Columns.Add($Col)
               }
               catch
               {
                  Write-Error -Message ("Could not add column {0} for property '{1}' with value '{2}' and type '{3}':`n{4}" -f ($Col | Out-String), $Name, $Value, $Value.GetType().FullName, $_)
               }
            }
            
            try
            {
               # Handle arrays and nulls
               if ($Property.GetType().IsArray)
               {
                  $DR.Item($Name) = ($Value | ConvertTo-Xml -As String -NoTypeInformation -Depth 1)
               }
               elseif ($Value -eq $null)
               {
                  $DR.Item($Name) = [DBNull]::Value
               }
               else
               {
                  $DR.Item($Name) = $Value
               }
            }
            catch
            {
               Write-Error -Message ("Could not add property '{0}' with value '{1}' and type '{2}'" -f $Name, $Value, $Value.GetType().FullName) -ErrorAction Continue
               continue
            }
            
            # Did we get a null or dbnull for a non-nullable item?  let the user know.
            if ($NonNullable -contains $Name -and ($Value -is [DBNull] -or $Value -eq $null))
            {
               Write-Verbose -Message ("NonNullable property '{0}' with null value found: {1}" -f $Name, ($Object | Out-String))
            }
         }
         
         try
         {
            $dt.Rows.Add($DR)
         }
         catch
         {
            Write-Error -Message ("Failed to add row '{0}':`n{1}" -f ($DR | Out-String), $_)
         }
         
         $First = $false
      }
   }
   
   end
   {
      Write-Output -InputObject @( ,$dt)
   }
}

function Out-DataTable2
{
   <#
         .SYNOPSIS
         Creates a DataTable for an object

         .DESCRIPTION
         Creates a DataTable based on an object's properties.

         .PARAMETER InputObject
         One or more objects to convert into a DataTable

         .PARAMETER NonNullable
         A list of columns to set disable AllowDBNull on

         .INPUTS
         Object
         Any object can be piped to Out-DataTable

         .OUTPUTS
         System.Data.DataTable

         .EXAMPLE
         $dt = Get-psdrive | Out-DataTable
    
         # This example creates a DataTable from the properties of Get-psdrive and assigns output to $dt variable

         .EXAMPLE
         Get-Process | Select Name, CPU | Out-DataTable | Invoke-SQLBulkCopy -ServerInstance $SQLInstance -Database $Database -Table $SQLTable -force -verbose

         # Get a list of processes and their CPU, create a datatable, bulk import that data

         .NOTES
         Adapted from script by Marc van Orsouw and function from Chad Miller
         Version History
         v1.0  - Chad Miller - Initial Release
         v1.1  - Chad Miller - Fixed Issue with Properties
         v1.2  - Chad Miller - Added setting column datatype by property as suggested by emp0
         v1.3  - Chad Miller - Corrected issue with setting datatype on empty properties
         v1.4  - Chad Miller - Corrected issue with DBNull
         v1.5  - Chad Miller - Updated example
         v1.6  - Chad Miller - Added column datatype logic with default to string
         v1.7  - Chad Miller - Fixed issue with IsArray
         v1.8  - ramblingcookiemonster - Removed if($Value) logic.  This would not catch empty strings, zero, $false and other non-null items
         - Added perhaps pointless error handling

         .LINK
         https://github.com/RamblingCookieMonster/PowerShell

         .LINK
         Invoke-SQLBulkCopy

         .LINK
         Invoke-Sqlcmd2

         .LINK
         New-SQLConnection

         .FUNCTIONALITY
         SQL
   #>
   [CmdletBinding(ConfirmImpact = 'None')]
   [OutputType([Data.DataTable])]
   param(
      [Parameter( Position=0,HelpMessage='Add help message for user',
            Mandatory,
      ValueFromPipeline)]
      [PSObject[]]$InputObject,

      [string[]]$NonNullable = @()
   )

   Begin
   {
      $dt = (New-Object -TypeName Data.datatable  )
      $First = $true 

      function Get-ODTType
      {
         [CmdletBinding(ConfirmImpact = 'None')]   
         param($type)

         begin
         {
            $types = @(
               'System.Boolean',
               'System.Byte[]',
               'System.Byte',
               'System.Char',
               'System.Datetime',
               'System.Decimal',
               'System.Double',
               'System.Guid',
               'System.Int16',
               'System.Int32',
               'System.Int64',
               'System.Single',
               'System.UInt16',
               'System.UInt32',
            'System.UInt64')
         }
           
         process
         {
            if ( $types -contains $type ) {
               Write-Output -InputObject ('{0}' -f $type)
            }
            else {
               Write-Output -InputObject 'System.String'
            }
         }
      }
    }

    Process
    {
        foreach ($Object in $InputObject)
        {
            $DR = $DT.NewRow()

            foreach ($Property in $Object.PsObject.Properties)
            {
                $Name = $Property.Name
                $Value = $Property.Value
                
                # RCM: what if the first property is not reflective of all the properties?  Unlikely, but...
                if ($First)
                {
                    $Col = (New-Object -TypeName Data.DataColumn  )
                    $Col.ColumnName = $Name  
                    
                    #If it's not DBNull or Null, get the type
                    if ($Value -isnot [DBNull] -and $Value -ne $null)
                    {
                        $Col.DataType = ([type]::GetType( $(Get-ODTType -type $property.TypeNameOfValue) ))
                    }
                    
                    #Set it to nonnullable if specified
                    if ($NonNullable -contains $Name )
                    {
                        $col.AllowDBNull = $false
                    }

                    try
                    {
                        $DT.Columns.Add($Col)
                    }
                    catch
                    {
                        Write-Error -Message ("Could not add column {0} for property '{1}' with value '{2}' and type '{3}':`n{4}" -f ($Col | Out-String), $Name, $Value, $Value.GetType().FullName, $_)
                    }
                }  
                
                Try
                {
                    # Handle arrays and nulls
                    if ($property.GetType().IsArray)
                    {
                        $DR.Item($Name) = ($Value | ConvertTo-XML -As String -NoTypeInformation -Depth 1)
                    }
                    elseif($Value -eq $null)
                    {
                        $DR.Item($Name) = [DBNull]::Value
                    }
                    else
                    {
                        $DR.Item($Name) = $Value
                    }
                }
                Catch
                {
                    Write-Error -Message ("Could not add property '{0}' with value '{1}' and type '{2}'" -f $Name, $Value, $Value.GetType().FullName) -ErrorAction Continue

                    continue
                }

                # Did we get a null or dbnull for a non-nullable item?  let the user know.
                if($NonNullable -contains $Name -and ($Value -is [DBNull] -or $Value -eq $null))
                {
                    write-verbose -Message ("NonNullable property '{0}' with null value found: {1}" -f $Name, ($object | out-string))
                }

            } 

            Try
            {
                $DT.Rows.Add($DR)
            }
            Catch
            {
                Write-Error -Message ("Failed to add row '{0}':`n{1}" -f ($DR | Out-String), $_)
            }

            $First = $false
        }
    } 
     
    End
    {
        Write-Output -InputObject @(,$dt)
    }
}

function Out-DataTable
{
<#
.SYNOPSIS
    Creates a DataTable for an object

.DESCRIPTION
    Creates a DataTable based on an object's properties.

.PARAMETER InputObject
    One or more objects to convert into a DataTable

.PARAMETER NonNullable
    A list of columns to set disable AllowDBNull on

.INPUTS
    Object
        Any object can be piped to Out-DataTable

.OUTPUTS
   System.Data.DataTable

.EXAMPLE
    $dt = Get-psdrive | Out-DataTable
    
    # This example creates a DataTable from the properties of Get-psdrive and assigns output to $dt variable

.EXAMPLE
    Get-Process | Select Name, CPU | Out-DataTable | Invoke-SQLBulkCopy -ServerInstance $SQLInstance -Database $Database -Table $SQLTable -force -verbose

    # Get a list of processes and their CPU, create a datatable, bulk import that data

.NOTES
    Adapted from script by Marc van Orsouw and function from Chad Miller
    Version History
    v1.0  - Chad Miller - Initial Release
    v1.1  - Chad Miller - Fixed Issue with Properties
    v1.2  - Chad Miller - Added setting column datatype by property as suggested by emp0
    v1.3  - Chad Miller - Corrected issue with setting datatype on empty properties
    v1.4  - Chad Miller - Corrected issue with DBNull
    v1.5  - Chad Miller - Updated example
    v1.6  - Chad Miller - Added column datatype logic with default to string
    v1.7  - Chad Miller - Fixed issue with IsArray
    v1.8  - ramblingcookiemonster - Removed if($Value) logic.  This would not catch empty strings, zero, $false and other non-null items
                                  - Added perhaps pointless error handling

.LINK
    https://github.com/RamblingCookieMonster/PowerShell

.LINK
    Invoke-SQLBulkCopy

.LINK
    Invoke-Sqlcmd2

.LINK
    New-SQLConnection

.FUNCTIONALITY
    SQL
#>
    [CmdletBinding()]
    [OutputType([System.Data.DataTable])]
    param(
        [Parameter( Position=0,
                    Mandatory=$true,
                    ValueFromPipeline = $true)]
        [PSObject[]]$InputObject,

        [string[]]$NonNullable = @()
    )

    Begin
    {
        $dt = New-Object Data.datatable  
        $First = $true 

        function Get-ODTType
        {
            param($type)

            $types = @(
                'System.Boolean',
                'System.Byte[]',
                'System.Byte',
                'System.Char',
                'System.Datetime',
                'System.Decimal',
                'System.Double',
                'System.Guid',
                'System.Int16',
                'System.Int32',
                'System.Int64',
                'System.Single',
                'System.UInt16',
                'System.UInt32',
                'System.UInt64')

            if ( $types -contains $type ) {
                Write-Output "$type"
            }
            else {
                Write-Output 'System.String'
            }
        } #Get-Type
    }
    Process
    {
        foreach ($Object in $InputObject)
        {
            $DR = $DT.NewRow()  
            foreach ($Property in $Object.PsObject.Properties)
            {
                $Name = $Property.Name
                $Value = $Property.Value
                
                #RCM: what if the first property is not reflective of all the properties?  Unlikely, but...
                if ($First)
                {
                    $Col = New-Object Data.DataColumn  
                    $Col.ColumnName = $Name  
                    
                    #If it's not DBNull or Null, get the type
                    if ($Value -isnot [System.DBNull] -and $Value -ne $null)
                    {
                        $Col.DataType = [System.Type]::GetType( $(Get-ODTType $property.TypeNameOfValue) )
                    }
                    
                    #Set it to nonnullable if specified
                    if ($NonNullable -contains $Name )
                    {
                        $col.AllowDBNull = $false
                    }

                    try
                    {
                        $DT.Columns.Add($Col)
                    }
                    catch
                    {
                        Write-Error "Could not add column $($Col | Out-String) for property '$Name' with value '$Value' and type '$($Value.GetType().FullName)':`n$_"
                    }
                }  
                
                Try
                {
                    #Handle arrays and nulls
                    if ($property.GetType().IsArray)
                    {
                        $DR.Item($Name) = $Value | ConvertTo-XML -As String -NoTypeInformation -Depth 1
                    }
                    elseif($Value -eq $null)
                    {
                        $DR.Item($Name) = [DBNull]::Value
                    }
                    else
                    {
                        $DR.Item($Name) = $Value
                    }
                }
                Catch
                {
                    Write-Error "Could not add property '$Name' with value '$Value' and type '$($Value.GetType().FullName)'"
                    continue
                }

                #Did we get a null or dbnull for a non-nullable item?  let the user know.
                if($NonNullable -contains $Name -and ($Value -is [System.DBNull] -or $Value -eq $null))
                {
                    write-verbose "NonNullable property '$Name' with null value found: $($object | out-string)"
                }

            } 

            Try
            {
                $DT.Rows.Add($DR)
            }
            Catch
            {
                Write-Error "Failed to add row '$($DR | Out-String)':`n$_"
            }

            $First = $false
        }
    } 
     
    End
    {
        Write-Output @(,$dt)
    }

} #Out-DataTable