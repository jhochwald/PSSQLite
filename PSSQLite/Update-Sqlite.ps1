function Update-Sqlite
{
   <#
      .SYNOPSIS
      Describe purpose of "Update-Sqlite" in 1-2 sentences.

      .DESCRIPTION
      Add a more complete description of what the function does.

      .PARAMETER version
      Describe parameter -version.

      .PARAMETER OS
      Describe parameter -OS.

      .EXAMPLE
      Update-Sqlite -version Value -OS Value
      Describe what this call does

      .NOTES
      Place additional notes here.

      .LINK
      URLs to related sites
      The first link is opened by Get-Help -Online Update-Sqlite

      .INPUTS
      List of input types that are accepted by this function.

      .OUTPUTS
      List of output types produced by this function.
   #>


   [CmdletBinding(ConfirmImpact = 'None')]

   param(
      [Parameter()]
      [string]
      $version = '1.0.112',
      [Parameter()]
      [ValidateSet('linux-x64','osx-x64','win-x64','win-x86')]
      [string]
      $OS
   )

   Process {
      Write-Verbose -Message 'Creating build directory'

      New-Item -ItemType directory -Path build
      Set-Location -Path build

      $file = ('system.data.sqlite.core.{0}' -f $version)

      Write-Verbose -Message 'downloading files from nuget'

      $dl = @{
         uri     = ('https://www.nuget.org/api/v2/package/System.Data.SQLite.Core/{0}' -f $version)
         outfile = ('{0}.nupkg' -f $file)
      }
      Invoke-WebRequest @dl

      Write-Verbose 'unpacking and copying files to module directory'

      Expand-Archive $dl.outfile
      $InstallPath = (Get-Module PSSQlite).path.TrimEnd('PSSQLite.psm1')
      Copy-Item $file/lib/netstandard2.0/System.Data.SQLite.dll -Destination $InstallPath/core/$OS/
      Copy-Item $file/runtimes/$OS/native/netstandard2.0/SQLite.Interop.dll -Destination $InstallPath/core/$OS/

      Write-Verbose 'removing build folder'

      Set-Location ..
      Remove-Item ./build -Recurse

      Write-Verbose 'complete'

      Write-Warning 'Please reimport the module to use the latest files'
   }
}
