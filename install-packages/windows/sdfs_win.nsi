; MUI Settings 
; MUI Settings / Icons
; Sets the theme path


!define VERSION '3.5.7.0'

!define MUI_PRODUCT "SDFS Cloud File System"


;--------------------------------
;Include Modern UI
!include LogicLib.nsh
!include "EnvVarUpdate.nsh"
!include x64.nsh
!include "MUI2.nsh"
!include WinVer.nsh
!include "MUI2.nsh"

!define MUI_ICON "sdfs.ico"
!define MUI_HEADERIMAGE
!define MUI_HEADERIMAGE_BITMAP "sdfs.ico"
!define MUI_HEADERIMAGE_RIGHT
;--------------------------------
;General
  ;Name and file
  Name "SDFS Cloud File System ${VERSION}"
  OutFile "..\SDFS-${VERSION}-Setup.exe"

  ;Default installation folder
  InstallDir $PROGRAMFILES64\sdfs
  

  ;Request application privileges for Windows Vista
  RequestExecutionLevel admin
  BrandingText "${MUI_PRODUCT} ${VERSION}"
  


;--------------------------------

;--------------------------------
;Interface Settings

  !define MUI_ABORTWARNING

;--------------------------------
;Pages

  !insertmacro MUI_PAGE_LICENSE "gnulicense.txt"
  !insertmacro MUI_PAGE_COMPONENTS
  !insertmacro MUI_PAGE_DIRECTORY
  !insertmacro MUI_PAGE_INSTFILES
  
  !insertmacro MUI_UNPAGE_CONFIRM
  !insertmacro MUI_UNPAGE_INSTFILES
  
;--------------------------------
;Languages
 
  !insertmacro MUI_LANGUAGE "English"


  ;--------------------------------
;Version Information


  VIProductVersion "3.5.7.0"

  VIAddVersionKey /LANG=${LANG_ENGLISH} "ProductName" "OpenDedupe SDFS"
  VIAddVersionKey /LANG=${LANG_ENGLISH} "Comments" "A Cloud Deduplication FileSystem"
  VIAddVersionKey /LANG=${LANG_ENGLISH} "CompanyName" "Datish Systems"
  VIAddVersionKey /LANG=${LANG_ENGLISH} "LegalCopyright" "Copyright Datish Systems LLC"
  VIAddVersionKey /LANG=${LANG_ENGLISH} "FileDescription" "SDFS Setup"
  VIAddVersionKey /LANG=${LANG_ENGLISH} "FileVersion" "3.5.7.0"
  VIAddVersionKey /LANG=${LANG_ENGLISH} "ProductVersion" "3.5.7.0"
;--------------------------------
;Installer Sections

Function .onInit
 ${If} ${RunningX64}
    
  ${Else}
	MessageBox MB_OK "Your OS is not supported. ${MUI_PRODUCT} supports Windows for x64."
      Abort
  ${EndIf}
FunctionEnd


Section "SDFS Setup" SecMain
  
  SetOutPath "$INSTDIR"
  SectionIn RO
  File *
  SetOutPath "$INSTDIR\bin"
  File /r bin\*
  SetOutPath "$INSTDIR\lib"
  File ..\..\target\sdfs-${VERSION}.jar
  File ..\..\target\lib\*
  SetOutPath "$INSTDIR\etc"
  File etc\*
  ;Store installation folder
  WriteRegStr HKLM "Software\SDFS" "path" $INSTDIR
  ${EnvVarUpdate} $0 "PATH" "A" "HKLM" $INSTDIR 
  ;Create uninstaller
  WriteUninstaller "$INSTDIR\Uninstall.exe"

  ; Write the uninstall keys for Windows
  WriteRegStr HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\SDFS" "DisplayName" "SDFS ${VERSION}  (remove only)"
  WriteRegStr HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\SDFS" "UninstallString" '"$INSTDIR\Uninstall.exe"'
  WriteRegDWORD HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\SDFS" "NoModify" 1
  WriteRegDWORD HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\SDFS" "NoRepair" 1
  AccessControl::GrantOnFile \
    "$INSTDIR" "(BU)" "GenericRead + GenericWrite"
SectionEnd
Section "Dokan Setup" SecDokan
	ExecWait '"$INSTDIR\DokanSetup_redist.exe'
SectionEnd
Section "-Quick Start Guide"
	ExecShell "open" "http://opendedup.org/odd/windows-quickstart/"
SectionEnd


Function .onInstSuccess
  IfSilent noreboot
    MessageBox MB_YESNO "A reboot is required to finish the installation. Do you wish to reboot now?" IDNO noreboot
    Reboot
  noreboot:
FunctionEnd
 
Function un.onUninstSuccess
  MessageBox MB_OK "You have successfully uninstalled ${MUI_PRODUCT}."
FunctionEnd
  

;--------------------------------
;Descriptions

  ;Language strings
  LangString DESC_SecMain ${LANG_ENGLISH} "SDFS Volume Binaries Setup."
  LangString DESC_SecDokan ${LANG_ENGLISH} "Dokan Windows FileSystem Driver."
  ;Assign language strings to sections
  !insertmacro MUI_FUNCTION_DESCRIPTION_BEGIN
    !insertmacro MUI_DESCRIPTION_TEXT ${SecMain} $(DESC_SecMain)
    !insertmacro MUI_DESCRIPTION_TEXT ${SecDokan} $(DESC_SecDokan)
  !insertmacro MUI_FUNCTION_DESCRIPTION_END

;--------------------------------
;Uninstaller Section

Section "Uninstall"
  ;ADD YOUR OWN FILES HERE...
  RMDir /r "$INSTDIR\*.*"
  RMDir /r "$INSTDIR\bin"
  RMDir /r "$INSTDIR\lib"
  ReadRegStr $0 HKLM "Software\SDFS" "path"
  ${un.EnvVarUpdate} $0 "PATH" "R" "HKLM" $INSTDIR
  DeleteRegKey HKLM "Software\SDFS"
  DeleteRegKey HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\SDFS"
SectionEnd