; MUI Settings 
; MUI Settings / Icons
; Sets the theme path

Unicode True
!define VERSION '3.12.0'
!define JARVERISON 'master'

!define MUI_PRODUCT "SDFS Cloud File System"

;--------------------------------
;Include Modern UI
!include LogicLib.nsh
!include x64.nsh
!include "MUI2.nsh"
!include WinVer.nsh
!include "MUI2.nsh"

!define MUI_ICON "sdfs.ico"
!define MUI_HEADERIMAGE
!define MUI_HEADERIMAGE_BITMAP "sdfs.ico"
!define MUI_HEADERIMAGE_RIGHT
!define SF_USELECTED  0

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
  

!macro SecUnSelect SecId
  Push $0
  IntOp $0 ${SF_USELECTED} | ${SF_RO}
  SectionSetFlags ${SecId} $0
  SectionSetText  ${SecId} ""
  Pop $0
!macroend

!define UnSelectSection '!insertmacro SecUnSelect'




;--------------------------------

;--------------------------------
;Interface Settings

  !define MUI_ABORTWARNING

;--------------------------------
;Pages

  !insertmacro MUI_PAGE_COMPONENTS
  !insertmacro MUI_PAGE_DIRECTORY
  !insertmacro MUI_PAGE_INSTFILES
  
  !insertmacro MUI_UNPAGE_CONFIRM
  !insertmacro MUI_UNPAGE_INSTFILES
  
;--------------------------------
;Languages
 
  !insertmacro MUI_LANGUAGE "English"
  !addplugindir "C:\Program Files (x86)\NSIS\Plugins\amd64-unicode"

  ;--------------------------------
  ;Version Information
  VIProductVersion "3.12.0.0"
  VIAddVersionKey /LANG=${LANG_ENGLISH} "ProductName" "OpenDedupe SDFS"
  VIAddVersionKey /LANG=${LANG_ENGLISH} "Comments" "A Cloud Deduplication FileSystem"
  VIAddVersionKey /LANG=${LANG_ENGLISH} "CompanyName" "Datish Systems"
  VIAddVersionKey /LANG=${LANG_ENGLISH} "LegalCopyright" "Copyright Datish Systems LLC"
  VIAddVersionKey /LANG=${LANG_ENGLISH} "FileDescription" "SDFS Setup"
  VIAddVersionKey /LANG=${LANG_ENGLISH} "FileVersion" 3.12.0.0"
  VIAddVersionKey /LANG=${LANG_ENGLISH} "ProductVersion" "3.12.0.0"
;--------------------------------
;Installer Sections





Section "SDFS Setup" SecMain
  SetOutPath "$INSTDIR"
  SectionIn RO
  File *
  SetOutPath "$INSTDIR\bin"
  File /r bin\*
  SetOutPath "$INSTDIR\lib"
  File /oname=sdfs.jar ..\..\target\sdfs-${JARVERISON}.jar
  File ..\..\target\lib\*.jar
  SetOutPath "$INSTDIR\etc"
  File etc\*
  ;Store installation folder
  WriteRegStr HKLM "Software\SDFS" "path" $INSTDIR
  EnVar::SetHKLM
  ; Add a value
  EnVar::AddValue "path" "$INSTDIR"
  Pop $0
  DetailPrint "EnVar::AddValue returned=|$0|"
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

Section "Visual Studio Runtime"
  SetOutPath "$INSTDIR"
  ExecWait '"$INSTDIR\VC_redist.x64.exe" /quiet'
  Delete "$INSTDIR\VC_redist.x64.exe"
SectionEnd


Function .onInstSuccess
  IfSilent noreboot
    MessageBox MB_OK "You have successfully installed ${MUI_PRODUCT}."
  noreboot:
FunctionEnd
 
Function un.onUninstSuccess
  MessageBox MB_OK "You have successfully uninstalled ${MUI_PRODUCT}."
FunctionEnd
  
Function .onInit
 ${If} ${RunningX64}
    
  ${Else}
	MessageBox MB_OK "Your OS is not supported. ${MUI_PRODUCT} supports Windows for x64."
      SetErrorlevel 1
      Abort
  ${EndIf}
  ReadRegStr $0 HKLM "Software\SDFS" "path"
  IfErrors 0 +2
    Goto done
  ReadRegStr $INSTDIR HKLM "Software\SDFS" "path"
	MessageBox MB_YESNO "Upgrade Existing Setup to ${VERSION}?" IDNO noupgrade
	RMDir /r "$INSTDIR\bin"
	RMDir /r "$INSTDIR\lib"
	Goto done
  noupgrade:
  SetErrorlevel 1
	Quit
  done:
FunctionEnd
;--------------------------------
;Descriptions
;Language strings
  LangString DESC_SecMain ${LANG_ENGLISH} "SDFS Volume Binaries Setup."
;Assign language strings to sections
  !insertmacro MUI_FUNCTION_DESCRIPTION_BEGIN
    !insertmacro MUI_DESCRIPTION_TEXT ${SecMain} $(DESC_SecMain)
  !insertmacro MUI_FUNCTION_DESCRIPTION_END

;--------------------------------
;Uninstaller Section

Section "Uninstall"
  ;ADD YOUR OWN FILES HERE...
  RMDir /r "$INSTDIR\*.*"
  RMDir /r "$INSTDIR\bin"
  RMDir /r "$INSTDIR\lib"
  ReadRegStr $0 HKLM "Software\SDFS" "path"
  EnVar::SetHKLM
  EnVar::DeleteValue "path" "$0"
  DeleteRegKey HKLM "Software\SDFS"
  DeleteRegKey HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\SDFS"
SectionEnd