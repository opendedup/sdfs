; MUI Settings 
; MUI Settings / Icons
; Sets the theme path


!define VERSION '2.0.13'
!define MUI_PRODUCT "SDFS Cloud File System"


;NSIS Modern User Interface
;Basic Example Script
;Written by Joost Verburg

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
;--------------------------------
;General
  ;Name and file
  Name "SDFS Cloud File System ${VERSION}"
  OutFile "SDFS-${VERSION}-Setup.exe"

  ;Default installation folder
  InstallDir $PROGRAMFILES64\sdfs
  

  ;Request application privileges for Windows Vista
  RequestExecutionLevel admin
  BrandingText "${MUI_PRODUCT} ${VERSION}"

;--------------------------------
;Interface Settings

  !define MUI_ABORTWARNING

;--------------------------------
;Pages

  !insertmacro MUI_PAGE_LICENSE "datishlicense.txt"
  !insertmacro MUI_PAGE_COMPONENTS
  !insertmacro MUI_PAGE_DIRECTORY
  !insertmacro MUI_PAGE_INSTFILES
  
  !insertmacro MUI_UNPAGE_CONFIRM
  !insertmacro MUI_UNPAGE_INSTFILES
  
;--------------------------------
;Languages
 
  !insertmacro MUI_LANGUAGE "English"


;--------------------------------
;Installer Sections

!macro GetVC++
  ClearErrors
  ReadRegDWORD $0 HKLM "SOFTWARE\Microsoft\VisualStudio\12.0\VC\Runtimes\x64\" "Installed"
  IfErrors 0 NewVCplusplus
  Pop $0
  DetailPrint "Pausing installation while downloaded VC++ installer runs."
  DetailPrint "Installation could take several minutes to complete."
  ExecWait '$INSTDIR\vcredist_x64.exe /passive /norestart'
		
  NewVCplusplus:
	  Pop $7
	  Pop $6
	  Pop $5
	  Pop $4
	  Pop $3
	  Pop $2
	  Pop $1
	  Pop $0
!macroend

Function .onInit
 ${If} ${RunningX64}
    ${If} ${IsWin2008R2}
    ${ElseIf} ${IsWin7}
	${ElseIf} ${IsWin2012}
	${ElseIf} ${IsWin8}
	${ElseIf} ${IsWin2012R2}
	${ElseIf} ${IsWin8.1}
    ${Else}
      MessageBox MB_OK "Your OS is not supported. ${MUI_PRODUCT} supports Windows 2008R2, 7, 2012, 8, 2012R2, 8.1 for x64."
      Abort
    ${EndIf}
  ${Else}
	MessageBox MB_OK "Your OS is not supported. ${MUI_PRODUCT} supports Windows 2008R2, 7, 2012, 8, 2012R2, 8.1 for x64."
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
  File lib\*
  SetOutPath "$INSTDIR\etc"
  File etc\*
  ;Store installation folder
  WriteRegStr HKLM "Software\SDFS" "path" $INSTDIR
  !insertmacro GetVC++

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
	ExecWait '"$INSTDIR\DokanInstall_0.7.4.exe"'
SectionEnd
Section "-Quick Start Guide"
	ExecShell "open" "http://www.opendedup.org/wqs"
SectionEnd


Function .onInstSuccess
  IfSilent noreboot
    MessageBox MB_YESNO "A reboot is required to finish the uninstallation. Do you wish to reboot now?" IDNO noreboot
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
  DeleteRegKey HKLM "Software\SDFS"
  DeleteRegKey HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\SDFS"
SectionEnd
