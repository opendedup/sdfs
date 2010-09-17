# define the name of the installer
outfile "simple installer.exe"
 
# define the directory to install to, the desktop in this case as specified  
# by the predefined $DESKTOP variable
installDir $PROGRAMFILES
 
# default section
section
 
# define the output path for this file
setOutPath $INSTDIR
 
# define what to install and place it in the output path
file /oname=sdfs sdfs-bin
 
sectionEnd