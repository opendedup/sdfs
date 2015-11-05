@echo off
bin\jre7\bin\java -Djava.library.path=bin\ -classpath lib\sdfs.jar;lib\* org.opendedup.sdfs.mgmt.cli.SDFSCmdline %*