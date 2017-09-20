// mountsdfs.cpp : Defines the entry point for the console application.
//

#include "stdafx.h"
#include <windows.h> 
#include <tchar.h>
#include <stdio.h> 
#include <vector>
#include <strsafe.h>
#include <iostream>
#include <string>
#include <sstream>
#include <process.h>
#include <tinyxml2.h>
#include <io.h>
#include <fcntl.h>
#include <cstdio>
#include <signal.h>
#include <comdef.h>

#define BUFSIZE 8192 

HANDLE g_hChildStd_IN_Rd = NULL;
HANDLE g_hChildStd_IN_Wr = NULL;
HANDLE g_hChildStd_OUT_Rd = NULL;
HANDLE hParentStdOut = NULL;
HANDLE g_hChildStd_OUT_Wr = NULL;
PROCESS_INFORMATION piProcInfo;
STARTUPINFO siStartInfo;
void CreateChildProcess(TCHAR *p);
void WriteToPipe(void);
void ReadFromPipe(void *param);
void ErrorExit(PTSTR);
bool threadFinished = false;
bool processCompleted = false;
bool dataWritten = false;
using namespace std;
using namespace tinyxml2;
bool mounted = false;
bool cpt = false;
int FileExists(TCHAR * file)
{
	WIN32_FIND_DATA FindFileData;
	HANDLE handle = FindFirstFile(file, &FindFileData);
	int found = handle != INVALID_HANDLE_VALUE;
	if (found)
	{
		//FindClose(&handle); this will crash
		FindClose(handle);
	}
	return found;
}

TCHAR * ReadRegValue(HKEY root, LPCSTR key, LPCSTR name)
{
	HKEY hKey;
	if (RegOpenKeyEx(root, key, 0, KEY_READ, &hKey) != ERROR_SUCCESS)
		return _T("");


	TCHAR szBuffer[512];
	DWORD dwBufferSize = sizeof(szBuffer);
	if (RegQueryValueExA(hKey, name, 0, NULL, (LPBYTE)szBuffer, &dwBufferSize) != ERROR_SUCCESS)
	{
		RegCloseKey(hKey);
		return _T("");
	}

	RegCloseKey(hKey);

	return szBuffer;
}

void signalHandler(int signum)
{
	//TerminateProcess(piProcInfo.hProcess, 0);
	//WaitForSingleObject(piProcInfo.hProcess, INFINITE);
	// cleanup and close up stuff here  
	// terminate program  

}

int _tmain(int argc, TCHAR *argv[])
{
	signal(SIGINT, signalHandler);

	SECURITY_ATTRIBUTES saAttr;

	//printf("\n->Start of parent execution.\n");

	// Set the bInheritHandle flag so pipe handles are inherited. 

	saAttr.nLength = sizeof(SECURITY_ATTRIBUTES);
	saAttr.bInheritHandle = TRUE;
	saAttr.lpSecurityDescriptor = NULL;

	// Create a pipe for the child process's STDOUT. 

	if (!CreatePipe(&g_hChildStd_OUT_Rd, &g_hChildStd_OUT_Wr, &saAttr, 0))
		ErrorExit(TEXT("StdoutRd CreatePipe"));

	// Ensure the read handle to the pipe for STDOUT is not inherited.

	if (!SetHandleInformation(g_hChildStd_OUT_Rd, HANDLE_FLAG_INHERIT, 0))
		ErrorExit(TEXT("Stdout SetHandleInformation"));

	// Create a pipe for the child process's STDIN. 

	if (!CreatePipe(&g_hChildStd_IN_Rd, &g_hChildStd_IN_Wr, &saAttr, 0))
		ErrorExit(TEXT("Stdin CreatePipe"));

	// Ensure the write handle to the pipe for STDIN is not inherited. 

	if (!SetHandleInformation(g_hChildStd_IN_Wr, HANDLE_FLAG_INHERIT, 0))
		ErrorExit(TEXT("Stdin SetHandleInformation"));
	TCHAR cmd[2048];
	TCHAR *val = ReadRegValue(HKEY_LOCAL_MACHINE, "SOFTWARE\\WOW6432Node\\SDFS", "path");
	
	TCHAR path[512];
	_tcscpy_s(path, val);
	//_tprintf("path=%s\n", path);
	bool mt = false;
	
	TCHAR configFile[512];
	__int64 mem = 256;
	__int64 basemem = 3000;
	for (int i = 1; i < argc; i++) {
		if (!_tcsncmp(argv[i], _T("-mem"), 4)) {
			mem = _ttoi(argv[i + 1]);
			break;
		}
		if (!_tcsncmp(argv[i], _T("-basemem"), 4)) {
			basemem = _ttoi(argv[i + 1]);
			break;
		}
		if (!_tcsncmp(argv[i], _T("-v"), 2)) {
			_tcscpy_s(configFile, val);
			_tcsncat_s(configFile, _T("\\etc\\"), 512);
			_tcsncat_s(configFile, argv[i + 1], 512);
			_tcsncat_s(configFile, _T("-volume-cfg.xml"), 512);
			mt = true;
		}
		if (!_tcsncmp(argv[i], _T("-vc"), 3)) {
			_tcscpy_s(configFile, argv[i + 1]);
			mt = true;
		}

	}
	if (mt && FileExists(configFile)) {
		//_tprintf("path=%s\n", configFile);
	}
	else if (mt) {
		_tprintf("path does not exist %s\n", configFile);
		exit(1);
	}
	if (mt) {
		HANDLE hFile = CreateFile(configFile,               // file to open
			GENERIC_READ, 0, 0,
			OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, 0);                 // no attr. template

		if (hFile == INVALID_HANDLE_VALUE)
		{
			_tprintf(TEXT("Terminal failure: unable to open file \"%s\" for read.\n"), configFile);
			return 1;
		}
		else {
			tinyxml2::XMLDocument doc(true, COLLAPSE_WHITESPACE);
			int file_descriptor = _open_osfhandle((intptr_t)hFile, _O_RDONLY);
			if (file_descriptor != -1) {
				FILE* file = _fdopen(file_descriptor, "r");
				std::string contents;
				std::fseek(file, 0, SEEK_END);
				contents.resize(std::ftell(file));
				std::rewind(file);
				std::fread(&contents[0], 1, contents.size(), file);
				std::fclose(file);
				//printf("array == %s\n", contents.c_str());
				doc.Parse(contents.c_str());
				if (doc.ErrorID() == 0) {
					string ssz = string(doc.FirstChildElement("subsystem-config")->FirstChildElement("local-chunkstore")->Attribute("allocation-size"));
					string bsz = string(doc.FirstChildElement("subsystem-config")->FirstChildElement("io")->Attribute("chunk-size"));
					string tsz = string(doc.FirstChildElement("subsystem-config")->FirstChildElement("io")->Attribute("write-threads"));
					string csz = string(doc.FirstChildElement("subsystem-config")->FirstChildElement("io")->Attribute("max-file-write-buffers"));
					bool lowm = false;
					if (doc.FirstChildElement("subsystem-config")->FirstChildElement("local-chunkstore")->Attribute("low-memory")) {
						string lm = string(doc.FirstChildElement("subsystem-config")->FirstChildElement("local-chunkstore")->Attribute("low-memory"));
						if (lm.compare("true") == 0) {
							lowm = true;
						}
					}
					std::stringstream sstr(ssz);
					std::stringstream bstr(bsz);
					std::stringstream tstr(tsz);
					std::stringstream cstr(csz);
					__int64 sz;
					__int64 bz;
					__int64 tz;
					__int64 cz;
					bstr >> bz;
					tstr >> tz;
					sstr >> sz;
					cstr >> cz;
					//long tt = (bz* tz*3)/1024;

					mem += basemem;
					if(cz < 512) {
						cz = 512;
					}
					mem += cz*3;
					//long gb = sz / (1073741824);
					//mem += .3 * gb;
					//cout << sz << " asz= " << gb << " mem=" << mem << "\n";
				}
				else {
					printf("XML Parsing Error ID: %s\n", doc.ErrorName());
					exit(1);
				}
			}
		}
	}
	TCHAR buf[300]; // where you put result
	_stprintf(buf, TEXT("%dM"), mem);
	std::string number;
	std::stringstream strstream;
	strstream << 1L;
	strstream >> number;
	_tcscpy_s(cmd, path);
	_tcsncat_s(cmd, _T("\\bin\\jre\\bin\\java.exe "), 2048);
	_tcsncat_s(cmd, _T("-Djava.library.path=\""), 2048);
	_tcsncat_s(cmd, path, 2048);
	_tcsncat_s(cmd, _T("\\bin/\"  -Xmx"), 2048);
	_tcsncat_s(cmd, buf, 2048);
	_tcsncat_s(cmd, _T(" -XX:+UseG1GC -Djava.awt.headless=true -server "), 2048);
	_tcsncat_s(cmd, _T("-cp \""), 2048);
	_tcsncat_s(cmd, path, 2048);
	_tcsncat_s(cmd, "\\lib\\*\" org.opendedup.sdfs.windows.fs.MountSDFS", 2048);

	for (int i = 1; i < argc; i++) {
		if (!_tcsncmp(argv[i], _T("-mem"), 4)) {
			i++;
		}
		else if (!_tcsncmp(argv[i], _T("-cp"), 3)) {
			cpt = true;
		}
		else {
			_tcsncat_s(cmd, _T(" "), 1024);
			_tcsncat_s(cmd, argv[i], 1024);
		}
	}
	CreateChildProcess(cmd);
	DWORD exit_code = 0;
	_beginthread(ReadFromPipe, 0, NULL);
	if (cpt)
	_tprintf("cp=%s", cmd);
	if (NULL != piProcInfo.hProcess)
	{
		while (!processCompleted) {
			WaitForSingleObject(piProcInfo.hProcess, 1500); // Change to 'INFINITE' wait if req'd
			if (FALSE == GetExitCodeProcess(piProcInfo.hProcess, &exit_code))
			{
				/*
				std::cerr << "GetExitCodeProcess() failure: " <<
				GetLastError() << "\n";
				*/
			}
			else if (STILL_ACTIVE == exit_code)
			{
				//std::cout << "Still running\n";
			}
			else
			{
				/*
				std::cout << "exit code=" << exit_code << "\n";
				*/
				break;
			}
		}
		CloseHandle(piProcInfo.hProcess);
		
	}

	// The remaining open handles are cleaned up when this process terminates. 
	// To avoid resource leaks in a larger application, close handles explicitly. 

	return exit_code;
}

void CreateChildProcess(TCHAR *p)
// Create a child process that uses the previously created pipes for STDIN and STDOUT.
{
	
	BOOL bSuccess = FALSE;

	// Set up members of the PROCESS_INFORMATION structure. 

	ZeroMemory(&piProcInfo, sizeof(PROCESS_INFORMATION));

	// Set up members of the STARTUPINFO structure. 
	// This structure specifies the STDIN and STDOUT handles for redirection.

	ZeroMemory(&siStartInfo, sizeof(STARTUPINFO));
	siStartInfo.cb = sizeof(STARTUPINFO);
	siStartInfo.hStdError = g_hChildStd_OUT_Wr;
	siStartInfo.hStdOutput = g_hChildStd_OUT_Wr;
	siStartInfo.hStdInput = g_hChildStd_IN_Rd;
	siStartInfo.dwFlags |= STARTF_USESTDHANDLES;

	// Create the child process. 
	DWORD dp = DETACHED_PROCESS;
	if (cpt)
		dp = 0;

	bSuccess = CreateProcess(NULL,
		p,     // command line 
		NULL,          // process security attributes 
		NULL,          // primary thread security attributes 
		TRUE,          // handles are inherited 
		dp,             // creation flags 
		NULL,          // use parent's environment 
		NULL,          // use parent's current directory 
		&siStartInfo,  // STARTUPINFO pointer 
		&piProcInfo);  // receives PROCESS_INFORMATION 

	// If an error occurs, exit the application. 
	if (!bSuccess)
		ErrorExit(TEXT("CreateProcess"));
	else
	{
		// Close handles to the child process and its primary thread.
		// Some applications might keep these handles to monitor the status
		// of the child process, for example. 

		//CloseHandle(piProcInfo.hProcess);
		//CloseHandle(piProcInfo.hThread);
	}
}

void ReadFromPipe(void *param)

// Read output from the child process's pipe for STDOUT
// and write to the parent process's pipe for STDOUT. 
// Stop when there is no more data. 
{
	DWORD dwRead, dwWritten;
	CHAR chBuf[BUFSIZE];
	BOOL bSuccess = FALSE;
	BOOL rSuccess = FALSE;
	hParentStdOut = GetStdHandle(STD_OUTPUT_HANDLE);

	while (!processCompleted)
	{
		rSuccess = ReadFile(g_hChildStd_OUT_Rd, chBuf, BUFSIZE, &dwRead, NULL);
		std::string contents = string(chBuf);
		if (!cpt && strcmp(chBuf, "volumemounted") > 0) {
			bSuccess = WriteFile(hParentStdOut, "volume mounted\n",
				dwRead, &dwWritten, NULL);
			bSuccess = WriteFile(hParentStdOut, "\n",
				dwRead, &dwWritten, NULL);
			processCompleted = true;
			break;
		}
		else {

			bSuccess = WriteFile(hParentStdOut, contents.c_str(),
				dwRead, &dwWritten, NULL);
		}
		if (!bSuccess) {
			processCompleted = true;
			break;
		}
		else {
			dataWritten = true;
		}
		if (!rSuccess || dwRead == 0) {
			processCompleted = true;
			break;
		}
	}
	threadFinished = true;
}

void ErrorExit(PTSTR lpszFunction)

// Format a readable error message, display a message box, 
// and exit from the application.
{
	
}
