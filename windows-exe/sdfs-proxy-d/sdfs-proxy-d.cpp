// mountsdfs.cpp : Defines the entry point for the console application.
//

#include <windows.h> 
#include <tchar.h>
#include <stdio.h> 
#include <vector>
#include <strsafe.h>
#include <iostream>
#include <string>
#include <sstream>
#include <process.h>
#include <io.h>
#include <fcntl.h>
#include <cstdio>
#include <signal.h>
#include <comdef.h>
#include <fstream>
#include <cstdlib>

#define BUFSIZE 8192 

HANDLE g_hChildStd_IN_Rd = NULL;
HANDLE g_hChildStd_IN_Wr = NULL;
HANDLE g_hChildStd_OUT_Rd = NULL;
HANDLE hParentStdOut = NULL;
HANDLE g_hChildStd_OUT_Wr = NULL;
PROCESS_INFORMATION piProcInfo;
STARTUPINFO siStartInfo;
void CreateChildProcess(TCHAR* p);
void WriteToPipe(void);
void ReadFromPipe(void* param);
void ErrorExit(PTSTR);
bool threadFinished = false;
bool processCompleted = false;
bool dataWritten = false;
using namespace std;
bool mounted = false;
bool cpt = false;
int FileExists(TCHAR* file)
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

TCHAR* ReadRegValue(HKEY root, LPCSTR key, LPCSTR name)
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

int _tmain(int argc, TCHAR* argv[])
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
	TCHAR* val = ReadRegValue(HKEY_LOCAL_MACHINE, "SOFTWARE\\SDFS", "path");

	TCHAR path[512];
	_tcscpy_s(path, val);
	bool mt = false;

	
	
	
		
	std::string number;
	std::stringstream strstream;
	strstream << 1L;
	strstream >> number;
	_tcscpy_s(cmd, path);
	_tcsncat_s(cmd, _T("\\sdfs-proxy-s.exe "), 2048);

	for (int i = 1; i < argc; i++) {

		if (!_tcsncmp(argv[i], _T("-s"), 3)) {
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
				Sleep(100);
				break;
			}
		}
		CloseHandle(piProcInfo.hProcess);

	}

	// The remaining open handles are cleaned up when this process terminates. 
	// To avoid resource leaks in a larger application, close handles explicitly. 

	return exit_code;
}

void CreateChildProcess(TCHAR* p)
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

void ReadFromPipe(void* param)

// Read output from the child process's pipe for STDOUT
// and write to the parent process's pipe for STDOUT. 
// Stop when there is no more data. 
{
	DWORD dwRead, dwWritten;
	CHAR chBuf[BUFSIZE];
	CHAR rchBuf[1];
	CHAR nl[1];
	CHAR rl[1];
	BOOL bSuccess = FALSE;
	BOOL rSuccess = FALSE;
	hParentStdOut = GetStdHandle(STD_OUTPUT_HANDLE);
	std::string contents;
	DWORD offset = 0;
	nl[0] = '\n';
	rl[0] = '\r';
	while (!processCompleted)
	{
		rSuccess = ReadFile(g_hChildStd_OUT_Rd, rchBuf, 1, &dwRead, NULL);
		if (!rSuccess || dwRead == 0) {
			processCompleted = true;
			break;
		}
		chBuf[offset] = rchBuf[0];
		offset++;
		contents = string(chBuf);
		if (rchBuf[0] == nl[0] || rchBuf[0] == rl[0]) {
			if (!cpt && strcmp(chBuf, "proxy ready\n") == 0) {
				
				bSuccess = WriteFile(hParentStdOut, "proxy ready\n",
					16, &dwWritten, NULL);
				processCompleted = true;
				break;
			}
			else {

				bSuccess = WriteFile(hParentStdOut, contents.c_str(),
					offset, &dwWritten, NULL);
			}
			if (!bSuccess) {
				processCompleted = true;
				
				break;
			}
			else {
				dataWritten = true;
			}
			memset(chBuf, 0, sizeof(chBuf));
			offset = 0;
		}
		/*
		else {
			bSuccess = WriteFile(hParentStdOut,"ww\n",
				8, &dwWritten, NULL);
		}
		*/

	}
	threadFinished = true;
}

void ErrorExit(PTSTR lpszFunction)

// Format a readable error message, display a message box, 
// and exit from the application.
{

}
