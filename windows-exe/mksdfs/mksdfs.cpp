// mksdfs.cpp : Defines the entry point for the console application.
//

#include "stdafx.h"
#include <windows.h> 
#include <tchar.h>
#include <stdio.h> 
#include <strsafe.h>
#include <iostream>
#include <string>
#include <sstream>
#include <process.h>

#define BUFSIZE 8192 

HANDLE g_hChildStd_IN_Rd = NULL;
HANDLE g_hChildStd_IN_Wr = NULL;
HANDLE g_hChildStd_OUT_Rd = NULL;
HANDLE hParentStdOut = NULL;
HANDLE g_hChildStd_OUT_Wr = NULL;
PROCESS_INFORMATION piProcInfo;

void CreateChildProcess(TCHAR *p);
void WriteToPipe(void);
void ReadFromPipe(void *param);
void ErrorExit(PTSTR);
bool threadFinished = false;
bool processCompleted = false;
bool dataWritten = false;
using namespace std;

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

int _tmain(int argc, TCHAR *argv[])
{
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

	// Create the child process. 
	TCHAR cmd[4096];
	TCHAR *val = ReadRegValue(HKEY_LOCAL_MACHINE, "SOFTWARE\\SDFS", "path");
	TCHAR path[1024];
	_tcscpy_s(path, val);
	_tcscpy_s(cmd, val);
	_tcsncat_s(cmd, _T("\\bin\\jre\\bin\\java.exe "), 4096);
	_tcsncat_s(cmd, _T("-cp \""), 4096);
	_tcsncat_s(cmd, path, 4096);
	_tcsncat_s(cmd, "\\lib\\sdfs.jar\";\"", 4096);
	_tcsncat_s(cmd, path, 4096);
	_tcsncat_s(cmd, "\\lib\\*\" org.opendedup.sdfs.VolumeConfigWriter", 4096);

	for (int i = 1; i < argc; i++) {
		_tcsncat_s(cmd, _T(" "), 4096);
		_tcsncat_s(cmd, argv[i], 4096);
	}
	//_tprintf("cmd=%s", cmd);
	CreateChildProcess(cmd);

	_beginthread(ReadFromPipe, 0, NULL);
	DWORD exit_code;
	if (NULL != piProcInfo.hProcess)
	{
		WaitForSingleObject(piProcInfo.hProcess, INFINITE); // Change to 'INFINITE' wait if req'd
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
			CloseHandle(piProcInfo.hProcess);
		}

	}

	//printf("\n->End of parent execution.\n");

	// The remaining open handles are cleaned up when this process terminates. 
	// To avoid resource leaks in a larger application, close handles explicitly. 

	return exit_code;
}

void CreateChildProcess(TCHAR *p)
// Create a child process that uses the previously created pipes for STDIN and STDOUT.
{
	STARTUPINFO siStartInfo;
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

	bSuccess = CreateProcess(NULL,
		p,     // command line 
		NULL,          // process security attributes 
		NULL,          // primary thread security attributes 
		TRUE,          // handles are inherited 
		0,             // creation flags 
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
		bSuccess = WriteFile(hParentStdOut, chBuf,
			dwRead, &dwWritten, NULL);
		if (!bSuccess) {
			break;
		}
		else {
			dataWritten = true;
		}
		if (!rSuccess || dwRead == 0) {
			break;
		}
	}
	threadFinished = true;
}

void ErrorExit(PTSTR lpszFunction)

// Format a readable error message, display a message box, 
// and exit from the application.
{
	LPVOID lpMsgBuf;
	LPVOID lpDisplayBuf;
	DWORD dw = GetLastError();

	FormatMessage(
		FORMAT_MESSAGE_ALLOCATE_BUFFER |
		FORMAT_MESSAGE_FROM_SYSTEM |
		FORMAT_MESSAGE_IGNORE_INSERTS,
		NULL,
		dw,
		MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
		(LPTSTR)&lpMsgBuf,
		0, NULL);

	lpDisplayBuf = (LPVOID)LocalAlloc(LMEM_ZEROINIT,
		(lstrlen((LPCTSTR)lpMsgBuf) + lstrlen((LPCTSTR)lpszFunction) + 40)*sizeof(TCHAR));
	StringCchPrintf((LPTSTR)lpDisplayBuf,
		LocalSize(lpDisplayBuf) / sizeof(TCHAR),
		TEXT("%s failed with error %d: %s"),
		lpszFunction, dw, lpMsgBuf);
	MessageBox(NULL, (LPCTSTR)lpDisplayBuf, TEXT("Error"), MB_OK);

	LocalFree(lpMsgBuf);
	LocalFree(lpDisplayBuf);
	ExitProcess(1);
}