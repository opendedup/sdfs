#include "stdafx.h"
#include "CliFunctions.h"
#include <curl/curl.h>
#include <sys/stat.h>
#include <iostream>
#include <sstream>
#include <algorithm>
#include <string.h>
#include <fcntl.h>
#include <errno.h>
#include <stdlib.h>
#include <direct.h>
#include <io.h>
#include <windows.h>
#include <process.h>
#include <tinyxml2.h>
#include <winreg.h>
#include <comdef.h>
#include <stdio.h>
using namespace std;
using namespace tinyxml2;
struct HttpMemoryStruct {
	char *memory;
	size_t size;
};

/*
* Wrapper function to read from file and do some basic error checking
*/
char * readline(char *line, int size, FILE *fp) {
	char *buf;
	char *res;

	buf = new char[size];
	printf("1\n");
	res = fgets(buf, size, fp);
	printf("2\n");
	if (NULL == res) {
		printf("3\n");
		delete[] buf;
		printf("4\n");
		return NULL;
	}
	else {
		printf("5\n");
		for (int i = 0, j = 0; i < size; i++) {
			printf("5\n");
			if (!isspace(buf[i])) {
				printf("6\n");
				line[j++] = buf[i];
				printf("7\n");
			}
		}
		printf("8\n");
		delete[] buf;
		return res;
	}
}

bool fileExists(const std::string& file) {
	struct stat buf;
	return (stat(file.c_str(), &buf) == 0);
}


static size_t WriteMemoryCallback(void *contents, size_t size, size_t nmemb,
	void *userp) {

	size_t realsize = size * nmemb;
	struct HttpMemoryStruct *mem = (struct HttpMemoryStruct *) userp;

	mem->memory = (char *)realloc(mem->memory, mem->size + realsize + 1);
	if (mem->memory == NULL) {
		/* out of memory! */
		return 0;
	}

	memcpy(&(mem->memory[mem->size]), contents, realsize);
	mem->size += realsize;
	mem->memory[mem->size] = 0;

	return realsize;
}

wstring ReadRegValue(HKEY root, LPCSTR key, LPCWSTR name)
{
	HKEY hKey;
	if (RegOpenKeyEx(root, key, 0, KEY_READ, &hKey) != ERROR_SUCCESS)
		return L"";


	WCHAR szBuffer[512];
	DWORD dwBufferSize = sizeof(szBuffer);
	if (RegQueryValueExW(hKey, name, 0, NULL, (LPBYTE)szBuffer, &dwBufferSize) != ERROR_SUCCESS)
	{
		RegCloseKey(hKey);
		return L"";
	}

	RegCloseKey(hKey);

	return szBuffer;
}

std::string char2hex(char dec) {
	char dig1 = (dec & 0xF0) >> 4;
	char dig2 = (dec & 0x0F);
	if (0 <= dig1 && dig1 <= 9)
		dig1 += 48;    //0,48 in ascii
	if (10 <= dig1 && dig1 <= 15)
		dig1 += 65 - 10; //A,65 in ascii
	if (0 <= dig2 && dig2 <= 9)
		dig2 += 48;
	if (10 <= dig2 && dig2 <= 15)
		dig2 += 65 - 10;

	std::string r;
	r.append(&dig1, 1);
	r.append(&dig2, 1);
	return r;
}

std::string urlencode(const std::string &c) {

	std::string escaped;
	int max = c.length();
	for (int i = 0; i < max; i++) {
		if ((48 <= c[i] && c[i] <= 57) || //0-9
			(65 <= c[i] && c[i] <= 90) || //ABC...XYZ
			(97 <= c[i] && c[i] <= 122) || //abc...xyz
			(c[i] == '~' || c[i] == '-' || c[i] == '_' || c[i] == '.')) {
			escaped.append(&c[i], 1);
		}
		else {
			escaped.append("%");
			escaped.append(char2hex(c[i])); //converts char 255 to string "FF"
		}
	}
	return escaped;
}

CliFunctions::CliFunctions()
{

}


CliFunctions::~CliFunctions()
{
	if (curl) {
		curl_easy_cleanup(curl);
		curl = NULL;
	}
}

void CliFunctions::getVolumeInfo() {
	CURLcode res;
	struct HttpMemoryStruct chunk;

	chunk.memory = (char *)malloc(1); /* will be grown as needed by the realloc above */
	chunk.size = 0; /* no data at this point */
	if (!curl) {
		curl = curl_easy_init();
	}
	if (curl) {
		std::ostringstream stream;
		std::string passwd = urlencode("password123");
		std::string url = "https://localhost:6442?file=null&cmd=volume-info&password=" + passwd;
		curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
		curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);
		curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0L);
		/* send all data to this function  */
		curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteMemoryCallback);

		/* we pass our 'chunk' struct to the callback function */
		curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void *)&chunk);
		/* Perform the request, res will get the return code */
		res = curl_easy_perform(curl);
		/* check for errors */
		if (res != CURLE_OK) {
			printf("nogood");
			return;
		}
		else {
			printf("good");
			printf("%s\n", chunk.memory);
			tinyxml2::XMLDocument doc;
			doc.Parse(chunk.memory);
			const char* msg= doc.FirstChildElement("result")->Attribute("msg");
			XMLElement* volElement = doc.FirstChildElement("result")->FirstChildElement("volume");
			const char* msgs = volElement->Attribute("capacity");
			printf("%s-%s\n", msg,msgs);
		}
		wstring val = ReadRegValue(HKEY_LOCAL_MACHINE, "SOFTWARE\\WOW6432Node\\SDFS", L"ostpath");
		if (val.empty()) {
			val = L"c:\\sdfs\\config\\ostconfig.xml";
		}
		_bstr_t b(val.c_str());
		const char* c = b;
		if (fileExists(c)) {
			printf("Output: %s\n", c);
			wprintf(L"Config file is %s\n", val.c_str());
			tinyxml2::XMLDocument doc(true, COLLAPSE_WHITESPACE);
			doc.LoadFile(c);
			if (doc.ErrorID() == 0) {
				int count = 0;

				for (XMLElement* ele = doc.FirstChildElement("CONNECTIONS")->FirstChildElement("CONNECTION");
					ele;
					ele = ele->NextSiblingElement())
				{
					const char* name = ele->FirstChildElement("NAME")->FirstChild()->ToText()->Value();
					if (ele->FirstChildElement("SERVER_SHARE_PATH")) {
						const char* name = ele->FirstChildElement("SERVER_SHARE_PATH")->FirstChild()->ToText()->Value();
						printf("Name of play (2): %s\n", name);
					}

					
				}
			}
			else {
				printf("Error ID: %d\n", doc.ErrorID());
			}
		}
		else {
			printf("File does not exists: %s\n", c);
		}
		

	}
}

