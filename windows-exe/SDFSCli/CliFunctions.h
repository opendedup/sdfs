#pragma once
#include <curl/curl.h>
class CliFunctions
{
private:
	CURL *curl;
public:
	void getVolumeInfo();
	CliFunctions();
	~CliFunctions();
};

