/*
 * Log.cpp
 *
 *  Created on: 2013-10-1
 *      Author: linzhaoxiang
 */


#include "Log.h"


#include <boost/date_time/posix_time/posix_time.hpp>
#include <vector>

static LogType s_LogType = LOG_DEBUG;
static std::string s_strBinaryPath;

std::string _GetCurrentTime()
{
	using namespace boost::posix_time;
	ptime now = second_clock::local_time();
	std::string now_str  =  to_iso_extended_string(now.date()) + " " + to_simple_string(now.time_of_day());
	return now_str;

}

std::ostream& _GetLogStream(LogType eLogType)
{
	return std::cout;
}

void SetLogStream(LogType eLogType, std::ostream& stream)
{
}

void ResetLogStream(LogType eLogType, std::ostream stream)
{

}


LogType SetLogLevel(LogType level)
{
	LogType result = s_LogType;
	s_LogType = level;
	return result;

}
LogType GetLogLevel()
{
	return s_LogType;
}


const std::string& GetBinaryPath()
{
	if(s_strBinaryPath.empty())
	{
		LogError() << "Binary Path not initialized";
	}
	return s_strBinaryPath;
}
void SetBinaryPath(const std::string& strPath)
{
	s_strBinaryPath = strPath;
	LogTrace() << "Set Binary Path:" << strPath;
}

