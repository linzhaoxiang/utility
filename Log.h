/*
 * Log.h
 *
 *  Created on: 2013-10-1
 *      Author: linzhaoxiang
 */

#ifndef LOG_H_
#define LOG_H_
#include <string>
#include <iostream>
#include <vector>
#include <algorithm>
#include <iterator>
#ifndef LOG_LEVEL
#define LOG_LEVEL LOG_ALL
#endif

enum LogType{
	LOG_OFF=0X00,
	LOG_CONSOLE=0x01,
	LOG_FATAL=0x02,
	LOG_EXCEPTION=0x03,
	LOG_WARN=4,
	LOG_INFO=5,
	LOG_ERROR=6,
	LOG_DEBUG=7,
	LOG_TRACE=8,
	LOG_TRACE2=9,
	LOG_TRACE3=10,
	LOG_ALL=11};


std::string _GetCurrentTime();
std::ostream& _GetLogStream(LogType eLogType);
void SetLogStream(LogType eLogType, std::ostream& stream);
void ResetLogStream(LogType eLogType, std::ostream& stream);
LogType SetLogLevel(LogType level);
LogType GetLogLevel();

const std::string& GetBinaryPath();
void SetBinaryPath(const std::string& strPath);

class CNextLine
{
public:
	CNextLine(std::ostream& os)
	{
		os << std::endl;
		//return false;
	}
};

//#ifndef RC_FAILED
//#define RC_FAILED(rc) (rc < 0)
//#endif
#define LogStream(LogType_) if(!(LOG_LEVEL >=LogType_ && GetLogLevel() >= LogType_)){} else CNextLine tempfornextline##__LINE__=_GetLogStream(LogType_)<<_GetCurrentTime()<<":"<<__FILE__<<":"<<__LINE__<<":"<<__FUNCTION__<<":" <<"\t"
#define Console() if(!(LOG_LEVEL >=LOG_CONSOLE && GetLogLevel() >= LOG_CONSOLE)){}else _GetLogStream(LOG_INFO)
#define LogConsole() if(!(LOG_LEVEL >=LOG_CONSOLE && GetLogLevel() >= LOG_CONSOLE)){}else CNextLine tempfornextline##__LINE__ = _GetLogStream(LOG_INFO)
#define LogFatal() LogStream(LOG_FATAL) << "Fatal:"
#define LogException() LogStream(LOG_EXCEPTION) << "Exception:"
#define LogError() LogStream(LOG_ERROR) << "Error:"
#define LogWarn() LogStream(LOG_WARN) << "Warn:"
#define LogInfo() LogStream(LOG_INFO) << "Info:"
#define Log() LogInfo()
#define LogDebug() LogStream(LOG_DEBUG)<<"Debug:"
#define LogTrace() LogStream(LOG_TRACE) << "Trace:"
#define LogTrace2() LogStream(LOG_TRACE2) << "TraceDeep:"
#define LogTrace3() LogStream(LOG_TRACE3) << "TraceAll:"
#define LogErrorCode(rc) if(RC_FAILED(rc)) LogError() << "ResutlCode:" << rc << ";"
#define LogDebugCode(rc) if(RC_FAILED(rc)) LogDebug() << "ResultCode:" << rc << ";"
#define LogReturn(rc) {LogErrorCode(rc); return rc;}


template<class firstT, class secondT>
	std::ostream& operator<<(std::ostream& outstream, const std::pair<firstT,secondT>& value)
{
	outstream<<"pair("<<value.first<<","<<value.second<<")";
	return outstream;
}


template<class T>
	std::ostream& operator<<(std::ostream& outstream, const std::vector<T>& vect)
{
	outstream << "vector(size:"<<vect.size()<<"):";
	for(typename std::vector<T>::const_iterator it = vect.begin(); it != vect.end(); it++)
	{
		if(it != vect.begin())
			outstream << ",";
		outstream << *it;
	}
	//copy(vect.begin(), vect.end(), std::ostream_iterator<T>(outstream, ","));
	return outstream;
}


#endif /* LOG_H_ */
