#include "CacheCluster.h"
#include "Log.h"
#include <string.h>
#include <chrono>
#include <thread>
#include "stock/utility/Utility.h"
using namespace Stock;
#define RETRY_COUNT 1
static const std::string SEPERATOR = "_";

// Constructors/Destructors
//  

CCacheCluster::CCacheCluster ()
{
}

CCacheCluster::~CCacheCluster ()
{
	if(this->m_pServer != nullptr)
	{
		redisFree(m_pServer);
		m_pServer = nullptr;
	}
}

//  
// Methods
//  

/**
 * @return ResultCode
 * @param  vectControlServer
 * @param  vectCacheServer
 * @param  nTimeoutInMS
 */
ResultCode CCacheCluster::ConnectCacheServer (const std::string& strServerAddr, int nPort,
		int nTimeoutInMS)
{
	if(strServerAddr.empty())
		return RE_INVALIDATE_PARAMETER;
	std::lock_guard<std::mutex> lock(m_mutex);
	m_strServerAddress = strServerAddr;
	m_nServerPort = nPort;
	this->m_nConnectTimeOutInMS = nTimeoutInMS;
	return this->Reconnect();
}

/**
 * @return ResultCode
 * @param  strOwner
 * @param  strItem
 * @param  strValue
 */
ResultCode CCacheCluster::GetItemValue (const std::string& strOwner, const std::string& strItem,
		std::string& strValue)
{
	std::lock_guard<std::mutex> lock(m_mutex);
	ResultCode rc = RE_ERROR;
	std::string strKey = GenerateKey(strOwner, strItem);
	redisReply* reply = nullptr;
	if(m_pServer == nullptr && RC_FAILED(Reconnect()))
		LogReturn(RE_ERROR);

	for(int retry = 0; retry <= RETRY_COUNT && RC_FAILED(rc); retry++)
	{
		reply = (redisReply*)redisCommand(this->m_pServer, "get %s", strKey.c_str());
		if ( reply ==nullptr || (reply->type != REDIS_REPLY_STRING && reply->type != REDIS_REPLY_NIL))
		{
			LogError() << "Failed to execute command:" << "get " << strKey;

			freeReplyObject(reply);
			if(RC_FAILED(Reconnect()))
				LogReturn(RE_COMMUNICATION);
			reply = nullptr;
			rc = RE_ERROR;
			continue;
		}
		if(reply->type == REDIS_REPLY_NIL)
			rc = RE_NOT_EXISTS;
		else
		{
			strValue.assign(reply->str, reply->len);
			rc = RS_SUCCESS;
		}
	}

	freeReplyObject(reply);
	if(RC_SUCCEEDED(rc))
	{
		std::string strTemp;
		strTemp.swap(strValue);
		rc = Stock::Utility::decompress(strTemp, strValue);
		LogErrorCode(rc);
	}
	LogTrace2() << "Get Item Value for " << strOwner << ":" << strItem << "=" << strValue <<
				";";
	return rc;

}


/**
 * @return ResultCode
 * @param  strOwner
 * @param  strItem
 * @param  strValue
 * @param  nLifeCycleInSecond 0 means not limited.
 */
ResultCode CCacheCluster::SetItemValue (const std::string& strOwner, const std::string& strItem,
		const std::string& strOrigValue, size_t nLifeCycleInSecond)
{
	LogTrace2() << "Set Item Value for " << strOwner << ":" << strItem << "=" << strOrigValue <<
			"; life cycle ="  <<  nLifeCycleInSecond;
	std::string strValue;
	Stock::Utility::compress(strOrigValue, strValue);
	std::lock_guard<std::mutex> lock(m_mutex);
	std::string strKey = GenerateKey(strOwner, strItem);
	redisReply* reply = nullptr;
	ResultCode rc = RE_ERROR;
	if(m_pServer == nullptr && RC_FAILED(Reconnect()))
		LogReturn(RE_ERROR);

	for(int retry = 0; retry <= RETRY_COUNT && RC_FAILED(rc); retry++)
	{
		if(nLifeCycleInSecond == size_t(-1))
		{
			reply = (redisReply*)redisCommand(this->m_pServer, "SET %s %b",
					strKey.c_str(), strValue.c_str(), strValue.size());
		}
		else
		{
			reply = (redisReply*)redisCommand(m_pServer, "SETEX %s %d %b",
					strKey.c_str(), nLifeCycleInSecond, strValue.c_str(), strValue.size());

		}
		if(reply == nullptr)
		{
			LogError() << "set key failed for " << strKey;
			if(RC_FAILED(Reconnect()))
				LogReturn(RE_COMMUNICATION);
			rc = RE_ERROR;
			continue;
		}
		if (!(reply->type == REDIS_REPLY_STATUS && strcasecmp(reply->str,"OK") == 0))
		{
			LogError() << "set key failed for " << strKey << ":" << reply->str;
			freeReplyObject(reply);
			reply = nullptr;
			rc = RE_ERROR;
			continue;
		}
		rc = RS_SUCCESS;
	}

	freeReplyObject(reply);
	return rc;
}


/**
 * @return ResultCode
 * @param  strOwner
 * @param  strItem
 */
ResultCode CCacheCluster::RemoveItemValue (const std::string& strOwner, const std::string& strItem)
{
	std::lock_guard<std::mutex> lock(m_mutex);
	std::string strKey = GenerateKey(strOwner, strItem);
	redisReply* reply = nullptr;
	ResultCode rc = RE_ERROR;
	if(m_pServer == nullptr && RC_FAILED(Reconnect()))
		LogReturn(RE_ERROR);

	for(int retry = 0; retry <= RETRY_COUNT && RC_FAILED(rc); retry++)
	{

		reply = (redisReply*)redisCommand(this->m_pServer, "DEL %s",
						strKey.c_str());

		if ( reply ==nullptr || reply->type != REDIS_REPLY_INTEGER || reply->integer != 1)
		{
			if(reply && reply->type == REDIS_REPLY_INTEGER &&  reply->integer == 0)
				rc = RE_NOT_EXISTS;
			else
				rc = RE_ERROR;
			LogTrace2() << "Failed to execute command:" << "DEL " << strKey;
			freeReplyObject(reply);
			if(RC_FAILED(Reconnect()))
				LogReturn(RE_COMMUNICATION);
			reply = nullptr;

			continue;
		}
		rc = RS_SUCCESS;
	}

	freeReplyObject(reply);
	return rc;
}


/**
 * @return ResultCode
 * @param  strOwner
 * @param  strItem
 */
ResultCode CCacheCluster::TryGetProduceRight (const std::string& strOwner, const std::string& strItem,
		int nRightSpanInSecond)
{
	if(nRightSpanInSecond <= 0)
		return RE_INVALIDATE_PARAMETER;
	std::string strKey = GenerateKey(strOwner, strItem);
	redisReply* reply = nullptr;
	ResultCode rc = RE_ERROR;
	if(m_pServer == nullptr && RC_FAILED(Reconnect()))
		LogReturn(RE_ERROR);

	for(int retry = 0; retry <= RETRY_COUNT && RC_FAILED(rc); retry++)
	{
		bool bExists = false;
		rc = Exists(strOwner, strItem, bExists);
		if(RC_FAILED(rc))
		{
			continue;
		}
		if(bExists)
		{
			rc = RE_ALREADY_EXISTS;
			break;
		}

		rc = this->TryLock("ProduceRight", strKey, nRightSpanInSecond, 0);
		if(rc == RE_TIME_OUT)
			rc = RE_BUSY;

		//check data exists again? no, we currently don't require a strict lock,
		//actually we will implement lock by zookeeper.

	}

	return rc;
}


/**
 * @return ResultCode
 * @param  strOwner
 * @param  strItem
 * @param  nTimeOutInMS
 */
ResultCode CCacheCluster::WaitForItemValue (const std::string& strOwner, const std::string& strItem,
		int nTimeOutInMS)
{
	ResultCode rc = RE_ERROR;
	std::chrono::time_point<std::chrono::system_clock> currentTime = std::chrono::system_clock::now();
	auto nCurrent = std::chrono::duration_cast<std::chrono::milliseconds>(currentTime.time_since_epoch()).count();
	auto nStart = nCurrent;
	do
	{
		bool bExists = false;
		rc = Exists(strOwner, strItem, bExists);
		if(RC_FAILED(rc))
		{
			break;
		}
		if(bExists)
			return RS_SUCCESS;
		if(nTimeOutInMS != 0)
			std::this_thread::sleep_for(std::chrono::milliseconds(200));
		currentTime = std::chrono::system_clock::now();
		nCurrent = std::chrono::duration_cast<std::chrono::milliseconds>(currentTime.time_since_epoch()).count();
	}
	while(nCurrent - nStart	<= nTimeOutInMS);
	return RE_TIME_OUT;

}

std::string CCacheCluster::GenerateKey(const std::string& strOwner, const std::string& strItem) const
{
	return strItem + SEPERATOR + strOwner;
}

ResultCode CCacheCluster::Reconnect()
{
	if(this->m_pServer != nullptr)
	{
		redisFree(m_pServer);
		m_pServer = nullptr;
	}
	timeval tv;
	tv.tv_sec = m_nConnectTimeOutInMS/1000;
	tv.tv_usec = m_nConnectTimeOutInMS%1000*1000;

	m_pServer = redisConnectWithTimeout(m_strServerAddress.c_str(), m_nServerPort, tv);
	if(m_pServer->err)
	{
		LogError() << "Connect to Redis server " << m_strServerAddress << ":" << m_nServerPort << " Failed:" << m_pServer->errstr;
		redisFree(m_pServer);
		m_pServer = nullptr;

		return Stock::RE_ERROR;
	}
	LogDebug() << "Connect To Redis server succeeded:" << m_strServerAddress << ":" << m_nServerPort;
	return Stock::RS_SUCCESS;
}

ResultCode CCacheCluster::Exists(const std::string& strOwner, const std::string& strItem, bool& bExists)
{
	std::lock_guard<std::mutex> lock(m_mutex);
	std::string strKey = GenerateKey(strOwner, strItem);
	redisReply* reply = nullptr;
	ResultCode rc = RE_ERROR;
	if(m_pServer == nullptr && RC_FAILED(Reconnect()))
		LogReturn(RE_ERROR);

	for(int retry = 0; retry <= RETRY_COUNT && RC_FAILED(rc); retry++)
	{

		/*1.check whether the data exists*/
		reply = (redisReply*)redisCommand(this->m_pServer, "EXISTS %s",
						strKey.c_str());
		if(reply == nullptr)
		{
			LogError() << "EXISTS " << strKey << " failed";
			if(RC_FAILED(Reconnect()))
				LogReturn(RE_COMMUNICATION);
			continue;
		}
		if(reply->type != REDIS_REPLY_INTEGER)
		{
			freeReplyObject(reply);
			reply = nullptr;
			continue;
		}
		if(reply->integer == 1)
		{
			bExists = true;
		}
		else
		{
			bExists = false;
		}
		freeReplyObject(reply);
		reply = nullptr;
		rc = RS_SUCCESS;
	}

	return rc;
}

ResultCode CCacheCluster::TryLock(const std::string& strOwner, const std::string& strItem, int nLockPeriodInSecond,
		int nTimeoutInMS)
{
	std::string strKey = GenerateKey(strOwner, strItem);
	std::string strKeyLock = strKey + SEPERATOR + "Lock";
	redisReply* reply = nullptr;
	ResultCode rc = RE_ERROR;
	if(m_pServer == nullptr && RC_FAILED(Reconnect()))
		LogReturn(RE_ERROR);
	std::chrono::time_point<std::chrono::system_clock> currentTime = std::chrono::system_clock::now();
	auto nCurrent = std::chrono::duration_cast<std::chrono::milliseconds>(currentTime.time_since_epoch()).count();
	auto nStart = nCurrent;
	do
	{
		for(int retry = 0; retry <= RETRY_COUNT && RC_FAILED(rc); retry++)
		{

			/*2. not exists, create the lock*/
			std::lock_guard<std::mutex> lock(m_mutex);
			std::string strKeyLock = strKey + SEPERATOR + "Lock";
			reply = (redisReply*)redisCommand(this->m_pServer, "SETNX %s %b",
							strKeyLock.c_str(), strKeyLock.c_str(), 1);
			if(reply == nullptr)
			{
				LogError() << "Lock failed:" << strKeyLock;
				if(RC_FAILED(Reconnect()))
					LogReturn(RE_COMMUNICATION);
				continue;
			}
			if (!(reply->type == REDIS_REPLY_INTEGER && reply->integer == 1))
			{
				if(reply->type == REDIS_REPLY_INTEGER && reply->integer == 0)
				{
					auto reply2= (redisReply*)redisCommand(this->m_pServer, "TTL %s",
							strKeyLock.c_str());
					if(reply2->type == REDIS_REPLY_INTEGER && reply2->integer == -1 )
					{
						LogException() << "Lock without expire, reset expire:" << strKeyLock;
						auto reply3 = (redisReply*)redisCommand(this->m_pServer, "Expire %s %d",
													strKeyLock.c_str(), nLockPeriodInSecond);
						freeReplyObject(reply3);
					}
					freeReplyObject(reply2);
					rc = RE_BUSY;
				}
				else
					rc = RE_ERROR;
				freeReplyObject(reply);
				reply = nullptr;
				LogError() << "Lock failed:" << strKeyLock;
				if(rc == RE_BUSY)
					break;
				continue;
			}
			rc = RE_ERROR;
			for(int retry = 0; retry <= RETRY_COUNT && RC_FAILED(rc); retry++)
			{
				if(nLockPeriodInSecond <= 0)
				{
					rc = RS_SUCCESS;
					break;
				}
				reply = (redisReply*)redisCommand(this->m_pServer, "Expire %s %d",
								strKeyLock.c_str(), nLockPeriodInSecond);

				if(reply == nullptr)
				{
					LogError() << "Lock failed:" << strKeyLock;
					if(RC_FAILED(Reconnect()))
						LogReturn(RE_COMMUNICATION);
					continue;
				}
				if (!(reply->type == REDIS_REPLY_INTEGER && reply->integer == 1))
				{
					rc = RE_ERROR;
					freeReplyObject(reply);
					reply = nullptr;
					LogError() << "Lock failed:" << strKeyLock;
					if(rc == RE_BUSY)
						break;
					continue;
				}
				else break;
			}

			rc = RS_SUCCESS;
			freeReplyObject(reply);
			reply = nullptr;
			return rc;

		}
		if(nTimeoutInMS != 0)
			std::this_thread::sleep_for(std::chrono::milliseconds(200));
		currentTime = std::chrono::system_clock::now();
		nCurrent = std::chrono::duration_cast<std::chrono::milliseconds>(currentTime.time_since_epoch()).count();
	}
	while(nCurrent - nStart	<= nTimeoutInMS);
	return RE_TIME_OUT;

	return rc;
}

ResultCode CCacheCluster::Unlock(const std::string& strOwner, const std::string& strItem)
{
	std::string strKey = GenerateKey(strOwner, strItem);
	std::string strKeyLock = strKey + SEPERATOR + "Lock";
	std::lock_guard<std::mutex> lock(m_mutex);
	redisReply* reply = nullptr;
	ResultCode rc = RE_ERROR;
	if(m_pServer == nullptr && RC_FAILED(Reconnect()))
		LogReturn(RE_ERROR);

	for(int retry = 0; retry <= RETRY_COUNT && RC_FAILED(rc); retry++)
	{

		reply = (redisReply*)redisCommand(this->m_pServer, "DEL %s",
				strKeyLock.c_str());

		if ( reply ==nullptr || reply->type != REDIS_REPLY_INTEGER || reply->integer != 1)
		{
			if(reply && reply->type == REDIS_REPLY_INTEGER &&  reply->integer == 0)
				rc = RS_NOT_EXISTS;
			else
				rc = RE_ERROR;
			LogTrace2() << "Failed to execute command:" << "DEL " << strKeyLock;
			freeReplyObject(reply);
			if(RC_FAILED(Reconnect()))
				LogReturn(RE_COMMUNICATION);
			reply = nullptr;

			continue;
		}
		rc = RS_SUCCESS;
	}

	freeReplyObject(reply);
	return rc;
}

bool CCacheCluster::IsLocalCacheAvail(const std::string& strOwner, const std::string& strItem) const
{
	auto it = m_mapLocalCacheAvail.find(GenerateKey(strOwner, strItem));
	if(it != m_mapLocalCacheAvail.end())
		return it->second;
	return false;
}

void CCacheCluster::SetLocalCacheAvail(const std::string& strOwner, const std::string& strItem)
{
	m_mapLocalCacheAvail.insert(std::make_pair(GenerateKey(strOwner, strItem), true));
}
