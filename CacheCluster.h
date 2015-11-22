
#ifndef CCACHECLUSTER_H
#define CCACHECLUSTER_H
#include "ResultCode.h"
#include <hiredis/hiredis.h>
#include <mutex>
#include <unordered_map>
#include <boost/shared_ptr.hpp>


#include <string>
class CCacheCluster
{
public:
	// Constructors/Destructors
	//  


	/**
	 * Empty Constructor
	 */
	CCacheCluster ();

	/**
	 * Empty Destructor
	 */
	virtual ~CCacheCluster ();


	/**
	 * @return ResultCode
	 * @param  vectControlServer
	 * @param  vectCacheServer
	 * @param  nTimeoutInMS
	 */
	ResultCode ConnectCacheServer (const std::string& strServerAddr, int nPort, int nTimeoutInMS);


	/**
	 * @return ResultCode
	 * @param  strOwner
	 * @param  strItem
	 * @param  strValue
	 */
	ResultCode GetItemValue (const std::string& strOwner, const std::string& strItem,
			std::string& strValue);


	/**
	 * @return ResultCode
	 * @param  strOwner
	 * @param  strItem
	 * @param  strValue
	 * @param  nLifeCycleInSecond 0 means not limited.
	 */
	ResultCode SetItemValue (const std::string& strOwner, const std::string& strItem,
			const std::string& strValue, size_t nLifeCycleInSecond = size_t(-1));


	/**
	 * @return ResultCode
	 * @param  strOwner
	 * @param  strItem
	 */
	ResultCode RemoveItemValue (const std::string& strOwner, const std::string& strItem);


	/**
	 * @return ResultCode:
	 * 		RS_SUCCESS:	get produce right success, the caller can produce the data now.
	 * 		RE_ALREADY_EXISTS: the cache is already exists, the caller can use it directly
	 * 		RE_BUSY: there is other worker on producing the cache data. the caller should
	 * 		wait for the result.(typically by calling WaitForItemCache() )
	 * @param  strOwner
	 * @param  strItem
	 * @param  nRightSpanInSecond
	 */
	ResultCode TryGetProduceRight (const std::string& strOwner, const std::string& strItem,
			int nRightSpanInSecond);


	bool IsLocalCacheAvail(const std::string& strOwner, const std::string& strItem) const;
	void SetLocalCacheAvail(const std::string& strOwner, const std::string& strItem);

	ResultCode TryLock(const std::string& strOwner, const std::string& strItem, int nLockPeriodInSecond, int nTimeoutInMS);
	ResultCode Unlock(const std::string& strOwner, const std::string& strItem);


	/**
	 * @return ResultCode
	 * @param  strOwner
	 * @param  strItem
	 * @param  nTimeOutInMS
	 */
	ResultCode WaitForItemValue (const std::string& strOwner, const std::string& strItem,
			int nTimeOutInMS);


	ResultCode Exists(const std::string& strOwner, const std::string& strItem, bool& bExists);

	void ResetLocalCache()
	{
		this->m_mapLocalCacheAvail.clear();
	}


protected:
	std::string GenerateKey(const std::string& strOwner, const std::string& strItem) const;
	ResultCode Reconnect();

	redisContext* m_pServer = nullptr;
	std::string m_strServerAddress;
	int m_nServerPort = 6379;
	int m_nConnectTimeOutInMS = 1000;
	std::unordered_map<std::string, bool> m_mapLocalCacheAvail;
	std::mutex m_mutex;


};


class CLockGuard
{
public:
	CLockGuard(boost::shared_ptr<CCacheCluster> pCacheCluster,
			const std::string& strOwner, const std::string& strItem,
			int nLockPeriodInSecond, int nTimeoutInMS):m_pCacheCluster(pCacheCluster),
			m_strOwner(strOwner), m_strItem(strItem)
{
		if(pCacheCluster == nullptr)
		{
			m_rc = Stock::RS_NOT_SUPPORT;
			return;
		}
		m_rc = pCacheCluster->TryLock(strOwner, strItem, nLockPeriodInSecond, nTimeoutInMS);
}
	~CLockGuard()
	{
		if(RC_SUCCEEDED(m_rc))
		{
			if(m_pCacheCluster)
				m_pCacheCluster->Unlock(m_strOwner, m_strItem);
		}

	}
	ResultCode Result(){return m_rc;};
protected:
	boost::shared_ptr<CCacheCluster> m_pCacheCluster;
	std::string m_strOwner;
	std::string m_strItem;
	ResultCode m_rc;


};

#endif // CCACHECLUSTER_H
