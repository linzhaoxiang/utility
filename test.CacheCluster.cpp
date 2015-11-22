/*
 * test.CacheCluster.cpp
 * this file is for functional test only
 *  Created on: Jun 21, 2015
 *      Author: zlin
 */
#include "CacheCluster.h"
#include "test.Base.h"
#include <thread>
#include "StockDataConfig.h"

static	std::string s_strServerAddr;
static	int s_nPort;

class CacheClusterTester:public Stock::CClusterTestBase
{
public:
	CacheClusterTester():CClusterTestBase("CacheClusterTester"){}
protected:
	virtual void SetUp()
	{
		auto server = Stock::CStockDataConfig::Instance().GetCacheClusterServer();
		s_strServerAddr = server.first;
		s_nPort = server.second;
		ResultCode rc = m_cc.ConnectCacheServer(s_strServerAddr, s_nPort, 1000);
		ASSERT_GE(rc, 0) << "could not connect to Redis server, please check whether the server is started.";
	}

	ResultCode SetItemValue(const std::string& strOwner, const std::string& strItem, std::string& strValue, size_t nTimeOutInSecond = -1)
	{
		m_vectKey.push_back(std::make_pair(strOwner, strItem));
		return m_cc.SetItemValue(strOwner, strItem, strValue, nTimeOutInSecond);
	}

	virtual void TearDown()
	{
		for(auto& key: m_vectKey)
		{
			m_cc.RemoveItemValue(key.first, key.second);
		}
		m_cc.Unlock("aa", "bb");
		m_cc.Unlock("aa", "bb1");
	}
	CCacheCluster m_cc;
	std::vector<std::pair<std::string, std::string>> m_vectKey;


};

TEST_F(CacheClusterTester, Set_GetItemValue)
{
	std::string strOwner, strItem, strValue;
	ResultCode rc = Stock::RS_SUCCESS;
	Case("Case1: Set with Owner=empty, the value can get only with owner == empty");
	strItem = "aa";
	strValue = "value";
	rc = SetItemValue(strOwner, strItem, strValue);
	ASSERT_GE(rc, 0);
	strValue.clear();
	rc = m_cc.GetItemValue(strOwner, strItem, strValue);
	ASSERT_GE(rc, 0);
	ASSERT_STREQ("value", strValue.c_str());
	strOwner = "owner";
	rc = m_cc.GetItemValue(strOwner, strItem, strValue);
	ASSERT_LT(rc, 0);


	Case("Case1.1: Set with Item=empty, the value can get only with owner == empty");
	strOwner = "aa";
	strValue = "value";
	rc = SetItemValue(strOwner, strItem, strValue);
	ASSERT_GE(rc, 0);
	strValue.clear();
	rc = m_cc.GetItemValue(strOwner, strItem, strValue);
	ASSERT_GE(rc, 0);
	ASSERT_STREQ("value", strValue.c_str());
	strItem = "owner";
	rc = m_cc.GetItemValue(strOwner, strItem, strValue);
	ASSERT_LT(rc, 0);


	Case("Case2:Get no exists key, the key not exists");
	strOwner = "unknown";
	strItem = "unknown";
	rc = m_cc.GetItemValue(strOwner, strItem ,strValue);
	ASSERT_EQ(rc, Stock::RE_NOT_EXISTS);


	Case("Case3:Set the key value and then get the key, the value returned");
	strOwner = "owner";
	strItem = "item";
	strValue = "The value";
	rc = SetItemValue(strOwner, strItem, strValue);
	ASSERT_GE(rc, 0);
	strValue.clear();
	rc = m_cc.GetItemValue(strOwner, strItem ,strValue);
	ASSERT_GE(rc, 0);
	ASSERT_STREQ(strValue.c_str(), "The value");



	Case("Case4:The value has some invalid character such as nil");
	char buffer[1024];
	strOwner = "Case4";
	strItem = "Value4";
	for(int i = 0; i < 1024; i++)
	{
		buffer[i] = char(i);
	}
	strValue.assign(buffer, 1024);
	rc = SetItemValue(strOwner, strItem, strValue);
	ASSERT_GE(rc, 0);
	strValue.clear();
	rc = m_cc.GetItemValue(strOwner, strItem, strValue);
	ASSERT_EQ(strValue.size(), 1024);
	for(int i = 0; i < 1024; i++)
	{
		ASSERT_EQ(strValue[i], buffer[i]);
	}

	Case("Case5:the value is empty")
	strOwner = "Case5";
	strItem = "Value5";
	strValue.clear();
	rc = SetItemValue(strOwner, strItem, strValue);
	ASSERT_GE(rc, 0);
	rc = m_cc.GetItemValue(strOwner, strItem, strValue);
	ASSERT_GE(rc, 0);
	ASSERT_TRUE(strValue.empty());

	Case("Case6:get a timeout value, return not exists")
	strOwner = "Case6";
	strItem = "Value6";
	strValue = "Value7";
	rc = SetItemValue(strOwner, strItem, strValue, 1);
	ASSERT_GE(rc, 0);
	rc = m_cc.GetItemValue(strOwner, strItem, strValue);
	ASSERT_GE(rc, 0);
	ASSERT_STREQ(strValue.c_str(), "Value7");
	std::this_thread::sleep_for(std::chrono::milliseconds(2000));
	rc = m_cc.GetItemValue(strOwner, strItem, strValue);
	ASSERT_EQ(rc, Stock::RE_NOT_EXISTS);

	Case("Case7:get a item after 1 second, succeed")
	strOwner = "Case4";
	strItem = "Value4";
	rc = m_cc.GetItemValue(strOwner, strItem, strValue);
	ASSERT_GE(rc, 0);
	ASSERT_EQ(strValue.size(), 1024);




}

TEST_F(CacheClusterTester, RemoveItemValue)
{
	std::string strOwner, strItem, strValue;
	ResultCode rc = Stock::RS_SUCCESS;

	Case("Case1:Remove non exists key, failed");
	strOwner = "Case1";
	strItem = "Item1";
	rc = m_cc.RemoveItemValue(strOwner, strItem);
	ASSERT_EQ(rc, Stock::RE_NOT_EXISTS);


	Case("Case2:Remove Normal Key");
	strOwner = "Case2";
	strItem = "Item2";
	rc = this->SetItemValue(strOwner, strItem, strValue);
	ASSERT_GE(rc, 0);
	rc = m_cc.GetItemValue(strOwner, strItem, strValue);
	ASSERT_GE(rc, 0);
	rc = m_cc.RemoveItemValue(strOwner, strItem);
	ASSERT_GE(rc, 0);
	rc = m_cc.GetItemValue(strOwner, strItem, strValue);
	ASSERT_EQ(rc, Stock::RE_NOT_EXISTS);


	Case("Case3:Remove with same owner but not same item, failed");
	strOwner = "Case3";
	strItem = "Item3";
	strValue = "value";
	rc = this->SetItemValue(strOwner, strItem, strValue);
	ASSERT_GE(rc, 0);
	rc = m_cc.GetItemValue(strOwner, strItem, strValue);
	ASSERT_GE(rc, 0);
	rc = m_cc.RemoveItemValue("wsd", strItem);
	ASSERT_LT(rc, 0);
	rc = m_cc.RemoveItemValue(strOwner, "dsds");
	ASSERT_LT(rc, 0);
	rc = m_cc.GetItemValue(strOwner, strItem, strValue);
	ASSERT_GE(rc, 0);


	Case("Case4:Remove with owner empty, failed, the value still avail")
	strOwner = "Case4";
	strItem = "Item4";
	strValue = "value";
	rc = this->SetItemValue(strOwner, strItem, strValue);
	ASSERT_GE(rc, 0);
	rc = m_cc.GetItemValue(strOwner, strItem, strValue);
	ASSERT_GE(rc, 0);
	rc = m_cc.RemoveItemValue("", strItem);
	ASSERT_LT(rc, 0);
	rc = m_cc.GetItemValue(strOwner, strItem, strValue);
	ASSERT_GE(rc, 0);
	Case("Case5:Remove with Item empty, failed, the value still avail")

	rc = m_cc.RemoveItemValue(strOwner, "");
	ASSERT_LT(rc, 0);
	rc = m_cc.GetItemValue(strOwner, strItem, strValue);
	ASSERT_GE(rc, 0);


	Case("Case6:Remove a timeout value, failed")
	strOwner = "Case6";
	strItem = "Value6";
	strValue = "Value7";
	rc = SetItemValue(strOwner, strItem, strValue, 1);
	ASSERT_GE(rc, 0);
	rc = m_cc.GetItemValue(strOwner, strItem, strValue);
	ASSERT_GE(rc, 0);
	ASSERT_STREQ(strValue.c_str(), "Value7");
	std::this_thread::sleep_for(std::chrono::milliseconds(3000));
	rc = m_cc.RemoveItemValue(strOwner, strItem);
	ASSERT_EQ(rc, Stock::RE_NOT_EXISTS);

}


TEST_F(CacheClusterTester, TryGetProduceRight)
{
	std::string strOwner, strItem, strValue;
	ResultCode rc = Stock::RS_SUCCESS;


	Case("Case1:Get for not exists key, succeeded")
	strOwner = "Case1";
	strItem = "Item1";
	rc = m_cc.TryGetProduceRight(strOwner, strItem, 1);
	ASSERT_GE(rc, 0);
	rc = m_cc.GetItemValue(strOwner, strItem ,strValue);
	ASSERT_LT(rc, 0);



	Case("Case2:Get for two times ,the second will failed")
	strOwner = "Case2";
	strItem = "Item2";
	rc = m_cc.TryGetProduceRight(strOwner, strItem, 1);
	ASSERT_GE(rc, 0);
	rc = m_cc.TryGetProduceRight(strOwner, strItem, 1);
	ASSERT_EQ(rc, Stock::RE_BUSY);



	Case("Case3:Get for a exists key, failed")
	strOwner = "case3";
	strItem = "item3";
	rc = this->SetItemValue(strOwner, strItem, strValue);
	rc = m_cc.TryGetProduceRight(strOwner, strItem, 1);
	ASSERT_EQ(rc, Stock::RE_ALREADY_EXISTS);


	Case("Case4:Get the key and then wait timeout, get again, success")
	strOwner = "Case4";
	strItem = "Item4";
	rc = m_cc.TryGetProduceRight(strOwner, strItem, 1);
	ASSERT_GE(rc, 0);
	std::this_thread::sleep_for(std::chrono::milliseconds(2010));
	rc = m_cc.TryGetProduceRight(strOwner, strItem, 1);
	ASSERT_GE(rc, 0);
}
void CreateItem(void* p)
{
	std::this_thread::sleep_for(std::chrono::milliseconds(50));
	std::string strOwner, strItem, strValue;
	ResultCode rc = Stock::RS_SUCCESS;
	CCacheCluster* pServer = (CCacheCluster*)p;
	if(pServer == nullptr)
		return;
	CCacheCluster m_cc;
	m_cc.ConnectCacheServer(s_strServerAddr, s_nPort, 1000);
	strOwner = "Case3";
	strItem = "Item3";
	m_cc.SetItemValue(strOwner, strItem, strValue);
	LogDebug() << "Async set itemvalue success";
	return;

}
TEST_F(CacheClusterTester, WaitForItemValue)
{
	std::string strOwner, strItem, strValue;
	ResultCode rc = Stock::RS_SUCCESS;
	std::chrono::time_point<std::chrono::system_clock> currentTime = std::chrono::system_clock::now();// = std::chrono::system_clock::now();
	Case("Case1:for not exists item,failed until timeout");
	strOwner = "Case1";
	strItem = "Item1";
	auto nStart = std::chrono::duration_cast<std::chrono::milliseconds>(currentTime.time_since_epoch()).count();
	rc = m_cc.WaitForItemValue(strOwner, strItem, 500);
	ASSERT_EQ(rc, Stock::RE_TIME_OUT);
	currentTime = std::chrono::system_clock::now();
	ASSERT_GE(std::chrono::duration_cast<std::chrono::milliseconds>(currentTime.time_since_epoch()).count() - nStart, 500);
	ASSERT_LT(std::chrono::duration_cast<std::chrono::milliseconds>(currentTime.time_since_epoch()).count() - nStart, 1000);

	Case("Case2:for exists item, return immediately")
	strOwner= "Case2";
	strItem = "Item2";
	rc = this->SetItemValue(strOwner, strItem, strValue);
	ASSERT_GE(rc, 0);
	currentTime = std::chrono::system_clock::now();
	nStart = std::chrono::duration_cast<std::chrono::milliseconds>(currentTime.time_since_epoch()).count();
	rc = m_cc.WaitForItemValue(strOwner, strItem, 500);
	currentTime = std::chrono::system_clock::now();
	ASSERT_GE(rc, 0);
	ASSERT_LT(std::chrono::duration_cast<std::chrono::milliseconds>(currentTime.time_since_epoch()).count() - nStart, 400);

	Case("Case3:for non-exists item, and then another thread create the item, return after the item created")
	strOwner= "Case3";
	strItem = "Item3";
	m_cc.RemoveItemValue(strOwner, strItem);
	currentTime = std::chrono::system_clock::now();
	nStart = std::chrono::duration_cast<std::chrono::milliseconds>(currentTime.time_since_epoch()).count();
	std::thread CreateItemThread(CreateItem, &m_cc);
	CreateItemThread.detach();
	rc = m_cc.WaitForItemValue(strOwner, strItem, 500);
	ASSERT_GE(rc, 0);
	currentTime = std::chrono::system_clock::now();
	ASSERT_LT(std::chrono::duration_cast<std::chrono::milliseconds>(currentTime.time_since_epoch()).count() - nStart, 500);
	ASSERT_GE(std::chrono::duration_cast<std::chrono::milliseconds>(currentTime.time_since_epoch()).count() - nStart, 50);
	SetItemValue(strOwner, strItem, strValue);//so that the tester will clean the key before exit.

}


TEST_F(CacheClusterTester, Exists)
{
	std::string strOwner, strItem, strValue;
	bool bExists = false;
	ResultCode rc = Stock::RS_SUCCESS;
	Case("Case1:exists for non-exists item");
	strOwner = "Case1";
	strItem = "Item1";
	rc = m_cc.Exists(strOwner, strItem, bExists);
	ASSERT_GE(rc, 0);
	ASSERT_FALSE(bExists);

	Case("Case2:exists for exists item");
	strOwner = "Case2";
	strItem = "Item2";
	SetItemValue(strOwner, strItem, strValue);
	rc = m_cc.Exists(strOwner, strItem, bExists);
	ASSERT_GE(rc, 0);
	ASSERT_TRUE(bExists);


	Case("Case3:exists for timeout item");
	strOwner = "Case3";
	strItem = "Item3";
	SetItemValue(strOwner, strItem, strValue, 1);
	std::this_thread::sleep_for(std::chrono::milliseconds(2000));
	rc = m_cc.Exists(strOwner, strItem, bExists);
	ASSERT_GE(rc, 0);
	ASSERT_FALSE(bExists);
}

TEST_F(CacheClusterTester, IsLocalCacheAvail_SetLocalAvail)
{
	std::string strOwner="owner", strItem = "item";
	Case("Case1:IsLocalCacheAvail without set");
	ASSERT_FALSE(m_cc.IsLocalCacheAvail(strOwner, strItem));

	Case("Case2:IsLocalCacheAvail with Set")
	m_cc.SetLocalCacheAvail(strOwner, strItem);
	ASSERT_TRUE(m_cc.IsLocalCacheAvail(strOwner, strItem));
}




TEST_F(CacheClusterTester, TryLock_Unlock)
{
	ResultCode rc = Stock::RS_SUCCESS;
	Case("Case1:first Lock,succeeded");
	rc = m_cc.TryLock("aa", "bb", 100, 50);
	ASSERT_GE(rc, 0);
	rc = m_cc.TryLock("aa", "bb1", 100, 50);
	ASSERT_GE(rc, 0);


	Case("Case2:second Lock, failed");
	rc = m_cc.TryLock("aa", "bb", 100, 50);
	ASSERT_LT(rc, 0);

	Case("Case3:unlock, succeeded");
	rc = m_cc.Unlock("aa", "bb");
	ASSERT_GE(rc, 0);
	Case("Case4:Lock again, succeeded")
	rc = m_cc.TryLock("aa", "bb", 100, 50);
	ASSERT_GE(rc, 0);

	Case("Case5:unlock, succeeded");
	rc = m_cc.Unlock("aa", "bb");
	ASSERT_GE(rc, 0);
	Case("Case6:unlock again, succeeded");
	rc = m_cc.Unlock("aa", "bb");
	ASSERT_GE(rc, 0);
	Case("Case7:Lock, wait time out, lock again")
	rc = m_cc.TryLock("aa", "bb", 2, 50);
	ASSERT_GE(rc, 0);
	rc = m_cc.TryLock("aa", "bb", 2, 0);
	ASSERT_LT(rc, 0);
	rc = m_cc.TryLock("aa", "bb", 2, 50);
	ASSERT_LT(rc, 0);
	rc = m_cc.TryLock("aa", "bb", 100, 4500);
	ASSERT_GE(rc, 0);


	//exit
	rc = m_cc.Unlock("aa", "bb1");
	ASSERT_GE(rc, 0);

}

