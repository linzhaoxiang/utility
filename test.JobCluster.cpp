/*
 * test.ServerCordinator.cpp
 *
 *  Created on: May 30, 2015
 *      Author: zlin
 */

#include <gtest/gtest.h>
#include "JobCluster.h"
#include <thread>
#include <chrono>
#include "Log.h"
#include "test.Base.h"
#include "StockDataConfig.h"

static std::string ZOO_KEEPER_SERVER;
static int ZOO_KEEPER_PORT;

class JobClusterTester:public Stock::CClusterTestBase
{
public:
	JobClusterTester():CClusterTestBase("JobClusterTester"){}
protected:
	virtual void SetUp()
	{
		if(Stock::CStockDataConfig::Instance().GetJobClusterServerList().empty()) return;
		ZOO_KEEPER_SERVER = Stock::CStockDataConfig::Instance().GetJobClusterServerList()[0].first;
		ZOO_KEEPER_PORT = Stock::CStockDataConfig::Instance().GetJobClusterServerList()[0].second;
		ResultCode rc = m_jc.ConnectToCluster({{ZOO_KEEPER_SERVER,ZOO_KEEPER_PORT}}, 3000);
		ASSERT_GE(rc, 0) << "could not connect to zookeeper server, please check whether the server is started.";
	}

	virtual void TearDown()
	{
		m_jc.UnlockJob("JobClusterTest_Job1");
		m_jc.UnlockJob("JobClusterTest_Job2");
		m_jc.UnlockJob("JobClusterTest_Job3");
		m_jc.TerminateJob("JobClusterTest_Job1");
		m_jc.TerminateJob("JobClusterTester_Job2");
		m_jc.TerminateJob("JobClusterTester_Job3");
		m_jc.DisconnectFromCluster();
	}
	CJobCluster m_jc;

};


TEST_F(JobClusterTester, ConnectToCluster)
{
	CJobCluster jc;

	//case 1. connect to normal server
	ResultCode rc = jc.ConnectToCluster({{ZOO_KEEPER_SERVER,ZOO_KEEPER_PORT}}, 3000);
	ASSERT_GE(rc, 0) << "could not connect to zookeeper server, please check whether the server is started.";

	//case 2.connect again to normal server
	rc = jc.ConnectToCluster({{ZOO_KEEPER_SERVER,ZOO_KEEPER_PORT}}, 3000);
	ASSERT_EQ(rc, 8);

	//case 3. connect to nonexists server, error.
	jc.DisconnectFromCluster();
	rc = jc.ConnectToCluster({{ZOO_KEEPER_SERVER, ZOO_KEEPER_PORT+1}}, 3000);
	ASSERT_LT(rc, 0);


	//case 4.after err, connect again
	rc = jc.ConnectToCluster({{ZOO_KEEPER_SERVER,ZOO_KEEPER_PORT}}, 3000);
	ASSERT_GE(rc, 0);

}


TEST_F(JobClusterTester, CreateJob_GetJobConfig)
{
	ResultCode rc = RS_SUCCESS;;
	std::string strJobConfig1 = "This is job config data 1111";
	std::string strJobConfig2 = "This is a second job config data";
	std::string strJobConfig;
	//case 1.Create a job;
	rc = m_jc.CreateJob("JobClusterTest_Job1", strJobConfig1);
	ASSERT_GE(rc, 0);

	//case1.1 create job again, succeeded.
	rc = m_jc.CreateJob("JobClusterTest_Job1", strJobConfig2);
	ASSERT_GE(rc, 0);

	//case 2.Create 2nd job
	rc = m_jc.CreateJob("JobClusterTester_Job2", strJobConfig2);
	ASSERT_GE(rc, 0);

	//case 3.GetJobConfig, data correct
	rc = m_jc.GetJobConfig("JobClusterTest_Job1", strJobConfig);
	ASSERT_GE(rc, 0);
	ASSERT_STREQ(strJobConfig1.c_str(), strJobConfig.c_str());

	rc = m_jc.GetJobConfig("JobClusterTester_Job2", strJobConfig);
	ASSERT_GE(rc, 0);
	ASSERT_STREQ(strJobConfig2.c_str(), strJobConfig.c_str());


	//case 4.GetJob     config for nonexists job, failed
	rc = m_jc.GetJobConfig("none", strJobConfig);
	ASSERT_LT(rc,0);

	m_jc.TerminateJob("JobClusterTest_Job1");
	rc = m_jc.GetJobConfig("JobClusterTest_Job1", strJobConfig);
	ASSERT_GE(rc, 0);
	m_jc.TerminateJob("JobClusterTest_Job1");
	rc = m_jc.GetJobConfig("JobClusterTest_Job1", strJobConfig);
	ASSERT_LT(rc, 0);
	m_jc.TerminateJob("JobClusterTest_Job2");




}



TEST_F(JobClusterTester, AddTask)
{
	ResultCode rc = RS_SUCCESS;
	std::string strJobName = "JobClusterTest_Job1", strTaskData = "datadatadfasdds";
	std::string strTaskName, strValue;
	//case 1. add task before create job, failed;
	rc = m_jc.AddTask(strJobName, strTaskData, strTaskName);
	ASSERT_LT(rc, 0);

	//case 2. add task after create job, succeeded.
	rc = m_jc.CreateJob(strJobName, "jobdata");
	ASSERT_GE(rc,0);
	rc = m_jc.AddTask(strJobName, strTaskData, strTaskName);
	ASSERT_GE(rc, 0);
	ASSERT_TRUE(!strTaskName.empty());
	rc = m_jc.RegisiterWorker();
	ASSERT_GE(rc, 0);
	rc = m_jc.TakeTask(strJobName, strTaskName, strValue);
	ASSERT_GE(rc, 0);
	ASSERT_STREQ(strValue.c_str(), strTaskData.c_str());
	rc = m_jc.TerminateJob(strJobName);
	ASSERT_GE(rc, 0);
	rc = m_jc.UnregisterWorker();
	ASSERT_GE(rc, 0);

	//case 3. add task after create job, job name is not correct.
	rc = m_jc.CreateJob(strJobName, "jobdata");
	ASSERT_GE(rc,0);
	std::string strJobName1 = strJobName + "1";
	rc = m_jc.AddTask(strJobName1, strTaskData, strTaskName);
	ASSERT_LT(rc, 0);
	rc = m_jc.RegisiterWorker();
	ASSERT_GE(rc, 0);

	rc = m_jc.TakeTask(strJobName1, strTaskName, strValue);
	ASSERT_LT(rc, 0);
	//ASSERT_STREQ(strValue.c_str(), strTaskData.c_str());


	Case("4.Add task two times, task name is not the same")
	rc = m_jc.AddTask(strJobName, strTaskData, strTaskName);
	ASSERT_GE(rc, 0);
	auto strName1 = strTaskName;
	rc = m_jc.AddTask(strJobName, strTaskData, strTaskName);
	ASSERT_GE(rc, 0);
	auto strName2 = strTaskName;
	ASSERT_TRUE(strName1 != strName2);
}


TEST_F(JobClusterTester, RegisterWorker_UnregisterWorker_GetWorkerCount)
{
	CJobCluster jc1,jc2,jc3;
	ResultCode rc = RS_SUCCESS;
	//case 1.ResterWorker before connect, failed.
	rc = jc1.RegisiterWorker();
	ASSERT_LT(rc, 0);
	//case 2.RegisterWorker after connnect ,succeed.
	rc = jc1.ConnectToCluster({{ZOO_KEEPER_SERVER,ZOO_KEEPER_PORT}}, 3000);
	ASSERT_GE(rc,0);
	rc = jc1.RegisiterWorker();
	ASSERT_GE(rc, 0);
	rc = jc1.UnregisterWorker();
	ASSERT_GE(rc, 0);
	rc = jc1.DisconnectFromCluster();
	ASSERT_GE(rc, 0);
	//case 3.Register two times, the second failed.
	rc = jc1.ConnectToCluster({{ZOO_KEEPER_SERVER,ZOO_KEEPER_PORT}}, 3000);
	ASSERT_GE(rc,0);
	rc = jc1.RegisiterWorker();
	ASSERT_GE(rc, 0);
	rc = jc1.RegisiterWorker();
	ASSERT_LT(rc, 0);
	rc = jc1.UnregisterWorker();
	ASSERT_GE(rc, 0);
	rc = jc1.DisconnectFromCluster();
	ASSERT_GE(rc,0);
	//case 4.Unregister before register.
	rc = jc1.ConnectToCluster({{ZOO_KEEPER_SERVER,ZOO_KEEPER_PORT}}, 3000);
	rc = jc1.UnregisterWorker();
	ASSERT_LT(rc, 0);
	rc = jc1.DisconnectFromCluster();
	ASSERT_GE(rc,0);
	//case 5.Register then unregister.
	rc = jc1.ConnectToCluster({{ZOO_KEEPER_SERVER,ZOO_KEEPER_PORT}}, 3000);
	ASSERT_GE(rc,0);
	rc = jc1.RegisiterWorker();
	ASSERT_GE(rc, 0);
	rc = jc1.UnregisterWorker();
	ASSERT_GE(rc, 0);
	rc = jc1.DisconnectFromCluster();
	ASSERT_GE(rc,0);
	//case 6. Register then unregister and then register again.
	rc = jc1.ConnectToCluster({{ZOO_KEEPER_SERVER,ZOO_KEEPER_PORT}}, 3000);
	ASSERT_GE(rc,0);
	rc = jc1.RegisiterWorker();
	ASSERT_GE(rc, 0);
	rc = jc1.UnregisterWorker();
	ASSERT_GE(rc, 0);
	rc = jc1.RegisiterWorker();
	ASSERT_GE(rc, 0);
	rc = jc1.DisconnectFromCluster();
	ASSERT_GE(rc,0);
	//case 7 two jc, the workercount is correct.
	rc = jc1.ConnectToCluster({{ZOO_KEEPER_SERVER,ZOO_KEEPER_PORT}}, 3000);
	ASSERT_GE(rc,0);
	rc = jc2.ConnectToCluster({{ZOO_KEEPER_SERVER,ZOO_KEEPER_PORT}}, 3000);
	ASSERT_GE(rc, 0);
	ASSERT_EQ(jc1.GetWorkerCount(), jc2.GetWorkerCount());
	ASSERT_EQ(jc1.GetWorkerCount(), 0);
	rc = jc1.RegisiterWorker();
	ASSERT_GE(rc, 0);
	ASSERT_EQ(jc1.GetWorkerCount(), jc2.GetWorkerCount());
	ASSERT_EQ(jc1.GetWorkerCount(), 1);
	rc = jc2.RegisiterWorker();
	ASSERT_GE(rc, 0);
	ASSERT_EQ(jc1.GetWorkerCount(), jc2.GetWorkerCount());
	ASSERT_EQ(jc1.GetWorkerCount(), 2);


	//case 8.three jc, on disconnected, one unregister, workcount = 1;
	rc = jc3.ConnectToCluster({{ZOO_KEEPER_SERVER,ZOO_KEEPER_PORT}}, 3000);
	ASSERT_GE(rc, 0);
	rc = jc3.RegisiterWorker();
	ASSERT_GE(rc, 0);
	ASSERT_EQ(jc1.GetWorkerCount(), jc2.GetWorkerCount());
	ASSERT_EQ(jc1.GetWorkerCount(), 3);
	rc = jc2.DisconnectFromCluster();
	ASSERT_EQ(rc, 0);
	ASSERT_EQ(jc1.GetWorkerCount(), jc3.GetWorkerCount());
	ASSERT_EQ(jc1.GetWorkerCount(), 2);
	rc = jc3.UnregisterWorker();
	ASSERT_GE(rc, 0);
	ASSERT_EQ(jc1.GetWorkerCount(), jc3.GetWorkerCount());
	ASSERT_EQ(jc1.GetWorkerCount(), 1);



}


TEST_F(JobClusterTester, GetTaskStatus)
{
	CJobCluster jc1,jc2;
	std::string strJobName, strTaskName="aa", strTaskContent;
	ResultCode rc = RS_SUCCESS;
	CJobCluster::EnumTaskStatus eStatus;
	rc = jc1.ConnectToCluster({{ZOO_KEEPER_SERVER,ZOO_KEEPER_PORT}}, 3000);
	ASSERT_GE(rc, 0);
	ASSERT_GE(rc, 0);
	rc = jc1.RegisiterWorker();
	ASSERT_GE(rc, 0);
	strJobName = "JobClusterTest_Job1";


	Case("Case1:Get status for non-exists job");
	rc = jc1.GetTaskStatus(strJobName, strTaskName ,eStatus);
	ASSERT_GE(rc, 0);
	ASSERT_EQ(eStatus, CJobCluster::TASK_STATUS_NOT_EXISTS);



	Case("Case2:Get Status for non-exists task");
	rc = jc1.CreateJob(strJobName, strTaskContent);
	ASSERT_GE(rc, 0);
	rc = jc1.GetTaskStatus(strJobName, strTaskName ,eStatus);
	ASSERT_GE(rc, 0);
	ASSERT_EQ(eStatus, CJobCluster::TASK_STATUS_NOT_EXISTS);


	Case("Case3:GetStatus for waiting task")
	rc = jc1.AddTask(strJobName, strTaskContent, strTaskName);
	ASSERT_GE(rc, 0);
	rc = jc1.GetTaskStatus(strJobName, strTaskName, eStatus);
	ASSERT_GE(rc, 0);
	ASSERT_EQ(eStatus, CJobCluster::TASK_STATUS_WAITING);

	Case("Case4:GetStatus for working task");
	rc = jc1.TakeTask(strJobName, strTaskName, strTaskContent);
	ASSERT_GE(rc, 0);
	rc = jc1.GetTaskStatus(strJobName, strTaskName, eStatus);
	ASSERT_GE(rc, 0);
	ASSERT_EQ(eStatus, CJobCluster::TASK_STATUS_RUNNING);


	Case("Case4:GetStatus for completed task")
	rc = jc1.FinishTask(strJobName, strTaskName);
	ASSERT_GE(rc, 0);
	rc = jc1.GetTaskStatus(strJobName, strTaskName ,eStatus);
	ASSERT_GE(rc, 0);
	ASSERT_EQ(eStatus, CJobCluster::TASK_STATUS_NOT_EXISTS);



}




TEST_F(JobClusterTester, TakeTask_FinishTask)
{
	CJobCluster jc1,jc2;
	std::string strJobName, strTaskName, strTaskContent;
	ResultCode rc = RS_SUCCESS;
	//case 1.take without register, failed
	rc = jc1.TakeTask(strJobName, strTaskName, strTaskContent);
	ASSERT_LT(rc, 0);

	//case 2.take when no job, failed.

	rc = jc1.ConnectToCluster({{ZOO_KEEPER_SERVER,ZOO_KEEPER_PORT}}, 3000);
	ASSERT_GE(rc, 0);
	ASSERT_GE(rc, 0);
	rc = jc1.RegisiterWorker();
	ASSERT_GE(rc, 0);
	rc = jc1.TakeTask(strJobName, strTaskName, strTaskContent);
	ASSERT_LT(rc, 0);


	//case 3.take when have job but no task,failed.
	rc = jc2.ConnectToCluster({{ZOO_KEEPER_SERVER,ZOO_KEEPER_PORT}}, 3000);
	rc = jc2.RegisiterWorker();
	ASSERT_GE(rc, 0);
	rc = jc2.CreateJob("JobClusterTest_Job1", "This is the second job");
	ASSERT_GE(rc, 0);
	rc = jc1.TakeTask(strJobName, strTaskName, strTaskContent);
	ASSERT_LT(rc, 0);


	//case 4.normal take, succeeded.
	rc = jc2.AddTask("JobClusterTest_Job1", "this is first task", strTaskName);
	ASSERT_GE(rc, 0);
	rc = jc1.TakeTask(strJobName, strTaskName, strTaskContent);
	ASSERT_GE(rc, 0);
	ASSERT_STREQ(strTaskContent.c_str(), "this is first task");
	rc = jc1.TerminateJob("JobClusterTest_Job1");
	strJobName.clear();
	strTaskName.clear();
	strTaskContent.clear();

	//case 5.only one task, take two times, the second failed.
	rc = jc1.CreateJob("JobClusterTest_Job1", "This is the first job");
	rc = jc2.AddTask("JobClusterTest_Job1", "this is first task", strTaskName);
	ASSERT_GE(rc, 0);
	rc = jc1.TakeTask(strJobName, strTaskName, strTaskContent);
	ASSERT_GE(rc, 0);
	ASSERT_STREQ(strTaskContent.c_str(), "this is first task");
	strJobName.clear();
	strTaskName.clear();
	strTaskContent.clear();
	rc = jc1.TakeTask(strJobName, strTaskName,strTaskContent);
	ASSERT_LT(rc, 0);
	rc = jc1.TerminateJob("JobClusterTest_Job1");
	ASSERT_GE(rc, 0);
	strJobName.clear();
	strTaskName.clear();
	strTaskContent.clear();

	//case 6 only one task, jc1 take it, and them jc2 take it, jc2 failed.
	rc = jc1.CreateJob("JobClusterTest_Job1", "This is the first job");
	rc = jc2.AddTask("JobClusterTest_Job1", "this is first task", strTaskName);
	ASSERT_GE(rc, 0);
	rc = jc1.TakeTask(strJobName, strTaskName, strTaskContent);
	ASSERT_GE(rc, 0);
	ASSERT_STREQ(strTaskContent.c_str(), "this is first task");
	strJobName.clear();
	strTaskName.clear();
	strTaskContent.clear();
	rc = jc2.TakeTask(strJobName, strTaskName,strTaskContent);
	ASSERT_LT(rc, 0);
	rc = jc1.TerminateJob("JobClusterTest_Job1");
	ASSERT_GE(rc, 0);
	strJobName.clear();
	strTaskName.clear();
	strTaskContent.clear();
	//case 7.only one task, jc1 take it and then jc1 disconnected, jc2 can take it again.
	rc = jc1.CreateJob("JobClusterTest_Job1", "This is the first job");
	rc = jc2.AddTask("JobClusterTest_Job1", "this is first task", strTaskName);
	ASSERT_GE(rc, 0);
	rc = jc1.TakeTask(strJobName, strTaskName, strTaskContent);
	ASSERT_GE(rc, 0);
	ASSERT_STREQ(strTaskContent.c_str(), "this is first task");
	strJobName.clear();
	strTaskName.clear();
	strTaskContent.clear();
	rc = jc1.DisconnectFromCluster();
	//sleep(1);
	rc = jc1.ConnectToCluster({{ZOO_KEEPER_SERVER,ZOO_KEEPER_PORT}}, 3000);
	ASSERT_GE(rc, 0);
	rc = jc2.TakeTask(strJobName, strTaskName,strTaskContent);
	ASSERT_GE(rc, 0);
	rc = jc1.TerminateJob("JobClusterTest_Job1");
	ASSERT_GE(rc, 0);
	strJobName.clear();
	strTaskName.clear();
	strTaskContent.clear();
	//case 8.only one task, jc1 take it and then jc1 unregister, jc2 can take it again.
	rc = jc1.CreateJob("JobClusterTest_Job1", "This is the first job");
	rc = jc2.AddTask("JobClusterTest_Job1", "this is first task", strTaskName);
	ASSERT_GE(rc, 0);
	rc = jc1.RegisiterWorker();
	ASSERT_GE(rc, 0);
	rc = jc1.TakeTask(strJobName, strTaskName, strTaskContent);
	ASSERT_GE(rc, 0);
	ASSERT_STREQ(strTaskContent.c_str(), "this is first task");
	strJobName.clear();
	strTaskName.clear();
	strTaskContent.clear();
	rc = jc1.UnregisterWorker();
	//sleep(1);
	rc = jc1.RegisiterWorker();
	//sleep(1);
	rc = jc2.TakeTask(strJobName, strTaskName,strTaskContent);
	ASSERT_GE(rc, 0);
	rc = jc1.TerminateJob("JobClusterTest_Job1");
	ASSERT_GE(rc, 0);
	strJobName.clear();
	strTaskName.clear();
	strTaskContent.clear();
	//case 9.only one task, jc1 take it and finished it, jc2 take task failed.
	rc = jc1.CreateJob("JobClusterTest_Job1", "This is the first job");
	rc = jc2.AddTask("JobClusterTest_Job1", "this is first task", strTaskName);
	ASSERT_GE(rc, 0);
	rc = jc1.TakeTask(strJobName, strTaskName, strTaskContent);
	ASSERT_GE(rc, 0);
	ASSERT_STREQ(strTaskContent.c_str(), "this is first task");

	rc = jc1.FinishTask(strJobName, strTaskName);
	strJobName.clear();
	strTaskName.clear();
	strTaskContent.clear();
	rc = jc2.TakeTask(strJobName, strTaskName,strTaskContent);
	ASSERT_LT(rc, 0);
	rc = jc1.TerminateJob("JobClusterTest_Job1");
	ASSERT_GE(rc, 0);
	strJobName.clear();
	strTaskName.clear();
	strTaskContent.clear();
	//case 10.only one task, jc1 take it and finished it, jc1 take task again will fail.
	rc = jc1.CreateJob("JobClusterTest_Job1", "This is the first job");
	rc = jc2.AddTask("JobClusterTest_Job1", "this is first task", strTaskName);
	ASSERT_GE(rc, 0);
	rc = jc1.TakeTask(strJobName, strTaskName, strTaskContent);
	ASSERT_GE(rc, 0);
	ASSERT_STREQ(strTaskContent.c_str(), "this is first task");

	rc = jc1.FinishTask(strJobName, strTaskName);
	strJobName.clear();
	strTaskName.clear();
	strTaskContent.clear();
	rc = jc1.TakeTask(strJobName, strTaskName,strTaskContent);
	ASSERT_LT(rc, 0);
	rc = jc1.TerminateJob("JobClusterTest_Job1");
	ASSERT_GE(rc, 0);
	strJobName.clear();
	strTaskName.clear();
	strTaskContent.clear();
	//case 11. take with jobname not empty, will only take the tasks whose name = jobname
	rc = jc1.CreateJob("JobClusterTest_Job1", "This is the first job");
	rc = jc2.AddTask("JobClusterTest_Job1", "this is first task", strTaskName);
	ASSERT_GE(rc, 0);
	strJobName = "Jobn";
	rc = jc1.TakeTask(strJobName, strTaskName, strTaskContent);
	ASSERT_LT(rc, 0);
	//ASSERT_STREQ(strTaskContent.c_str(), "this is first task");
	strJobName = "JobClusterTest_Job1";
	rc = jc1.TakeTask(strJobName, strTaskName, strTaskContent);
	ASSERT_GE(rc, 0);
	ASSERT_STREQ(strTaskContent.c_str(), "this is first task");

	rc = jc1.TerminateJob("JobClusterTest_Job1");
	ASSERT_GE(rc, 0);
	strJobName.clear();
	strTaskName.clear();
	strTaskContent.clear();


	//case 12. finish a none exists task,fail
	rc = jc1.CreateJob("JobClusterTest_Job1", "This is the first Job");
	ASSERT_GE(rc, 0);
	rc = jc1.AddTask("JobClusterTest_Job1", "TASSDF", strTaskName);
	ASSERT_GE(rc, 0);
	rc = jc1.FinishTask("JobClusterTest_Job1", "testtask");
	ASSERT_LT(rc, 0);
}

unsigned long GetTickCount()
{
    struct timespec ts;

    clock_gettime(CLOCK_MONOTONIC, &ts);

    return (ts.tv_sec * 1000 + ts.tv_nsec / 1000000);

}

void TerminateTask(void * pParameter)
{
	CJobCluster * pJc = (CJobCluster*)pParameter;
	if(pJc == nullptr) return;
	std::this_thread::sleep_for(std::chrono::milliseconds(50));
	//sleep(1);
	ResultCode rc = pJc->DisconnectFromCluster();
	delete pJc;
}

void create_task(void* pPara)
{
	CJobCluster newJc;
	std::string strTaskName;
	newJc.ConnectToCluster({{ZOO_KEEPER_SERVER,ZOO_KEEPER_PORT}}, 3000);
	std::this_thread::sleep_for(std::chrono::milliseconds(50));
	//sleep(1);
	ResultCode rc = newJc.AddTask("JobClusterTest_Job1", "async job.", strTaskName);
	LogErrorCode(rc);
	LogDebug() << "async add task succeeded";
	newJc.DisconnectFromCluster();
}


TEST_F(JobClusterTester, WaitForNewTask_TakeTask)
{
	CJobCluster jc1,jc2;
	std::string strTaskName;
	ResultCode rc = RS_SUCCESS;
	rc = jc1.ConnectToCluster({{ZOO_KEEPER_SERVER,ZOO_KEEPER_PORT}}, 3000);
	ASSERT_GE(rc, 0);
	rc = jc2.ConnectToCluster({{ZOO_KEEPER_SERVER,ZOO_KEEPER_PORT}}, 3000);
	ASSERT_GE(rc, 0);
	rc = jc1.RegisiterWorker();
	ASSERT_GE(rc, 0);
	rc = jc2.RegisiterWorker();
	ASSERT_GE(rc, 0);

	Case("case 1.no task, wait until time out");
	size_t ticket = GetTickCount();
	rc = jc1.WaitForNewTask("", 100);
	ASSERT_GE(GetTickCount() - ticket, 90);
	ticket = GetTickCount();
	ASSERT_EQ(rc, RE_TIME_OUT);
	rc = jc2.CreateJob("JobClusterTest_Job1", "This is a job config data");
	ASSERT_GE(rc, 0);
	rc = jc1.WaitForNewTask("", 100);
	ASSERT_EQ(rc, RE_TIME_OUT);
	ASSERT_GE(GetTickCount() - ticket, 90);
	ticket = GetTickCount();
	rc = jc1.TerminateJob("JobClusterTest_Job1");
	ASSERT_GE(rc, 0);

	Case("case 2: no task, time out = 0");
	ticket = GetTickCount();
	rc = jc1.WaitForNewTask("", 0);
	ASSERT_LT(GetTickCount() - ticket, 50);
	ticket = GetTickCount();
	ASSERT_EQ(rc, RE_TIME_OUT);
	rc = jc2.CreateJob("JobClusterTest_Job1", "This is a job config data");
	ASSERT_GE(rc, 0);
	rc = jc1.WaitForNewTask("", 0);
	ASSERT_EQ(rc, RE_TIME_OUT);
	ASSERT_LT(GetTickCount() - ticket, 500);
	ticket = GetTickCount();

	Case("case 3.has job, return immediately");
	rc = jc2.AddTask("JobClusterTest_Job1", "The task data", strTaskName);
	ASSERT_GE(rc, 0);
	rc = jc1.WaitForNewTask("", 1000);
	ASSERT_GE(rc, 0);
	ASSERT_LT(GetTickCount() - ticket, 500) << ticket << "vs"<<GetTickCount();
	ticket = GetTickCount();

	Case("case 4.has job, time out = 0, return immediate ly and the job result");
	rc = jc1.WaitForNewTask("", 0);
	ASSERT_GE(rc, 0);
	ASSERT_LT(GetTickCount() - ticket, 500) << ticket << "vs"<<GetTickCount();
	ticket = GetTickCount();


	Case("case 5.wait for given job, no task on given job, wait till timeout");
	rc = jc2.CreateJob("JobClusterTester_Job2", "JobData for JobClusterTester_Job2");
	ASSERT_GE(rc, 0);
	rc = jc1.WaitForNewTask("JobClusterTester_Job2", 100);
	ASSERT_EQ(rc, RE_TIME_OUT);
	ASSERT_GE(GetTickCount() - ticket, 90);
	ticket = GetTickCount();


	Case("case 6.wait for given job, has task on given job, return immediately");
	rc = jc2.WaitForNewTask("JobClusterTest_Job1", 1000);
	ASSERT_GE(rc, 0);
	ASSERT_LT(GetTickCount() - ticket, 500);
	ticket = GetTickCount();


	Case("case 7.wait for all job, no task on jobs, and then another thread add a task, return when new tasked added.");
	rc = jc1.TerminateJob("JobClusterTest_Job1");
	ASSERT_GE(rc, 0);
	rc = jc1.CreateJob("JobClusterTest_Job1", "new job again");
	std::thread createTaskThread2(create_task, &jc2);
	createTaskThread2.detach();
	rc = jc1.WaitForNewTask("", 1000);
	ASSERT_GE(rc, 0);
	ASSERT_GE(GetTickCount() - ticket, 49);
	ASSERT_LT(GetTickCount() - ticket, 1000);
	ticket = GetTickCount();



	Case("case 8.wait for given job, no task on jobs, and then another thread add a task, return when new tasked added");
	rc = jc1.TerminateJob("JobClusterTest_Job1");
	ASSERT_GE(rc, 0);
	rc = jc1.CreateJob("JobClusterTest_Job1", "new job again");
	std::thread createTaskThread(create_task, &jc2);
	createTaskThread.detach();
	rc = jc1.WaitForNewTask("JobClusterTest_Job1", 500);
	ASSERT_GE(rc, 0);
	ASSERT_GE(GetTickCount() - ticket, 49);
	ASSERT_LT(GetTickCount() - ticket, 1000);
	ticket = GetTickCount();

	ASSERT_FALSE(createTaskThread.joinable());

	Case("case 9.wait for all job, no available task on jobs, and then another thread disconnected from cluster, retrun when distinnected");
	CJobCluster *pJc = new CJobCluster;
	ASSERT_TRUE(pJc != nullptr);
	rc = pJc->ConnectToCluster({{ZOO_KEEPER_SERVER,ZOO_KEEPER_PORT}}, 3000);
	ASSERT_GE(rc, 0);
	rc = pJc->RegisiterWorker();
	ASSERT_GE(rc, 0);
	rc = jc1.TerminateJob("JobClusterTest_Job1");
	ASSERT_GE(rc, 0);
	rc = jc1.CreateJob("JobClusterTest_Job1", "new job again");
	ASSERT_GE(rc, 0);

	rc = jc1.AddTask("JobClusterTest_Job1", "task data", strTaskName);
	ASSERT_GE(rc, 0);

	std::string strJobName, strTaskContent;
	rc = pJc->TakeTask(strJobName, strTaskName, strTaskContent);
	ASSERT_GE(rc, 0);

	std::thread TerminateTaskThread(TerminateTask, pJc);
	TerminateTaskThread.detach();
	rc = jc1.WaitForNewTask("JobClusterTest_Job1", 2000);
	ASSERT_GE(rc, 0);
	ASSERT_GE(GetTickCount() - ticket, 49);
	ASSERT_LT(GetTickCount() - ticket, 2000);
	ticket = GetTickCount();

	ASSERT_FALSE(TerminateTaskThread.joinable());

}
string g_strTaskName;
void ReleaseTask(void * pParameter)
{
	CJobCluster * pJc = (CJobCluster*)pParameter;
	if(pJc == nullptr) return;
	std::this_thread::sleep_for(std::chrono::milliseconds(50));
	//sleep(1);
	ResultCode rc = pJc->ReleaseTask("JobClusterTest_Job1", g_strTaskName);
	delete pJc;
}



TEST_F(JobClusterTester, ReleaseTask)
{
	CJobCluster jc1,jc2;
	std::string strJobName, strTaskName, strTaskData,strJobName2, strTaskName2;
	ResultCode rc = RS_SUCCESS;
	rc = jc1.ConnectToCluster({{ZOO_KEEPER_SERVER,ZOO_KEEPER_PORT}}, 3000);
	ASSERT_GE(rc, 0);
	rc = jc2.ConnectToCluster({{ZOO_KEEPER_SERVER,ZOO_KEEPER_PORT}}, 3000);
	ASSERT_GE(rc, 0);
	rc = jc1.RegisiterWorker();
	ASSERT_GE(rc, 0);
	rc = jc2.RegisiterWorker();
	ASSERT_GE(rc, 0);
	Case("Case1:Release Task for not exists tasks");
	strJobName = "JobClusterTest_Job1";
	strTaskName = "Task11";
	rc = jc1.ReleaseTask(strJobName, strTaskName);
	ASSERT_LT(rc, 0);

	Case("Case2:Release Task, take again");
	rc = jc2.CreateJob("JobClusterTest_Job1", "This is a job config data");
	ASSERT_GE(rc, 0);
	rc = jc2.AddTask("JobClusterTest_Job1", "The task data", strTaskName);
	ASSERT_GE(rc, 0);
	rc = jc1.TakeTask(strJobName, strTaskName, strTaskData);
	ASSERT_GE(rc, 0);
	rc = jc2.TakeTask(strJobName2, strTaskName2, strTaskData);
	ASSERT_LT(rc, 0);
	rc = jc1.ReleaseTask(strJobName, strTaskName);
	ASSERT_GE(rc, 0);
	rc = jc2.TakeTask(strJobName2, strTaskName2, strTaskData);
	ASSERT_GE(rc, 0);
	ASSERT_STREQ(strTaskName.c_str(), strTaskName2.c_str());


	Case("Case3:pending on wait for new task, release task, waiting will return immediately")
	CJobCluster *pJc = new CJobCluster;
	ASSERT_TRUE(pJc != nullptr);
	rc = pJc->ConnectToCluster({{ZOO_KEEPER_SERVER,ZOO_KEEPER_PORT}}, 3000);
	ASSERT_GE(rc, 0);
	rc = pJc->RegisiterWorker();
	ASSERT_GE(rc, 0);
	rc = jc1.TerminateJob("JobClusterTest_Job1");
	ASSERT_GE(rc, 0);
	rc = jc1.CreateJob("JobClusterTest_Job1", "new job again");
	ASSERT_GE(rc, 0);

	rc = jc1.AddTask("JobClusterTest_Job1", "task data", strTaskName);
	ASSERT_GE(rc, 0);


	rc = pJc->TakeTask(strJobName, strTaskName, strTaskData);
	ASSERT_GE(rc, 0);
	g_strTaskName = strTaskName;

	std::thread ReleaseTaskThread(ReleaseTask, pJc);
	ReleaseTaskThread.detach();
	size_t ticket = GetTickCount();
	rc = jc1.WaitForNewTask("JobClusterTest_Job1", 2000);
	ASSERT_GE(rc, 0);
	ASSERT_GE(GetTickCount() - ticket, 49);
	ASSERT_LT(GetTickCount() - ticket, 2000);
	ticket = GetTickCount();

	ASSERT_FALSE(ReleaseTaskThread.joinable());
	rc = jc1.TakeTask(strJobName, strTaskName, strTaskData);
	ASSERT_GE(rc, 0);
}

TEST_F(JobClusterTester, TerminateJob)
{
	ResultCode rc = RS_SUCCESS;
	std::string strTaskName;
	//case 1.terminate a non-exists job, succeeded.
	rc = m_jc.TerminateJob("JobClusterTester_Job3");
	ASSERT_GE(rc, 0);


	//case 2.terminate a newly created job, succeeded.
	rc = m_jc.TerminateJob("JobClusterTester_Job3");
	ASSERT_GE(rc, 0);
	rc = m_jc.CreateJob("JobClusterTester_Job3", "adsafsadf");
	ASSERT_GE(rc, 0);
	rc = m_jc.TerminateJob("JobClusterTester_Job3");
	ASSERT_GE(rc, 0);


	//case 3.terminate a none-empty job, succeeded.
	rc = m_jc.CreateJob("JobClusterTester_Job3", "dsfasdafasdf");
	ASSERT_GE(rc, 0);
	rc = m_jc.AddTask("JobClusterTester_Job3", "sadfsdafsdf", strTaskName);
	ASSERT_GE(rc, 0);
	rc = m_jc.TerminateJob("JobClusterTester_Job3");
	ASSERT_GE(rc, 0);


	//case 4.terminate two times, succeeded.
	rc = m_jc.CreateJob("JobClusterTester_Job3", "dsfasdafasdf");
	ASSERT_GE(rc, 0);
	rc = m_jc.AddTask("JobClusterTester_Job3", "sadfsdafsdf", strTaskName);
	ASSERT_GE(rc, 0);
	rc = m_jc.TerminateJob("JobClusterTester_Job3");
	ASSERT_GE(rc, 0);
	rc = m_jc.TerminateJob("JobClusterTester_Job3");

	//case 5. add task after terminated, failed.
	rc = m_jc.CreateJob("JobClusterTester_Job3", "dsfasdafasdf");
	ASSERT_GE(rc, 0);
	rc = m_jc.TerminateJob("JobClusterTester_Job3");
	ASSERT_GE(rc, 0);
	rc = m_jc.AddTask("JobClusterTester_Job3", "sadfsdafsdf", strTaskName);
	ASSERT_LT(rc, 0);


	//case 6.terminate and create again, succeeded.
	rc = m_jc.CreateJob("JobClusterTester_Job3", "dsfasdafasdf");
	ASSERT_GE(rc, 0);
	rc = m_jc.TerminateJob("JobClusterTester_Job3");
	ASSERT_GE(rc, 0);
	rc = m_jc.CreateJob("JobClusterTester_Job3", "bbbbbb");
	ASSERT_GE(rc,0);
	std::string strData;
	rc = m_jc.GetJobConfig("JobClusterTester_Job3", strData);
	ASSERT_STREQ(strData.c_str(), "bbbbbb");
	rc = m_jc.TerminateJob("JobClusterTester_Job3");

}


TEST_F(JobClusterTester, AddTaskResult_)
{
	ResultCode rc = RS_SUCCESS;
	std::string strJob, strTask, strTaskData, strTaskName;

	//case 1.Add task result for non exists job, failed.
	rc = m_jc.RegisiterWorker();
	ASSERT_GE(rc, 0);
	rc = m_jc.AddTaskResult_("none", "none2", "Task dataresult");
	ASSERT_LT(rc, 0);
	//case 2.Add task result for not a working job, failed.
	rc = m_jc.CreateJob("JobClusterTest_Job1", "afdafd");
	ASSERT_GE(rc, 0);
	rc = m_jc.AddTaskResult_("JobClusterTest_Job1", "task", "adsf");
	ASSERT_LT(rc, 0);
	//case 3.add task result for a working job ,succeeded.
	rc = m_jc.AddTask("JobClusterTest_Job1", "sdafsd", strTaskName);
	ASSERT_GE(rc, 0);
	rc = m_jc.TakeTask(strJob, strTask, strTaskData);
	ASSERT_GE(rc, 0);
	rc = m_jc.AddTaskResult_(strJob, "TResultName", "This is a result datadddd");
	ASSERT_GE(rc, 0);
	rc = m_jc.AddTaskResult_(strJob, "TResultName", "This is a result data");
	ASSERT_LT(rc, 0);
	//case 4.add task result for a working job more than one time, succeed
	rc = m_jc.AddTaskResult_(strJob, "TResultName2", "This is a result data2222");
	ASSERT_GE(rc, 0);
	//case 5.Add tast result when not a worker, failed.
	rc = m_jc.UnregisterWorker();
	ASSERT_GE(rc, 0);
	rc = m_jc.AddTaskResult_(strJob, "TResultName3", "This is the third result");
	ASSERT_LT(rc, 0);



}

TEST_F(JobClusterTester, GetTaskResult_)
{
	ResultCode rc = RS_SUCCESS;
	std::string strJob, strTask, strTaskData, strTaskName;
	rc = m_jc.RegisiterWorker();
	ASSERT_GE(rc, 0);

	rc = m_jc.CreateJob("JobClusterTest_Job1", "afdafd");
	ASSERT_GE(rc, 0);
	rc = m_jc.AddTask("JobClusterTest_Job1", "this is a task data", strTaskName);
	ASSERT_GE(rc, 0);
	rc = m_jc.TakeTask(strJob, strTask, strTaskData);
	ASSERT_GE(rc, 0);
	rc = m_jc.AddTaskResult_(strJob, "TResultName", "This is a result datadddd");
	ASSERT_GE(rc, 0);
	rc = m_jc.AddTaskResult_(strJob, "TResultName2", "This is a result data222");
	ASSERT_GE(rc, 0);


	//case 1(GetTaskResult): get task for non_exists JobName failed
	std::string strResultData;
	rc = m_jc.GetTaskResult_("sadfds0", "TResultName", strResultData);
	ASSERT_LT(rc, 0);
	ASSERT_STREQ(strResultData.c_str(), "");

	//case 2(GetTaskResult):Get task for non-exists result name failed.
	rc = m_jc.GetTaskResult_("JobClusterTest_Job1", "TResultNamekkk", strResultData);
	ASSERT_LT(rc, 0);

	//case 3(GetTaskResult):get task result for normal result
	rc = m_jc.GetTaskResult_("JobClusterTest_Job1", "TResultName", strResultData);
	ASSERT_GE(rc, 0);
	ASSERT_STREQ(strResultData.c_str(), "This is a result datadddd");
	rc = m_jc.GetTaskResult_("JobClusterTest_Job1", "TResultName2", strResultData);
	ASSERT_GE(rc, 0);
	ASSERT_STREQ(strResultData.c_str(), "This is a result data222");


}





TEST_F(JobClusterTester, ReleaseTaskResult_)
{
	ResultCode rc = RS_SUCCESS;
	std::string strJob, strTask, strTaskData, strTaskName;
	rc = m_jc.RegisiterWorker();
	ASSERT_GE(rc, 0);

	rc = m_jc.CreateJob("JobClusterTest_Job1", "afdafd");
	ASSERT_GE(rc, 0);
	rc = m_jc.AddTask("JobClusterTest_Job1", "this is a task data", strTaskName);
	ASSERT_GE(rc, 0);
	rc = m_jc.TakeTask(strJob, strTask, strTaskData);
	ASSERT_GE(rc, 0);
	rc = m_jc.AddTaskResult_(strJob, "TResultName", "This is a result datadddd");
	ASSERT_GE(rc, 0);
	rc = m_jc.AddTaskResult_(strJob, "TResultName2", "This is a result data222");
	ASSERT_GE(rc, 0);

	//case 1.release a not exists task result, failed.
	rc = m_jc.ReleaseTaskResult_("aa", "bb");
	ASSERT_LT(rc, 0);
	rc = m_jc.ReleaseTaskResult_("JobClusterTest_Job1", "bb");
	ASSERT_LT(rc, 0);
	//case 2.release a task result succeeded.
	rc = m_jc.GetTaskResult_("JobClusterTest_Job1", "TResultName", strTaskData);
	ASSERT_GE(rc, 0);
	rc = m_jc.ReleaseTaskResult_("JobClusterTest_Job1", "TResultName");
	ASSERT_GE(rc, 0);
	rc = m_jc.GetTaskResult_("JobClusterTest_Job1", "TResultName", strTaskData);
	ASSERT_LT(rc, 0);
	rc = m_jc.GetTaskResult_("JobClusterTest_Job1", "TResultName2", strTaskData);
	ASSERT_GE(rc, 0);

	//case 3.release a task result again, failed.
	rc = m_jc.ReleaseTaskResult_("JobClusterTest_Job1", "TResultName");
	ASSERT_LT(rc, 0);


}













