#include "JobCluster.h"
#include <sstream>
#include <iostream>
#include "Log.h"
#include "Stock.h"
#include "CacheCluster.h"



// Constructors/Destructors
//  

CJobCluster::CJobCluster ()
{
	initAttributes();
}

CJobCluster::~CJobCluster ()
{
	DisconnectFromCluster();
}



/**
 * @return ResultCode
 * @param  strServerConfigPath
 */
ResultCode CJobCluster::ConnectToCluster (const std::vector<ServerAddress>& vectMasterServer, int nRecvTimeout)
{
	zoo_set_debug_level(ZOO_LOG_LEVEL_ERROR);
	std::lock_guard<std::mutex> lock(this->m_mutex);

	std::string strHost;
	if(this->m_serverHandle != nullptr)
	{
		LogReturn(RS_ALREADY_EXISTS);
//		zookeeper_close(this->m_serverHandle);
//		LogDebug() << "server handle closed";
//		this->m_serverHandle = nullptr;
	}
	if(!vectMasterServer.empty())
		m_vectServer = vectMasterServer;
	if(m_vectServer.empty())
	{
		LogError() << "no cluster server provided";
		return RE_NOT_EXISTS;
	}
	for(auto server:m_vectServer)
	{
		if(server.strServerAddress.empty())
			continue;
		if(!strHost.empty())
			strHost += ",";
		std::stringstream ss;
		ss << server.nPort;
		strHost += server.strServerAddress + ":" + ss.str();
	}
	if(strHost.empty())
		return RE_INVALIDATE_PARAMETER;

	this->m_serverHandle = zookeeper_init(strHost.c_str(),
            CJobCluster::OnStatusChanged, nRecvTimeout, nullptr, (void*)this, 0);
	if(m_serverHandle == nullptr)
	{
		LogException() << "Server start error on " << strHost;
		return RE_COMMUNICATION;
	}


	//check the connection.
	std::string strJobConfigPath = this->GetJobConfigPath("");
	ResultCode rc = this->ZooCreateDirChain(strJobConfigPath);
	if(RC_FAILED(rc))
	{
		zookeeper_close(this->m_serverHandle);
		LogDebug() << "server handle closed";
		this->m_serverHandle = nullptr;
		LogReturn(rc);
	}
	std::string strWorkerPath = this->GetWorkerPath("");
	rc = this->ZooCreateDirChain(strWorkerPath);
	if(RC_FAILED(rc))
	{
		zookeeper_close(this->m_serverHandle);
		LogDebug() << "server handle closed";
		this->m_serverHandle = nullptr;
		LogReturn(rc);
	}


	return RS_SUCCESS;

}


/**
 * @return ResultCode
 */
ResultCode CJobCluster::DisconnectFromCluster ()
{
	ResultCode rc = this->UnregisterWorker();//must be called before lock
	LogErrorCode(rc);
	{
	std::unique_lock<std::mutex> lock(this->m_mutex);
	if(m_serverHandle == nullptr)
		LogReturn(RE_NOT_INITIALIZE);

	LogDebug() << "closing server handle";
	zookeeper_close(this->m_serverHandle);
	LogDebug() << "server handle closed";
	this->m_serverHandle = nullptr;
	m_strWorkerName.clear();
	this->m_cvTaskChanged.notify_all();
	//this->m_cvConnectionClose.wait_for(lock, std::chrono::milliseconds(10));
	return RS_SUCCESS;
	}
}


/**
 * @return ResultCode
 */
ResultCode CJobCluster::RegisiterWorker ()
{
	std::lock_guard<std::mutex> lock(m_mutex);
	char pathResult[1024];
	if(m_serverHandle == nullptr)
		LogReturn(RE_NOT_INITIALIZE);
	if(!m_strWorkerName.empty())
	{
		LogError() << "already exists";
		return RE_ALREADY_EXISTS;
	}
	//ZOOAPI int zoo_create(zhandle_t * zh, const char *path,
//    const char *value, int valuelen,
//    const struct ACL_vector *acl, int flags,
//    char *path_buffer, int path_buffer_len);
	std::string strWorkerName = this->GetWorkerPath("worker");
	std::vector<std::string> vectPathNode;
	int result = zoo_create(this->m_serverHandle, strWorkerName.c_str(), strWorkerName.c_str(),
			strWorkerName.size(), &ZOO_OPEN_ACL_UNSAFE, ZOO_SEQUENCE|ZOO_EPHEMERAL, pathResult, 1024);

	if(result != ZOK)
	{
		LogError() << "create worker failed" << strWorkerName << ",errorcode:" << result;
		return RE_ERROR;
	}
	Split(pathResult, "/", vectPathNode);
	if(vectPathNode.empty())
		LogReturn(RE_UNEXPECT);
	m_strWorkerName = *vectPathNode.rbegin();
	LogDebug() << "new worker created successfully:" << m_strWorkerName;
	return RS_SUCCESS;
}


/**
 * @return ResultCode
 */
ResultCode CJobCluster::UnregisterWorker ()
{
	std::lock_guard<std::mutex> lock(m_mutex);
	if(m_serverHandle == nullptr)
		LogReturn(RE_NOT_INITIALIZE);
	if(m_strWorkerName.empty())
	{
		LogError() << "worker not exists:" << m_strWorkerName;
		return RE_NOT_EXISTS;
	}
	if(!m_strWorkingTask.empty())
	{
		std::string strWorkingTaskPath = this->GetTaskWorkingPath(m_strWorkingJob, m_strWorkingTask);
		int nResult = zoo_delete(this->m_serverHandle, strWorkingTaskPath.c_str(), -1);
	}
	std::string strWorkerPath = this->GetWorkerPath(m_strWorkerName);
	int nResult = zoo_delete(this->m_serverHandle, strWorkerPath.c_str(), -1);
	if(nResult == ZNONODE)
		nResult = ZOK;
	if(nResult != ZOK)
		LogError() << "delete worker failed:" << m_strWorkerName;
	m_strWorkerName.clear();
	return RS_SUCCESS;
}


/**
 * @return size_t
 */
size_t CJobCluster::GetWorkerCount ()
{
	std::lock_guard<std::mutex> lock(m_mutex);
	if(m_serverHandle == nullptr)
		LogReturn(RE_NOT_INITIALIZE);

	std::string strWorkerPath = this->GetWorkerPath("");
	std::vector<std::string> vectWorker;
	ResultCode rc = this->ZooReadDir(strWorkerPath, vectWorker);
	if(RC_FAILED(rc))
		return 0;
	return vectWorker.size();
}


/**
 * @return ResultCode
 * @param  strJobName
 * @param  strTaskData
 */
ResultCode CJobCluster::AddTask (const std::string& strJobName, const std::string& strTaskData,
		std::string& strTaskName)
{
	std::lock_guard<std::mutex> lock(m_mutex);
	if(m_serverHandle == nullptr)
		LogReturn(RE_NOT_INITIALIZE);
	char pathResult[1024];

	//ZOOAPI int zoo_create(zhandle_t * zh, const char *path,
//    const char *value, int valuelen,
//    const struct ACL_vector *acl, int flags,
//    char *path_buffer, int path_buffer_len);
	strTaskName = this->GetTaskOrderPath(strJobName, "Task");
	std::vector<std::string> vectPathNode;
	int result = zoo_create(this->m_serverHandle, strTaskName.c_str(), strTaskData.c_str(),
			strTaskData.size(), &ZOO_OPEN_ACL_UNSAFE, ZOO_SEQUENCE, pathResult, 1024);

	if(result != ZOK)
	{
		LogError() << "create task failed:" << strJobName;
		return RE_ERROR;
	}
	LogDebug() << "task created succeeded:" << pathResult;
	strTaskName = pathResult;
	std::vector<std::string> vectName;
	this->Split(strTaskName, "/\\", vectName);
	if(!vectName.empty())
		strTaskName = *vectName.rbegin();

	return RS_SUCCESS;
}


/**
 * @return int
 * @param  strJobName
 * @param  strTaskName
 * @param  strTaskData
 */
ResultCode CJobCluster::TakeTask (std::string& strJobName, std::string& strTaskName, std::string& strTaskData)
{
	std::lock_guard<std::mutex> lock(m_mutex);
	if(this->m_strWorkerName.empty())
		LogReturn(RE_NOT_INITIALIZE);
	if(m_serverHandle == nullptr)
		LogReturn(RE_NOT_INITIALIZE);
	char pathResult[1024];


	while(true)
	{
		std::string strCurrentJob = strJobName;
		if(strCurrentJob.empty())
			strCurrentJob = SelectWaitingJob();
		if(strCurrentJob.empty())
			LogReturn(RE_NOT_EXISTS);

		strTaskName = SelectWaitingTask(strCurrentJob);
		if(strTaskName.empty())
			LogReturn(RE_NOT_EXISTS);

		std::string strTaskWorkingPath = this->GetTaskWorkingPath(strCurrentJob, strTaskName);
		if(strTaskWorkingPath.empty())
			LogReturn(RE_UNEXPECT);

		int nResult = zoo_create(this->m_serverHandle, strTaskWorkingPath.c_str(), strTaskData.c_str(),
				0, &ZOO_OPEN_ACL_UNSAFE, ZOO_EPHEMERAL, pathResult, 1024);
		if(nResult != ZOK)
		{
			LogDebug() << "take task failed:" << strTaskWorkingPath << ", try another task...";
			continue;
		}


		std::string strTaskOrderPath = this->GetTaskOrderPath(strCurrentJob, strTaskName);
		ResultCode rc = ZooGetFileData(strTaskOrderPath, strTaskData);
		if(RC_SUCCEEDED(rc))
		{
			strJobName = strCurrentJob;
			break;
		}
	}

	this->m_strWorkingJob = strJobName;
	this->m_strWorkingTask = strTaskName;


	return RS_SUCCESS;

}

ResultCode CJobCluster::WaitForNewTask(const std::string& strJobName, int nTimeoutInMS)
{
	LogTrace2() << "Wait For New Task:" << strJobName << ",timeout:"<< nTimeoutInMS;
	std::unique_lock<std::mutex> lock(m_mutex);
//	std::string strWorkingTaskPath = this->GetTaskWorkingPath(strJobName, "");
//	std::string strOrderingTaskPath = this->GetTaskOrderPath(strJobName, "");
//	zoo_get_children()
	if(this->m_strWorkerName.empty())
		LogReturn(RE_NOT_INITIALIZE);
	if(m_serverHandle == nullptr)
		LogReturn(RE_NOT_INITIALIZE);
	ResultCode rc = RS_SUCCESS;
	if(!this->SelectWaitingTask(strJobName).empty())
		return RS_ALREADY_EXISTS;

	//no waiting tasks.
	if( nTimeoutInMS == 0 ||
			this->m_cvTaskChanged.wait_for(lock, std::chrono::milliseconds(nTimeoutInMS)) == std::cv_status::timeout)
	{
		if(m_serverHandle == nullptr)
			LogReturn(RE_NOT_INITIALIZE);
		return (RE_TIME_OUT);
	}

	if(m_serverHandle == nullptr)
		LogReturn(RE_NOT_INITIALIZE);
	LogTrace() << "Wait for new task success.";
	return RS_SUCCESS;
}


/**
 * @return ResultCode
 * @param  strJobName
 * @param  strTaskName
 */
ResultCode CJobCluster::FinishTask (const std::string& strJobName, const std::string& strTaskName)
{
	std::lock_guard<std::mutex> lock(m_mutex);
	ResultCode rc = RS_SUCCESS;
	if(this->m_strWorkerName.empty())
		LogReturn(RE_NOT_INITIALIZE);
	if(m_serverHandle == nullptr)
		LogReturn(RE_NOT_INITIALIZE);
	if(strJobName.empty() || strTaskName.empty())
	{
		LogReturn(RE_INVALIDATE_PARAMETER);
	}



	std::string strTaskOrderPath = this->GetTaskOrderPath(strJobName, strTaskName);

	int nResult = zoo_delete(this->m_serverHandle, strTaskOrderPath.c_str(), -1);
	if(nResult != ZOK)
	{
		LogError() << "delete failed:" << strTaskOrderPath;
	}


	std::string strTaskWorkingPath = this->GetTaskWorkingPath(strJobName, strTaskName);
	if(strTaskWorkingPath.empty())
		LogReturn(RE_UNEXPECT);


	nResult = zoo_delete(this->m_serverHandle, strTaskWorkingPath.c_str(), -1);
	if(nResult != ZOK)
	{
		LogError() << "delete failed:" << strTaskWorkingPath;
		rc = RE_ERROR;
	}

	this->m_strWorkingJob.clear();
	this->m_strWorkingTask.clear();


	return rc;
}


/**
 * @return ResultCode
 * @param  strJobName
 * @param  strTaskName
 */
ResultCode CJobCluster::ReleaseTask (const std::string& strJobName, const std::string& strTaskName)
{
	std::lock_guard<std::mutex> lock(m_mutex);
	ResultCode rc = RS_SUCCESS;
	if(this->m_strWorkerName.empty())
		LogReturn(RE_NOT_INITIALIZE);
	if(m_serverHandle == nullptr)
		LogReturn(RE_NOT_INITIALIZE);
	if(strJobName.empty() || strTaskName.empty())
	{
		LogReturn(RE_INVALIDATE_PARAMETER);
	}


	std::string strTaskWorkingPath = this->GetTaskWorkingPath(strJobName, strTaskName);
	if(strTaskWorkingPath.empty())
		LogReturn(RE_UNEXPECT);


	int nResult = zoo_delete(this->m_serverHandle, strTaskWorkingPath.c_str(), -1);
	if(nResult != ZOK)
	{
		LogError() << "delete failed:" << strTaskWorkingPath;
		rc = RE_ERROR;
	}

	this->m_strWorkingJob.clear();
	this->m_strWorkingTask.clear();


	return rc;
}

ResultCode CJobCluster::GetTaskStatus(const std::string& strJobName,
		const std::string& strTaskName,
		EnumTaskStatus& eStatus)
{
	std::lock_guard<std::mutex> lock(m_mutex);
	ResultCode rc = RS_SUCCESS;
//	if(this->m_strWorkerName.empty())
//		LogReturn(RE_NOT_INITIALIZE);
	if(m_serverHandle == nullptr)
		LogReturn(RE_NOT_INITIALIZE);
	if(strJobName.empty() || strTaskName.empty())
	{
		LogReturn(RE_INVALIDATE_PARAMETER);
	}

	std::string strTaskOrderPath = this->GetTaskOrderPath(strJobName, strTaskName);

	bool bExists = this->ZooFileExists(strTaskOrderPath);
	if(!bExists)
	{
		eStatus = TASK_STATUS_NOT_EXISTS;
		return RS_SUCCESS;
	}


	std::string strTaskWorkingPath = this->GetTaskWorkingPath(strJobName, strTaskName);
	if(strTaskWorkingPath.empty())
		LogReturn(RE_UNEXPECT);


	bExists = this->ZooFileExists(strTaskWorkingPath);
	if(!bExists)
	{
		eStatus = TASK_STATUS_WAITING;
		return RS_SUCCESS;
	}
	else
	{
		eStatus = TASK_STATUS_RUNNING;
	}

	return RS_SUCCESS;
}


/**
 * @return ResultCode
 * @param  strJobName
 */
ResultCode CJobCluster::TerminateJob (const std::string& strJobName)
{

	LogTrace() << "Job Terminate/Finished:" << strJobName;
	if(m_serverHandle == nullptr)
		LogReturn(RE_NOT_INITIALIZE);
	if(strJobName.empty())
		LogReturn(RE_INVALIDATE_PARAMETER);
	ResultCode rc = RS_SUCCESS;


	if(RC_FAILED(this->TryLockJob(strJobName, 60*1000)))
		LogReturn(RE_TIME_OUT);

	{
		std::lock_guard<std::mutex> lock(m_mutex);
		std::string strCounterPath = this->GetJobCounterPath(strJobName, "");
		std::vector<std::string> vectChildren;
		rc = this->ZooReadDir(strCounterPath, vectChildren);
		if(RC_SUCCEEDED(rc))
		{
			if(!vectChildren.empty())
			{
				strCounterPath = this->GetJobCounterPath(strJobName, vectChildren[0]);
				rc = this->ZooForceDelete(strCounterPath);
				LogErrorCode(rc);
				rc = RS_SUCCESS;
			}

			if(vectChildren.size() > 1)
			{
				goto Out;
			}
		}


		std::string strTaskConfigPath = this->GetJobConfigPath(strJobName);
		 rc = this->ZooForceDelete(strTaskConfigPath);
		LogErrorCode(rc) << strTaskConfigPath;
		std::string strTaskOrderPath = this->GetTaskOrderPath(strJobName, "");

		rc = this->ZooForceDelete(strTaskOrderPath);
		LogErrorCode(rc);


		std::string strTaskWorkingPath = this->GetTaskWorkingPath(strJobName, "");
		rc = this->ZooForceDelete(strTaskWorkingPath);
		LogErrorCode(rc);

		std::string strTaskResultPath = this->GetTaskResultPath(strJobName, "");
		rc = this->ZooForceDelete(strTaskResultPath);
		LogErrorCode(rc);

		std::string strJobCounterPath = this->GetJobCounterPath(strJobName, "");
		rc = this->ZooForceDelete(strJobCounterPath);
		LogErrorCode(rc);
		rc = RS_SUCCESS;
	}

Out:

	this->UnlockJob(strJobName);


	return rc;
}


/**
 * @return ResultCode
 * @param  strJobName
 * @param  strResultName
 * @param  strResultData
 */
ResultCode CJobCluster::AddTaskResult_ (const std::string& strJobName, const std::string& strResultName, const std::string& strResultData)
{
	std::lock_guard<std::mutex> lock(m_mutex);
	if(this->m_strWorkerName.empty())
		LogReturn(RE_NOT_INITIALIZE);
	char pathResult[1024];
	if(m_serverHandle == nullptr)
		LogReturn(RE_NOT_INITIALIZE);
	if(strJobName.empty() || strResultName.empty())
		LogReturn(RE_INVALIDATE_PARAMETER);
	if(strJobName != this->m_strWorkingJob)
		LogReturn(RE_INVALIDATE_PARAMETER);
	std::string strResultPath = this->GetTaskResultPath(strJobName, strResultName);
//	ZOOAPI int zoo_create(zhandle_t *zh, const char *path, const char *value,
//	        int valuelen, const struct ACL_vector *acl, int flags,
//	        char *path_buffer, int path_buffer_len);
	int nResult = zoo_create(this->m_serverHandle, strResultPath.c_str(), strResultData.c_str(),
			strResultData.size(), &ZOO_OPEN_ACL_UNSAFE, 0, pathResult, 1024);
	if(nResult != ZOK)
		LogReturn(RE_ERROR);
	return RS_SUCCESS;
}


/**
 * @return ResultCode
 * @param  strJobName
 * @param  strResultName
 * @param  strResultData
 */
ResultCode CJobCluster::GetTaskResult_ (const std::string& strJobName, const std::string& strResultName, std::string& strResultData)
{
	std::lock_guard<std::mutex> lock(m_mutex);
	if(m_serverHandle == nullptr)
		LogReturn(RE_NOT_INITIALIZE);
	if(strJobName.empty() || strResultName.empty())
		LogReturn(RE_INVALIDATE_PARAMETER);
	std::string strTaskResultPath = this->GetTaskResultPath(strJobName, strResultName);
	return ZooGetFileData(strTaskResultPath, strResultData);

}


/**
 * @return ResultCode
 * @param  strJobName
 * @param  strResultName
 */
ResultCode CJobCluster::ReleaseTaskResult_ (const std::string& strJobName, const std::string& strResultName)
{
	std::lock_guard<std::mutex> lock(m_mutex);
	if(m_serverHandle == nullptr)
		LogReturn(RE_NOT_INITIALIZE);
	if(strJobName.empty() || strResultName.empty())
		LogReturn(RE_INVALIDATE_PARAMETER);
	std::string strTaskResultPath = this->GetTaskResultPath(strJobName, strResultName);
	int nResult = zoo_delete(this->m_serverHandle, strTaskResultPath.c_str(), -1);
	if(nResult != ZOK)
		LogReturn(RE_ERROR);
	return RS_SUCCESS;
}


/**
 * @return ResultCode
 * @param  strJobName
 * @param  strJobConfigData
 */
ResultCode CJobCluster::CreateJob (const std::string& strJobName, const std::string& strJobConfigData)
{
	if(m_serverHandle == nullptr)
		LogReturn(RE_NOT_INITIALIZE);
	if(strJobName.empty())
		LogReturn(RE_INVALIDATE_PARAMETER);
	ResultCode rc = RS_SUCCESS;

	if(RC_FAILED(this->TryLockJob(strJobName, 60*1000)))
		return RE_TIME_OUT;
	{
		std::lock_guard<std::mutex> lock(m_mutex);
		char pathResult[1024];


		std::string strCounterPath = this->GetJobCounterPath(strJobName, "");
		rc = this->ZooCreateDirChain(strCounterPath);
		if(RC_FAILED(rc))
			LogErrorCode(rc) << strCounterPath;
		strCounterPath = this->GetJobCounterPath(strJobName, "count");
		int nResult = zoo_create(this->m_serverHandle, strCounterPath.c_str(), strCounterPath.c_str(),
				0, &ZOO_OPEN_ACL_UNSAFE, ZOO_SEQUENCE, pathResult, 1024);
		if(nResult != ZOK)
		{
			rc = RE_ERROR;
			LogErrorCode(rc);
			goto Out;
		}

		std::string strJobConfigPath = this->GetJobConfigPath(strJobName);
		if(this->ZooFileExists(strJobConfigPath))
		{
			rc = (RS_ALREADY_EXISTS);
			goto Out;
		}

		nResult = zoo_create(this->m_serverHandle, strJobConfigPath.c_str(), strJobConfigData.c_str(),
					strJobConfigData.size(), &ZOO_OPEN_ACL_UNSAFE, 0, pathResult, 1024);
		if(nResult != ZOK)
			LogErrorCode(RE_ERROR) << strJobConfigPath;


		std::string strTaskOrderPath = this->GetTaskOrderPath(strJobName, "");
		rc = this->ZooCreateDirChain(strTaskOrderPath);
		if(RC_FAILED(rc))
			LogErrorCode(rc) << strTaskOrderPath;
		std::string strTaskWorkingPath = this->GetTaskWorkingPath(strJobName, "");
		rc = this->ZooCreateDirChain(strTaskWorkingPath);
		if(RC_FAILED(rc))
			LogErrorCode(rc) << strTaskWorkingPath;;
		std::string strTaskResultPath = this->GetTaskResultPath(strJobName, "");
		rc = this->ZooCreateDirChain(strTaskResultPath);
		if(RC_FAILED(rc))
			LogErrorCode(rc) << strTaskResultPath;
		rc = RS_SUCCESS;
	}
Out:
	UnlockJob(strJobName);

	return rc;
}


/**
 * @return ResultCode
 * @param  strJobName
 * @param  strJobConfigData
 */
ResultCode CJobCluster::GetJobConfig (const std::string& strJobName, std::string& strJobConfigData)
{
	std::lock_guard<std::mutex> lock(m_mutex);
	if(m_serverHandle == nullptr)
		LogReturn(RE_NOT_INITIALIZE);
	if(strJobName.empty())
		LogReturn(RE_INVALIDATE_PARAMETER);
	std::string strJobConfigPath = this->GetJobConfigPath(strJobName);
	return ZooGetFileData(strJobConfigPath, strJobConfigData);
}

void CJobCluster::Split(const std::string& strValue, const std::string& strDelim,
		std::vector<std::string>& vectResult)
{
	size_t last = 0;
	size_t index = strValue.find_first_of(strDelim, last);
	while (index != std::string::npos)
	{
		vectResult.push_back(strValue.substr(last, index - last));
		last = index + 1;
		index=strValue.find_first_of(strDelim, last);
	}
	if (index - last > 0)
	{
		vectResult.push_back(strValue.substr(last, index - last));
	}
}

std::string CJobCluster::SelectWaitingJob()
{
	std::vector<std::string> vectJobName;
	std::string strJobPath = this->GetJobConfigPath("");
	ResultCode rc = this->ZooReadDir(strJobPath, vectJobName);
	if(RC_FAILED(rc))
	{
		LogErrorCode(rc);
		return "";
	}

	for(auto strJobName: vectJobName)
	{
		if(!SelectWaitingTask(strJobName).empty())
		{
			return strJobName;
		}
	}
	return "";
}

std::string CJobCluster::SelectWaitingTask(const std::string& strJobName)
{
	ResultCode rc = RS_SUCCESS;
	std::vector<std::string> vectJobName;
	if(strJobName.empty())
	{
		std::string strJobConfigPath = this->GetJobConfigPath("");
		rc = this->ZooReadDir(strJobConfigPath, vectJobName);
		if(RC_FAILED(rc))
		{
			LogErrorCode(rc);
			return "";
		}
	}
	else
		vectJobName.push_back(strJobName);

	for(auto strJobName: vectJobName)
	{
		std::string strJobOrderPath = this->GetTaskOrderPath(strJobName, "");
		std::string strJobWorkingPath = this->GetTaskWorkingPath(strJobName, "");
		std::vector<std::string> vectOrder, vectWorking, vectWaiting;
		rc = this->ZooReadDir(strJobOrderPath, vectOrder);
		if(RC_FAILED(rc))
			continue;
		rc = this->ZooReadDir(strJobWorkingPath, vectWorking);
		if(RC_FAILED(rc))
			continue;
		std::sort(vectOrder.begin(), vectOrder.end());
		std::sort(vectWorking.begin(), vectWorking.end());
		std::set_difference(vectOrder.begin(), vectOrder.end(), vectWorking.begin(), vectWorking.end(), std::back_inserter(vectWaiting));
		if(!vectWaiting.empty())
			return vectWaiting[0];//return random so that servers will not always competing on the same task
	}

	return "";
}

//ResultCode CServerCordinator::EnumJobName(std::vector<std::string>& vectJobName) const
//{
//}

ResultCode CJobCluster::ZooGetFileData(const std::string& strFilePath, std::string& strData)
{
	Stat stat;
	if(strFilePath.empty())
		LogReturn(RE_INVALIDATE_PARAMETER);
	int nResult = zoo_exists(this->m_serverHandle, strFilePath.c_str(), 0, &stat);
	if(nResult != ZOK)
	{
		LogDebug() << "GetTaskResult failed:" << strFilePath ;
		LogReturn(RE_NOT_EXISTS);
	}


	int nBufferSize = stat.dataLength + 1;
	char* pBuffer = new char[nBufferSize];
	if(pBuffer == nullptr)
	{
		delete []pBuffer;
		LogDebug() << "GetTaskResult failed:" << strFilePath ; ", out of resource!";
		LogReturn(RE_OUT_OF_RESOURCE);
	}

//		ZOOAPI int zoo_get(zhandle_t *zh, const char *path, int watch, char *buffer,
//		                   int* buffer_len, struct Stat *stat);


	nResult = zoo_get(this->m_serverHandle, strFilePath.c_str(), 0, pBuffer, &nBufferSize, nullptr);
	if(nResult != ZOK)
	{
		delete []pBuffer;
		LogDebug() << "GetTaskResult failed:" << strFilePath ;
		LogReturn(RE_INVALIDATE_DATA);
	}

	strData.assign(pBuffer, nBufferSize);
	delete []pBuffer;
}

ResultCode CJobCluster::ZooReadDir(const std::string& strDirPath, std::vector<std::string>& vectChildren)
{
	//		ZOOAPI int zoo_get_children(zhandle_t *zh, const char *path, int watch,
	//		                            struct String_vector *strings);
	String_vector strvectChildren;
	//set watch = 1 so that we can get notify when the dir changed(and then WaitForNewTask can work)
	int nResult = zoo_get_children(this->m_serverHandle, strDirPath.c_str(), 1, &strvectChildren);
	if(nResult != ZOK)
	{
		LogTrace() << "zoo_get_children failed on :" << strDirPath;
		//LogErrorCode(RE_ERROR) << strDirPath;
		return RE_ERROR;
	}
	for(int i = 0; i < strvectChildren.count; i++)
	{
		vectChildren.push_back(strvectChildren.data[i]);
		delete [] strvectChildren.data[i];
	}
	return RS_SUCCESS;
}

ResultCode CJobCluster::ZooCreateDirChain(const std::string& strDirPath)
{
	Stat stat;
	std::string strPath;
	std::vector<std::string> vectNode;
	char buffer[1024];
	int nResult = 0;
	if(strDirPath.empty())
		LogReturn(RE_INVALIDATE_PARAMETER);

	this->Split(strDirPath, "/", vectNode);


	for(auto strNode:vectNode)
	{
		if(strNode.empty()) continue;
		strPath += "/" + strNode;
		nResult = zoo_exists(this->m_serverHandle, strPath.c_str(), 0, &stat);
		if(nResult == ZOK)
			continue;
//		int nResult = zoo_create(this->m_serverHandle, strJobConfigPath.c_str(), strJobConfigData.c_str(),
//					strJobConfigData.size(), &ZOO_OPEN_ACL_UNSAFE, 0, pathResult, 1024);
		nResult = zoo_create(this->m_serverHandle, strPath.c_str(), strPath.c_str(),
				0, &ZOO_OPEN_ACL_UNSAFE, 0, buffer, 1024);

		if(nResult != ZOK)
		{
			LogErrorCode(RE_ERROR) << strPath;
			return RE_ERROR;
		}

	}

	return RS_SUCCESS;

}

bool CJobCluster::ZooFileExists(const std::string& strFilePath)
{
	Stat stat;
	if(strFilePath.empty())
		LogReturn(RE_INVALIDATE_PARAMETER);
	int nResult = zoo_exists(this->m_serverHandle, strFilePath.c_str(), 0, &stat);
	if(nResult == ZNONODE)
		return false;
	else
		return true;

}

ResultCode CJobCluster::ZooForceDelete(const std::string& strDirPath)
{
	if(strDirPath.empty())
		LogReturn(RE_INVALIDATE_PARAMETER);
	ResultCode rc = RS_SUCCESS;
	std::vector<std::string> vectChildren;
	int nResult;
	rc = this->ZooReadDir(strDirPath, vectChildren);
	//LogErrorCode(rc);
	LogDebug() << "ReadDir: child count:" << vectChildren.size();
	for(auto& child:vectChildren)
	{
		if(child.empty()) continue;
		rc = ZooForceDelete(strDirPath + "/" + child);
		LogErrorCode(rc) << strDirPath + "/" + child;
	}

	nResult = zoo_delete(this->m_serverHandle, strDirPath.c_str(), -1);
	if(nResult != ZOK)
	{
		LogDebug() << RE_ERROR << ":" << strDirPath;
	}
	return RS_SUCCESS;
}

ResultCode CJobCluster::TryLockJob(const std::string& strJobName, int nTimeOutInMS)
{
	auto pCacheCluster = Stock::CStock::Instance().GetCacheCluster();
	if(pCacheCluster == nullptr)
		LogReturn(RE_NOT_INITIALIZE);
	return pCacheCluster->TryLock(strJobName, "Lock", 60*2, nTimeOutInMS);
}

ResultCode CJobCluster::UnlockJob(const std::string& strJobName)
{
	auto pCacheCluster = Stock::CStock::Instance().GetCacheCluster();
	if(pCacheCluster == nullptr)
		LogReturn(RE_NOT_INITIALIZE);
	return pCacheCluster->Unlock(strJobName, "Lock");

}

void CJobCluster::initAttributes () {
}


