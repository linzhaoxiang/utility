
#ifndef CSERVERCORDINATOR_H
#define CSERVERCORDINATOR_H

#include <string>
#include <zookeeper/zookeeper.h>
//#include <zookeeper/zookeeper_log.h>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <vector>
#include "ResultCode.h"
using namespace Stock;

/**
  * class CServerCordinator
  * 
  */

/*
Folder structure for server cordinator
zx/
zx/WorkerList/
zx/WorkerList/WorkerName->"server addr && desc"
zx/JobConfig/JobName-->configData
zx/Tasks/Ordering/JobName/Taskname-->taskdata
zx/Tasks/Processing/JobName/TaskName-->ownerWorker
zx/Tasks/Result/JobName/ResultName-->ResultData
*/
class CJobCluster
{
public:
	struct ServerAddress{
		std::string strServerAddress;//ip;
		int nPort;

		const std::string& GetStrServerAddress() const
		{
			return strServerAddress;
		}

		void SetStrServerAddress(const std::string& strServerAddress)
		{
			this->strServerAddress = strServerAddress;
		}


	};

	// Constructors/Destructors
	//  


	/**
	 * Empty Constructor
	 */
	CJobCluster ();

	/**
	 * Empty Destructor
	 */
	virtual ~CJobCluster ();

	const std::vector<ServerAddress>& GetVectServer() const
	{
		return m_vectServer;
	}

	/**
	 * @return ResultCode
	 * @param  strServerConfigPath
	 */
	ResultCode ConnectToCluster (const std::vector<ServerAddress>& vectServer, int nRecvTimeOut = 30000);


	/**
	 * @return ResultCode
	 */
	ResultCode DisconnectFromCluster ();


	/**
	 * @return ResultCode
	 */
	ResultCode RegisiterWorker ();


	/**
	 * @return ResultCode
	 */
	ResultCode UnregisterWorker ();


	/**
	 * @return size_t
	 */
	size_t GetWorkerCount ();


	/**
	 * @return ResultCode
	 * @param  strJobName
	 * @param  strTaskData
	 */
	ResultCode AddTask (const std::string& strJobName, const std::string& strTaskData, std::string& strTaskName);


	/**
	 * @return ResultCode
	 * @param  strJobName:
	 * 		[in]:the JobName, if not empty, this function will only return the task whose job name = strJobName,
	 * 			otherwise, will return any available job's task
	 * 		[out]:the task's job name.
	 * @param  strTaskName:
	 * 		[out]:return the task's name;
	 * @param  strTaskData:
	 * 		[out]:the task's data
	 */
	ResultCode TakeTask (std::string& strJobName, std::string& strTaskName, std::string& strTaskData);
	enum EnumTaskStatus{
		TASK_STATUS_WAITING = 0X01,
		TASK_STATUS_RUNNING = 0X02,
		TASK_STATUS_NOT_EXISTS = 0X03
	};
	ResultCode GetTaskStatus(const std::string& strJobName, const std::string& strTaskName, EnumTaskStatus& eStatus);


	/*
	 * @desc Pend until there is a new waiting task.
	 * @return Result
	 * @para strJobName: the job for waitting, if empty, will waitting for any job.
	 * This function return when timeout or there is possibly(not guarantee) a new task.
	 * Return:
	 * 		RE_TIME_OUT: time out;
	 * 		RS_SUCCESS: there is a new task available, or there is a job task status changed(may not a new job)
	 *
	 * @note: when there are already some available tasks, this function will return immediately.
	 */
	ResultCode WaitForNewTask(const std::string& strJobName = "", int nTimeoutInMS = -1);


	/**
	 * @return ResultCode
	 * @param  strJobName
	 * @param  strTaskName
	 */
	ResultCode FinishTask (const std::string& strJobName, const std::string& strTaskName);
	ResultCode ReleaseTask (const std::string& strJobName, const std::string& strTaskName);


	/**
	 * @return ResultCode
	 * @param  strJobName
	 */
	ResultCode TerminateJob (const std::string& strJobName);


	/**
	 * This function should be deprecated, use CClusterCache::SetItemValue() instead.
	 * @return ResultCode
	 * @param  strJobName
	 * @param  strResultName
	 * @param  strResultData
	 */
	ResultCode AddTaskResult_ (const std::string& strJobName, const std::string& strResultName, const std::string& strResultData);


	/**
	 * This function should be deprecated, use CClusterCache::GetItemValue() instead.
	 * @return ResultCode
	 * @param  strJobName
	 * @param  strResultName
	 * @param  strResultData
	 */
	ResultCode GetTaskResult_ (const std::string& strJobName, const std::string& strResultName, std::string& strResultData);


	/**
	 *
	 * This function should be deprecated, use CClusterCache::RemoveItem() instead.
	 * @return ResultCode
	 * @param  strJobName
	 * @param  strResultName
	 */
	ResultCode ReleaseTaskResult_ (const std::string& strJobName, const std::string& strResultName);


	/**
	 * @return ResultCode
	 * @param  strJobName
	 * @param  strJobConfigData
	 */
	ResultCode CreateJob (const std::string& strJobName, const std::string& strJobConfigData);


	ResultCode TryLockJob(const std::string& strJobName, int nTimeOutInMS);

	ResultCode UnlockJob(const std::string& strJobName);


	/**
	 * @return ResultCode
	 * @param  strJobName
	 * @param  strJobConfigData
	 */
	ResultCode GetJobConfig (const std::string& strJobName, std::string& strJobConfigData);

protected:

	static void OnStatusChanged(zhandle_t* zh, int type, int state,
	        const char* path, void* watcherCtx)
	{
//	    printf("OnStatusChanged.\n");
//	    printf("type: %d\n", type);
//	    printf("state: %d\n", state);
//	    printf("path: %s\n", path);
//	    printf("watcherCtx: %s\n", (char *)watcherCtx);
	    CJobCluster* pThis = (CJobCluster*)watcherCtx;
	    if(pThis == nullptr) return;
	    //std::lock_guard<std::mutex> lock(pThis->m_mutex);
	    pThis->m_cvTaskChanged.notify_all();
	    if(type == ZOO_SESSION_EVENT && state == ZCLOSING)
	    	pThis->m_cvConnectionClose.notify_all();

	}
	//ResultCode EnumJobName(std::vector<std::string>& vectJobName) const ;
	static void Split(const std::string& strValue, const std::string& strDelim, std::vector<std::string>& vectItem) ;
	ResultCode ZooGetFileData(const std::string& strFilePath, std::string& strData);
	ResultCode ZooReadDir(const std::string& strDirPath, std::vector<std::string>& vectChildren);
	ResultCode ZooCreateDirChain(const std::string& strDirPath);
	bool ZooFileExists(const std::string& strFilePath);
	ResultCode ZooForceDelete(const std::string& strDirPath);


	//  

	// Protected attributes
	//  

	std::string m_strWorkerName;
	zhandle_t *m_serverHandle = nullptr;
	std::string m_strWorkingJob, m_strWorkingTask;

	size_t m_nLastChangeTimestamp = 1;
	std::mutex m_mutex;
	std::condition_variable m_cvTaskChanged; //
	std::condition_variable m_cvConnectionClose;


//	filesystem structure
//	zx/
//	zx/WorkerList/
//	zx/WorkerList/WorkerName->"server addr && desc"
//	zx/JobConfig/JobName-->configData
//	zx/Tasks/Ordering/JobName/Taskname-->taskdata
//	zx/Tasks/Processing/JobName/TaskName-->ownerWorker
//	zx/Tasks/Result/JobName/ResultName-->ResultData
	const std::string m_strClusterRoot = "/stock_analyzer";
	const std::string m_strWorkerRoot = "/stock_analyzer/worker_list";
#define ToPath(str) (str.empty()?"":(std::string("/")+str))
	std::string GetWorkerPath(const std::string& strWorkerName) const
	{
		return m_strWorkerRoot + ToPath(strWorkerName);
	}
	const std::string m_strJobConfigRoot = "/stock_analyzer/job_config";
	std::string GetJobConfigPath(const std::string& strJobName) const {return m_strJobConfigRoot + ToPath(std::string("Config"))+ ToPath(strJobName) ;}
	std::string GetJobCounterPath(const std::string& strJobName, const std::string& strWorkerName= "worker")const
	{
		return m_strJobConfigRoot + ToPath(std::string("Counter"))+ ToPath(strJobName)
			 + ToPath(strWorkerName);
	};

	const std::string m_strOrderRoot = "/stock_analyzer/ordering";
	std::string GetTaskOrderPath(const std::string& strJob, const std::string& strTask) const
	{
		return m_strOrderRoot + ToPath(strJob) + ToPath(strTask);
	}
	const std::string m_strWorkingRoot = "/stock_analyzer/working";
	std::string GetTaskWorkingPath(const std::string& strJob, const std::string& strTask) const
	{
		return m_strWorkingRoot + ToPath(strJob) + ToPath(strTask);
	}
	const std::string m_strResultRoot = "/stock_analyzer/result";
	std::string GetTaskResultPath(const std::string& strJob, const std::string strResultName) const
	{
		return m_strResultRoot + ToPath(strJob) + ToPath(strResultName);
	}
	//clientid_t* m_pClientID = nullptr;


	std::string SelectWaitingJob();
	std::string SelectWaitingTask(const std::string& strJobName);


private:
	std::vector<ServerAddress> m_vectServer;


	void initAttributes () ;

};

#endif // CSERVERCORDINATOR_H
