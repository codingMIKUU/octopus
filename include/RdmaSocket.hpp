/***********************************************************************
* 
* 
* Tsinghua Univ, 2016
*
***********************************************************************/

#ifndef RDMASOCKET_HEADER
#define RDMASOCKET_HEADER

#include <infiniband/verbs.h>
#include <sys/socket.h>
#include <unordered_map>
#include <stdlib.h>
#include <unistd.h>
#include <time.h>
#include <netdb.h>
#include <errno.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <string>
#include <thread>
#include <stdint.h>
#include <assert.h>
#include <deque>
#include <mutex>
#include <atomic>
#include <vector>
#include "Configuration.hpp"
#include "debug.hpp"
#include "global.h"
/*
* Important global information.
*/
#define MAX_POST_LIST 24
#define QPS_MAX_DEPTH 1024
#define SIGNAL_BATCH  1
#define WORKER_NUMBER 2
#define CONTROL_QP_INDEX 0
#define DATA_QP_SMALL_INDEX 1
#define DATA_QP_LARGE_INDEX 2
#define DATA_QP_COUNT 2
#define DATA_QP_SPLIT_SIZE (10 * 1024)
#define QP_NUMBER (1 + DATA_QP_COUNT)

/* Important information of node-to-node connection */
typedef struct {
	struct ibv_qp *qp[QP_NUMBER];
	struct ibv_cq *control_cq;
	struct ibv_cq *data_cq;
	int control_cq_index;
	int data_cq_index;
	uint32_t qpNum[QP_NUMBER];
	uint64_t RegisteredMemory;
	uint32_t rkey;
	uint16_t lid;
	uint8_t  gid[16];
	int 	 sock;
	uint16_t NodeID;
	uint64_t counter = 0;
} PeerSockData;

typedef struct {
	uint16_t NodeID;
	bool isServer;
	uint16_t GivenID;
} ExchangeID;

typedef struct {
	uint32_t rkey;
	uint32_t qpNum[QP_NUMBER];
	uint16_t lid;
	uint8_t  gid[16];
	uint64_t RegisteredMemory;
	uint16_t NodeID;
} ExchangeMeta;

typedef struct {
	bool OpType;	/* false - Read, true - Write */
	uint16_t NodeID;
	uint64_t size;
	uint64_t bufferSend;
	uint64_t bufferReceive;
} TransferTask;

class RdmaSocket {
private:
	struct PendingConnectTask {
		int sock;
		uint16_t presetNodeID;
		bool hasPresetNodeID;
		int workerId;
	};

	// unordered_map<uint16_t, PeerSockData*> peers;
	PeerSockData*			peers[1000];
	char 					*DeviceName;
	uint32_t 				Port;			/* Used by RDMA */
	uint32_t 				ServerPort;		/* Used by socket for data exchange */
	int 					GidIndex;
	struct ibv_port_attr 	PortAttribute;
	bool 					isRunning;		/* Indicate the state of system */
	bool 					isServer;
	struct ibv_context		*ctx;        	/* device handle */
	struct ibv_pd			*pd;            /* Protection Domain handler */
	struct ibv_cq			**cq;			/* Completion Queue */
	int 					cqNum;			/* Number of created CQ */
	int 					cqPtr;			/* Indicate the next cq entry, initialized as 0. */
	uint64_t 				mm;				/* Local Memory Region base address. */
	uint64_t				mmSize;			/* Local Memory Region size. */
	Configuration 			*conf;			/* System Configuration. */
	uint16_t				MyNodeID;		/* My NodeID, used for identification. */
	uint16_t 				MaxNodeID;		/* Max NodeID for client */
	thread 					Listener;		/* Wait for client connection */
	int                     listenSock;
	uint8_t					Mode;			/* RC-0, UC-1, UD-2 */
	int 					ServerCount;	/* The total number of servers */
	uint32_t                srmAppThreads;
	uint32_t                createWorkerCount;
	uint32_t                clientCreateWorkerCount;
	std::vector<uint8_t>    createPhaseOpenByWorker;
	std::atomic<uint32_t>   nextCreateWorker;
	std::atomic<uint32_t>   nextClientCreateIndex;
	std::mutex              pendingConnectMutex;
	std::deque<PendingConnectTask> pendingConnectTasks;
	Queue<TransferTask *>   queue[WORKER_NUMBER];/* Used for Data transfer. */
	uint16_t TransferSignal;				/* Used to notify compeletion of data transfer. */
	thread 					worker[WORKER_NUMBER];
	std::mutex              dataPathMutex[DATA_QP_COUNT];

	/* Performance Checker. */
	uint64_t WriteSize[WORKER_NUMBER];
	uint64_t ReadSize[WORKER_NUMBER];
	uint64_t WriteTimeCost[WORKER_NUMBER];
	uint64_t ReadTimeCost[WORKER_NUMBER];
	bool 	 WriteTest;

	bool CreateResources();
	bool CreateQueuePair(PeerSockData *peer, int MaxWr, int workerIdHint = -1);
	bool CreateSrmDataQueuePair(PeerSockData *peer, int offset);
	bool ModifyQPtoInit(struct ibv_qp *qp);
	bool ModifyQPtoRTR(struct ibv_qp *qp, uint32_t remote_qpn, uint16_t dlid, uint8_t *dgid);
	bool ModifyQPtoRTS(struct ibv_qp *qp);
	int GetControlCqIndex(int peerIndex) const;
	int GetDataCqIndex(int peerIndex) const;
	bool BindPeerCqs(PeerSockData *peer);
	int PickDataQpBySize(uint64_t size) const;
	bool ConnectQueuePair(PeerSockData *peer, int workerIdHint = -1);
	bool EnqueueConnectTask(int sock, uint16_t presetNodeID, bool hasPresetNodeID);
	int DataSyncwithSocket(int sock, int size, char *LocalData, char *RemoteData);
	bool ResourcesDestroy();
	void RdmaAccept(int fd);
	int  SocketConnect(uint16_t NodeID);
	void ServerConnect();
	bool DataTransferWorker(int id);
public:
	struct ibv_mr			*mr;			/* Memory registration handler */
	RdmaSocket(int _cqNum, uint64_t _mm, uint64_t _mmSize, Configuration* _conf, bool isServer, uint8_t Mode, uint32_t srmAppThreads = 17);
	~RdmaSocket();
	/*
	 * Request a graceful shutdown. This is intended to unblock polling/wait loops
	 * so RPCServer worker threads can exit.
	 */
	void Stop();
	/* Called by server side to accept the connection of clients. */
	void RdmaListen();
	/* Called by client side to connect to each of server actively. */
	void RdmaConnect();
	/* Called to check completion of RDMA operations */
	int PollCompletion(uint16_t NodeID, int PollNumber, struct ibv_wc *wc, bool isDataPath = false);
	int PollWithCQ(int cqPtr, int PollNumber, struct ibv_wc *wc);
	int PollOnce(int cqPtr, int PollNumber, struct ibv_wc *wc);
	/* Used for synchronization based on socket communication */
	void SyncTool(uint16_t NodeID);
	int getCQCount();
	uint16_t getNodeID();
	bool ProcessPendingConnectTask(int workerId);
	void NotifyWorkerSawCqe(int workerId);
	void WaitClientConnection(uint16_t NodeID);
	void RdmaQueryQueuePair(uint16_t NodeID);
	void NotifyPerformance();
	PeerSockData* getPeerInformation(uint16_t NodeID);
	/**
	*RdmaSend - Send data with RDMA_SEND
	*@param NodeID, Node ID where to write the data.
	*@param SourceBuffer, Local *absolute* address that keep the sending data.
	*@param BufferSize, Size of data to be sent.
	*return true on success, false on error.
	**/
	bool RdmaSend(uint16_t NodeID, uint64_t SourceBuffer, uint64_t BufferSize);
	bool _RdmaBatchSend(uint16_t NodeID, uint64_t SourceBuffer, uint64_t BufferSize, int BatchSize);
	/**
	*RdmaReceive - Receive the remote data with RDMA_RECV
	*@param NodeID, Node ID from which receive the data.
	*@param SourceBuffer, Local *absolute* address that keep the remote data.
	*@param BufferSize, Size of data to be receive.
	*return true on success, false on error.
	**/
	bool RdmaReceive(uint16_t NodeID, uint64_t SourceBuffer, uint64_t BufferSize);
	bool _RdmaBatchReceive(uint16_t NodeID, uint64_t SourceBuffer, uint64_t BufferSize, int BatchSize);
	/**
	*RdmaRead - Read data with RDMA_READ
	*@param NodeID, Node ID where to read the data.
	*@param SourceBuffer, Local *absolute* address that keep the read data.
	*@param DesBuffer, Remote *relative* address where to read.
	*@param BufferSize, Size of data to be read.
	*return true on success, false on error.
	**/
	bool RdmaRead(uint16_t NodeID, uint64_t SourceBuffer, uint64_t DesBuffer, uint64_t BufferSize, int TaskID);
	bool _RdmaBatchRead(uint16_t NodeID, uint64_t SourceBuffer, uint64_t DesBuffer, uint64_t BufferSize, int BatchSize);
	bool RemoteRead(uint64_t bufferSend, uint16_t NodeID, uint64_t bufferReceive, uint64_t size);
	bool InboundHamal(int TaskID, uint64_t bufferSend, uint16_t NodeID, uint64_t bufferReceive, uint64_t size);
	/**
	*RdmaWrite - WRITE data with RDMA_WRITE
	*@param NodeID, Node ID where to write the data.
	*@param SourceBuffer, Local *absolute* address that keep the sending data.
	*@param DesBuffer, Remote *relative* address to receive the data.
	*@param BufferSize, Size of data to be sent.
	*@param imm, -1 - RDMA_WRITE, otherwise, - RDMA_WRITE_WITH_IMM
	*return true on success, false on error.
	**/
	bool RdmaWrite(uint16_t NodeID, uint64_t SourceBuffer, uint64_t DesBuffer, uint64_t BufferSize, uint32_t imm, int TaskID);
	bool _RdmaBatchWrite(uint16_t NodeID, uint64_t SourceBuffer, uint64_t DesBuffer, uint64_t BufferSize, uint32_t imm, int BatchSize);
	bool RemoteWrite(uint64_t bufferSend, uint16_t NodeID, uint64_t bufferReceive, uint64_t size);
	bool OutboundHamal(int TaskID, uint64_t bufferSend, uint16_t NodeID, uint64_t bufferReceive, uint64_t size);
	/**
	*RdmaFetchAndAdd - Fetch data from DesBuffer to SourceBuffer, and add with "Add" remotely.
	*@param NodeID, Node ID where to write the data.
	*@param SourceBuffer, Local *relative* address that keep the fetching data.
	*@param DesBuffer, Remote *relative* address to add atomically.
	*@param Add, A 64-bits number add to the DesBuffer.
	*return true on success, false on error.
	**/
	bool RdmaFetchAndAdd(uint16_t NodeID, uint64_t SourceBuffer, uint64_t DesBuffer, uint64_t Add);
	/**
	*RdmaCompareAndSwap - Compare data at DesBuffer and value of "Compare", swap with "Swap" if not equal.
	*@param NodeID, Node ID where to write the data.
	*@param SourceBuffer, Local *relative* address that keep the fetching data.
	*@param DesBuffer, Remote *relative* address to compare&swap atomically.
	*@param Compare, A 64-bits number used to compare.
	*@param Swap, A 64-bits number used to swap.
	*return true on success, false on error.
	**/
	bool RdmaCompareAndSwap(uint16_t NodeID, uint64_t SourceBuffer, uint64_t DesBuffer, uint64_t Compare, uint64_t Swap);
};

#endif
