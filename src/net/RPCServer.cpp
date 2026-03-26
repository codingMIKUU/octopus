#include "RPCServer.hpp"
// __thread struct  timeval startt, endd;
RPCServer::RPCServer(int _cqSize) :cqSize(_cqSize) {
	running.store(true);
	mm = 0;
	UnlockWait = false;
	conf = new Configuration();
	mem = new MemoryManager(mm, conf->getServerCount(), 2);
	mm = mem->getDmfsBaseAddress();
	Debug::notifyInfo("DmfsBaseAddress = %lx, DmfsTotalSize = %lx",
		mem->getDmfsBaseAddress(), mem->getDmfsTotalSize());
	ServerCount = conf->getServerCount();
	socket = new RdmaSocket(cqSize * 2, mm, mem->getDmfsTotalSize(), conf, true, 0,1);
	client = new RPCClient(conf, socket, mem, (uint64_t)mm);
	tx = new TxManager(mem->getLocalLogAddress(), mem->getDistributedLogAddress());
	socket->RdmaListen();
	fs = new FileSystem((char *)mem->getMetadataBaseAddress(),
              (char *)mem->getDataAddress(),
              1024 * 20,/* Constructor of file system. */
              1024 * 30,
              2000,
              conf->getServerCount(),    
              socket->getNodeID());
	fs->rootInitialize(socket->getNodeID());
	wk = new thread[cqSize]();
	for (int i = 0; i < cqSize; i++)
		wk[i] = thread(&RPCServer::Worker, this, i);
}
RPCServer::~RPCServer() {
	Debug::notifyInfo("Stop RPCServer.");
	running.store(false);
	if (socket != nullptr) {
		socket->Stop();
	}
	if (wk != nullptr) {
		for (int i = 0; i < cqSize; i++) {
			if (wk[i].joinable()) {
				wk[i].join();
			}
		}
		delete[] wk;
		wk = nullptr;
	}
	delete fs;
	fs = nullptr;
	delete client;
	client = nullptr;
	delete tx;
	tx = nullptr;
	delete socket;
	socket = nullptr;
	delete mem;
	mem = nullptr;
	delete conf;
	conf = nullptr;
	Debug::notifyInfo("RPCServer is closed successfully.");
}

RdmaSocket* RPCServer::getRdmaSocketInstance() {
	return socket;
}

MemoryManager* RPCServer::getMemoryManagerInstance() {
	return mem;
}

RPCClient* RPCServer::getRPCClientInstance() {
	return client;
}

TxManager* RPCServer::getTxManagerInstance() {
	return tx;
}

void RPCServer::Worker(int id) {
	uint32_t tid = octopus_gettid_u32();
	// gettimeofday(&startt, NULL);
	Debug::notifyInfo("Worker %d, tid = %d", id, tid);
	th2id[tid] = id;
	mem->setID(id);
	while (running.load(std::memory_order_relaxed)) {
		RequestPoller(id);
	}
}

void RPCServer::RequestPoller(int id) {
	static int data_cnt = 0;
	struct ibv_wc wc[1];
	uint16_t NodeID;
	uint16_t offset;
	int ret = 0, count = 0;
	uint64_t bufferRecv;
	// unsigned long diff;
	ret = socket->PollOnce(id, 1, wc);
	if (ret <= 0) {
		/*gettimeofday(&endd, NULL);
		diff = 1000000 * (endd.tv_sec - startt.tv_sec) + endd.tv_usec - startt.tv_usec;
		if (diff > 2000000) {
			printf("ID = %d, Polling, ret = %d\n", id, ret);
			diff = 0;
			gettimeofday(&startt, NULL);
			uint64_t bufferRecv = mem->getClientMessageAddress(2);
			ExtentWriteSendBuffer *send = (ExtentWriteSendBuffer *)bufferRecv;
			socket->RdmaReceive(2, mm + 2 * 4096, 0);
			socket->RdmaReceive(2, mm + 2 * 4096, 0);
			Debug::debugItem("Path = %s, size = %x, offset = %x", send->path, send->size, send->offset);
		}*/
		return;
	} else if (wc[0].opcode == IBV_WC_RECV_RDMA_WITH_IMM) {
		NodeID = wc[0].imm_data >> 20;
		if (NodeID == 0XFFF) {
			/* Unlock request, process it directly. */
			// uint64_t hashAddress = wc[0].imm_data & 0x000FFFFF;
			// fs->unlockWriteHashItem(0, 0, hashAddress);
			return;
		}
		NodeID = (uint16_t)(wc[0].imm_data << 16 >> 16);
		offset = (uint16_t)(wc[0].imm_data >> 16);
		Debug::debugItem("NodeID = %d, offset = %d", NodeID, offset);
		count += 1;
		if (NodeID > 0 && NodeID <= ServerCount) {
			/* Recv Message From Other Server. */
			bufferRecv = mem->getServerRecvAddress(NodeID, offset);
		} else if (NodeID > ServerCount) {
			/* Recv Message From Client. */
			bufferRecv = mem->getClientMessageAddress(NodeID);
		}
		GeneralSendBuffer *send = (GeneralSendBuffer*)bufferRecv;
		switch (send->message) {
			case MESSAGE_TEST: {

			}
			default: {
				// if (id == 0 && UnlockWait == false && (send->message == MESSAGE_ADDMETATODIRECTORY || send->message == MESSAGE_REMOVEMETAFROMDIRECTORY)) {
				// 	/* 
				// 	* When process addmeta or remove meta, lock will be added without release,
				// 	* So we will wait until update meta arrives.
				// 	*/
				// 	UnlockWait = true;
				// 	ProcessRequest(send, NodeID, offset);
				// 	return;
				// } else if (id == 0 && send->message == MESSAGE_DOCOMMIT) {
				// 	UnlockWait = false;
				// 	ProcessRequest(send, NodeID, offset);
				// 	printf("a\n");
				// 	ProcessQueueRequest();
				// 	printf("b\n");
				// 	return;
				// }else if (id == 0 && UnlockWait == true) {
				// 	/* Just push the requests into the queue and return. */
				// 	RPCTask *task = (RPCTask *)malloc(sizeof(RPCTask));
				// 	task->send = (uint64_t)send;
				// 	task->NodeID = NodeID;
				// 	task->offset = offset;
				// 	tasks.push_back(task);
				// 	// printf("process docommit end.");
				// 	return;
				// }
				ProcessRequest(send, NodeID, offset);
				// printf("id = %d,end\n", id);
			}
		}
		
	} else {
		if(USE_SRM){
			bool handled_data_qp = false;
			for (uint16_t peer_id = 1; peer_id < 1000 && !handled_data_qp; ++peer_id) {
				PeerSockData *peer = socket->getPeerInformation(peer_id);
				if (peer == NULL)
					continue;

				for (int data_qp = DATA_QP_SMALL_INDEX;
					data_qp <= DATA_QP_LARGE_INDEX;
					++data_qp) {
					if (peer->qp[data_qp] == NULL)
						continue;
					if (wc[0].qp_num != peer->qp[data_qp]->qp_num)
						continue;

					if(ibv_srm_add_tot_recv_cqes(peer->qp[data_qp], ret)){
						Debug::debugItem("RequestPoller[%d]: ibv_srm_add_tot_recv_cqes failed (peer_id=%d, data_qp=%d, ret=%d)",
							id, peer_id, data_qp, ret);
					}
					Debug::debugItem("RequestPoller[%d]: handle data qp completion (peer_id=%d, data_qp=%d, ret=%d, data_cqe_count=%d, wc_qpn=%u, peer_qpn=%u, data_cq_index=%d, data_cq=%p)",
						id, peer_id, data_qp, ret, ++data_cnt,
						(unsigned)wc[0].qp_num,
						(unsigned)peer->qp[data_qp]->qp_num,
						peer->data_cq_index,
						(void*)peer->data_cq);
					handled_data_qp = true;
					break;
				}
			}
			if(!handled_data_qp)
				Debug::debugItem("RequestPoller[%d]: unexpected wc opcode=%d flags=0x%x, qp_num:%d (no data-qp owner found)",
					id, (int)wc[0].opcode, (unsigned)wc[0].wc_flags, wc[0].qp_num);
		}

		// if (!handled_data_qp) {
		// 	Debug::notifyInfo("RequestPoller[%d]: unexpected wc opcode=%d flags=0x%x",
		// 		id, (int)wc[0].opcode, (unsigned)wc[0].wc_flags);
		// }
	}
}

void RPCServer::ProcessQueueRequest() {
	for (auto task = tasks.begin(); task != tasks.end(); ) {
		// printf("1\n");
		ProcessRequest((GeneralSendBuffer *)(*task)->send, (*task)->NodeID, (*task)->offset);
		free(*task);
		task = tasks.erase(task);
	}
	// printf("2\n");
}

void RPCServer::ProcessRequest(GeneralSendBuffer *send, uint16_t NodeID, uint16_t offset) {
	char receiveBuffer[CLIENT_MESSAGE_SIZE];
	memset(receiveBuffer, 0, sizeof(receiveBuffer));
	uint64_t bufferRecv = (uint64_t)send;
	GeneralReceiveBuffer *recv = (GeneralReceiveBuffer*)receiveBuffer;
	recv->sourceNodeID = socket->getNodeID();
	recv->taskID = send->taskID;
	recv->sizeReceiveBuffer = 0;
	recv->message = MESSAGE_RESPONSE;
	recv->result = true;
	uint64_t size = send->sizeReceiveBuffer;
	if (send->message < MESSAGE_ADDMETATODIRECTORY || send->message >= MESSAGE_INVALID) {
		Debug::notifyError("ProcessRequest: invalid message=%d from NodeID=%u offset=%u, reply with failure",
			(int)send->message, (unsigned)NodeID, (unsigned)offset);
		recv->result = false;
		size = sizeof(GeneralReceiveBuffer);
		//exit(-1);
	} else if (size == 0 || size > CLIENT_MESSAGE_SIZE) {
		Debug::notifyError("ProcessRequest: invalid reply size=%lu for message=%d from NodeID=%u offset=%u",
			size, (int)send->message, (unsigned)NodeID, (unsigned)offset);
		recv->result = false;
		size = sizeof(GeneralReceiveBuffer);
		//exit(-1);
	}
	if (send->message == MESSAGE_DISCONNECT) {
        //rdma->disconnect(send->sourceNodeID);
        return;
    } else if (send->message == MESSAGE_TEST) {
    	;
    } else if (send->message == MESSAGE_UPDATEMETA) {
    	/* Write unlock. */
    	// UpdateMetaSendBuffer *bufferSend = (UpdateMetaSendBuffer *)send;
    	// fs->unlockWriteHashItem(bufferSend->key, NodeID, bufferSend->offset);
    	return;
    } else if (send->message == MESSAGE_EXTENTREADEND) {
    	/* Read unlock */
    	// ExtentReadEndSendBuffer *bufferSend = (ExtentReadEndSendBuffer *)send;
    	// fs->unlockReadHashItem(bufferSend->key, NodeID, bufferSend->offset);
    	return;
	} else {
		if (recv->result) {
	    		fs->parseMessage((char*)send, receiveBuffer);
	    		// fs->recursivereaddir("/", 0);
			Debug::debugItem("Contract Receive Buffer, size = %lu.", size);
			uint64_t contract_size = ContractReceiveBuffer(send, recv);
			if (contract_size > size) {
				Debug::notifyError("ProcessRequest: contract size overflow, size=%lu, contract=%lu, message=%d",
					size, contract_size, (int)send->message);
				size = sizeof(GeneralReceiveBuffer);
			} else {
				size -= contract_size;
			}
	    		if (send->message == MESSAGE_RAWREAD) {
	    			ExtentReadSendBuffer *bufferSend = (ExtentReadSendBuffer *)send;
				int data_qp = (bufferSend->size >= DATA_QP_SPLIT_SIZE) ?
					DATA_QP_LARGE_INDEX : DATA_QP_SMALL_INDEX;
	    			uint64_t *value = (uint64_t *)mem->getDataAddress();
	    			// printf("rawread size = %d\n", (int)bufferSend->size);
	    			*value = 1;
				if (!socket->RdmaWrite(NodeID, mem->getDataAddress(), 2 * 4096, bufferSend->size, -1, data_qp)) {
					Debug::notifyError("ProcessRequest: MESSAGE_RAWREAD data write failed (NodeID=%u, size=%lu, data_qp=%d)",
						(unsigned)NodeID, bufferSend->size, data_qp);
					recv->result = false;
				}
	    		} else if (send->message == MESSAGE_RAWWRITE) {
	    			ExtentWriteSendBuffer *bufferSend = (ExtentWriteSendBuffer *)send;
	    			// printf("rawwrite size = %d\n", (int)bufferSend->size);
				if (!socket->RemoteRead(mem->getDataAddress(), NodeID, 2 * 4096, bufferSend->size)) {
					Debug::notifyError("ProcessRequest: MESSAGE_RAWWRITE data read failed (NodeID=%u, size=%lu)",
						(unsigned)NodeID, bufferSend->size);
					recv->result = false;
					//exit(-1);
				}
	    		}
		}

		Debug::debugItem("Copy Reply Data, size = %lu.", size);
		memcpy((void *)send, receiveBuffer, size);
		Debug::debugItem("Select Buffer.");
		if (NodeID > 0 && NodeID <= ServerCount) {
			/* Recv Message From Other Server. */
			bufferRecv = bufferRecv - mm;
		} else if (NodeID > ServerCount) {
			/* Recv Message From Client. */
			bufferRecv = 0;
		}
		Debug::debugItem("send = %lx, recv = %lx", send, bufferRecv);
		socket->_RdmaBatchWrite(NodeID, (uint64_t)send, bufferRecv, size, 0, 1);
		// socket->_RdmaBatchReceive(NodeID, mm, 0, 2);
		socket->RdmaReceive(NodeID, mm + NodeID * 4096, 0);
		// printf("process end\n");
    }
}

int RPCServer::getIDbyTID() {
	uint32_t tid = octopus_gettid_u32();
	return th2id[tid];
}
uint64_t RPCServer::ContractReceiveBuffer(GeneralSendBuffer *send, GeneralReceiveBuffer *recv) {
	uint64_t length;
	switch (send->message) {
		case MESSAGE_GETATTR: {
			GetAttributeReceiveBuffer *bufferRecv = 
			(GetAttributeReceiveBuffer *)recv;
			if (bufferRecv->attribute.count >= 0 && bufferRecv->attribute.count < MAX_FILE_EXTENT_COUNT)
				length = (MAX_FILE_EXTENT_COUNT - bufferRecv->attribute.count) * sizeof(FileMetaTuple);
			else 
				length = sizeof(FileMetaTuple) * MAX_FILE_EXTENT_COUNT;
			break;
		}
		case MESSAGE_READDIR: {
			ReadDirectoryReceiveBuffer *bufferRecv = 
			(ReadDirectoryReceiveBuffer *)recv;
			if (bufferRecv->list.count >= 0 && bufferRecv->list.count <= MAX_DIRECTORY_COUNT)
				length = (MAX_DIRECTORY_COUNT - bufferRecv->list.count) * sizeof(DirectoryMetaTuple);
			else 
				length = MAX_DIRECTORY_COUNT * sizeof(DirectoryMetaTuple);
			break;
		}
		case MESSAGE_EXTENTREAD: {
			ExtentReadReceiveBuffer *bufferRecv = 
			(ExtentReadReceiveBuffer *)recv;
			if (bufferRecv->fpi.len >= 0 && bufferRecv->fpi.len <= MAX_MESSAGE_BLOCK_COUNT)
				length = (MAX_MESSAGE_BLOCK_COUNT - bufferRecv->fpi.len) * sizeof(file_pos_tuple);
			else 
				length = MAX_MESSAGE_BLOCK_COUNT * sizeof(file_pos_tuple);
			break;
		}
		case MESSAGE_EXTENTWRITE: {
			ExtentWriteReceiveBuffer *bufferRecv = 
			(ExtentWriteReceiveBuffer *)recv;
			if (bufferRecv->fpi.len >= 0 && bufferRecv->fpi.len <= MAX_MESSAGE_BLOCK_COUNT)
				length = (MAX_MESSAGE_BLOCK_COUNT - bufferRecv->fpi.len) * sizeof(file_pos_tuple);
			else 
				length = MAX_MESSAGE_BLOCK_COUNT * sizeof(file_pos_tuple);
			break;
		}
		case MESSAGE_READDIRECTORYMETA: {
			ReadDirectoryMetaReceiveBuffer *bufferRecv = 
			(ReadDirectoryMetaReceiveBuffer *)recv;
			if (bufferRecv->meta.count >= 0 && bufferRecv->meta.count <= MAX_DIRECTORY_COUNT)
				length = (MAX_DIRECTORY_COUNT - bufferRecv->meta.count) * sizeof(DirectoryMetaTuple);
			else 
				length = MAX_DIRECTORY_COUNT * sizeof(DirectoryMetaTuple);
			break;
		}
		default: {
			length = 0;
			break;
		}
	}	
	// printf("contract length = %d", (int)length);
	return length;
}
