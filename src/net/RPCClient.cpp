#include "RPCClient.hpp"
#include "TxManager.hpp"

RPCClient::RPCClient(Configuration *_conf, RdmaSocket *_socket, MemoryManager *_mem, uint64_t _mm)
:conf(_conf), socket(_socket), mem(_mem), mm(_mm) {
	isServer = true;
	taskID  = 1;
}

RPCClient::RPCClient(uint32_t srmAppThreads) {
	isServer = false;
	taskID = 1;
	mm = (uint64_t)malloc(sizeof(char) * (1024 * 4 + 1024 * 1024 * 4));
	conf = new Configuration();
	socket = new RdmaSocket(2, mm, (1024 * 4 + 1024 * 1024 * 4), conf, false, 0, srmAppThreads);
	socket->RdmaConnect();
}

RPCClient::~RPCClient() {
	Debug::notifyInfo("Stop RPCClient.");
	if (!isServer) {
		delete conf;
		delete socket;
		free((void *)mm);
	}
	Debug::notifyInfo("RPCClient is closed successfully.");
}

RdmaSocket* RPCClient::getRdmaSocketInstance() {
	return socket;
}

Configuration* RPCClient::getConfInstance() {
	return conf;
}

bool RPCClient::RdmaCall(uint16_t DesNodeID, char *bufferSend, uint64_t lengthSend, char *bufferReceive, uint64_t lengthReceive) {
	uint32_t ID = __sync_fetch_and_add( &taskID, 1 ), temp;
	uint64_t sendBuffer, receiveBuffer, remoteRecvBuffer;
	uint16_t offset = 0;
	uint32_t imm = (uint32_t)socket->getNodeID();
	static thread_local uint64_t rpc_call_idx = 0;
	rpc_call_idx++;
	// struct  timeval startt, endd;
	// unsigned long diff, tempCount = 0;
	GeneralSendBuffer *send = (GeneralSendBuffer*)bufferSend;
	lengthReceive -= ContractSendBuffer(send);
	send->taskID = ID;
	send->sourceNodeID = socket->getNodeID();
	send->sizeReceiveBuffer = lengthReceive;
	if (isServer) {
		offset = mem->getServerSendAddress(DesNodeID, &sendBuffer);
		printf("offset = %d\n", offset);
		receiveBuffer = mem->getServerRecvAddress(socket->getNodeID(), offset);
		remoteRecvBuffer = receiveBuffer - mm;
	} else {
		sendBuffer = mm;
		receiveBuffer = mm;
		remoteRecvBuffer = (socket->getNodeID() - conf->getServerCount() - 1) * CLIENT_MESSAGE_SIZE;
	}
	GeneralReceiveBuffer *recv = (GeneralReceiveBuffer*)receiveBuffer;
	recv->message = MESSAGE_INVALID;
	memcpy((void *)sendBuffer, (void *)bufferSend, lengthSend);
	for (uint64_t off = 0; off < lengthReceive; off += 64) {
		_mm_clflush((void *)(receiveBuffer + off));
	}
	asm volatile ("sfence\n" : : );
	temp = (uint32_t)offset;
	imm = imm + (temp << 16);
	if (imm == 0) {
		Debug::notifyError("RdmaCall[%lu]: local node id is 0, IMM is 0, server will not get RECV event. request msg=%d dst=%u",
			rpc_call_idx, (int)send->message, DesNodeID);
		return false;
	}
	if (rpc_call_idx <= 8 || (rpc_call_idx % 1000 == 0)) {
		Debug::debugItem("RdmaCall[%lu]: local_node=%u imm=0x%x offset=%u",
			rpc_call_idx, (unsigned)socket->getNodeID(), imm, (unsigned)offset);
	}
	Debug::debugItem("sendBuffer = %lx, receiveBuffer = %lx, remoteRecvBuffer = %lx, ReceiveSize = %d", 
		sendBuffer, receiveBuffer, remoteRecvBuffer, lengthReceive);
	if (send->message == MESSAGE_DISCONNECT
		|| send->message == MESSAGE_UPDATEMETA
		|| send->message == MESSAGE_EXTENTREADEND) {
		// socket->_RdmaBatchWrite(DesNodeID, sendBuffer, remoteRecvBuffer, lengthSend, imm, 1);
		// socket->PollCompletion(DesNodeID, 1, &wc);
		return true;
	}
	if (!socket->_RdmaBatchWrite(DesNodeID, sendBuffer, remoteRecvBuffer, lengthSend, imm, 1)) {
		Debug::notifyError("RdmaCall: failed to send request via RDMA to node %d", DesNodeID);
		return false;
	}
	// {
	// 	struct ibv_wc send_wc;
	// 	int poll_ret = socket->PollCompletion(DesNodeID, 1, &send_wc);
	// 	if (poll_ret < 0) {
	// 		Debug::notifyError("RdmaCall[%lu]: request send completion failed msg=%d dst=%u",
	// 			rpc_call_idx, (int)send->message, DesNodeID);
	// 		return false;
	// 	}
	// 	if (rpc_call_idx <= 8 || (rpc_call_idx % 1000 == 0)) {
	// 		Debug::notifyInfo("RdmaCall[%lu]: send completion ok msg=%d dst=%u opcode=%d wr_id=%llu",
	// 			rpc_call_idx,
	// 			(int)send->message,
	// 			DesNodeID,
	// 			(int)send_wc.opcode,
	// 			(unsigned long long)send_wc.wr_id);
	// 	}
	// }
	if (isServer) {
		while (recv->message == MESSAGE_INVALID || recv->message != MESSAGE_RESPONSE)
			;
	} else {
		struct timespec wait_start, wait_now;
		clock_gettime(CLOCK_MONOTONIC, &wait_start);
		double last_report_s = 0.0;
		while (recv->message != MESSAGE_RESPONSE) {
			clock_gettime(CLOCK_MONOTONIC, &wait_now);
			double waited = (wait_now.tv_sec - wait_start.tv_sec) +
				(wait_now.tv_nsec - wait_start.tv_nsec) / 1e9;
			if (waited - last_report_s >= 1.0) {
				Debug::notifyInfo("RdmaCall[%lu]: waiting response msg=%d dst=%u waited=%.2fs recv_msg=%d",
					rpc_call_idx, (int)send->message, DesNodeID, waited, (int)recv->message);
				last_report_s = waited;
			}
			;
			/* gettimeofday(&endd,NULL);
			diff = 1000000 * (endd.tv_sec - startt.tv_sec) + endd.tv_usec - startt.tv_usec;
			if (diff > 1000000) {
				Debug::debugItem("Send the Fucking Message Again.");
				ExtentWriteSendBuffer *tempsend = (ExtentWriteSendBuffer *)sendBuffer;
				tempsend->offset = (uint64_t)tempCount;
				tempCount += 1;
				socket->_RdmaBatchWrite(DesNodeID, sendBuffer, remoteRecvBuffer, lengthSend, imm, 1);
				gettimeofday(&startt,NULL);
				diff = 0;
			}*/
		}
	}
	if (rpc_call_idx <= 8 || (rpc_call_idx % 1000 == 0)) {
		Debug::debugItem("RdmaCall[%lu]: response arrived msg=%d dst=%u", rpc_call_idx,
			(int)send->message, DesNodeID);
	}
	if (send->message == MESSAGE_EXTENTWRITE) {
		ExtentWriteReceiveBuffer *wr = (ExtentWriteReceiveBuffer *)receiveBuffer;
		uint32_t spin = 0;
		while (wr->fpi.len > MAX_MESSAGE_BLOCK_COUNT && spin < 1000000) {
			asm volatile("pause" ::: "memory");
			spin += 1;
		}
	} else if (send->message == MESSAGE_EXTENTREAD) {
		ExtentReadReceiveBuffer *rr = (ExtentReadReceiveBuffer *)receiveBuffer;
		uint32_t spin = 0;
		while (rr->fpi.len > MAX_MESSAGE_BLOCK_COUNT && spin < 1000000) {
			asm volatile("pause" ::: "memory");
			spin += 1;
		}
	}
	asm volatile ("lfence\n" : : );
	memcpy((void*)bufferReceive, (void *)receiveBuffer, lengthReceive);
	return true;
}

uint64_t RPCClient::ContractSendBuffer(GeneralSendBuffer *send) {
	uint64_t length = 0;
	switch (send->message) {
		case MESSAGE_MKNODWITHMETA: {
			MakeNodeWithMetaSendBuffer *bufferSend = 
	                (MakeNodeWithMetaSendBuffer *)send;
        	    	length = (MAX_FILE_EXTENT_COUNT - bufferSend->metaFile.size) * sizeof(FileMetaTuple);
			length = 0;
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
