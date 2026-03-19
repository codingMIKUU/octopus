/***********************************************************************
* 
* 
* Tsinghua Univ, 2016
*
***********************************************************************/
#include "RdmaSocket.hpp"
#include <ifaddrs.h>
#include <stdio.h>
#include <mutex>
#include <unordered_set>

static bool octopus_gid_all_zero(const union ibv_gid &gid) {
    for (int i = 0; i < 16; ++i) {
        if (gid.raw[i] != 0) {
            return false;
        }
    }
    return true;
}

static int octopus_pick_valid_gid_index(struct ibv_context *ctx, uint8_t port, int maxProbe = 32) {
    union ibv_gid gid;
    for (int idx = 0; idx < maxProbe; ++idx) {
        if (ibv_query_gid(ctx, port, idx, &gid) != 0) {
            continue;
        }
        if (!octopus_gid_all_zero(gid)) {
            return idx;
        }
    }
    return -1;
}

static bool octopus_get_local_ipv4_addrs(std::vector<std::string> &out) {
    struct ifaddrs *ifaddr = nullptr;
    if (getifaddrs(&ifaddr) != 0) {
        return false;
    }
    for (struct ifaddrs *ifa = ifaddr; ifa != nullptr; ifa = ifa->ifa_next) {
        if (ifa->ifa_addr == nullptr)
            continue;
        if (ifa->ifa_addr->sa_family != AF_INET)
            continue;
        char buf[INET_ADDRSTRLEN];
        void *addr = &((struct sockaddr_in *)ifa->ifa_addr)->sin_addr;
        if (inet_ntop(AF_INET, addr, buf, sizeof(buf)) != nullptr) {
            out.emplace_back(buf);
        }
    }
    freeifaddrs(ifaddr);
    return true;
}

static uint16_t octopus_pick_node_id_from_conf(Configuration *conf) {
    if (conf == nullptr)
        return 0;
    auto confMap = conf->getInstance();
    std::vector<std::string> localIps;
    if (!octopus_get_local_ipv4_addrs(localIps)) {
        return 0;
    }
    for (const auto &localIp : localIps) {
        for (const auto &kv : confMap) {
            if (kv.second == localIp) {
                return kv.first;
            }
        }
    }
    return 0;
}

static inline uint64_t rdtsc() {
    unsigned int lo, hi;
    __asm__ __volatile__("rdtsc" : "=a"(lo), "=d"(hi));
    return ((uint64_t)hi << 32) | lo;
}

static inline void octopus_log_wqe(uint16_t local_id, uint16_t remote_id, uint64_t size) {
    char path[96];
    snprintf(path, sizeof(path), "/root/zxm/log_data/rdma_peer_%u_to_%u.log",
             (unsigned int)local_id, (unsigned int)remote_id);
    const char *mode = "a";
    static std::mutex opened_mutex;
    static std::unordered_set<uint16_t> opened;
    {
        std::lock_guard<std::mutex> lock(opened_mutex);
        if (opened.insert(local_id).second) {
            mode = "w";
        }
    }
    FILE *fp = fopen(path, mode);
    if (fp == NULL) {
        return;
    }
    unsigned long long cycles = (unsigned long long)rdtsc();
    fprintf(fp, "%llu %llu\n", (unsigned long long)size, cycles);
    fclose(fp);
}

using namespace std;

int RdmaSocket::PickDataQpBySize(uint64_t size) const {
    if (size >= DATA_QP_SPLIT_SIZE) {
        return DATA_QP_LARGE_INDEX;
    }
    return DATA_QP_SMALL_INDEX;
}

RdmaSocket::RdmaSocket(int _cqNum, uint64_t _mm, uint64_t _mmSize, Configuration* _conf, bool _isServer, uint8_t _Mode, uint32_t _srmAppThreads) :
DeviceName(NULL), Port(1), ServerPort(5678), GidIndex(0), 
isRunning(true), isServer(_isServer), cqNum(_cqNum), cqPtr(0), 
mm(_mm), mmSize(_mmSize), conf(_conf), MaxNodeID(1), listenSock(-1), Mode(_Mode), srmAppThreads(_srmAppThreads) {
    for (int i = 0; i < 1000; i++) {
        peers[i] = NULL;
    }
	/* Use multiple cq to parallelly process new request. */
	cq = (struct ibv_cq **)malloc(cqNum * sizeof(struct ibv_cq *));
    for (int i = 0; i < cqNum; i++)
        cq[i] = NULL;
	/* Find my IP, and initialize my NodeID (At server side). */
	/* NodeID at client side will be given on connection */
    ServerCount = conf->getServerCount();
    MaxNodeID = ServerCount + 1;
	if (isServer) {
        MyNodeID = octopus_pick_node_id_from_conf(conf);
        if (MyNodeID == 0) {
            char hname[128];
            struct hostent *hent;
            gethostname(hname, sizeof(hname));
            hent = gethostbyname(hname);
            string ip(inet_ntoa(*(struct in_addr*)(hent->h_addr_list[0])));
            MyNodeID = conf->getIDbyIP(ip);
            Debug::notifyInfo("IP = %s, NodeID = %d", ip.c_str(), MyNodeID);
        }
        if (MyNodeID == 0) {
            Debug::notifyError("Failed to map local IP to a NodeID from conf.xml. Please ensure conf.xml contains a local interface IP.");
            MyNodeID = 1;
        }
	} else {
        cqPtr = 0;
    }
	CreateResources();
    for (int  i = 0; i < WORKER_NUMBER; i++) {
        WriteSize[i] = 0;
        ReadSize[i] = 0;
        WriteTimeCost[i] = 0;
        ReadTimeCost[i] = 0;
    }
    WriteTest = false;
}

RdmaSocket::~RdmaSocket() {
    Debug::notifyInfo("Stop RdmaSocket.");
	isRunning = false;
    if (isServer) {
        Debug::debugItem("1");
		if (listenSock != -1) {
			shutdown(listenSock, SHUT_RDWR);
			close(listenSock);
			listenSock = -1;
		}
		if (Listener.joinable()) {
			Listener.join();
		}
    }
    Debug::debugItem("2");
	ResourcesDestroy();
    Debug::notifyInfo("RdmaSocket is closed successfully.");
}

void RdmaSocket::Stop() {
    isRunning = false;
    if (isServer) {
        if (listenSock != -1) {
            shutdown(listenSock, SHUT_RDWR);
            close(listenSock);
            listenSock = -1;
        }
        if (Listener.joinable()) {
            Listener.join();
        }
    }
}

void RdmaSocket::NotifyPerformance() {
    for (int i = 0; i < WORKER_NUMBER; i++) {
        printf("\n");
        Debug::notifyInfo("TotalWriteSize = %ld, WriteTimeCost = %ld", WriteSize[i], WriteTimeCost[i]);
        Debug::notifyInfo("TotalReadSize = %ld, ReadTimeCost = %ld", ReadSize[i], ReadTimeCost[i]);
    }
}

bool RdmaSocket::CreateResources() {
	/* Open device, create PD */
	struct ibv_device **DeviceList = NULL;
	struct ibv_device *dev = NULL;
	int rc = 0, mrFlags, DevicesNum, i;
    /* get device names in the system */
    DeviceList = ibv_get_device_list(&DevicesNum);
    if (!DeviceList) {
        Debug::notifyError("failed to get IB devices list");
        rc = 1;
        goto CreateResourcesExit;
    }
    /* if there isn't any IB device in host */
    if (!DevicesNum) {
        Debug::notifyInfo("found %d device(s)", DevicesNum);
        rc = 1;
        goto CreateResourcesExit;
    }
    Debug::notifyInfo("Open IB Device");
    /* search for the specific device we want to work with */
    for (i = 0; i < DevicesNum; i ++) {
        if (!DeviceName) {
            DeviceName = strdup(ibv_get_device_name(DeviceList[i]));
        }
        if (!strcmp(ibv_get_device_name(DeviceList[i]), DeviceName)) {
            dev = DeviceList[i];
            break;
        }
    }
    /* if the device wasn't found in host */
    if (!dev) {
        Debug::notifyError("IB device wasn't found");
        rc = 1;
        goto CreateResourcesExit;
    }
    /* get device handle */
    ctx = ibv_open_device(dev);
    if (!ctx) {
        Debug::notifyError("failed to open device");
        rc = 1;
        goto CreateResourcesExit;
    }
    /* We are now done with device list, free it */
    ibv_free_device_list(DeviceList);
    DeviceList = NULL;
    dev = NULL;
    /* query port properties */
    if (ibv_query_port(ctx, Port, &PortAttribute)) {
        Debug::notifyError("ibv_query_port failed");
        rc = 1;
        goto CreateResourcesExit;
    }
    if (PortAttribute.lid == 0) {
        int pickedGidIndex = octopus_pick_valid_gid_index(ctx, Port);
        if (pickedGidIndex < 0) {
            Debug::notifyError("RoCE detected (lid=0) but no valid GID found on port %d", Port);
            rc = 1;
            goto CreateResourcesExit;
        }
        GidIndex = pickedGidIndex;
        union ibv_gid gid;
        if (ibv_query_gid(ctx, Port, GidIndex, &gid) == 0) {
            Debug::notifyInfo("RoCE mode: use GID index %d, gid=%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x",
                GidIndex,
                gid.raw[0], gid.raw[1], gid.raw[2], gid.raw[3],
                gid.raw[4], gid.raw[5], gid.raw[6], gid.raw[7],
                gid.raw[8], gid.raw[9], gid.raw[10], gid.raw[11],
                gid.raw[12], gid.raw[13], gid.raw[14], gid.raw[15]);
        }
    }
    Debug::notifyInfo("Create Completion Queue");
    /* Create CQ for a certain number. */
 	for (i = 0; i < cqNum; i++) {
    	cq[i] = ibv_create_cq(ctx, QPS_MAX_DEPTH, NULL, NULL, 0);
	    if (cq[i] == NULL) {
	        Debug::notifyError("failed to create CQ");
            rc = 1;
            goto CreateResourcesExit;
	    }
    }

    /* allocate Protection Domain */
    Debug::notifyInfo("Allocate Protection Domain");
    pd = ibv_alloc_pd(ctx);
    if (!pd) {
        Debug::notifyError("ibv_alloc_pd failed");
        rc = 1;
        goto CreateResourcesExit;
    }

    mrFlags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC;
   
    /* Test Registration Time Cost. */
    // struct  timeval start, end;
    // int size;
    // long long diff;
    // char *testmm;
    // size = 0;
    // for (int i = 1; i <= 4; i++) {
    //     /* From 4 KB to 400 KB. */
    //     if (size ==0) {
    //         size = 1024 * 1024;
    //     } else {
    //         size = size * 10;
    //     }
    //     testmm = (char *)malloc(size);
    //     memset(testmm, 0, size);
    //     gettimeofday(&start, NULL);
    //     mr = ibv_reg_mr(pd, (void*)testmm, size, mrFlags);
    //     ibv_dereg_mr(mr);
    //     gettimeofday(&end, NULL);
    //     diff = 1000000 * (end.tv_sec - start.tv_sec) + end.tv_usec - start.tv_usec;
    //     printf("%d\t%ld\n", size, (long)diff);
    //     free(testmm);
    // }

    /* register the memory buffer */
    Debug::notifyInfo("Register Memory Region");
    
    mr = ibv_reg_mr(pd, (void*)mm, mmSize, mrFlags);
    if (mr == NULL) {
        Debug::notifyError("Memory registration failed");
        rc = 1;
        goto CreateResourcesExit;
    }

    CreateResourcesExit:
    if (rc) {
        /* Error encountered, cleanup */
        Debug::notifyError("Error Encountered, Cleanup ...");
        for (i = 0; i < cqNum; i++) {
		    if (cq[i] != NULL)
		       ibv_destroy_cq(cq[i]);
    	}
        if (pd) {
        	ibv_dealloc_pd(pd);
        	pd = NULL;
        }
        if (ctx) {
            ibv_close_device(ctx);
            ctx = NULL;
        }
        if (DeviceList) {
            ibv_free_device_list(DeviceList);
            DeviceList = NULL;
        }
        return false;
    }
    return true;
}

bool RdmaSocket::CreateQueuePair(PeerSockData *peer, int offset) {

	struct ibv_qp_init_attr attr;
	memset(&attr, 0, sizeof(attr));

	if(Mode == 0) {
        attr.qp_type = IBV_QPT_RC;
    } else if (Mode == 1) {
        attr.qp_type = IBV_QPT_UC;
    }
    attr.sq_sig_all = 0;

    if (isServer && peer->NodeID > 0 && peer->NodeID <= ServerCount) {
        /* Server interconnect: always use CQ 0. */
        attr.send_cq = cq[0];
        attr.recv_cq = cq[0];
        peer->cq = cq[0];
    } else if (isServer) {
        /* Connection between server and client. */
        if (offset == 0) {
            /* Each client will create two qps, we use same cq at server side. */
            cqPtr += 1;
            if (cqPtr >= cqNum)
                cqPtr = 1;
        }
        attr.send_cq = cq[cqPtr];
        attr.recv_cq = cq[cqPtr];
        peer->cq = cq[cqPtr];
    } else {
        /* Client: one CQ per peer, and all QPs of that peer share it. */
        if (offset == CONTROL_QP_INDEX) {
            int next = cqPtr;
            cqPtr += 1;
            if (cqPtr >= cqNum) {
                cqPtr = 0;
            }
            attr.send_cq = cq[next];
            attr.recv_cq = cq[next];
            peer->cq = cq[next];
        } else {
            attr.send_cq = peer->cq;
            attr.recv_cq = peer->cq;
        }
    }

    if (isServer || offset == CONTROL_QP_INDEX) {
        attr.cap.max_send_wr = QPS_MAX_DEPTH;
        attr.cap.max_recv_wr = QPS_MAX_DEPTH;
        attr.cap.max_send_sge = 1;
        attr.cap.max_recv_sge = 1;
        attr.cap.max_inline_data = 0;
        peer->qp[offset] = ibv_create_qp(pd, &attr);
        if (!peer->qp[offset]) {
	        Debug::notifyError("Failed to create native QP (offset=%d)", offset);
	        return false;
        }
        Debug::notifyInfo("Create native Queue Pair(offset=%d) with Num = %d", offset, peer->qp[offset]->qp_num);
        return true;
    }

    return CreateSrmDataQueuePair(peer, offset);
}

bool RdmaSocket::CreateSrmDataQueuePair(PeerSockData *peer, int offset) {
    struct ibv_qp_init_attr_ex attr_ex;
    memset(&attr_ex, 0, sizeof(attr_ex));

    attr_ex.qp_type = (Mode == 1) ? IBV_QPT_UC : IBV_QPT_RC;
    attr_ex.sq_sig_all = 0;
    attr_ex.send_cq = peer->cq;
    attr_ex.recv_cq = peer->cq;
    attr_ex.pd = pd;
    attr_ex.comp_mask = IBV_QP_INIT_ATTR_PD;

    attr_ex.cap.max_send_wr = QPS_MAX_DEPTH;
    attr_ex.cap.max_recv_wr = QPS_MAX_DEPTH;
    attr_ex.cap.max_send_sge = 1;
    attr_ex.cap.max_recv_sge = 1;
    attr_ex.cap.max_inline_data = 0;

    if (attr_ex.qp_type == IBV_QPT_RC) {
        attr_ex.sender_side = 1;
        attr_ex.rnode_num = 1;
        attr_ex.srm_app_threads = (srmAppThreads > 0) ? srmAppThreads : 1;
        attr_ex.srm_xrc_qp_num_per_srm = 1;//Server端就一个
    }

    peer->qp[offset] = ibv_create_qp_ex(ctx, &attr_ex);
    if (!peer->qp[offset]) {
        Debug::notifyError("Failed to create SRM data QP offset=%d", offset);
        return false;
    }

    Debug::notifyInfo("Create SRM data Queue Pair(offset=%d) with Num = %d",
                      offset, peer->qp[offset]->qp_num);
    return true;
}

bool RdmaSocket::ModifyQPtoInit(struct ibv_qp *qp) {
    if (qp == NULL) {
        Debug::notifyError("Bad QP, Return");
    }
    struct ibv_qp_attr attr;
    int flags;
    int rc;
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_INIT;
    attr.port_num = Port;
    attr.pkey_index = 0;
    if (Mode == 0) {
        attr.qp_access_flags = IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC;
    } else if (Mode == 1) {
        attr.qp_access_flags = IBV_ACCESS_REMOTE_WRITE;
    }
    flags = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;
    rc = ibv_modify_qp(qp, &attr, flags);
    if (rc) {
    	Debug::notifyError("Failed to modify QP state to INIT");
    	return false;
    }
    return true;
}

bool RdmaSocket::ModifyQPtoRTR(struct ibv_qp *qp, uint32_t remote_qpn, uint16_t dlid, uint8_t *dgid) {
    struct ibv_qp_attr attr;
    int flags;
    int rc;
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTR;
    attr.path_mtu = PortAttribute.active_mtu ? PortAttribute.active_mtu : IBV_MTU_1024;
    attr.dest_qp_num = remote_qpn;
    attr.rq_psn = 3185;
    // attr.max_dest_rd_atomic = 1;
    // attr.min_rnr_timer = 0x12;
    const bool use_grh = (PortAttribute.lid == 0) || (dlid == 0);
    attr.ah_attr.is_global = use_grh ? 1 : 0;
    attr.ah_attr.dlid = use_grh ? 0 : dlid;
    attr.ah_attr.sl = 0;
    attr.ah_attr.src_path_bits = 0;
    attr.ah_attr.port_num = Port;
    if (use_grh) {
        memcpy(&attr.ah_attr.grh.dgid, dgid, 16);
        attr.ah_attr.grh.flow_label = 0;
        attr.ah_attr.grh.hop_limit = 1;
        attr.ah_attr.grh.sgid_index = GidIndex;
        attr.ah_attr.grh.traffic_class = 0;
    }
    flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN | IBV_QP_RQ_PSN;
    // IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER
    if (Mode == 0) {
        attr.max_dest_rd_atomic = 16;
        attr.min_rnr_timer = 12;
        flags |= IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;
    }
    rc = ibv_modify_qp(qp, &attr, flags);
    if (rc) {
   		Debug::notifyError("failed to modify QP state to RTR");
   		return false;
    }
    return true;
}

bool RdmaSocket::ModifyQPtoRTS(struct ibv_qp *qp) {
    struct ibv_qp_attr attr;
    int flags;
    int rc;
    memset(&attr, 0, sizeof(attr));
    
    attr.qp_state = IBV_QPS_RTS;
    attr.sq_psn = 3185;
    flags = IBV_QP_STATE | IBV_QP_SQ_PSN;

    if (Mode == 0) {
        attr.timeout = 14;
        attr.retry_cnt = 7;
        attr.rnr_retry = 3;
        attr.max_rd_atomic = 16;
        attr.max_dest_rd_atomic = 16;
        flags |= IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY | IBV_QP_MAX_QP_RD_ATOMIC;
    }
    // attr.max_rd_atomic = 1;
    rc = ibv_modify_qp(qp, &attr, flags);
    if (rc) {
    	Debug::notifyError("failed to modify QP state to RTS");
    	return false;
    }
    return true;
}

bool RdmaSocket::ConnectQueuePair(PeerSockData *peer) {
	ExchangeMeta LocalMeta, RemoteMeta;
    ExchangeID LocalID, RemoteID;
	int rc = 0, N;
    bool ret;
	union ibv_gid MyGid;
    bool DoubleQP = false;
    if (isServer) {
        LocalID.NodeID = MyNodeID;
        LocalID.isServer = true;
        LocalID.GivenID = 0;
        if (isServer && MyNodeID == 1) {
            LocalID.GivenID = MaxNodeID;
        }
    } else {
        LocalID.NodeID = MyNodeID;
        LocalID.isServer = false;
        LocalID.GivenID = 0;
    }
    /* Change NodeID first */
    if (DataSyncwithSocket(peer->sock, sizeof(ExchangeID), (char *)&LocalID, (char *)&RemoteID) < 0) {
        Debug::notifyError("failed to exchange connection data between sides");
        rc = 1;
        goto ConnectQPExit;
    }
    if (isServer && RemoteID.isServer) {
        /* A server is connecting to me, we are both servers. */
        peer->NodeID = RemoteID.NodeID;
    } else if (isServer && !RemoteID.isServer && MyNodeID != 1) {
        peer->NodeID = RemoteID.NodeID;
    } else if (isServer && !RemoteID.isServer && MyNodeID == 1) {
        peer->NodeID = MaxNodeID;
        MaxNodeID += 1;
    } else if (!isServer && RemoteID.GivenID != 0) {
        MyNodeID = RemoteID.GivenID;
    }

	if (!CreateQueuePair(peer, 0)) {
        rc = 1;
        goto ConnectQPExit;
    }

    if (!isServer || (isServer && peer->NodeID > conf->getServerCount())) {
        /* Connection between server and client, create data channel. */
        DoubleQP = true;
        for (int i = 1; i < QP_NUMBER; i++) {
            if (!CreateQueuePair(peer, i)) {
                rc = 1;
                goto ConnectQPExit;
            }
        }
    }
	if (GidIndex >= 0) {
		rc = ibv_query_gid(ctx, Port, GidIndex, &MyGid);
        if (rc) {
            Debug::notifyError("could not get gid for port: %d, index: %d", Port, GidIndex);
            return false;
        }
	} else {
		memset(&MyGid, 0, sizeof(MyGid));
	}

	LocalMeta.rkey = mr->rkey;
	LocalMeta.qpNum[0] = peer->qp[0]->qp_num;
    LocalMeta.qpNum[1] = peer->qp[1]->qp_num;
    if (DoubleQP) {
        for (int i = 2; i < QP_NUMBER; i++)
        LocalMeta.qpNum[i] = peer->qp[i]->qp_num;
    }
	LocalMeta.lid = PortAttribute.lid;
	LocalMeta.RegisteredMemory = mm;

	memcpy(LocalMeta.gid, &MyGid, 16);
	if (DataSyncwithSocket(peer->sock, sizeof(ExchangeMeta), (char *)&LocalMeta, (char *)&RemoteMeta) < 0) {
		Debug::notifyError("failed to exchange connection data between sides");
        rc = 1;
        goto ConnectQPExit;
	}
	peer->rkey = RemoteMeta.rkey;
    for (int  i = 0; i < QP_NUMBER; i++)
	   peer->qpNum[i] = RemoteMeta.qpNum[i];
	peer->lid = RemoteMeta.lid;
	peer->RegisteredMemory = RemoteMeta.RegisteredMemory;
    Debug::notifyInfo("ConnectQP: localNode=%u peerNode=%u localQPN(ctrl)=%u remoteQPN(ctrl)=%u remoteRKey=0x%x remoteLid=0x%x",
        (unsigned)MyNodeID,
        (unsigned)peer->NodeID,
        (unsigned)peer->qp[CONTROL_QP_INDEX]->qp_num,
        (unsigned)peer->qpNum[CONTROL_QP_INDEX],
        (unsigned)peer->rkey,
        (unsigned)peer->lid);

	memcpy(peer->gid, RemoteMeta.gid, 16);
    N = (DoubleQP) ? QP_NUMBER : 2;
    for (int i = 0; i < N; i++) {

        /* modify the QP to init */
        ret = ModifyQPtoInit(peer->qp[i]);
        if (ret == false)  {
            Debug::notifyError("change QP state to INIT failed");
            rc = 1;
            goto ConnectQPExit;
        }
        /* modify the QP to RTR */
        ret = ModifyQPtoRTR(peer->qp[i], peer->qpNum[i], peer->lid, peer->gid);
        if (ret == false) {
            Debug::notifyError("failed to modify QP state to RTR");
            rc = 1;
            goto ConnectQPExit;
        }
        /* Modify the QP to RTS */
        ret = ModifyQPtoRTS(peer->qp[i]);
        if (ret == false) {
            Debug::notifyError("failed to modify QP state to RTR");
            rc = 1;
            goto ConnectQPExit;
        }
    }
    ConnectQPExit:
    if(rc != 0) {
    	return false;
    } else {
    	return true;
    }
}

int RdmaSocket::DataSyncwithSocket(int sock, int size, char *LocalData, char *RemoteData) {
    int rc;
    int readBytes = 0;
    int totalReadBytes = 0;
    rc = write(sock, LocalData, size);
    if (rc < size) {
	    Debug::notifyError("Failed writing data during sock_sync_data");
    } else {
	    rc = 0;
    }
    while (!rc && totalReadBytes < size) {
        readBytes = read(sock, RemoteData, size);
        if (readBytes > 0) {
        totalReadBytes += readBytes;
        } else {
	        rc = readBytes;
    }
    }
    return rc;
}

void RdmaSocket::SyncTool(uint16_t NodeID) {
    while (peers[NodeID] == NULL)
        usleep(100000);
    char bufferSend, bufferReceive;
    DataSyncwithSocket(peers[NodeID]->sock, 1, &bufferSend, &bufferReceive);
}

bool RdmaSocket::ResourcesDestroy() {
	bool rc = true;
    int i, j;
    for (i = 1; i <= ServerCount; i++) {
        if (peers[i] != NULL) {
            for (j = 0; j < QP_NUMBER; j++) {
                if (peers[i]->qp[j] != NULL) {
                    ibv_destroy_qp(peers[i]->qp[j]);
                    peers[i]->qp[j] = NULL;
                }
            }
        }
        free(peers[i]);
    }

    // for (i = 0; i < cqNum; i++) {
    //     if (cq[i]) {
    //         if (ibv_destroy_cq(cq[i])) {
    //                 Debug::notifyError("Failed to destroy CQ");
    //                 rc = 1;
    //             }
    //     }
    // }
    // free(cq);

    if (mr) {
        if (ibv_dereg_mr(mr)) {
            Debug::notifyError("Failed to deregister MR");
            rc = 1;
        }
    }

    if (pd) {
    	if (ibv_dealloc_pd(pd)) {
            Debug::notifyError("Failed to deallocate PD");
            rc = false;
        }
    }

    if (ctx) {
    	if (ibv_close_device(ctx)) {
            Debug::notifyError("failed to close device context");
            rc = false;
        }
    }
        
    return rc;
}

void RdmaSocket::RdmaListen() {
	struct sockaddr_in MyAddress;
	int sock;
	int on = 1;
    const int backlog = 1024;
	/* Socket Initialization */
	memset(&MyAddress,0,sizeof(MyAddress));
	MyAddress.sin_family=AF_INET;
	MyAddress.sin_addr.s_addr=INADDR_ANY;
	MyAddress.sin_port=htons(ServerPort);

	if ((sock = socket(PF_INET, SOCK_STREAM, 0)) < 0) {
		Debug::debugItem("Socket creation failed");
	}

   	if ((setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on))) < 0) {
        Debug::debugItem("Setsockopt failed");
    }

	if (bind(sock, (struct sockaddr*)&MyAddress, sizeof(struct sockaddr)) < 0) {
		Debug::debugItem("Bind failed with errnum ", errno);
	}

	listen(sock, backlog);
	listenSock = sock;
	
    Listener = thread(&RdmaSocket::RdmaAccept, this, sock);
    /* Connect to other servers. */
    ServerConnect();

}

void RdmaSocket::RdmaAccept(int sock) {
    struct sockaddr_in RemoteAddress;
    int fd;
    // struct timespec start, end;
    socklen_t sin_size = sizeof(struct sockaddr_in);
    while (isRunning && (fd = accept(sock, (struct sockaddr *)&RemoteAddress, &sin_size)) != -1)
    {
        Debug::notifyInfo("Discover New Client");
        PeerSockData *peer = (PeerSockData *)malloc(sizeof(PeerSockData));
        peer->sock = fd;
        peer->counter = 0;
        if (ConnectQueuePair(peer) == false) {
            Debug::notifyError("RDMA connect with error");
            close(fd);
            free(peer);
        } else {
            if (peer->NodeID >= 1000) {
                Debug::notifyError("Client NodeID %d exceeds peer table limit", peer->NodeID);
                close(fd);
                free(peer);
                continue;
            }
            peers[peer->NodeID] = peer;
            Debug::notifyInfo("Client %d Joined Us", peer->NodeID);
            /* Rdma Receive in Advance. */
            int posted_recv = 0;
            for (int i = 0; i < QPS_MAX_DEPTH; i++) {
                if (RdmaReceive(peer->NodeID, mm + peer->NodeID * 4096, 0)) {
                    posted_recv += 1;
                }
            }
            Debug::notifyInfo("Client %d pre-post recv on control QP: %d/%d", peer->NodeID, posted_recv, QPS_MAX_DEPTH);

            Debug::debugItem("Accepted to Node%d", peer->NodeID);
        }
    }
}

void RdmaSocket::ServerConnect() {
    int sock;
    auto id2ip = conf->getInstance();
    for (auto &kv : id2ip) {
        if (kv.first < MyNodeID) {
            sock = SocketConnect(kv.first);
            if (sock < 0) {
                Debug::notifyError("Socket connection failed to servers");
                return;
            }            PeerSockData *peer = (PeerSockData *)malloc(sizeof(PeerSockData));
            peer->sock = sock;
            peer->NodeID = kv.first;
            if (ConnectQueuePair(peer) == false) {
                Debug::notifyError("RDMA connect with error");
                return;
            } else {
                //SyncTool(peer->NodeID);
                peers[peer->NodeID] = peer;
                peer->counter = 0;
                for (int i = 0; i < QPS_MAX_DEPTH; i++) {
                    RdmaReceive(peer->NodeID, mm + peer->NodeID * 4096, 0);
                }
                Debug::debugItem("Finished Connecting to Node%d", peer->NodeID);
            }
        }
    }
}

int RdmaSocket::SocketConnect(uint16_t NodeID) {
	struct sockaddr_in RemoteAddress;
	int sock;
    struct timeval timeout = {30, 0};
	memset(&RemoteAddress, 0, sizeof(RemoteAddress));
	RemoteAddress.sin_family = AF_INET;
	inet_aton(conf->getIPbyID(NodeID).c_str(), (struct in_addr*)&RemoteAddress.sin_addr);
	RemoteAddress.sin_port = htons(ServerPort);
	if ((sock = socket(PF_INET, SOCK_STREAM, 0)) < 0) {
		Debug::notifyError("Socket Creation Failed");
		return -1;
	}
	int ret = setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, (const char*)&timeout, sizeof(timeout));
	if (ret < 0)
		Debug::notifyError("Set timeout failed!");

	int t = 3;
	while (t >= 0 && connect(sock, (struct sockaddr *)&RemoteAddress, sizeof(struct sockaddr)) < 0) {
		Debug::notifyError("Fail to connect to the server");
		t -= 1;
		usleep(1000000);
	}
	if (t < 0) {
		return -1;
	}
	return sock;
}

void RdmaSocket::RdmaConnect() {
	int sock;
	/* Connect to Node 1 firstly to get clientID. */
	sock = SocketConnect(1);
	if(sock < 0) {
		Debug::notifyError("Socket connection failed to server 1");
		return;
	}
	PeerSockData *peer = (PeerSockData *)malloc(sizeof(PeerSockData));
	peer->sock = sock;
	/* Add server's NodeID to the structure */
	peer->NodeID = 1;
	if (ConnectQueuePair(peer) == false) {
		Debug::notifyError("RDMA connect with error");
		return;
	} else {
		peers[peer->NodeID] = peer;
        peer->counter = 0;
        Debug::debugItem("Finished Connecting to Node%d", peer->NodeID);
	}
	/* Connect to other servers. */
	auto id2ip = conf->getInstance();
	for (auto &kv : id2ip) {
		if (kv.first != 1) {
			sock = SocketConnect(kv.first);
			if (sock < 0) {
				Debug::notifyError("Socket connection failed to servers");
				return;
			}
			PeerSockData *peer = (PeerSockData *)malloc(sizeof(PeerSockData));
			peer->sock = sock;
			peer->NodeID = kv.first;
			if (ConnectQueuePair(peer) == false) {
				Debug::notifyError("RDMA connect with error");
				return;
			} else {
                //SyncTool(peer->NodeID);
				peers[peer->NodeID] = peer;
                peer->counter = 0;
                Debug::debugItem("Finished Connecting to Node%d", peer->NodeID);
			}
		}
	}
}
/*
* Only responsible for data transfer, memory copy is not maintained here.
* Assume that data has already been copied.
*/
bool RdmaSocket::RdmaSend(uint16_t NodeID, uint64_t SourceBuffer, uint64_t BufferSize) {
    if (NodeID >= 1000) {
        Debug::notifyError("RdmaSend: invalid NodeID %d", NodeID);
        return false;
    }
    PeerSockData *peer = peers[NodeID];
    if (peer == NULL) {
        Debug::notifyError("RdmaSend: no RDMA peer for NodeID %d", NodeID);
        return false;
    }
    struct ibv_sge sg;
    struct ibv_send_wr wr;
    struct ibv_send_wr *wrBad;
     
    memset(&sg, 0, sizeof(sg));
    sg.addr   = (uintptr_t)SourceBuffer;
    sg.length = BufferSize;
    sg.lkey   = mr->lkey;
     
    memset(&wr, 0, sizeof(wr));
    wr.wr_id      = 0;
    wr.sg_list    = &sg;
    wr.num_sge    = 1;
    wr.imm_data   = htonl((uint32_t)MyNodeID);
    wr.opcode     = IBV_WR_SEND_WITH_IMM;
    wr.send_flags = IBV_SEND_SIGNALED;

    if (ibv_post_send(peer->qp[CONTROL_QP_INDEX], &wr, &wrBad)) {
        Debug::notifyError("Send with RDMA_SEND failed.");
        return false;
    }
	return true;
}

bool RdmaSocket::_RdmaBatchSend(uint16_t NodeID, uint64_t SourceBuffer, uint64_t BufferSize, int BatchSize) {
    if (NodeID >= 1000) {
        Debug::notifyError("_RdmaBatchSend: invalid NodeID %d", NodeID);
        return false;
    }
    PeerSockData *peer = peers[NodeID];
    if (peer == NULL) {
        Debug::notifyError("_RdmaBatchSend: no RDMA peer for NodeID %d", NodeID);
        return false;
    }
    struct ibv_sge sgl[MAX_POST_LIST];
    struct ibv_send_wr send_wr[MAX_POST_LIST];
    struct ibv_send_wr *wrBad;
    struct ibv_wc wc;
    int w_i;
    for (w_i = 0; w_i < BatchSize; w_i++) {
        if ((peer->counter & SIGNAL_BATCH) == 0 && peer->counter > 0 && !isServer) {
            PollCompletion(NodeID, 1, &wc);
        }
        sgl[w_i].addr   = (uintptr_t)SourceBuffer + w_i * 4096;
        sgl[w_i].length = BufferSize;
        sgl[w_i].lkey   = mr->lkey;
        send_wr[w_i].sg_list    = &sgl[w_i];
        send_wr[w_i].num_sge    = 1;
        send_wr[w_i].next       = (w_i == BatchSize - 1) ? NULL : &send_wr[w_i + 1];
        send_wr[w_i].wr_id      = 0;
        send_wr[w_i].imm_data   = (uint32_t)MyNodeID;
        send_wr[w_i].opcode     = IBV_WR_SEND_WITH_IMM;
        send_wr[w_i].send_flags = (peer->counter & SIGNAL_BATCH) == 0 ? IBV_SEND_SIGNALED : 0;
        //send_wr[w_i].send_flags |= IBV_SEND_INLINE;
        peer->counter += 1;
    }
    if (ibv_post_send(peer->qp[CONTROL_QP_INDEX], &send_wr[0], &wrBad)) {
        Debug::notifyError("Send with RDMA_SEND failed.");
        return false;
    }
    return true;
}

bool RdmaSocket::RdmaReceive(uint16_t NodeID, uint64_t SourceBuffer, uint64_t BufferSize) {
    if (NodeID >= 1000) {
        Debug::notifyError("RdmaReceive: invalid NodeID %d", NodeID);
        return false;
    }
    PeerSockData *peer = peers[NodeID];
    if (peer == NULL) {
        Debug::notifyError("RdmaReceive: no RDMA peer for NodeID %d", NodeID);
        return false;
    }
    struct ibv_sge sg;
    struct ibv_recv_wr wr;
    struct ibv_recv_wr *wrBad;
    int ret; 
    memset(&sg, 0, sizeof(sg));
    sg.addr   = (uintptr_t)SourceBuffer;
    sg.length = BufferSize;
    sg.lkey   = mr->lkey;
     
    memset(&wr, 0, sizeof(wr));
    wr.wr_id      = 0;
    if (BufferSize == 0) {
        wr.sg_list = NULL;
        wr.num_sge = 0;
    } else {
        wr.sg_list = &sg;
        wr.num_sge = 1;
    }
    ret = ibv_post_recv(peer->qp[CONTROL_QP_INDEX], &wr, &wrBad);
    if (ret) {
        Debug::notifyError("Receive with RDMA_RECV failed, ret = %d.", ret);
        return false;
    }
	return true;
}

bool RdmaSocket::_RdmaBatchReceive(uint16_t NodeID, uint64_t SourceBuffer, uint64_t BufferSize, int BatchSize) {
    if (NodeID >= 1000) {
        Debug::notifyError("_RdmaBatchReceive: invalid NodeID %d", NodeID);
        return false;
    }
    PeerSockData *peer = peers[NodeID];
    if (peer == NULL) {
        Debug::notifyError("_RdmaBatchReceive: no RDMA peer for NodeID %d", NodeID);
        return false;
    }
    struct ibv_recv_wr recv_wr[MAX_POST_LIST], *bad_recv_wr;
    struct ibv_sge sgl[MAX_POST_LIST];
    int w_i;
    int ret;
    for(w_i = 0; w_i < BatchSize; w_i++) {
        sgl[w_i].length = BufferSize;
        sgl[w_i].lkey = mr->lkey;
        sgl[w_i].addr = (uintptr_t)SourceBuffer + w_i * 4096;
        if (BufferSize == 0) {
            recv_wr[w_i].sg_list = NULL;
            recv_wr[w_i].num_sge = 0;
        } else {
            recv_wr[w_i].sg_list = &sgl[w_i];
            recv_wr[w_i].num_sge = 1;
        }
        recv_wr[w_i].next = (w_i == BatchSize - 1) ? NULL : &recv_wr[w_i + 1];
    }
    ret = ibv_post_recv(peer->qp[CONTROL_QP_INDEX], &recv_wr[0], &bad_recv_wr);
    if (ret) {
	Debug::notifyError("Receive with RDMA_RECV failed, ret = %d.", ret);
	return false;
    }
    return true;
}

bool RdmaSocket::RdmaRead(uint16_t NodeID, uint64_t SourceBuffer, uint64_t DesBuffer, uint64_t BufferSize, int TaskID) {
    if (NodeID >= 1000) {
        Debug::notifyError("RdmaRead: invalid NodeID %d", NodeID);
        return false;
    }
    PeerSockData *peer = peers[NodeID];
    if (peer == NULL) {
        Debug::notifyError("RdmaRead: no RDMA peer for NodeID %d", NodeID);
        return false;
    }
    struct ibv_sge sg;
    struct ibv_send_wr wr;
    struct ibv_send_wr *wrBad;
     
    memset(&sg, 0, sizeof(sg));
    sg.addr   = (uintptr_t)SourceBuffer;
    sg.length = BufferSize;
    sg.lkey   = mr->lkey;
     
    memset(&wr, 0, sizeof(wr));
    wr.wr_id      = 0;
    wr.sg_list    = &sg;
    wr.num_sge    = 1;
    wr.opcode     = IBV_WR_RDMA_READ;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.rdma.remote_addr = DesBuffer + peer->RegisteredMemory;
    wr.wr.rdma.rkey        = peer->rkey;
     
    if (ibv_post_send(peer->qp[TaskID], &wr, &wrBad)) {
        Debug::notifyError("Send with RDMA_READ failed.");
        return false;
    }
	return true;
}

bool RdmaSocket::_RdmaBatchRead(uint16_t NodeID, uint64_t SourceBuffer, uint64_t DesBuffer, uint64_t BufferSize, int BatchSize) {
    if (NodeID >= 1000) {
        Debug::notifyError("_RdmaBatchRead: invalid NodeID %d", NodeID);
        return false;
    }
    PeerSockData *peer = peers[NodeID];
    if (peer == NULL) {
        Debug::notifyError("_RdmaBatchRead: no RDMA peer for NodeID %d", NodeID);
        return false;
    }
    struct ibv_sge sgl[MAX_POST_LIST];
    struct ibv_send_wr send_wr[MAX_POST_LIST];
    struct ibv_send_wr *wrBad;
    struct ibv_wc wc;
    int w_i;
    for (w_i = 0; w_i < BatchSize; w_i++) {
        if ((peer->counter & SIGNAL_BATCH) == 0 && peer->counter > 0 && !isServer) {
            PollCompletion(NodeID, 1, &wc);
        }
        sgl[w_i].addr   = (uintptr_t)SourceBuffer + w_i * 8;
        sgl[w_i].length = BufferSize;
        sgl[w_i].lkey   = mr->lkey;
        send_wr[w_i].sg_list    = &sgl[w_i];
        send_wr[w_i].num_sge    = 1;
        send_wr[w_i].next       = (w_i == BatchSize - 1) ? NULL : &send_wr[w_i + 1];
        send_wr[w_i].wr_id      = 0;
        send_wr[w_i].opcode     = IBV_WR_RDMA_READ;
        send_wr[w_i].send_flags = (peer->counter & SIGNAL_BATCH) == 0 ? IBV_SEND_SIGNALED : 0;
        send_wr[w_i].wr.rdma.remote_addr = DesBuffer + peer->RegisteredMemory;// + w_i * 4096;
        send_wr[w_i].wr.rdma.rkey        = peer->rkey;
        peer->counter += 1;
    }

    if (ibv_post_send(peer->qp[0], &send_wr[0], &wrBad)) {
        Debug::notifyError("Send with RDMA_READ failed.");
        return false;
    }
    return true;
}

bool RdmaSocket::RemoteRead(uint64_t bufferSend, uint16_t NodeID, uint64_t bufferReceive, uint64_t size) {
    int data_qp = PickDataQpBySize(size);
    std::lock_guard<std::mutex> lock(dataPathMutex[data_qp - DATA_QP_SMALL_INDEX]);
    return InboundHamal(data_qp, bufferSend, NodeID, bufferReceive, size);
}

bool RdmaSocket::DataTransferWorker(int id) {
    TransferTask *task;
    while (true) {
        task = queue[id].PopPolling();
        if (task->OpType) {
            /* Write opration. */
            OutboundHamal(id, task->bufferSend, task->NodeID, task->bufferReceive, task->size);
        } else {
            InboundHamal(id, task->bufferSend, task->NodeID, task->bufferReceive, task->size);
        }
        delete task;
    }
}

bool RdmaSocket::InboundHamal(int TaskID, uint64_t bufferSend, uint16_t NodeID, uint64_t bufferReceive, uint64_t size) {
    uint64_t SendPoolSize = 1024 * 1024;
    int pool_idx = (TaskID == DATA_QP_LARGE_INDEX) ? 1 : 0;
    uint64_t SendPoolAddr = mm + 4 * 1024 + pool_idx * 1024 * 1024;
    uint64_t TotalSizeSend = 0; 
    uint64_t SendSize;
    struct ibv_wc wc;
    struct  timeval start, end;
    uint64_t diff;
    while (TotalSizeSend < size) {
        SendSize = (size - TotalSizeSend) >= SendPoolSize ? SendPoolSize : (size - TotalSizeSend);
        // _RdmaBatchRead(NodeID, 
        //                SendPoolAddr, 
        //                bufferReceive + TotalSizeSend, 
        //                SendSize, 
        //                1);
        gettimeofday(&start, NULL);
        if (!RdmaRead(NodeID, SendPoolAddr, bufferReceive + TotalSizeSend, SendSize, TaskID)) {
            Debug::notifyError("InboundHamal: RdmaRead failed (NodeID=%d, TaskID=%d, size=%lu)", NodeID, TaskID, SendSize);
            return false;
        }
        if (PollCompletion(NodeID, 1, &wc) < 0) {
            Debug::notifyError("InboundHamal: PollCompletion failed (NodeID=%d, TaskID=%d)", NodeID, TaskID);
            return false;
        }
        memcpy((void *)(bufferSend + TotalSizeSend), (void *)SendPoolAddr, SendSize);
        gettimeofday(&end, NULL);
        diff = 1000000 * (end.tv_sec - start.tv_sec) + end.tv_usec - start.tv_usec;
        ReadSize[pool_idx] += SendSize;
        ReadTimeCost[pool_idx] += diff;
        TotalSizeSend += SendSize;
    }
    return true;
}

bool RdmaSocket::RdmaWrite(uint16_t NodeID, uint64_t SourceBuffer, uint64_t DesBuffer, uint64_t BufferSize, uint32_t imm, int TaskID) {
    if (NodeID >= 1000) {
        Debug::notifyError("RdmaWrite: invalid NodeID %d", NodeID);
        return false;
    }
    struct ibv_sge sg;
    struct ibv_send_wr wr;
    struct ibv_send_wr *wrBad;
    PeerSockData *peer = peers[NodeID];
    if (peer == NULL) {
        Debug::notifyError("RdmaWrite: no RDMA peer for NodeID %d", NodeID);
        return false;
    }
    memset(&sg, 0, sizeof(sg));
    sg.addr   = (uintptr_t)SourceBuffer;
    sg.length = BufferSize;
    sg.lkey   = mr->lkey;
     
    memset(&wr, 0, sizeof(wr));
    wr.wr_id      = 0;
    wr.sg_list    = &sg;
    wr.num_sge    = 1;
    if((int32_t)imm == -1) {
        wr.opcode     = IBV_WR_RDMA_WRITE;
    } else {
        wr.opcode     = IBV_WR_RDMA_WRITE_WITH_IMM;
        wr.imm_data   = htonl(imm);
    }
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.rdma.remote_addr = DesBuffer + peer->RegisteredMemory;
    Debug::debugItem("Post RDMA_WRITE with remote address = %lx", wr.wr.rdma.remote_addr);
    wr.wr.rdma.rkey        = peer->rkey;
    if (ibv_post_send(peer->qp[TaskID], &wr, &wrBad)) {
        Debug::notifyError("Send with RDMA_WRITE(WITH_IMM) failed.");
        printf("%s\n", strerror(errno));
        return false;
    }
	return true;
}

bool RdmaSocket::RemoteWrite(uint64_t bufferSend, uint16_t NodeID, uint64_t bufferReceive, uint64_t size) {
    int data_qp = PickDataQpBySize(size);
    std::lock_guard<std::mutex> lock(dataPathMutex[data_qp - DATA_QP_SMALL_INDEX]);
    return OutboundHamal(data_qp, bufferSend, NodeID, bufferReceive, size);
}

bool RdmaSocket::OutboundHamal(int TaskID, uint64_t bufferSend, uint16_t NodeID, uint64_t bufferReceive, uint64_t size) {
    uint64_t SendPoolSize = 1024 * 1024;
    int pool_idx = (TaskID == DATA_QP_LARGE_INDEX) ? 1 : 0;
    uint64_t SendPoolAddr = mm + 4 * 1024 + pool_idx * 1024 * 1024;
    uint64_t TotalSizeSend = 0; 
    uint64_t SendSize;
    struct ibv_wc wc;
    struct  timeval start, end;
    uint64_t diff;
    while (TotalSizeSend < size) {
        SendSize = (size - TotalSizeSend) >= SendPoolSize ? SendPoolSize : (size - TotalSizeSend);
        // if(!isServer)
        //     octopus_log_wqe(MyNodeID, NodeID, SendSize);
        gettimeofday(&start,NULL);
        memcpy((void *)SendPoolAddr, (void *)(bufferSend + TotalSizeSend), SendSize);
        // _RdmaBatchWrite(NodeID, 
        //                SendPoolAddr, 
        //                bufferReceive + TotalSizeSend, 
        //                SendSize, 
        //                (uint32_t)-1,
        //                1);
        if (!RdmaWrite(NodeID, SendPoolAddr, bufferReceive + TotalSizeSend, SendSize, (uint32_t)-1, TaskID)) {
            Debug::notifyError("OutboundHamal: RdmaWrite failed (NodeID=%d, TaskID=%d, size=%lu)", NodeID, TaskID, SendSize);
            return false;
        }
        /* Shared CQ: drain completions until we get one from our target QP. */
        uint32_t expect_qpn = peers[NodeID]->qp[TaskID]->qp_num;
        do {
            if (PollCompletion(NodeID, 1, &wc) < 0) {
                Debug::notifyError("OutboundHamal: PollCompletion failed (NodeID=%d, TaskID=%d)", NodeID, TaskID);
                return false;
            }
        } while (wc.qp_num != expect_qpn);
        // if (SendSize > 32 * 1024) {
        //     /* Wait Until write finish, May help. */
        //     RdmaRead(NodeID, SendPoolAddr, bufferReceive + TotalSizeSend, 1);
        //     PollCompletion(NodeID, 1, &wc);
        // }
        gettimeofday(&end,NULL);
        diff = 1000000 * (end.tv_sec - start.tv_sec) + end.tv_usec - start.tv_usec;
        WriteSize[pool_idx] += SendSize;
        WriteTimeCost[pool_idx] += diff;
        /* RdmaWrite Testing. */
        if (WriteTest) {
            gettimeofday(&start,NULL);
            for (int i = 0; i < 10; i ++) {
                if (!RdmaWrite(NodeID, SendPoolAddr, bufferReceive, 1024 * 1024, (uint32_t)-1, TaskID)) {
                    Debug::notifyError("OutboundHamal: RdmaWrite failed during WriteTest");
                    return false;
                }
                if (PollCompletion(NodeID, 1, &wc) < 0) {
                    Debug::notifyError("OutboundHamal: PollCompletion failed during WriteTest");
                    return false;
                }
            }
            gettimeofday(&end,NULL);
            diff = 1000000 * (end.tv_sec - start.tv_sec) + end.tv_usec - start.tv_usec;
            printf("diff = %d, size = 10MB.\n", (int)diff);
            WriteTest = false;
        }
        Debug::debugItem("Source Addr = %lx, Des Addr = %lx, Size = %d", SendPoolAddr, bufferReceive + TotalSizeSend, SendSize);
        TotalSizeSend += SendSize;
    }
    return true;
}

bool RdmaSocket::_RdmaBatchWrite(uint16_t NodeID, uint64_t SourceBuffer, uint64_t DesBuffer, uint64_t BufferSize, uint32_t imm, int BatchSize) {
    struct ibv_sge sgl[MAX_POST_LIST];
    struct ibv_send_wr send_wr[MAX_POST_LIST];
    struct ibv_send_wr *wrBad;
    if (NodeID >= 1000) {
        Debug::notifyError("_RdmaBatchWrite: invalid NodeID %d", NodeID);
        return false;
    }
    PeerSockData *peer = peers[NodeID];
    if (peer == NULL) {
        Debug::notifyError("_RdmaBatchWrite: no RDMA peer for NodeID %d", NodeID);
        return false;
    }
    struct ibv_wc wc;
    int w_i;
    //printf("NodeID = %d, qp_num = %lx, cq = %lx, rkey = %x\n", NodeID, peer->qp->qp_num, peer->cq, peer->rkey);
    for (w_i = 0; w_i < BatchSize; w_i++) {
        // if(!isServer)
        //     octopus_log_wqe(MyNodeID, NodeID, BufferSize);
        if ((peer->counter & SIGNAL_BATCH) == 0 && peer->counter > 0 && !isServer) {
            PollCompletion(NodeID, 1, &wc);
        }
        sgl[w_i].addr   = (uintptr_t)SourceBuffer + w_i * 4096;
        sgl[w_i].length = BufferSize;
        sgl[w_i].lkey   = mr->lkey;
        send_wr[w_i].sg_list    = &sgl[w_i];
        send_wr[w_i].num_sge    = 1;
        send_wr[w_i].next       = (w_i == BatchSize - 1) ? NULL : &send_wr[w_i + 1];
        if ((int32_t)imm == 0) {
             send_wr[w_i].opcode = IBV_WR_RDMA_WRITE;
         } else {
             send_wr[w_i].opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
             send_wr[w_i].imm_data = imm;
         }
        send_wr[w_i].wr_id      = 0;
        
        send_wr[w_i].send_flags = 0;
            send_wr[w_i].send_flags = (peer->counter & SIGNAL_BATCH) == 0 ? IBV_SEND_SIGNALED : 0;
        
        //send_wr[w_i].send_flags |= IBV_SEND_INLINE;
        send_wr[w_i].wr.rdma.remote_addr = DesBuffer + peer->RegisteredMemory + w_i * 4096;
        Debug::debugItem("remote address = %lx, Counter = %d, imm = %lx", send_wr[w_i].wr.rdma.remote_addr, peer->counter, imm);
        send_wr[w_i].wr.rdma.rkey        = peer->rkey;
        peer->counter += 1;
    }

    if (ibv_post_send(peer->qp[CONTROL_QP_INDEX], &send_wr[0], &wrBad)) {
        Debug::notifyError("Send with RDMA_WRITE(WITH_IMM) failed.");
        printf("%s\n", strerror(errno));
        return false;
    }
    return true;
}

bool RdmaSocket::RdmaFetchAndAdd(uint16_t NodeID, uint64_t SourceBuffer, uint64_t DesBuffer, uint64_t Add) {
    if (NodeID >= 1000) {
        Debug::notifyError("RdmaFetchAndAdd: invalid NodeID %d", NodeID);
        return false;
    }
    //assert(peers[NodeID]);
    struct ibv_sge sg;
    struct ibv_send_wr wr;
    struct ibv_send_wr *wrBad;
    PeerSockData *peer = peers[NodeID];
    memset(&sg, 0, sizeof(sg));
    sg.addr   = (uintptr_t)SourceBuffer;
    sg.length = 8;
    sg.lkey   = mr->lkey;
     
    memset(&wr, 0, sizeof(wr));
    wr.wr_id      = 0;
    wr.sg_list    = &sg;
    wr.num_sge    = 1;
    wr.opcode     = IBV_WR_ATOMIC_FETCH_AND_ADD;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.atomic.remote_addr = DesBuffer + peer->RegisteredMemory;
    wr.wr.atomic.rkey        = peer->rkey;
    wr.wr.atomic.compare_add = Add; /* value to be added to the remote address content */
     
    if (ibv_post_send(peer->qp[0], &wr, &wrBad)) {
        Debug::notifyError("Send with ATOMIC_FETCH_AND_ADD failed.");
        return false;
    }
	return true;
}

bool RdmaSocket::RdmaCompareAndSwap(uint16_t NodeID, uint64_t SourceBuffer, uint64_t DesBuffer, uint64_t Compare, uint64_t Swap) {
    if (NodeID >= 1000) {
        Debug::notifyError("RdmaCompareAndSwap: invalid NodeID %d", NodeID);
        return false;
    }
    //assert(peers[NodeID]);
    struct ibv_sge sg;
    struct ibv_send_wr wr;
    struct ibv_send_wr *wrBad;
    PeerSockData *peer = peers[NodeID];
    memset(&sg, 0, sizeof(sg));
    sg.addr   = (uintptr_t)SourceBuffer;
    sg.length = 8;
    sg.lkey   = mr->lkey;
     
    memset(&wr, 0, sizeof(wr));
    wr.wr_id      = 0;
    wr.sg_list    = &sg;
    wr.num_sge    = 1;
    wr.opcode     = IBV_WR_ATOMIC_CMP_AND_SWP;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.atomic.remote_addr = DesBuffer + peer->RegisteredMemory;
    wr.wr.atomic.rkey        = peer->rkey;
    wr.wr.atomic.compare_add = Compare; /* expected value in remote address */
    wr.wr.atomic.swap        = Swap; /* the value that remote address will be assigned to */
     
    if (ibv_post_send(peer->qp[0], &wr, &wrBad)) {
        Debug::notifyError("Send with ATOMIC_CMP_AND_SWP failed.");
        return false;
    }
    return true;
}

int RdmaSocket::PollCompletion(uint16_t NodeID, int PollNumber, struct ibv_wc *wc) {
    if (!isRunning) {
        return -1;
    }
    if (NodeID >= 1000 || peers[NodeID] == NULL || peers[NodeID]->cq == NULL) {
        Debug::notifyError("PollCompletion: invalid peer/cq (NodeID=%d)", NodeID);
        return -1;
    }
    int count = 0;

    while (count < PollNumber) {
        if (!isRunning) {
            return -1;
        }
        int rc = ibv_poll_cq(peers[NodeID]->cq, 1, wc);
        if (rc < 0) {
            Debug::notifyError("PollCompletion: ibv_poll_cq failed (NodeID=%d, rc=%d)", NodeID, rc);
            return -1;
        }
        count += rc;
    }
     
    /* Check Completion Status */
    if (wc->status != IBV_WC_SUCCESS) {
        Debug::notifyError("Failed status %s (%d) for wr_id %d", 
            ibv_wc_status_str(wc->status),
            wc->status, (int)wc->wr_id);
        return -1;
    }
    Debug::debugItem("Find New Completion Message");
    return count;
}

int RdmaSocket::PollWithCQ(int cqPtr, int PollNumber, struct ibv_wc *wc) {
    if (!isRunning) {
        return -1;
    }
    int count = 0;

    while (count < PollNumber) {
        if (!isRunning) {
            return -1;
        }
        int rc = ibv_poll_cq(cq[cqPtr], 1, wc);
        if (rc < 0) {
            Debug::notifyError("PollWithCQ: ibv_poll_cq failed (cqPtr=%d, rc=%d)", cqPtr, rc);
            return -1;
        }
        count += rc;
    }

    /* Check Completion Status */
    if (wc->status != IBV_WC_SUCCESS) {
        Debug::notifyError("Failed status %s (%d) for wr_id %d", 
            ibv_wc_status_str(wc->status),
            wc->status, (int)wc->wr_id);
        return -1;
    }
    Debug::debugItem("Find New Completion Message");
    return count;
}

int RdmaSocket::PollOnce(int cqPtr, int PollNumber, struct ibv_wc *wc) {
    if (!isRunning) {
        return -1;
    }
    int count = ibv_poll_cq(cq[cqPtr], PollNumber, wc);
    if (count == 0) {
        return 0;
    } else if (count < 0) {
	Debug::notifyError("Failure occurred when reading work completions, ret = %d", count);
	return -1;
    }
    if (wc->status != IBV_WC_SUCCESS) {
	Debug::notifyError("Failed status %s (%d) for wr_id %d",
            ibv_wc_status_str(wc->status),
            wc->status, (int)wc->wr_id);
        return -1;
    } else {
        return count;
    }
}

int RdmaSocket::getCQCount() {
    return cqNum;
}

uint16_t RdmaSocket::getNodeID() {
    return MyNodeID;
}
void RdmaSocket::WaitClientConnection(uint16_t NodeID) {
    while (isRunning && peers[NodeID] == NULL) {
        usleep(1);
    }
}

PeerSockData* RdmaSocket::getPeerInformation(uint16_t NodeID) {
    return peers[NodeID];
}

void RdmaSocket::RdmaQueryQueuePair(uint16_t NodeID) {
    struct ibv_qp_attr attr;
    struct ibv_qp_init_attr init_attr;
    ibv_query_qp(peers[NodeID]->qp[0], &attr, IBV_QP_STATE, &init_attr);
    switch (attr.qp_state) {
        case IBV_QPS_RESET:
            printf("Client %d with QP state: IBV_QPS_RESET\n", NodeID);
            break;
        case IBV_QPS_INIT:
            printf("Client %d with QP state: IBV_QPS_INIT\n", NodeID);
            break;
        case IBV_QPS_RTR:
            printf("Client %d with QP state: IBV_QPS_RTR\n", NodeID);
            break;
        case IBV_QPS_RTS:
            printf("Client %d with QP state: IBV_QPS_RTS\n", NodeID);
            break;
        case IBV_QPS_SQD:
            printf("Client %d with QP state: IBV_QPS_SQD\n", NodeID);
            break;
        case IBV_QPS_SQE:
            printf("Client %d with QP state: IBV_QPS_SQE\n", NodeID);
            break;
        case IBV_QPS_ERR:
            printf("Client %d with QP state: IBV_QPS_ERR\n", NodeID);
            break;
        case IBV_QPS_UNKNOWN:
            printf("Client %d with QP state: IBV_QPS_UNKNOWN\n", NodeID);
            break;
    }
}
