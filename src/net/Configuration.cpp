/***********************************************************************
* 
* 
* Tsinghua Univ, 2016
*
***********************************************************************/

#include "Configuration.hpp"
#include "debug.hpp"

using namespace std;

Configuration::Configuration() {
	ServerCount = 0;
	read_xml("../conf.xml", pt);
	ptree child = pt.get_child("address");
	for(BOOST_AUTO(pos,child.begin()); pos != child.end(); ++pos) 
    {  
        id2ip[(uint16_t)(pos->second.get<int>("id"))] = pos->second.get<string>("ip");
        ip2id[pos->second.get<string>("ip")] = pos->second.get<int>("id");
        ServerCount += 1;
    }
}

Configuration::~Configuration() {
	Debug::notifyInfo("Configuration is closed successfully.");
}

string Configuration::getIPbyID(uint16_t id) {
	auto it = id2ip.find(id);
	if (it == id2ip.end())
		return string();
	return it->second;
}

uint16_t Configuration::getIDbyIP(string ip) {
	auto it = ip2id.find(ip);
	if (it == ip2id.end())
		return 0;
	return it->second;
}

unordered_map<uint16_t, string> Configuration::getInstance() {
	return id2ip;
}

int Configuration::getServerCount() {
	return ServerCount;
}
