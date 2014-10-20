// Copyright (c) 2014 Guanjia Inc. All rights reserved.
// Author: wangjuntao@guanjia.com (Wang Juntao)

#include "guanjia/crawler/hubhandle/crawler_server.h"

namespace guanjia {
namespace crawler {

  
CrawlerHubHandleServer::CrawlerHubHandleServer() {
  general_server_ = NULL;
}

CrawlerHubHandleServer::~CrawlerHubHandleServer() {
  if (general_server_) {
    delete general_server_;
  }
  general_server_ = NULL;
}

void CrawlerHubHandleServer::Serve() {
  shared_ptr<CrawlerServletHandler> handler(
        new CrawlerServletHandler());
  shared_ptr<TProcessor> processor(
        new CrawlerServletProcessor(handler));

  /// In GeneralServer(): create monitor_server
  general_server_ = new news::GeneralServer(processor);
  
  /// Add Monitor
  LOG(INFO) << "Add Monitor...";
  general_server_->AddMonitor(
        CRAWLER_HUBHANDLE_MONITOR_VALIDATE_KEY,
        base::NewPermanentCallback(
          this,
          &CrawlerHubHandleServer::ValidateMonitorCrawlerHubHandleServer));

  /// Start the server
  LOG(INFO) << "Starting GeneralServer Instance for CrawlerHubHandleServer...";
  bool isStart = true;
  try {
    general_server_->Start();
  } catch(const ::apache::thrift::TException& err) {
    LOG(ERROR) << "Start GeneralServer failed(0): " << err.what();
    isStart = false;
  } catch(...) {
    LOG(ERROR) << "Failed to start GeneralServer, unexplained exception";
    isStart = false;
  }

  CHECK(isStart) << "Failed to Start CrawlerHubHandleServer";
  LOG(INFO) << "Start CrawlerHubHandleServer successfully";
}

void CrawlerHubHandleServer::ValidateMonitorCrawlerHubHandleServer(
      babysitter::MonitorResult* result) {
  std::string monitor_info;
#if 0
  if (false == handler_->IsHealthy) {
    monitor_info = "Bad";
  } else {
    monitor_info = "Good";
  }
#endif
  monitor_info = "Everything is OK";
  result->AddKv("Crawler HubHandle Server Monitor", monitor_info);
}
}  // namespace crawler
}  // namespace guanjia
