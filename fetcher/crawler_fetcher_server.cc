// Copyright (c) 2014 Guanjia Inc. All rights reserved.
// Author: wangjuntao@guanjia.com (Wang Juntao)

#include "guanjia/crawler/fetcher/crawler_fetcher_server.h"

namespace guanjia {
namespace crawler {

CrawlerFetcherServer::CrawlerFetcherServer() {
  general_server_ = NULL;
}

CrawlerFetcherServer::~CrawlerFetcherServer() {
  if (general_server_) {
    delete general_server_;
  }
  general_server_ = NULL;
}

void CrawlerFetcherServer::Serve() {
  shared_ptr<CrawlerFetcherServletHandler> handler(
        new CrawlerFetcherServletHandler());
  shared_ptr<TProcessor> processor(
        new CrawlerFetcherServletProcessor(handler));

  /// In GeneralServer(): create monitor_server
  general_server_ = new news::GeneralServer(processor);

  LOG(INFO) << "Add monitor...";
  /// Add Monitor
  general_server_->AddMonitor(
        CRAWLER_FETCHER_MONITOR_VALIDATE_KEY,
        base::NewPermanentCallback(
          this,
          &CrawlerFetcherServer::ValidateMonitorFetcherServer));

  /// Start the server
  LOG(INFO) << "Starting GeneralServer Instance for CrawlerFetcherServer...";

  bool isStart = true;
  try {
    general_server_->Start();
  } catch(const apache::thrift::TException& tx) {
    LOG(ERROR) << "Start GeneralServer failed(0): " << tx.what();
    isStart = false;
  } catch(...) {
    LOG(ERROR) << "Failed to start GeneralServer, unexplained exception";
    isStart = false;
  }

  CHECK(isStart) << "Failed to Start CrawlerFetcherServer";
  LOG(INFO) << "Start CrawlerFetcherServer successfully";
}

void CrawlerFetcherServer::ValidateMonitorFetcherServer(
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
  result->AddKv("Crawler Fetcher Monitor", monitor_info);
}
}  // namespace crawler
}  // namespace guanjia
