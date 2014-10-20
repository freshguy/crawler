// Copyright (c) 2014 Guanjia Inc. All rights reserved.
// Author: wangjuntao@guanjia.com (Wang Juntao)

#ifndef GUANJIA_CRAWLER_FETCHER_CRAWLER_FETCHER_SERVER_H_
#define GUANJIA_CRAWLER_FETCHER_CRAWLER_FETCHER_SERVER_H_

#include "base/logging.h"
#include "base/flags.h"
#include "news_search/base/general_server/general_server.h"
#include "guanjia/crawler/fetcher/crawler_fetcher_servlet.h"

#define CRAWLER_FETCHER_MONITOR_VALIDATE_KEY ("Crawler Fetcher Server")

namespace guanjia {
namespace crawler {

class CrawlerFetcherServer {
 public:
  CrawlerFetcherServer();
  virtual ~CrawlerFetcherServer();

  void Serve();

 private:
  void ValidateMonitorFetcherServer(
        babysitter::MonitorResult* result);

 private:
  news::GeneralServer* general_server_;
};
}  // namespace crawler
}  // namespace guanjia
#endif  // GUANJIA_CRAWLER_FETCHER_CRAWLER_FETCHER_SERVER_H_
