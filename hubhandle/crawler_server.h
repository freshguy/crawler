// Copyright (c) 2014 Guanjia Inc. All rights reserved.
// Author: wangjuntao@guanjia.com (Wang Juntao)

#ifndef GUANJIA_CRAWLER_HUBHANDLE_CRAWLER_SERVER_H_
#define GUANJIA_CRAWLER_HUBHANDLE_CRAWLER_SERVER_H_

#include "base/logging.h"
#include "base/flags.h"
#include "news_search/base/general_server/general_server.h"
#include "guanjia/crawler/hubhandle/crawler_servlet.h"

#define CRAWLER_HUBHANDLE_MONITOR_VALIDATE_KEY ("Hubhandle Crawler Server")

namespace guanjia {
namespace crawler {

class CrawlerHubHandleServer {
 public:
  CrawlerHubHandleServer();
  virtual ~CrawlerHubHandleServer();

  void Serve();

 private:
  void ValidateMonitorCrawlerHubHandleServer(
        babysitter::MonitorResult* result);

 private:
  news::GeneralServer* general_server_;
};
}  // namespace crawler
}  // namespace guajia
#endif  // GUANJIA_CRAWLER_HUBHANDLE_CRAWLER_SERVER_H_
