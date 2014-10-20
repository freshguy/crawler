// Copyright (c) 2014 Guanjia Inc. All rights reserved.
// Author: wangjuntao@guanjia.com (Wang Juntao)

#include "guanjia/crawler/fetcher/crawler_fetcher_servlet.h"

namespace guanjia {
namespace crawler {

CrawlerFetcherServletHandler::CrawlerFetcherServletHandler() {
  crawler_fetcher_handler_ = NULL;
  crawler_fetcher_handler_ = new CrawlerFetcherHandler;
  CHECK(crawler_fetcher_handler_)
    << "Null pointer for crawler_fetcher_handler_ "
    << "when CrawlerFetcherServletHandler constrcut";
}

CrawlerFetcherServletHandler::~CrawlerFetcherServletHandler() {
  if (crawler_fetcher_handler_) {
    delete crawler_fetcher_handler_;
  }
  crawler_fetcher_handler_ = NULL;
}

void CrawlerFetcherServletHandler::PushGrabTask(
      PushGrabTaskResponse& _return,
      const PushGrabTaskRequest& pushGrabTaskRequest) {
  VLOG(4) << "[RPC] PushGrabTask...";
  if (!crawler_fetcher_handler_->PushGrabTaskInternal(
          _return, pushGrabTaskRequest)) {
    LOG(ERROR) << "PushGrabTask error";
  }
}
}  // namespace crawler
}  // namespace guanjia
