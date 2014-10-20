// Copyright (c) 2014 Guanjia Inc. All rights reserved.
// Author: wangjuntao@guanjia.com (Wang Juntao)

#include "guanjia/crawler/hubhandle/crawler_servlet.h"

namespace guanjia {
namespace crawler {

CrawlerServletHandler::CrawlerServletHandler() {
  hubpage_extract_handler_ = NULL;
  hubpage_extract_handler_ = new HubPageExtractHandler;
  CHECK(hubpage_extract_handler_)
    << "Null pointer for hubpage_extract_handler_ "
    << "when CrawlerServletHandler construct";
}

CrawlerServletHandler::~CrawlerServletHandler() {
  if (hubpage_extract_handler_) {
    delete hubpage_extract_handler_;
  }
  hubpage_extract_handler_ = NULL;
}

}  // namespace crawler
}  // namespace guanjia
