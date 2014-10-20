// Copyright (c) 2014 Guanjia Inc. All rights reserved.
// Author: wangjuntao@guanjia.com (Wang Juntao)

#ifndef GUANJIA_CRAWLER_FETCHER_CRAWLER_FETCHER_SERVLET_H_
#define GUANJIA_CRAWLER_FETCHER_CRAWLER_FETCHER_SERVLET_H_

#include <string>
#include <vector>
#include "base/logging.h"
#include "base/flags.h"
#include "base/thrift.h"
#include "guanjia/crawler/proto/gen-cpp/CrawlerFetcherServlet.h"
#include "guanjia/crawler/fetcher/crawler_fetcher_handler.h"

using boost::shared_ptr;  // NOLINT

namespace guanjia {
namespace crawler {

class CrawlerFetcherServletHandler : virtual public CrawlerFetcherServletIf {
 public:
  CrawlerFetcherServletHandler();
  virtual ~CrawlerFetcherServletHandler();


  void PushGrabTask(
        PushGrabTaskResponse& _return,
        const PushGrabTaskRequest& pushGrabTaskRequest);

 private:
  CrawlerFetcherHandler* crawler_fetcher_handler_;
};
}  // namespace crawler
}  // namespace guanjia
#endif  // GUANJIA_CRAWLER_FETCHER_CRAWLER_FETCHER_SERVLET_H_
