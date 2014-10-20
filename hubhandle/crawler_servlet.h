// Copyright (c) 2014 Guanjia Inc. All rights reserved.
// Author: wangjuntao@guanjia.com (Wang Juntao)

#ifndef GUANJIA_CRAWLER_HUBHANDLE_CRAWLER_SERVLET_H_
#define GUANJIA_CRAWLER_HUBHANDLE_CRAWLER_SERVLET_H_

#include <string>
#include <vector>
#include "base/flags.h"
#include "base/logging.h"
#include "base/thrift.h"
#include "guanjia/crawler/proto/gen-cpp/CrawlerServlet.h"
#include "guanjia/crawler/hubhandle/hubpage_extract_handler.h"

using boost::shared_ptr;  // NOLINT

namespace guanjia {
namespace crawler {

class CrawlerServletHandler : virtual public CrawlerServletIf {
 public:
  CrawlerServletHandler();
  
  virtual ~CrawlerServletHandler();

 private:
  HubPageExtractHandler* hubpage_extract_handler_;
};
}  // namespace crawler
}  // namespace guanjia
#endif  // GUANJIA_CRAWLER_HUBHANDLE_CRAWLER_SERVLET_H_
