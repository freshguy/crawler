// Copyright 2014 Guanjia Inc. All Rights Reserved.
// Author: wangjuntao@guanjia.com (Wang Juntao)

include "guanjia/common_proto/common_info.thrift"
include "guanjia/crawler/proto/crawler_base.thrift"

namespace cpp guanjia.crawler

struct PushGrabTaskResponse {
  1: common_info.ReturnCode returnCode,
}

struct PushGrabTaskRequest {
  1: list<crawler_base.CrawlDocInfo> crawlDocInfos;
}


service CrawlerFetcherServlet {
  PushGrabTaskResponse PushGrabTask(
        1: PushGrabTaskRequest pushGrabTaskRequest); 
}
