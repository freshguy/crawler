// Copyright 2014 Guanjia Inc. All Rights Reserved.
// Author: wangjuntao@guanjia.com (Wang Juntao)

namespace cpp guanjia.crawler

enum DocHandleStatus {
  UNDEFINED = 0,
  DONE = 1,
  CRAWL_FAILED = 2,
  PARSE_FAILED = 3,
}

struct CrawlDocInfo {
  1: string requestUrl,
  2: optional string title,
  3: optional string docId,
  4: optional string openId,
  5: optional i64 urlHashId,
}
