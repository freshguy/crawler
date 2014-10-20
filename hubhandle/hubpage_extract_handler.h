// Copyright (c) 2014 Guanjia Inc. All rights reserved.
// Author: wangjuntao@guanjia.com (Wang Juntao)

#ifndef GUANJIA_CRAWLER_HUBHANDLE_HUBPAGE_EXTRACT_HANDLER_H_
#define GUANJIA_CRAWLER_HUBHANDLE_HUBPAGE_EXTRACT_HANDLER_H_

#include <string>
#include <vector>
#include <deque>
#include "base/logging.h"
#include "base/flags.h"
#include "base/thread.h"
#include "base/thrift.h"
#include "base/mutex.h"
#include "base/scoped_ptr.h"
#include "util/mysql_client2/connection.h"
#include "util/mysql_client2/result.h"
#include "util/mysql_client2/query.h"
#include "guanjia/crawler/proto/gen-cpp/CrawlerFetcherServlet.h"
#include "guanjia/crawler/proto/gen-cpp/CrawlerServlet.h"

using boost::shared_ptr;  // NOLINT
using util_mysql::kMysqlIllegalValue;  // NOLINT

#define EXIST_CNT_THRESHHOLD 60

/// for mysql
DECLARE_string(mysql_host);
DECLARE_int32(mysql_port);
DECLARE_string(mysql_db);
DECLARE_string(mysql_user);
DECLARE_string(mysql_password);
DECLARE_string(mysql_socket);

DECLARE_int32(base_update_interval);
DECLARE_int32(get_hubpage_cnt_per_round);

DECLARE_int32(page_fetche_interval);

DECLARE_string(crawler_fetcher_host_name);
DECLARE_int32(crawler_fetcher_host_port);

namespace guanjia {
namespace crawler {

class HubPageExtractThread;
class HubPageExtractHandler {
 public:
  HubPageExtractHandler();
  ~HubPageExtractHandler();

  /// getter and setter
 public:
  boost::shared_ptr<util_mysql::MysqlQuery>& get_mysql_query() {
    return mysql_query_;
  }
  
  boost::shared_ptr< ::base::ThriftClient<
    guanjia::crawler::CrawlerFetcherServletClient> >& get_crawler_fetcher_servlet_client() {
    return crawler_fetcher_servlet_client_;
  }

 private:
  scoped_ptr<HubPageExtractThread> hubpage_extract_thread_;
  
  /// for mysql client
  boost::shared_ptr<util_mysql::MysqlConnection> mysql_connection_;
  boost::shared_ptr<util_mysql::MysqlQuery> mysql_query_;

  /// Client for CrawlerFetcherServer
  boost::shared_ptr< ::base::ThriftClient<
    guanjia::crawler::CrawlerFetcherServletClient> > crawler_fetcher_servlet_client_;
};

class HubPageExtractThread : public base::Thread {
 public:
  explicit HubPageExtractThread(
        HubPageExtractHandler* const hubpage_extract_handler);
  virtual ~HubPageExtractThread();

  bool HandleHubPageExtract(
        util_mysql::MysqlResult* const hubInfosMysqlResult);

 protected:
  virtual void Run();
 
 private:
  HubPageExtractHandler* hubpage_extract_handler_;

  DISALLOW_COPY_AND_ASSIGN(HubPageExtractThread);
};
}  // namespace crawler
}  // namespace guanjia
#endif  // GUANJIA_CRAWLER_HUBHANDLE_HUBPAGE_EXTRACT_HANDLER_H_
