// Copyright (c) 2014 Guanjia Inc. All rights reserved.
// Author: wangjuntao@guanjia.com (Wang Juntao)

#ifndef GUANJIA_CRAWLER_FETCHER_CRAWLER_FETCHER_HANDLER_H_
#define GUANJIA_CRAWLER_FETCHER_CRAWLER_FETCHER_HANDLER_H_

#include <string>
#include <vector>
#include <deque>
#include "base/basictypes.h"
#include "base/logging.h"
#include "base/flags.h"
#include "base/thread.h"
#include "base/thrift.h"
#include "base/mutex.h"
#include "base/scoped_ptr.h"
#include "file/file.h"
#include "util/mysql_client2/connection.h"
#include "util/mysql_client2/result.h"
#include "util/mysql_client2/query.h"
#include "guanjia/crawler/proto/gen-cpp/CrawlerFetcherServlet.h"
#include "guanjia/crawler/parser/public/page_normalizer.h"
#include "guanjia/remote_file_serving/proto/gen-cpp/RemoteFileHandleServlet.h"

#define FEED_ID_PREFIX ("FEED_")

using boost::shared_ptr;  // NOLINT
using util_mysql::kMysqlIllegalValue;  // NOLINT
using file::File;  // NOLINT

DECLARE_int32(url_fetch_base_interval);
DECLARE_int32(max_increment_fetch_interval);

DECLARE_string(author_userids_file);

/// for RemoteFileHandlerServlet
DECLARE_string(small_file_server_host);
DECLARE_int32(small_file_server_port);

/// for mysql
DECLARE_string(mysql_host);
DECLARE_int32(mysql_port);
DECLARE_string(mysql_db);
DECLARE_string(mysql_user);
DECLARE_string(mysql_password);
DECLARE_string(mysql_socket);

/// 
DECLARE_string(crawl_docs_dir_prefix);

namespace guanjia {
namespace crawler {

class GrabTaskHandleThread;
class CrawlerFetcherHandler {
 public:
  CrawlerFetcherHandler();
  virtual ~CrawlerFetcherHandler();

 public:
  bool PushGrabTaskInternal(
        PushGrabTaskResponse& _return,
        const PushGrabTaskRequest& pushGrabTaskRequest);

 private:
  GrabTaskHandleThread* grabTaskHandleThread_;

 public:
  /// Data queue
  std::deque<CrawlDocInfo> grabTaskQueue_;
  base::Mutex grabTaskQueueMutex_;
  base::CondVar grabTaskQueueCondVar_;

 private:
  DISALLOW_COPY_AND_ASSIGN(CrawlerFetcherHandler);
};

class GrabTaskHandleThread : public base::Thread {
 public:
  explicit GrabTaskHandleThread(
        CrawlerFetcherHandler* const crawlerFetcherHandler);
  virtual ~GrabTaskHandleThread();

  virtual void Run();
  
 private:
  CrawlerFetcherHandler* crawlerFetcherHandler_;
  Magick::Image profileBackGroundImg_;
  std::vector<std::string> authorUserIds_;
};

class FetchDocsThread : public base::Thread {
 public:
  FetchDocsThread(const std::vector<CrawlDocInfo>& crawlDocInfos,
        const int& threadIndex,
        Magick::Image* const profileBackGroundImg,
        std::vector<std::string>* const authorUserIds);

  virtual ~FetchDocsThread();

  virtual void Run();

 private:
  std::vector<CrawlDocInfo> crawlDocInfos_;
  int threadIndex_;

 private:
  boost::shared_ptr< ::base::ThriftClient<
    ::guanjia::RemoteFileHandleServletClient> > remote_file_server_client_;

  Magick::Image* profileBackGroundImg_;
  std::vector<std::string>* authorUserIds_;
  
 private:
  /// for mysql client
  boost::shared_ptr<util_mysql::MysqlConnection> mysql_connection_;
  boost::shared_ptr<util_mysql::MysqlQuery> mysql_query_;
};
}  // namespace crawler
}  // namespace guanjia
#endif  // GUANJIA_CRAWLER_FETCHER_CRAWLER_FETCHER_HANDLER_H_
