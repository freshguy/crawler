// Copyright (c) 2014 Guanjia Inc. All rights reserved.
// Author: wangjuntao@guanjia.com (Wang Juntao)

#include "file/simple_line_reader.h"
#include "guanjia/remote_file_serving/public/utility.h"
#include "guanjia/crawler/handler/curl_fetcher.h"
#include "guanjia/crawler/fetcher/crawler_fetcher_handler.h"

DEFINE_int32(max_fetcher_thread_cnt, 20,
      "The max fetcher thread count");

DEFINE_int32(max_grab_cnt_per_thread, 1000,
      "The max grab count per thread");

DEFINE_int32(min_grab_cnt_per_thread, 30,
      "The min grab count per thread");

DEFINE_int32(url_fetch_base_interval, 1,
      "The url page fetch interval");
DEFINE_int32(max_increment_fetch_interval, 5,
      "The max increment fetch interval");

DEFINE_string(profile_background_file,
      "../../static/fetcher/profile_background.png",
      "The profile background file");

DEFINE_string(author_userids_file,
      "../../static/fetcher/author_userids_list",
      "The author userids file");

/// for RemoteFileHandlerServlet
DEFINE_string(small_file_server_host, "219.232.227.247",
      "The small file server host");
DEFINE_int32(small_file_server_port, 9320,
      "The small file server port");

/// for mysql
DEFINE_string(mysql_host, "219.232.227.247",
      "The mysql-server host");
DEFINE_int32(mysql_port, 3306,
      "The mysql-server port");
DEFINE_string(mysql_db, "crawler",
      "The mysql database");
DEFINE_string(mysql_user, "root",
      "The mysql access user");
DEFINE_string(mysql_password, "123456",
      "The mysql accesser password");
DEFINE_string(mysql_socket, "/var/run/mysqld/mysqld.sock",
      "The socket file for connection to mysql-server");

/// mysql table
DEFINE_string(crawlDocs_table, "crawlDocs",
      "The crawlDocs table");
DEFINE_string(urls_table, "urls",
      "The urls table");

DEFINE_string(crawl_docs_dir_prefix, "/home/housekeeper/crawlDocs",
      "The crawl docs file dir prefix");

namespace guanjia {
namespace crawler {

CrawlerFetcherHandler::CrawlerFetcherHandler() {
  grabTaskHandleThread_ = NULL;
  grabTaskHandleThread_ = new GrabTaskHandleThread(this);
  CHECK(grabTaskHandleThread_)
    << "Null pointer for grabTaskHandleThread_ when CrawlerFetcherHandler construct";

  /// Start the GrabTaskHandleThread
  grabTaskHandleThread_->Start();
}

CrawlerFetcherHandler::~CrawlerFetcherHandler() {
  if (grabTaskHandleThread_) {
    delete grabTaskHandleThread_;
  }
  grabTaskHandleThread_ = NULL;
}

bool CrawlerFetcherHandler::PushGrabTaskInternal(
      PushGrabTaskResponse& _return,
      const PushGrabTaskRequest& pushGrabTaskRequest) {
  if (pushGrabTaskRequest.crawlDocInfos.empty()) {
    LOG(ERROR) << "Empty crawlDocInfos when PushGrabTask";
    _return.returnCode = ReturnCode::BAD_PARAMETER;
    return false;
  }

  grabTaskQueueMutex_.Lock();    // Lock
  for (std::vector<CrawlDocInfo>::const_iterator it
        = pushGrabTaskRequest.crawlDocInfos.begin();
        it != pushGrabTaskRequest.crawlDocInfos.end(); ++it) {
    grabTaskQueue_.push_back(*it);
  }
  VLOG(4) << "The Grab-task queue size is: " << grabTaskQueue_.size();
  grabTaskQueueMutex_.Unlock();  // Unlock
  grabTaskQueueCondVar_.Signal();

  _return.returnCode = ReturnCode::SUCCESSFUL;
  return true;
}

/// for the GrabTaskHandleThread class
GrabTaskHandleThread::GrabTaskHandleThread(
      CrawlerFetcherHandler* const crawlerFetcherHandler) {
  crawlerFetcherHandler_ = crawlerFetcherHandler;

  /// Read the profileBackGroundImg
  profileBackGroundImg_.read(FLAGS_profile_background_file);

  /// Read the author userids
  file::SimpleLineReader simple_line_reader(
        FLAGS_author_userids_file, true);
  simple_line_reader.ReadLines(&authorUserIds_);
  CHECK(!authorUserIds_.empty())
    << "Author userIds is empty";
}

GrabTaskHandleThread::~GrabTaskHandleThread() {
}

void GrabTaskHandleThread::Run() {
  while (true) {
    std::vector<CrawlDocInfo> crawlDocInfos;
    bool isItemsValid = false;
    crawlerFetcherHandler_->grabTaskQueueMutex_.Lock();  // Lock
    while (crawlerFetcherHandler_->grabTaskQueue_.size() == 0) {
      crawlerFetcherHandler_->grabTaskQueueCondVar_.Wait(
            &crawlerFetcherHandler_->grabTaskQueueMutex_);
      isItemsValid = false;
    }

    while (!crawlerFetcherHandler_->grabTaskQueue_.empty()) {
      crawlDocInfos.push_back(
            crawlerFetcherHandler_->grabTaskQueue_.front());
      crawlerFetcherHandler_->grabTaskQueue_.pop_front();
    }
    isItemsValid = true;
    crawlerFetcherHandler_->grabTaskQueueMutex_.Unlock();  // Unlock

    if (isItemsValid) {
      /// calculate the thread-num
      int threadNum = 0;
      for (int tCnt = FLAGS_max_fetcher_thread_cnt; tCnt >= 1; tCnt--) {
        if (crawlDocInfos.size() / tCnt >= FLAGS_min_grab_cnt_per_thread) {
          threadNum = tCnt;
          break;
        }
      }

      if (0 == threadNum) {
        threadNum = 1;
      }
      VLOG(4) << "This PushGrabTask's url cnt = " << crawlDocInfos.size()
        << ", threadNum = " << threadNum;

      //threadNum = 1;  // force the thread num is 1(tmp)
      /// Start all the fetcher child-thread
      shared_ptr<FetchDocsThread> fetchDocsThreads[threadNum];
      int grabCntPerThread = crawlDocInfos.size() / threadNum;
      int docIndex = 0;
      for (int i = 0; i < threadNum; i++) {
        std::vector<CrawlDocInfo> crawlDocInfoThread;
        int thisStart = docIndex;
        if (i != threadNum - 1) {
          while (docIndex < thisStart + grabCntPerThread) {
            crawlDocInfoThread.push_back(crawlDocInfos[docIndex++]);
          }
        } else {
          while (docIndex < crawlDocInfos.size()) {
            crawlDocInfoThread.push_back(crawlDocInfos[docIndex++]);
          }
        }

        fetchDocsThreads[i] = shared_ptr<FetchDocsThread>(
              new FetchDocsThread(crawlDocInfoThread, i,
                &profileBackGroundImg_,
                &authorUserIds_));

        fetchDocsThreads[i]->SetJoinable(true);
        fetchDocsThreads[i]->Start();  // start
        VLOG(4) << i << " th thread have been started, whose url's cnt = "
          << crawlDocInfoThread.size();
      }

      /// This thread wait all the child-threads to join
      for (int i = 0; i < threadNum; i++) {
        fetchDocsThreads[i]->Join();
        VLOG(4) << i << " th thread have been joined";
      }
    }
  }
}

/// for the FetchDocsThread class
FetchDocsThread::FetchDocsThread(
      const std::vector<CrawlDocInfo>& crawlDocInfos,
      const int& threadIndex,
      Magick::Image* const profileBackGroundImg,
      std::vector<std::string>* const authorUserIds) {
  crawlDocInfos_ = crawlDocInfos;
  threadIndex_ = threadIndex;
   
  /// Create remote-file-server-client
  remote_file_server_client_ = boost::shared_ptr< ::base::ThriftClient<
    ::guanjia::RemoteFileHandleServletClient> >(new ::base::ThriftClient<
          ::guanjia::RemoteFileHandleServletClient>(FLAGS_small_file_server_host,
            FLAGS_small_file_server_port));
  CHECK(remote_file_server_client_.get())
    << "Null pointer for remote_file_server_client_ when FetchDocsThread";

  profileBackGroundImg_ = profileBackGroundImg;
  authorUserIds_ = authorUserIds;

  /// Create mysql_connection
  util_mysql::ConnectionOption connection_option;
  connection_option.host_ = FLAGS_mysql_host;
  connection_option.port_ = FLAGS_mysql_port;
  connection_option.database_ = FLAGS_mysql_db;
  connection_option.user_ = FLAGS_mysql_user;
  connection_option.password_ = FLAGS_mysql_password;
  connection_option.socket_ = FLAGS_mysql_socket;
  LOG(INFO) << "Create mysql_connection";
  mysql_connection_
    = boost::shared_ptr<util_mysql::MysqlConnection>(
          new util_mysql::MysqlConnection(connection_option));
  mysql_query_
    = boost::shared_ptr<util_mysql::MysqlQuery>(
          new util_mysql::MysqlQuery(&(*mysql_connection_)));
  CHECK(mysql_connection_->Connect())
    << "Connect to MySQL OK";
  CHECK(mysql_connection_->IsAlive())
    << "MySQL connection client is not alive";
  VLOG(4) << "Have construct FetchDocsThread, threadIndex = " << threadIndex_;
}

FetchDocsThread::~FetchDocsThread() {
}

void FetchDocsThread::Run() {
  std::string pathDirPrefix = FLAGS_crawl_docs_dir_prefix + "/";
  CurlFetcher curlFetcher;
  for (int i = 0; i < crawlDocInfos_.size(); i++) {
    std::string requestUrl = crawlDocInfos_[i].requestUrl;
    //uint64 urlHashId = base::Fingerprint(
          //base::StringPiece(crawlDocInfos_[i].requestUrl));
    int64 urlHashId = crawlDocInfos_[i].urlHashId;      

    /*
     * Check whether have fetched this doc
     */
    bool checkOk = true;
    util_mysql::MysqlResult checkMysqlResult;
    int64_t autoIndex = -1;
    std::string urlStatus;
    try {
      if (!mysql_query_->Select(
              FLAGS_urls_table,
              "AutoIndex, Url, UrlStatus",
              ("UrlHashId = " + Int64ToString(urlHashId)),
              &checkMysqlResult)) {
        LOG(ERROR) << "Check the url exist error when FetchDocsThread::Run "
          << "with UrlHashId = " << urlHashId
          << ", requestUrl = " << requestUrl
          << ", docId = " << crawlDocInfos_[i].docId
          << ", openId = " << crawlDocInfos_[i].openId
          << ". threadIndex = " << threadIndex_;
        checkOk = false;
      }
    } catch(...) {
      LOG(ERROR) << "Check the url exist exception when FetchDocsThread::Run "
        << "with UrlHashId = " << urlHashId
        << ", requestUrl = " << requestUrl
        << ", docId = " << crawlDocInfos_[i].docId
        << ", openId = " << crawlDocInfos_[i].openId
        << ". threadIndex = " << threadIndex_;
      checkOk = false;
    }

    if (checkOk) {
      if (checkMysqlResult.GetRowsNum() == 1) {  // An exist url
        checkMysqlResult.FetchRow();  // FetchRow
        checkMysqlResult.GetValue("AutoIndex", &autoIndex);
        checkMysqlResult.GetValue("UrlStatus", &urlStatus);
        if ("CRAWLED" == urlStatus || "PARSED" == urlStatus || "DONE" == urlStatus) {  // urlStatus != 'EXTRACTED'
          LOG(ERROR) << "Have fetched or handle this doc when FetchDocsThread::Run "
            << "with UrlHashId = " << urlHashId
            << ", requestUrl = " << requestUrl
            << ", docId = " << crawlDocInfos_[i].docId
            << ", openId = " << crawlDocInfos_[i].openId
            << ", UrlStatus = " << urlStatus
            << ". threadIndex = " << threadIndex_;
          continue;
        }
      } 
    }

    /*
     * Fetch the pageContent
     */
    DocHandleStatus::type docHandleStatus = DocHandleStatus::DONE;
    std::string pageContent;
    if (!curlFetcher.FetchPageContent(requestUrl,
            pageContent)) {
      LOG(ERROR) << "Get pageContent error when FetchDocsThread::Run "
        << "with requestUrl = " << requestUrl
        << ", docId = " << crawlDocInfos_[i].docId
        << ", openId = " << crawlDocInfos_[i].openId
        << ". threadIndex = " << threadIndex_;
      docHandleStatus = DocHandleStatus::CRAWL_FAILED;
    }
   
    /*
     * Write the pageContent to disk
     */
    if (docHandleStatus != DocHandleStatus::CRAWL_FAILED) {
      std::string pathDir = pathDirPrefix;
      pathDir += crawlDocInfos_[i].openId;
      std::string fileFullName = pathDir + "/" + Int64ToString(urlHashId) + "_" + crawlDocInfos_[i].docId;
      if (File::IsDir(pathDir)) {  // an exist dir
        /// Write the pageContent
        if (!File::WriteStringToFile(pageContent,
                fileFullName)) {
          LOG(ERROR) << "Write pageContent error when FetchDocsThread::Run "
            << "with requestUrl = " << requestUrl
            << ", docId = " << crawlDocInfos_[i].docId
            << ", openId = " << crawlDocInfos_[i].openId
            << ". threadIndex = " << threadIndex_;
          //continue;
        }
      } else {  // a new dir
        /// Create the new dir firstly
        if (!File::RecursivelyCreateDir(pathDir,
                S_IRWXG | S_IRWXU | S_IRWXO)) {
          LOG(ERROR) << "Failed to create dir when FetchDocsThread::Run "
            << "with requestUrl = " << requestUrl
            << ", docId = " << crawlDocInfos_[i].docId
            << ", openId = " << crawlDocInfos_[i].openId
            << ". threadIndex = " << threadIndex_;
          //continue;
        }

        /// Secondly, write the pageContent
        if (!File::WriteStringToFile(pageContent,
                fileFullName)) {
          LOG(ERROR) << "Write pageContent error when FetchDocsThread::Run "
            << "with requestUrl = " << requestUrl
            << ", docId = " << crawlDocInfos_[i].docId
            << ", openId = " << crawlDocInfos_[i].openId
            << ". threadIndex = " << threadIndex_;
          //continue;
        }
      }
    }

    /*
     * Parse the pageContent and normalize it
     */
    srand(time(NULL));
    std::string imgOwnerId = (*authorUserIds_)[rand() % authorUserIds_->size()];
    // std::string imgOwnerId = "abc";  // Just for test
    VLOG(4) << "imgOwnerId: " << imgOwnerId;
    PageNormalizer pageNormalizer(pageContent,
          imgOwnerId,
          profileBackGroundImg_,
          remote_file_server_client_.get());

    std::string title;
    if (!pageNormalizer.GetPageTitle(title)) {
      LOG(ERROR) << "Get page title error when FetchDocsThread::Run "
        << "with requestUrl = " << requestUrl
        << ", docId = " << crawlDocInfos_[i].docId
        << ", openId = " << crawlDocInfos_[i].openId
        << ". threadIndex = " << threadIndex_;
      docHandleStatus = DocHandleStatus::PARSE_FAILED;
    }

    std::string normalizedPageContent;
    ContentType contentType;
    if (!pageNormalizer.GetNormalizedPageContent(
            normalizedPageContent, contentType)) {
      LOG(ERROR) << "Get page normalized page content error "
        << "when FetchDocsThread::Run "
        << "with requestUrl = " << requestUrl
        << ", docId = " << crawlDocInfos_[i].docId
        << ", openId = " << crawlDocInfos_[i].openId
        << ". threadIndex = " << threadIndex_;
      docHandleStatus = DocHandleStatus::PARSE_FAILED;
    }

    /// Escape the normalized pageContent
    std::string normalizedPageContentEscape;
    if (docHandleStatus == DocHandleStatus::DONE) {
      try {
        if (!mysql_query_->EscapeString(normalizedPageContent,
                &normalizedPageContentEscape)) {
          LOG(ERROR) << "Escape the normalizedPageContent error when FetchDocsThread::Run "
            << "with requestUrl = " << requestUrl
            << ", docId = " << crawlDocInfos_[i].docId
            << ", openId = " << crawlDocInfos_[i].openId
            << ". threadIndex = " << threadIndex_;
          continue;
        }
      } catch(...) {
        LOG(ERROR) << "Escape the normalizedPageContent error when FetchDocsThread::Run "
          << "with requestUrl = " << requestUrl
          << ", docId = " << crawlDocInfos_[i].docId
          << ", openId = " << crawlDocInfos_[i].openId
          << ". threadIndex = " << threadIndex_;
        continue;
      }
    }

    /*
     * Update this url's UrlStatus in urls table
     */
    std::string expression = "UrlStatus = '";
    if (docHandleStatus == DocHandleStatus::DONE) {
      expression += "DONE'";       // 完成了抓取和解析
    } else if (docHandleStatus == DocHandleStatus::PARSE_FAILED) {
      expression += "CRAWLED'";    // 需要重新解析（甚至需要重新抓取）
    } else {
      expression += "EXTRACTED'";  // 需要重新抓取
    }
    std::string condition = "AutoIndex = ";
    condition += Int64ToString(autoIndex);
    
    try {
      if (kMysqlIllegalValue == mysql_query_->Update(FLAGS_urls_table,
              expression, condition)) {
        LOG(ERROR) << "Update the url's UrlStatus error when FetchDocsThread::Run "
          << "with requestUrl = " << requestUrl
          << ", docId = " << crawlDocInfos_[i].docId
          << ", openId = " << crawlDocInfos_[i].openId
          << ", urlStatus = " << expression
          << ". threadIndex = " << threadIndex_;
        continue;
      }
    } catch(...) {
      LOG(ERROR) << "Update the url's UrlStatus error when FetchDocsThread::Run "
        << "with requestUrl = " << requestUrl
        << ", docId = " << crawlDocInfos_[i].docId
        << ", openId = " << crawlDocInfos_[i].openId
        << ", urlStatus = " << expression
        << ". threadIndex = " << threadIndex_;
      continue;
    }

    if (docHandleStatus != DocHandleStatus::DONE) {
      LOG(ERROR) << "DocHandleStatus isn't DocHandleStatus::DONE when FetchDocsThread::Run "
        << "with requestUrl = " << requestUrl
        << ", docId = " << crawlDocInfos_[i].docId
        << ", openId = " << crawlDocInfos_[i].openId
        << ", urlStatus = " << expression
        << ". threadIndex = " << threadIndex_;
      continue;
    }

    /*
     * Write the crawlDocs table
     */
    std::string column = "AccountId, DocId, Url, Title, DocListImg, DocProfileImg, Content, ContentType, DocOwnerId, DocHandleStatus, UrlHashId";
    std::string values = "'" + crawlDocInfos_[i].openId + "', '";  // AccountId
    values += Utility::GetUuid(FEED_ID_PREFIX);  // DocId
    values += "', '";
    values += crawlDocInfos_[i].requestUrl;      // Url
    values += "', '";
    if (title == crawlDocInfos_[i].title) {
      values += title;
    } else {
      values += crawlDocInfos_[i].title;  // Title
    }
    
    values += "', '";
    if (docHandleStatus == DocHandleStatus::DONE) {  // DoclistImg, DocProfileImg, Content, ContentType
      values += pageNormalizer.get_list_img_url();
      values += "', '";
      values += pageNormalizer.get_profile_img_url();
      values += "', '";
      values += normalizedPageContentEscape;
      values += "', '";
      if (IMAGE == contentType) {
        values += "IMAGE', '";
      } else if (TEXT == contentType) {
        values += "TEXT', '";
      } else {
        values += "IMAGE', '";
      }
    } else {
      values += "', '', '', 'IMAGE', '";
    }
    values += imgOwnerId;
    values += "', '";
    if (docHandleStatus == DocHandleStatus::DONE) {
      values += "DONE'";
    } else if (docHandleStatus == DocHandleStatus::CRAWL_FAILED) {
      values += "CRAWL_FAILED'";
    } else if (docHandleStatus == DocHandleStatus::PARSE_FAILED) {
      values += "PARSE_FAILED'";
    } else {
      values += "UNDEFINED'";
    }
    values += ", ";
    values += Int64ToString(urlHashId);

    bool isRecorded = true;
    try {
      if (kMysqlIllegalValue == mysql_query_->Insert(
              FLAGS_crawlDocs_table,
              column, values)) {
        LOG(ERROR) << "Write crawlDocs table error when FetchDocsThread::Run "
          << "with requestUrl = " << requestUrl
          << ", docId = " << crawlDocInfos_[i].docId
          << ", openId = " << crawlDocInfos_[i].openId
          << ". threadIndex = " << threadIndex_;
        isRecorded = false;
      }
    } catch(...) {
      LOG(ERROR) << "Write crawlDocs table error when FetchDocsThread::Run "
        << "with requestUrl = " << requestUrl
        << ", docId = " << crawlDocInfos_[i].docId
        << ", openId = " << crawlDocInfos_[i].openId
        << ". threadIndex = " << threadIndex_;
      isRecorded = false;
    }
    
    if (!isRecorded) {
      /// (TODO)
      /// 记录数据库失败的时候，将该失败的条目写进重抓取队列(基于redis等内存队列)
    }

    srand(time(NULL));
    int this_fetch_url_interval
      = FLAGS_url_fetch_base_interval + (rand() % FLAGS_max_increment_fetch_interval);
    sleep(this_fetch_url_interval);
  }
}
}  // namespace crawler
}  // namespace guanjia
