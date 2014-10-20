// Copyright (c) 2014 Guanjia Inc. All rights reserved.
// Author: wangjuntao@guanjia.com (Wang Juntao)

#include "base/hash.h"
#include "base/basictypes.h"
#include "base/string_util.h"
#include "third_party/jsoncpp/include/json.h"
#include "file/simple_line_writer.h"
#include "guanjia/crawler/hubhandle/hubpage_extract_handler.h"
#include "guanjia/crawler/handler/curl_fetcher.h"
#include "guanjia/crawler/handler/xml_xpath.h"

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

DEFINE_string(weixinHubPages_table, "weixinHubPages",
      "The weixinHubPages table");
DEFINE_string(urls_table, "urls",
      "The extract urls table");

DEFINE_int32(base_update_interval, 60 * 60 * 1,
      "The update interval(ms)");
DEFINE_int32(max_increment_interval, 60 * 20,
      "The max increment interval");

DEFINE_int32(get_hubpage_cnt_per_round, 10,
      "The get hubpage count per round");

DEFINE_int32(base_page_fetch_interval, 1,
      "The page fetche interval");
DEFINE_int32(max_increment_fetch_interval, 5,
      "The max increment fetch page interval");

DEFINE_string(hub_url_common_prefix,
      "http://weixin.sogou.com/gzhjs?cb=sogou.weixin.gzhcb",
      "The hubpage url common prefix");

DEFINE_string(crawler_fetcher_host_name, "localhost",
      "The crawler fetcher host name");
DEFINE_int32(crawler_fetcher_host_port, 20010,
      "The crawler fetcher host port");

namespace guanjia {
namespace crawler {

HubPageExtractHandler::HubPageExtractHandler() {
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

  /// Connect to CrawlerFetcherServer
  crawler_fetcher_servlet_client_ = boost::shared_ptr< ::base::ThriftClient<
    ::guanjia::crawler::CrawlerFetcherServletClient> >(new ::base::ThriftClient<
          ::guanjia::crawler::CrawlerFetcherServletClient>(
            FLAGS_crawler_fetcher_host_name,
            FLAGS_crawler_fetcher_host_port));
  CHECK(crawler_fetcher_servlet_client_.get())
    << "Null pointer for crawler_fetcher_servlet_client_ when HubPageExtractHandler construct";
  
  /// Start the HubPageExtractThread
  hubpage_extract_thread_.reset(new HubPageExtractThread(this));
  CHECK(hubpage_extract_thread_.get())
    << "Null pointer for hubpage_extract_thread_ "
    << "when HubPageExtractHandler constrcut";
  hubpage_extract_thread_->Start();
  
  LOG(INFO) << "Have construct a HubPageExtractHandler";
}

HubPageExtractHandler::~HubPageExtractHandler() {
}

/// For HubPageExtractThread
HubPageExtractThread::HubPageExtractThread(
      HubPageExtractHandler* const hubpage_extract_handler) {
  CHECK(hubpage_extract_handler)
    << "Null pointer for hubpage_extract_handler_ "
    << "when HubPageExtractThread construct";
  hubpage_extract_handler_ = hubpage_extract_handler;
}

HubPageExtractThread::~HubPageExtractThread() {
}

void HubPageExtractThread::Run() {
  int roundCnt = 0;
  while (true) {
    roundCnt++;
    int totalHubPageCount = 0;
    int totalHandledOkCount = 0;
    bool isFirst = true;
    int startId = 0;
    while (true) {
      std::string getHubInfosExecuteSql = "SELECT id, account_id, account_name, latest_doc_url, latest_doc_id, total_page_num, total_doc_num, latest_update_time FROM ";
      getHubInfosExecuteSql += FLAGS_weixinHubPages_table;
      if (isFirst) {
        getHubInfosExecuteSql += " WHERE enable_crawl = 1 ORDER BY id ASC LIMIT ";
        getHubInfosExecuteSql += IntToString(FLAGS_get_hubpage_cnt_per_round);
        isFirst = false;
      } else {
        getHubInfosExecuteSql += " WHERE enable_crawl = 1 AND id > ";
        getHubInfosExecuteSql += IntToString(startId);
        getHubInfosExecuteSql += " ORDER BY id ASC LIMIT ";
        getHubInfosExecuteSql += IntToString(FLAGS_get_hubpage_cnt_per_round);
      }

      util_mysql::MysqlResult getHubInfosMysqlResult;
      try {
        if (!hubpage_extract_handler_->get_mysql_query()->Execute(
                getHubInfosExecuteSql, &getHubInfosMysqlResult)) {
          LOG(ERROR) << "Get hubpage infos from db error when HubPageExtractThread::Run "
            << "with startId = " << startId;
          continue;
        }
      } catch(...) {
        LOG(ERROR) << "Get hubpage infos from db exception when HubPageExtractThread::Run "
          << "with startId = " << startId;
        continue;
      }
     
      int hubInfosCnt = getHubInfosMysqlResult.GetRowsNum();
      totalHubPageCount += hubInfosCnt;
      if (hubInfosCnt == 0) {
        LOG(INFO) << "No hubpages need to be handled this round "
          << "when HubPageExtractThread::Run with startId = " << startId
          << ", roundCnt = " << roundCnt;
        break;
      }

      int itemIndex = 0;
      while (getHubInfosMysqlResult.FetchRow()) {
        if (itemIndex++ == hubInfosCnt - 1) {
          getHubInfosMysqlResult.GetValue("id", &startId);
        }

        std::string accountId;
        getHubInfosMysqlResult.GetValue("account_id", &accountId);
        if (!HandleHubPageExtract(&getHubInfosMysqlResult)) {
          LOG(ERROR) << "HandleHubPageExtract error when HubPageExtractThread::Run "
            << "with openId = " << accountId;
          continue;
        }
        totalHandledOkCount++; 
      }

      if (hubInfosCnt < FLAGS_get_hubpage_cnt_per_round) {
        LOG(INFO) << "This " << hubInfosCnt << " hubPageInfos is the final batch this round "
          << "when HubPageExtractThread::Run with startId = " << startId
          << ", roundCnt = " << roundCnt;
        break;
      }
    }

    /// Sleep udpate interval
    srand(time(NULL));
    int this_update_interval
      = FLAGS_base_update_interval + (rand() % FLAGS_max_increment_interval);
    LOG(INFO) << "Have done " << roundCnt << "th round. "
      << "totalHubPageCnt = " << totalHubPageCount
      << ", successfully Handle hubPageCnt = " << totalHandledOkCount
      << ". Sleep " << this_update_interval << " s for the next round";
    sleep(this_update_interval);
  }
}

bool HubPageExtractThread::HandleHubPageExtract(
      util_mysql::MysqlResult* const hubInfosMysqlResult) {
  std::string accountId;
  hubInfosMysqlResult->GetValue("account_id", &accountId);
  std::string accountName;
  hubInfosMysqlResult->GetValue("account_name", &accountName);
  std::string latestDocUrl;
  hubInfosMysqlResult->GetValue("latest_doc_url", &latestDocUrl);
  std::string latestDocId;
  hubInfosMysqlResult->GetValue("latest_doc_id", &latestDocId);
  int totalPageNum;
  hubInfosMysqlResult->GetValue("total_page_num", &totalPageNum);
  int totalDocNum;
  hubInfosMysqlResult->GetValue("total_doc_num", &totalDocNum);
  std::string latestUpdateTime;
  hubInfosMysqlResult->GetValue("latest_update_time", &latestUpdateTime);

  bool newOpenId = false;
  if (latestDocUrl.empty() && latestDocId.empty()) {
    newOpenId = true;
  }

  std::vector<CrawlDocInfo> crawlDocInfos;
  bool shouldGetMore = true;  // should get more pages
  CurlFetcher curlFetcher;
  /*
   * Try get the first page, then can get the totalPageNum
   */
  struct timeval currentTimeval;
  gettimeofday(&currentTimeval, NULL);
  long currentTimeMs = (long)currentTimeval.tv_sec * 1000 + (long)currentTimeval.tv_usec / 1000;
  std::string firstHubPageUrl = FLAGS_hub_url_common_prefix + "&openid=";
  firstHubPageUrl += accountId;
  firstHubPageUrl += "&page=";
  firstHubPageUrl += IntToString(1);  // The first page index
  firstHubPageUrl += "&t=";
  firstHubPageUrl += Int64ToString(currentTimeMs);
  VLOG(4) << "firstHubPageUrl = " << firstHubPageUrl
    << ", openId = " << accountId;

  std::string pageContent;
  if (!curlFetcher.FetchPageContent(firstHubPageUrl,
          pageContent)) {
    LOG(ERROR) << "Get the first pageContent error when FetchPageContent "
      << "with firstHubPageUrl = " << firstHubPageUrl;
    return false;
  }

  std::string startPattern = "sogou.weixin.gzhcb(";
  std::string endPattern = "})";
  int startIndex = pageContent.find(startPattern) + startPattern.size();
  int endIndex = pageContent.rfind(endPattern);
  std::string pageContent_ = pageContent.substr(startIndex,
        endIndex - startIndex + 1);
  Json::Reader reader;
  Json::Value contentJsonValue;
  if (!reader.parse(pageContent_, contentJsonValue)) {
    LOG(ERROR) << "Parse pageContent error for the first page when FetchPageContent "
      << "with firstHubPageUrl = " << firstHubPageUrl;
    return false;
  }
  // int page = contentJsonValue["page"].asInt();
  int totalItems = contentJsonValue["totalItems"].asInt();  // use the first page's totalItems as the standard
  int totalPages = contentJsonValue["totalPages"].asInt();  // use the first page's totalPages as the standard

  int docExistCnt = 0;
  std::string thisLatestDocUrl;  //  = latestDocUrl;
  std::string thisLatestDocId;  //  = latestDocId;
  std::string thisLatestTitle;
  for (int iCnt = 0; iCnt < contentJsonValue["items"].size(); iCnt++) {
    std::string itemXmlContent = contentJsonValue["items"][iCnt].asString();
    XmlXpath xmlXpath(itemXmlContent);
    
    std::string url;  // url
    if (!xmlXpath.GetXmlNodeValues("//url",
            url)) {
      LOG(ERROR) << "----- The first page, item " << iCnt
        << " get xpathNode for url error -----";
      continue;
    }
    uint64 urlHashId = base::Fingerprint(base::StringPiece(url));

    std::string docId;
    if (!xmlXpath.GetXmlNodeValues("//docid",
            docId)) {
      LOG(ERROR) << "----- The fist page, item " << iCnt
        << " get xpathNode for docid error -----";
      // continue;
    }

    std::string title;
    if (!xmlXpath.GetXmlNodeValues("//title",
            title)) {
      LOG(ERROR) << "----- The first page, item " << iCnt
        << " get xpathNode for title error -----";
      // continue;
    }

    if (iCnt == 0) {  // the newest docInfo
      thisLatestDocUrl = url;
      thisLatestDocId = docId;
      thisLatestTitle = title;
    }

    /// for an old account
    if (!latestDocUrl.empty() && !latestDocId.empty()) {
      if (url == latestDocUrl || docId == latestDocId) {
        shouldGetMore = false;
        break;
      }
    }

    /// Check whether the doc exist
    bool checkOk = true;
    util_mysql::MysqlResult checkMysqlResult;
    try {
      if (!hubpage_extract_handler_->get_mysql_query()->Select(
              FLAGS_urls_table,
              "AutoIndex, Url",
              ("UrlHashId = " + Int64ToString(urlHashId)),
              &checkMysqlResult)) {
        LOG(ERROR) << "Check the url exist error when HandleHubPageExtract "
          << "with UrlHashId = " << urlHashId;
        checkOk = false;
      }
    } catch(...) {
      LOG(ERROR) << "Check the url exist error when HandleHubPageExtract "
        << "with UrlHashId = " << urlHashId;
      checkOk = false;
    }

    if (checkOk) {
      if (checkMysqlResult.GetRowsNum() > 0) {
        VLOG(4) << "An url have been extracted yet when HandleHubPageExtract "
          << "with UrlHashId = " << urlHashId
          << ", url = " << url;
        docExistCnt++;
        continue;
      }
    }
    
    CrawlDocInfo crawlDocInfo;
    crawlDocInfo.requestUrl = url;
    crawlDocInfo.title = title;
    crawlDocInfo.__isset.title = true;  // set
    crawlDocInfo.docId = docId;
    crawlDocInfo.__isset.docId = true;  // set
    crawlDocInfo.openId = accountId;
    crawlDocInfo.__isset.openId = true;  // set
    crawlDocInfo.urlHashId = urlHashId;
    crawlDocInfo.__isset.urlHashId = true;  // set
    crawlDocInfos.push_back(crawlDocInfo);
  }
 
  /// for a new account
  if (latestDocUrl.empty() && latestDocId.empty()) {
    if (crawlDocInfos.size() == totalItems) {  // only have the first page
      shouldGetMore = false;
    }
  }

  /*
   * Should get more pages
   */
  if (shouldGetMore) {
    bool shouldBreak = false;
    for (int pageIndex = 2; pageIndex <= totalPages; pageIndex++) {
      /// Get one page's hubpage content
      gettimeofday(&currentTimeval, NULL);
      long currentTimeMs = (long)currentTimeval.tv_sec * 1000 + (long)currentTimeval.tv_usec / 1000;
      std::string hubPageUrl = FLAGS_hub_url_common_prefix + "&openid=";
      hubPageUrl += accountId;
      hubPageUrl += "&page=";
      hubPageUrl += IntToString(pageIndex);  // The first page index
      hubPageUrl += "&t=";
      hubPageUrl += Int64ToString(currentTimeMs);
      VLOG(4) << "Get More hubpage's content with "
        << "OpenId = " << accountId << ", pageIndex = " << pageIndex;

      std::string _pageContent;
      if (!curlFetcher.FetchPageContent(hubPageUrl,
              _pageContent)) {
        LOG(ERROR) << "Get More hubpage's content when FetchPageContent "
          << "with OpendId = " << accountId << ", pageIndex = " << pageIndex;
        return false;
      }
  
      startIndex = _pageContent.find(startPattern) + startPattern.size();
      endIndex = _pageContent.rfind(endPattern);
     
      /// the jsonString content
      std::string __pageContent = _pageContent.substr(startIndex,
            endIndex - startIndex + 1);
      Json::Reader _reader;
      Json::Value _contentJsonValue;
      if (!_reader.parse(__pageContent, _contentJsonValue)) {
        LOG(ERROR) << "Parse pageContent error "
          << "with OpendId = " << accountId << ", pageIndex = " << pageIndex;
        return false;
      }
      // page = _contentJsonValue["page"].asInt();
      // totalItems = _contentJsonValue["totalItems"].asInt();
      // totalPages = _contentJsonValue["totalPages"].asInt();

      for (int iCnt = 0; iCnt < _contentJsonValue["items"].size(); iCnt++) {
        std::string itemXmlContent = _contentJsonValue["items"][iCnt].asString();
        XmlXpath xmlXpath(itemXmlContent);
        
        std::string url;
        if (!xmlXpath.GetXmlNodeValues("//url",
                url)) {
          LOG(ERROR) << "----- Get More page. page = " << pageIndex
            << ", item = " << iCnt << ". Get xpathNode for url error. "
            << "with OpenId = " << accountId << ", pageIndex = " << pageIndex;
          continue;
        }
        uint64 _urlHashId = base::Fingerprint(base::StringPiece(url));

        std::string docId;
        if (!xmlXpath.GetXmlNodeValues("//docid",
                docId)) {
          LOG(ERROR) << "----- Get More page. page = " << pageIndex
            << ", item = " << iCnt << ". Get xpathNode for docId error. "
            << "with OpenId = " << accountId << ", pageIndex = " << pageIndex;
          continue;
        }

        std::string title;
        if (!xmlXpath.GetXmlNodeValues("//title",
                title)) {
          LOG(ERROR) << "----- Get More page. page = " << pageIndex
            << ", item = " << iCnt << ". Get xpathNode for title error. "
            << "with OpenId = " << accountId << ", pageIndex = " << pageIndex;
          continue;
        }

        /// for an old account, will check the last update-point, but no need for an new account
        if (!latestDocUrl.empty() && !latestDocId.empty()) {
          if (url == latestDocUrl || docId == latestDocId) {
            LOG(ERROR) << "----- Get More page. page = " << pageIndex
              << ", item = " << iCnt << " is the last update-point, no need to fetch after this doc "
              << " with OpenId = " << accountId << ", pageIndex = " << pageIndex;
            shouldBreak = true;
            break;
          }
        }
    
        /// Check whether the doc exist
        bool checkOk = true;
        util_mysql::MysqlResult checkMysqlResult;
        try {
          if (!hubpage_extract_handler_->get_mysql_query()->Select(
                  FLAGS_urls_table,
                  "AutoIndex, Url",
                  ("UrlHashId = " + Int64ToString(_urlHashId)),
                  &checkMysqlResult)) {
            LOG(ERROR) << "Check the url exist error when HandleHubPageExtract(for more pages) "
              << "with UrlHashId = " << _urlHashId << ", item = " << iCnt
              << ", OpenId = " << accountId << ", pageIndex = " << pageIndex;
            checkOk = false;
          }
        } catch(...) {
          LOG(ERROR) << "Check the url exist error when HandleHubPageExtract(for more pages) "
            << "with UrlHashId = " << _urlHashId << ", item = " << iCnt
            << ", OpenId = " << accountId << ", pageIndex = " << pageIndex;
          checkOk = false;
        }

        if (checkOk) {
          if (checkMysqlResult.GetRowsNum() > 0) {
            VLOG(4) << "An url have been extracted yet when HandleHubPageExtract(for more pages) "
              << "with UrlHashId = " << _urlHashId << ", item = " << iCnt
              << ", OpenId = " << accountId << ", pageIndex = " << pageIndex
              << ", url = " << url;
            docExistCnt++;
            if (docExistCnt > EXIST_CNT_THRESHHOLD) {
              LOG(INFO) << "Have exceed the max exist cnt threshhold when HandleHubPageExtract "
                << "with OpenId = " << accountId << ", pageIndex = " << pageIndex
                << ", item = " << iCnt;
              shouldBreak = true;
              break;
            }
            continue;
          }
        }

        CrawlDocInfo crawlDocInfo;
        crawlDocInfo.requestUrl = url;
        crawlDocInfo.title = title;
        crawlDocInfo.__isset.title = true;      // set
        crawlDocInfo.docId = docId;
        crawlDocInfo.__isset.docId = true;      // set
        crawlDocInfo.openId = accountId;
        crawlDocInfo.__isset.openId = true;     // set
        crawlDocInfo.urlHashId = _urlHashId;
        crawlDocInfo.__isset.urlHashId = true;  // set
        crawlDocInfos.push_back(crawlDocInfo);
      }

      if (shouldBreak) {
        LOG(INFO) << "Have found the last update-point when HandleHubPageExtract "
          << "with OpenId = " << accountId << ", pageIndex = " << pageIndex;
        break;
      }

      srand(time(NULL));
      int this_page_fetch_interval
        = FLAGS_base_page_fetch_interval + (rand() % FLAGS_max_increment_fetch_interval);
      sleep(this_page_fetch_interval);
    }
  }
 
#if 0
  /// Just for test
  std::vector<std::string> docUrls;
  std::vector<std::string> docTitles;
  for (std::vector<CrawlDocInfo>::iterator it = crawlDocInfos.begin();
        it != crawlDocInfos.end(); ++it) {
    docUrls.push_back(it->requestUrl);
    docTitles.push_back(it->title);
  }
  file::SimpleLineWriter urlWriter("/tmp/" + accountId + "_docUrls");
  urlWriter.WriteLines(docUrls);
  file::SimpleLineWriter titleWriter("/tmp/" + accountId + "_docTitles");
  titleWriter.WriteLines(docTitles);

  std::vector<std::string> dupDocUrls;
  std::vector<std::string> dupDocTitles;
  file::SimpleLineWriter dupUrlWriter("/tmp/" + accountId + "_dupDocUrls");
  file::SimpleLineWriter dupTitleWriter("/tmp/" + accountId + "_dupDocTitles");
#endif

  PushGrabTaskRequest pushGrabTaskRequest; 
  /// URL Duplicate-removal(Local, not global)
  /// (TODO) Use the bloom-filter algorithm
  std::set<std::string> urlSets;
  /// In reverse order. Because the tail is the older doc, should get firstly
  for (int i = crawlDocInfos.size() - 1; i >= 0; i--) {
    std::string url = crawlDocInfos[i].requestUrl;
    if (urlSets.insert(url).second) {  // not a already exist url
      /// Write the url info into db
      std::string column = "UrlHashId, Url, DocId, Title, AccountId";
      std::string values = Int64ToString(crawlDocInfos[i].urlHashId);
      values += ", '";
      values += crawlDocInfos[i].requestUrl;
      values += "', '";
      values += crawlDocInfos[i].docId;
      values += "', '";
      values += crawlDocInfos[i].title;
      values += "', '";
      values += crawlDocInfos[i].openId;
      values += "'";
      bool insertOk = true;
      try {
        if (kMysqlIllegalValue == hubpage_extract_handler_->get_mysql_query()->Insert(
                FLAGS_urls_table,
                column, values)) {
          LOG(ERROR) << "Insert a url info error when HandleHubPageExtract "
            << "with UrlHashId = " << crawlDocInfos[i].urlHashId
            << ", url = " << crawlDocInfos[i].requestUrl;
          insertOk = false;
        }
      } catch(...) {
        LOG(ERROR) << "Insert a url info error when HandleHubPageExtract "
          << "with UrlHashId = " << crawlDocInfos[i].urlHashId
          << ", url = " << crawlDocInfos[i].requestUrl;
        insertOk = false;
      }

      if (insertOk) {
        pushGrabTaskRequest.crawlDocInfos.push_back(crawlDocInfos[i]);
      }

      //dupDocUrls.push_back(crawlDocInfos[i].requestUrl);
      //dupDocTitles.push_back(crawlDocInfos[i].title);
    } else {
      LOG(ERROR) << "A duplicate CrawlDocInfo, whose url = " << url;
    }
  }
  //dupUrlWriter.WriteLines(dupDocUrls);
  //dupTitleWriter.WriteLines(dupDocTitles);


  if (!pushGrabTaskRequest.crawlDocInfos.empty()) {
    VLOG(4) << "This PushGrabTask's CrawlDocInfo cnt = " << pushGrabTaskRequest.crawlDocInfos.size();
    /// Dispatch these page-fectcher task to dispatcher server
    PushGrabTaskResponse pushGrabTaskResponse;
    try {
      hubpage_extract_handler_->get_crawler_fetcher_servlet_client().get()->GetService()->PushGrabTask(
            pushGrabTaskResponse, pushGrabTaskRequest);
      if (ReturnCode::SUCCESSFUL != pushGrabTaskResponse.returnCode) {
        LOG(ERROR) << "PushGrabTask to CrawlerFetchServer error when HandleHubPageExtract "
          << "with opendId = " << accountId;
        return false;
      }
    } catch(const ::apache::thrift::TException& err) {
      LOG(ERROR) << "PushGrabTask to CrawlerFetcherServer exception when HandleHubPageExtract "
        << "with opendId = " << accountId
        << ". EXCEPTION: " << err.what();
      return false;
    } catch(...) {
      LOG(ERROR) << "PushGrabTask to CrawlerFetcherServer exception when HandleHubPageExtract "
        << "with opendId = " << accountId
        << ". Unexplained exception";
      return false;
    }
  }

  /// Update the hubpage's info in db
  std::string expression = "latest_update_time = CURRENT_TIMESTAMP, latest_add_item_cnt = ";
  expression += IntToString(pushGrabTaskRequest.crawlDocInfos.size());
  expression += ", total_page_num = ";
  expression += IntToString(totalPages);
  expression += ", total_doc_num = ";
  expression += IntToString(totalItems);
 
  if (newOpenId) {
    expression += ", first_add_item_cnt = ";
    expression += IntToString(pushGrabTaskRequest.crawlDocInfos.size());
  }
  
  if (!pushGrabTaskRequest.crawlDocInfos.empty()) {
    expression += ", latest_doc_url = '";
    expression += thisLatestDocUrl;
    expression += "', latest_doc_id = '";
    expression += thisLatestDocId;
    expression += "', latest_doc_title = '";
    expression += thisLatestTitle;
    expression += "', latest_update_effective_time = CURRENT_TIMESTAMP";
  }
  std::string condition = "account_id = '";
  condition += accountId;
  condition += "'";

  try {
    if (kMysqlIllegalValue == hubpage_extract_handler_->get_mysql_query()->Update(
            FLAGS_weixinHubPages_table,
            expression, condition)) {
      LOG(ERROR) << "Update the hubpage's info error when HandleHubPageExtract "
        << "with openId = " << accountId;
    }
  } catch(...) {
    LOG(ERROR) << "Update the hubpage's info exception when HandleHubPageExtract "
      << "with openId = " << accountId;
  }
  return true;
}
}  // namesapce crawler
}  // namespace guanjia
