// Copyright (c) 2014 Guanjia Inc. All rights reserved.
// Author: wangjuntao@guanjia.com (Wang Juntao)
//

#ifndef GUANJIA_CRAWLER_HANDLER_CURL_FETCHER_H_
#define GUANJIA_CRAWLER_HANDLER_CURL_FETCHER_H_

#include <string>
#include <vector>
#include "base/flags.h"
#include "base/logging.h"

#define HTML_FILE_SIZE 1024 * 1024

DECLARE_int32(curlopt_timeout_ms);
DECLARE_string(user_agent);

namespace guanjia {
namespace crawler {

class CurlFetcher {
 public:
  CurlFetcher();
  virtual ~CurlFetcher();

  bool FetchPageContent(const std::string& page_url,
        std::string& paget_content);

 private:
  bool DownLoadPageFile(const std::string& page_url,
        std::string& page_content);
};

size_t write_data(void* ptr, size_t size,
      size_t nmemb, void* stream);
}  // namespace crawler
}  // namespace guanjia
#endif  // GUANJIA_CRAWLER_HANDLER_CURL_FETCHER_H_
