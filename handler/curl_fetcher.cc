// Copyright (c) 2014 Guanjia Inc. All rights reserved.
// Author: wangjuntao@guanjia.com (Wang Juntao)

#include <stdlib.h>
#include <curl/curl.h>
#include "util/url_parser/yurl.h"
#include "guanjia/crawler/handler/curl_fetcher.h"

DEFINE_int32(curlopt_timeout_ms, 120000,  // 120s
      "The curl fetch timeout in ms");

DEFINE_string(user_agent,
      "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/29.0.1547.65 Safari/537.36",
      "The browser user agent");


namespace guanjia {
namespace crawler {

struct MemoryStruct {
  char* memory;
  size_t size;
};

static size_t WriteMemoryCallback(
      void *contents,size_t size, size_t nmemb, void *userp) {
  size_t realsize = size * nmemb;
  struct MemoryStruct* mem = (struct MemoryStruct *)userp;
 
  mem->memory = reinterpret_cast<char*>(
        realloc(mem->memory, mem->size + realsize + 1));
  if(mem->memory == NULL) {
    LOG(FATAL) << "not enough memory (realloc returned NULL)";
  }
 
  memcpy(&(mem->memory[mem->size]), contents, realsize);
  mem->size += realsize;
  mem->memory[mem->size] = 0;
 
  return realsize;
}

CurlFetcher::CurlFetcher() {
  curl_global_init(CURL_GLOBAL_ALL);
}

CurlFetcher::~CurlFetcher() {
  curl_global_cleanup();
}
  
bool CurlFetcher::FetchPageContent(
      const std::string& page_url,
      std::string& page_content) {
  if (!DownLoadPageFile(page_url, page_content)) {
    LOG(ERROR) << "FetchPageContent error with page-url: "
      << page_url;
    return false;
  }
  return true;
}

bool CurlFetcher::DownLoadPageFile(const std::string& page_url,
      std::string& page_content) {
  VLOG(4) << "Page_url: " << page_url;
  std::string encodeUrl;
  util::yurl::NormalizeURLForCrawler(page_url, &encodeUrl);
  VLOG(4) << "Encode-PageUrl: " << encodeUrl;

#if 0
  /// method 1
  char* str = new char[HTML_FILE_SIZE];
  strcpy(str, "");
  CURL* curl = curl_easy_init();  // init curl
  curl_easy_setopt(curl, CURLOPT_URL, page_url.c_str());  // download address
  //curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);  // 屏蔽其它信号
  //curl_easy_setopt(curl, CURLOPT_TIMEOUT, 3);    // timeout
  curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, 10000);  // timeout
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_data);
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, str);
  curl_easy_setopt(curl, CURLOPT_ACCEPT_ENCODING, "");
  curl_easy_setopt(curl, CURLOPT_TRANSFER_ENCODING, 1);
  //curl_easy_setopt(curl, CURLOPT_REFERER, "weixin.sogou.com");
  //curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
  curl_easy_setopt(curl, CURLOPT_AUTOREFERER, 1L);
  curl_easy_setopt(curl, CURLOPT_HEADER, 1L);
  curl_easy_setopt(curl, CURLOPT_USERAGENT, FLAGS_user_agent.c_str());

  CURLcode res = curl_easy_perform(curl);
  str[HTML_FILE_SIZE - 1] = '\0';
  if (CURLE_OK != res) {
    curl_easy_cleanup(curl);
    LOG(ERROR) << "curl_easy_perfrom error when DownLoadPageFile "
      << "with page-url: " << page_url
      << ". ERROR: " << curl_easy_strerror(res);
    return false;
  }
  curl_easy_cleanup(curl);
  page_content = str;
  return true;
#endif

  /// GetInMemory
  struct MemoryStruct chunk;
  chunk.memory = reinterpret_cast<char*>(malloc(1));  // will be grown as needed by the realloc above
  chunk.size = 0;                                     // no data at this point
  
  CURL* curl = curl_easy_init();  // init curl
  curl_easy_setopt(curl, CURLOPT_URL, encodeUrl.c_str());  // download address
  curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);  // 屏蔽其它信号
  /// send all data to this function
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteMemoryCallback);
  /// we pass our 'chunk' struct to the callback function
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void *)&chunk);
  curl_easy_setopt(curl, CURLOPT_USERAGENT, FLAGS_user_agent.c_str());
  curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, FLAGS_curlopt_timeout_ms);  // timeout curlopt_timeout_ms
  //curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
  curl_easy_setopt(curl, CURLOPT_AUTOREFERER, 1L);
  //curl_easy_setopt(curl, CURLOPT_HEADER, 1L);
  
  /// curl_exec和wget执行太慢，IPv6惹的祸:
  //http://www.lovelucy.info/curl-exec-wget-slow-due-to-ipv6.html
  curl_easy_setopt(curl, CURLOPT_IPRESOLVE, CURL_IPRESOLVE_V4);
  
  CURLcode res = curl_easy_perform(curl);
  if (CURLE_OK != res) {
    curl_easy_cleanup(curl);
    if (chunk.memory) {
      free(chunk.memory);
    }
    LOG(ERROR) << "curl_easy_perform error when DownLoadPageFile "
      << "with page-url: " << page_url
      << ". ERROR: " << curl_easy_strerror(res);
    return false;
  }

  curl_easy_cleanup(curl);
  page_content.assign(chunk.memory);
  
  if (chunk.memory) {
    free(chunk.memory);
  }
  return true;
}

size_t write_data(void* ptr, size_t size,
      size_t nmemb, void* stream) {
  if (strlen((char*)ptr) + strlen((char*)stream) > HTML_FILE_SIZE) {
    return 0;
  }
  strcat((char*)stream, (char*)ptr);
  return size * nmemb;  // 必须返回这个大小, 否则只回调一次
}
}  // namespace crawler
}  // namespace guanjia
