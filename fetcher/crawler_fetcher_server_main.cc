// Copyright (c) 2014 Guanjia Inc. All rights reserved.
// Author: wangjuntao@guanjia.com (Wang Juntao)

#include <string>
#include <vector>
#include "base/flags.h"
#include "base/logging.h"
#include "news_search/base/json_config/config.h"
#include "util/global_init/global_init.h"
#include "crawler_fetcher_server.h"

using news_search::Config;  // NOLINT

DEFINE_string(config_file, "", "The configuration file");

int main(int argc, char* argv[]) {
  util::GlobalInit global_init(&argc, &argv);
#if 0
  /// Read the configuration file
  if (!Config::Init(FLAGS_config_file)) {
    LOG(FATAL) << "Failed to read config -- "
      << FLAGS_config_file;
    exit(0);
  }
#endif

  /// Start CrawlerFetcherServer
  ::guanjia::crawler::CrawlerFetcherServer crawler_fetcher_server;
  try {
    crawler_fetcher_server.Serve();
    LOG(INFO) << "Have started CrawlerFetcherServer in main processor successfully";
  } catch(const std::exception& err) {
    LOG(ERROR) << "Failed to start CrawlerFetcherServer in main processor. "
      << "EXCEPTION: " << err.what();
  } catch(...) {
    LOG(ERROR) << "Failed to start CrawlerFetcherServer in main processor, "
      << "unexplained exception";
  }
  return 0;
}

