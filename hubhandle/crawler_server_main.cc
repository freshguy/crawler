// Copyright (c) 2014 Guanjia Inc. All rights reserved.
// Author: wangjuntao@guanjia.com (Wang Juntao)

#include <string>
#include <vector>
#include "base/logging.h"
#include "base/flags.h"
#include "news_search/base/json_config/config.h"
#include "util/global_init/global_init.h"
#include "crawler_server.h"

using news_search::Config;  // NOLINT

DEFINE_string(config_file, "", "The configuration file");

int main(int argc, char* argv[]) {
  util::GlobalInit global_init(&argc, &argv);
#if 0
  /// Read configuration file
  if (!Config::Init(FLAGS_config)) {
    LOG(FATAL) << "Failed to read config -- "
      << FLAGS_config_file;
    exit(0);
  }
#endif

  /// Start CrawlerHubHandleServer
  guanjia::crawler::CrawlerHubHandleServer crawler_hubhandle_server;
  try {
    crawler_hubhandle_server.Serve();
    LOG(INFO) << "Have started CrawlerHubHandleServer in main processor successfully";
  } catch(const std::exception& err) {
    LOG(ERROR) << "Failed to start CrawlerHubHandleServer in main processor "
      << err.what();
  } catch(...) {
    LOG(ERROR) << "Failed to start CrawlerHubHandleServer in main processor "
      << "unexplained exception";
  }
  return 0;
}
