// Copyright (c) 2014 Guanjia Inc. All rights reserved.
// Author: wangjuntao@guanjia.com (Wang Juntao)

#include <time.h>
#include <string>
#include <vector>
#include "base/logging.h"
#include "base/flags.h"
#include "base/thrift.h"
#include "file/file.h"
#include "third_party/jsoncpp/include/json.h"
#include "third_party/tinyxml/tinyxml.h"
#include "testing/gtest/include/gtest/gtest.h"
#include "guanjia/crawler/handler/xml_xpath.h"
#include "guanjia/crawler/handler/curl_fetcher.h"
#include "guanjia/base/pugixml/src/pugixml.hpp"

using boost::shared_ptr;  // NOLINT

DEFINE_string(xpath_expr, "//a",
      "The xpath expr");

DEFINE_string(page_url,
      "http://mp.weixin.qq.com/s?__biz=MzA4OTcwNTEzMA==&mid=235447058&idx=3&sn=726ef64b570a850622a4de494c079093&3rd=MzA3MDU4NTYzMw==&scene=6#rd",
      "The page url");

DEFINE_string(url_common_prefix,
      "http://weixin.sogou.com/gzhjs?cb=sogou.weixin.gzhcb",
      "The url common prefix");
DEFINE_string(openid,
      "oIWsFt6QeqUjZRO4NSZ_I6w3rH0E",
      "The weixin openid");
DEFINE_int32(page_num, 1,
      "The page num");

namespace guanjia {
namespace crawler {

#if 0
TEST(CurlFetcher, FetchPageContent_pagetest) {
  CurlFetcher curlFetcher;
  std::string pageContent;
  if (!curlFetcher.FetchPageContent(FLAGS_page_url,
          pageContent)) {
    LOG(ERROR) << "FetchPageContent error "
      << "with page_url: " << FLAGS_page_url;
    return;
  }

  file::File::WriteStringToFile(pageContent,
        "/tmp/curl_fetcher.html");
}
#endif

TEST(XmlXpath, GetXmlNodeValues_test) {
  std::string xmlContentStr;
  file::File::ReadFileToString("/tmp/test.xml",
        &xmlContentStr);
  XmlXpath xmlXpath(xmlContentStr);
  std::vector<std::string> nodeValues;
  xmlXpath.GetXmlNodeValues(FLAGS_xpath_expr,
        nodeValues);
  
  for (int i = 0; i < nodeValues.size(); i++) {
    LOG(INFO) << i << "th nodeValue: " << nodeValues[i];
  }
}

TEST(PUGIXML, pugixml_test) {
  std::string xmlContentStr;
  file::File::ReadFileToString("/tmp/test.xml",
        &xmlContentStr);
  pugi::xml_document doc;
  pugi::xml_parse_result result = doc.load_buffer(
        xmlContentStr.c_str(), xmlContentStr.size());
  if (!result) {
    return;
  }
  
  pugi::xpath_query rootNode_tools((pugi::char_t*)"//DOCUMENT/item/display");
  pugi::xpath_node_set tools = rootNode_tools.evaluate_node_set(doc);
  for (pugi::xpath_node_set::const_iterator it = tools.begin(); 
        it !=  tools.end(); ++it)
  {
    pugi::xpath_node node = *it;
    string url = (char*)node.node().attribute((pugi::char_t*)"url").value();
    string title = (char*)node.node().attribute((pugi::char_t*)"title").value();
    LOG(INFO) << "url: " << url
      << ", title: " << title;
  }
}

#if 0
TEST(TinyXml, TinyXml_parse) {
  std::string xmlContentStr;
  file::File::ReadFileToString("/tmp/test.xml",
        &xmlContentStr);
      
  TiXmlDocument xmlDocument;
  xmlDocument.Parse(xmlContentStr.c_str());
  TiXmlElement* rootElement = xmlDocument.RootElement();
  VLOG(4) << "RootName: " << rootElement->Value();
  TiXmlPrinter printer;
  xmlDocument.Accept(&printer);
  VLOG(4) << "xmlContent: " << printer.CStr();
#if 0
  TinyXPath::xpath_processor xproc(xmain, "//title");
  unsigned  nodeCount = xproc.u_compute_xpath_node_set();
  if (nodeCount == 0) {
    return;
  }
  TiXmlNode * xnode = xproc.XNp_get_xpath_node(0);
  LOG(INFO) << "title: " << xnode->Value();
#endif
  TiXmlDocument* pDoc = new TiXmlDocument;
  TiXmlDeclaration* pDeclaration = new TiXmlDeclaration("1.0", "utf-8", "");
  pDoc->LinkEndChild(pDeclaration);
  TiXmlElement* rootNode = new TiXmlElement(*rootElement);
  pDoc->LinkEndChild(rootNode);
  TiXmlPrinter pPrinter;
  pDoc->Accept(&pPrinter);
  VLOG(4) << "utfDeclaration xmlContent: " << pPrinter.CStr();
  delete pDoc;
}
#endif

TEST(CurlFetcher, FetchPageContent_hubtest) {
  CurlFetcher curlFetcher;
  for (int i = 0; i < 8; i++) {
    sleep(1);
    time_t currentTimet = time(NULL);
    struct timeval currentTimeval;
    gettimeofday(&currentTimeval, NULL);
    long currentTimeMs = (long)currentTimeval.tv_sec * 1000 + (long)currentTimeval.tv_usec / 1000;
    LOG(INFO) << "currentTimet: " << currentTimet
      << ", currentTimeMs: " << currentTimeMs;
    std::string hub_page_url = FLAGS_url_common_prefix + "&openid=";
    hub_page_url += FLAGS_openid;
    hub_page_url += "&page=";
    hub_page_url += IntToString(i + 1);
    hub_page_url += "&t=";
    hub_page_url += Int64ToString(currentTimeMs);
    LOG(INFO) << "HubPageUrl: " << hub_page_url;

    std::string pageContent;
    if (!curlFetcher.FetchPageContent(hub_page_url,
            pageContent)) {
      LOG(ERROR) << "FetchPageContent error "
        << "with pageNum: " << i + 1
        << ", pageUrl: " << hub_page_url;
      continue;
    }

    file::File::WriteStringToFile(pageContent,
          "/tmp/hub_" + IntToString(i) + ".html");

    std::string startPattern = "sogou.weixin.gzhcb(";
    std::string endPattern = "})";
    int startIndex = pageContent.find(startPattern) + startPattern.size();
    int endIndex = pageContent.rfind(endPattern);
    std::string pageContent_ = pageContent.substr(startIndex,
          endIndex - startIndex + 1);
    Json::Reader reader;
    Json::Value contentJsonValue;
    if (!reader.parse(pageContent_, contentJsonValue)) {
      LOG(ERROR) << "Parse pageContent json error";
      continue;
    }
    LOG(INFO) << "page: " << contentJsonValue["page"].asInt();
    LOG(INFO) << "totalItems: " << contentJsonValue["totalItems"].asInt();
    LOG(INFO) << "totalPages: " << contentJsonValue["totalPages"].asInt();
    
    for (int iCnt = 0; iCnt < contentJsonValue["items"].size(); iCnt++) {
      std::string itemXmlContent = contentJsonValue["items"][iCnt].asString();
      LOG(INFO) << "page " << i << ", item " << iCnt
        << ", content: " << itemXmlContent;
     
#if 0
      pugi::xml_document doc;
      pugi::xml_parse_result result = doc.load_buffer(
            itemXmlContent.c_str(), itemXmlContent.size());
      if (!result) {
        LOG(ERROR) << "********************* page "
          << i << ", item " << iCnt << " pugixml parse error *****************";
        continue;
      }
#endif

      XmlXpath xmlXpath(itemXmlContent);
      std::vector<std::string> nodeValues;
      if (!xmlXpath.GetXmlNodeValues("//url",
            nodeValues)) {
        LOG(ERROR) << "-------------page " << i << ", item " << iCnt
          << " get xpathNode for url error ---------------";
        continue;
      }
      if (nodeValues.empty()) {
        continue;
      }
      LOG(INFO) << "page " << i << ", item " << iCnt
        << ". url = " << nodeValues[0];

      nodeValues.clear();
      if (!xmlXpath.GetXmlNodeValues("//title",
              nodeValues)) {
        LOG(ERROR) << "-------------page " << i << ", item " << iCnt
          << " get xpathNode for title error ---------------";
        continue;
      }
      if (nodeValues.empty()) {
       continue;
      }
      LOG(INFO) << "page " << i << ", item " << iCnt
        << ". title = " << nodeValues[0];
    }
  }
}
}  // namespace crawler
}  // namespace guanjia
