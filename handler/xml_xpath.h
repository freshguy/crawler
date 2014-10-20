// Copyright (c) 2014 Guanjia Inc. All rights reserved.
// Author: wangjuntao@guanjia.com (Wang Juntao)

#ifndef GUANJIA_CRAWLER_HANDLER_XML_XPATH_H_
#define GUANJIA_CRAWLER_HANDLER_XML_XPATH_H_

#include <string>
#include <vector>
#include "base/flags.h"
#include "base/logging.h"
#include "third_party/libxml/parser.h"  
#include "third_party/libxml/xpath.h"


namespace guanjia {
namespace crawler {

class XmlXpath {
 public:
  explicit XmlXpath(const std::string& xmlContentStr);
  virtual ~XmlXpath();

  bool GetXmlNodeValues(
        const std::string& xpathExpr,
        std::vector<std::string>& nodeValues);
  
  bool GetXmlNodeValues(
        const std::string& xpathExpr,
        std::string& nodeValue);

 private:
  bool GetXmlXPathObjectPtr(const xmlChar* xpathExpr,
        xmlXPathObjectPtr& xpathObjectPtr);

 private:
  xmlDocPtr docPtr_;
  bool isOk_;
};

bool convert(const char* instr, const char *encoding,
      std::string& convertedXmlContent);

bool ConvertInput(const char *in, const char *encoding,
      std::string& convertedInput);
}  // namespace crawler
}  // namespace guanjia
#endif  // GUANJIA_CRAWLER_HANDLER_XML_XPATH_H_
