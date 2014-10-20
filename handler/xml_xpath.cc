// Copyright (c) 2014 Guanjia Inc. All rights reserved.
// Author: wangjuntao@guanjia.com (Wang Juntao)

#include <iconv.h>
#include "third_party/tinyxml/tinyxml.h"
#include "util/utf8_converter/utf8_converter.h"
#include "guanjia/crawler/handler/xml_xpath.h"

namespace guanjia {
namespace crawler {

XmlXpath::XmlXpath(const std::string& xmlContentStr) {
  /// Init libxml
  xmlInitParser();
  docPtr_ = NULL;
  isOk_ = true;
  
  if (xmlContentStr.empty()) {
    LOG(ERROR) << "Empty xmlContentStr when GetXmlNodeValues";
    isOk_ = false;
  }
  std::string _normalizeXmlContent = "<?xml version='1.0' encoding='utf-8' ?>";
  _normalizeXmlContent += xmlContentStr.substr(xmlContentStr.find('>') + 1);

#if 0
  std::string normalizeXmlContent;
  util::Utf8Converter converter;
  if (!converter.ConvertToUtf8(_normalizeXmlContent,
          &normalizeXmlContent)) {
    LOG(ERROR) << "Failed to convert to utf8 encoding when XmlXPath ";
    isOk_ = false;
  }
#endif

  std::string normalizeXmlContent = _normalizeXmlContent;
  try {
    docPtr_ = xmlParseMemory(normalizeXmlContent.c_str(),
          normalizeXmlContent.size());
    if (!docPtr_) {
      LOG(ERROR) << "xmlParseMemory error when XmlXPath "
        << "with xmlContentStr: " << normalizeXmlContent;
      isOk_ = false;
    }
  } catch(...) {
    LOG(ERROR) << "xmlParseMemory exception when XmlXpath";
    isOk_ = false;
  }
}

XmlXpath::~XmlXpath() {
  if (docPtr_) {
    xmlFreeDoc(docPtr_);
  }
  docPtr_ = NULL;
  /// Shutdown libxml
  xmlCleanupParser();
}
  
bool XmlXpath::GetXmlXPathObjectPtr(const xmlChar* xpathExpr,
      xmlXPathObjectPtr& xpathObjectPtr) {
  xpathObjectPtr = NULL;
  xmlXPathContextPtr context = xmlXPathNewContext(docPtr_);
  xmlXPathObjectPtr result
    = xmlXPathEvalExpression(xpathExpr, context);  
  xmlXPathFreeContext(context);
  if (!result) {
    LOG(ERROR) << "xmlXPathEvalExpression error when GetXmlXPathObjectPtr";
    return false;
  }
 
  if(xmlXPathNodeSetIsEmpty(result->nodesetval)) {
    LOG(ERROR) << "Empty NodeSet when GetXmlXPathObjectPtr";
    xmlXPathFreeObject(result);  
    return false;
  }
  xpathObjectPtr = result;
  return true;
}
  
bool XmlXpath::GetXmlNodeValues(
      const std::string& xpathExpr,
      std::vector<std::string>& nodeValues) {
  if (!isOk_) {
    LOG(ERROR) << "xmlDocPtr isn't OK";
    return false;
  }

  xmlXPathObjectPtr xpathObjectPtr = NULL;
  if (!GetXmlXPathObjectPtr(
          (const xmlChar*)xpathExpr.c_str(),
          xpathObjectPtr)) {
    LOG(ERROR) << "GetXmlXPathObjectPtr error when GetXmlNodeValues";
    return false;
  }

  xmlNodeSetPtr nodeSetPtr = xpathObjectPtr->nodesetval;
  for (int i = 0; i < nodeSetPtr->nodeNr; i++) {
    xmlChar* value = xmlNodeListGetString(docPtr_,
          nodeSetPtr->nodeTab[i]->xmlChildrenNode, 1);
    nodeValues.push_back(std::string((char*)value));
    xmlFree(value);
  }
  xmlXPathFreeObject(xpathObjectPtr);
  return true;
}
  
bool XmlXpath::GetXmlNodeValues(
      const std::string& xpathExpr,
      std::string& nodeValue) {
  if (!isOk_) {
    LOG(ERROR) << "xmlDocPtr isn't OK";
    return false;
  }

  xmlXPathObjectPtr xpathObjectPtr = NULL;
  if (!GetXmlXPathObjectPtr(
          (const xmlChar*)xpathExpr.c_str(),
          xpathObjectPtr)) {
    LOG(ERROR) << "GetXmlXPathObjectPtr error when GetXmlNodeValues";
    return false;
  }

  xmlNodeSetPtr nodeSetPtr = xpathObjectPtr->nodesetval;
  for (int i = 0; i < nodeSetPtr->nodeNr; i++) {
    xmlChar* value = xmlNodeListGetString(docPtr_,
          nodeSetPtr->nodeTab[i]->xmlChildrenNode, 1);
    nodeValue = (char*)value;
    xmlFree(value);
    break;  // get the first nodeValue 
  }
  xmlXPathFreeObject(xpathObjectPtr);
  return true;
}

bool convert(const char* instr, const char *encoding,
      std::string& convertedXmlContent) {
  xmlCharEncodingHandlerPtr handler; 
  xmlBufferPtr in, out; 
  handler = xmlFindCharEncodingHandler(encoding); 
  if(NULL != handler) {
    in = xmlBufferCreate(); 
    xmlBufferWriteChar(in, instr); 
    out = xmlBufferCreate(); 
    if(xmlCharEncInFunc(handler, out, in) < 0) {
      xmlBufferFree(in); 
      xmlBufferFree(out);
      LOG(ERROR) << "xmlCharEncInFunc error when convert";
      return false;
    } else {
      convertedXmlContent = (char*)out->content;
      xmlBufferFree(in); 
      xmlBufferFree(out);
      return true;
    } 
  } else {
    LOG(ERROR) << "xmlFindCharEncodingHandler error when convert";
    return false;
  }
}

bool ConvertInput(const char *in, const char *encoding,
      std::string& convertedInput) {
  if (in == NULL) {
    return false;
  }
  
  xmlChar *out;
  int ret;
  int size;
  int out_size;
  int temp;
  xmlCharEncodingHandlerPtr handler
    = xmlFindCharEncodingHandler(encoding);
  
  if (!handler) {
    LOG(ERROR) << "No encoding handler found when ConvertInput "
      << "with encoding = " << (encoding ? encoding : "");
    return false;
  }

  size = (int) strlen(in) + 1;
  out_size = size * 2 - 1;
  out = (unsigned char *) xmlMalloc((size_t) out_size);

  bool retFlag = true;
  if (out != 0) {
    temp = size - 1;
    ret = handler->input(out, &out_size, (const xmlChar *) in, &temp);
    if ((ret < 0) || (temp - size + 1)) {
      if (ret < 0) {
        LOG(ERROR) << "ConvertInput: conversion wasn't successful.";
      } else {
        LOG(ERROR) << "ConvertInput: conversion wasn't successful. converted: "
          << temp << " octets.";
      }
      out = 0;
      retFlag = false;
    } else {
      out = (unsigned char *) xmlRealloc(out, out_size + 1);
      out[out_size] = 0;  /* null terminating out */
      retFlag = true;
    }
    convertedInput = (char*)out;
    xmlFree(out);
  } else {
    LOG(ERROR) << "ConvertInput: no mem";
    retFlag = false;
  }
  return retFlag;
}
}  // namespace crawler
}  // namespace guanjia
