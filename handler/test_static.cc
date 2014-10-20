#include <string>
#include <iostream>
#include <vector>
#include <algorithm>
#include <iconv.h>


class TestStatic {
 public:
  TestStatic() {
    std::cout << "TestStatic constructor" << std::endl;
  }

  static void PrintStatic() {
    std::cout << "static function." << std::endl;
  }
};

int main(int argc, char* argv[]) {
  TestStatic::PrintStatic();

  iconv_t cd;
  cd = iconv_open("utf-8//IGNORE", argv[1]);
  if (cd == (iconv_t)-1) {
    std::cout << "Can't create convert descriptor for: "
      << argv[1] << std::endl;
  }

  std::vector<int> intVec;
  for (int i = 1; i < 100; i++) {
    intVec.push_back(i);
    std::cout << i << std::endl;
    // copyVec.push_back(i);
  }

  std::vector<int> copyVec(20);
  std::vector<int>::iterator it = intVec.begin();
  //std::vector<int> copyVec;
  std::copy(it, it + 10, copyVec.begin());
  for (int i = 0; i < copyVec.size(); i++) {
    std::cout << i << "th value: " << copyVec[i] << std::endl;
  }
  return 0;
}
