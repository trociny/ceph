// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#include <iostream>
#include <string>

#include "include/buffer.h"
#include "osd/SnapMapper.h"

int main(int argc, char *argv[]) {
  std::string encoded_object_snaps;
  while (std::getline(std::cin, encoded_object_snaps)) {
    bufferlist encoded_bl;
    encoded_bl.append(encoded_object_snaps);
    bufferlist bl;
    bl.decode_base64(encoded_bl);

    SnapMapper::object_snaps snaps;
    bufferlist::iterator it = bl.begin();
    ::decode(snaps, it);

    if (argc > 1 && argv[1] == std::string("-d")) {
      std::cerr << "clearing " << snaps.snaps.size() << " snap(s) for "
                << snaps.oid << std::endl;
    }
    snaps.snaps.clear();
    bl.clear();
    ::encode(snaps, bl);

    encoded_bl.clear();
    bl.encode_base64(encoded_bl);
    encoded_bl.append('\0');

    std::cout << encoded_bl.c_str() << std::endl;
  }
}
