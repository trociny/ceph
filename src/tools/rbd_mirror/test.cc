// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/rbd/librbd.hpp"

#include "global/global_init.h"

#include <iostream>
#include <string>
#include <vector>

int main(int argc, const char **argv)
{
    std::vector<const char*> args;
    argv_to_vec(argc, argv, args);
    env_to_vec(args);

    if (argc < 4) {
        std::cerr << "usage: " << argv[0] << " <pool1> <pool2> <image>" << std::endl;
        return 1;
    }

    global_init(nullptr, args, CEPH_ENTITY_TYPE_CLIENT,
                CODE_ENVIRONMENT_DAEMON,
                CINIT_FLAG_UNPRIVILEGED_DAEMON_DEFAULTS);


    std::string pool1_name = argv[0];
    std::string pool2_name = argv[1];
    std::string image_name = argv[2];

    librados::Rados rados1(new librados::Rados());

    int r = rados1.init_with_context(g_ceph_context);
    if (r < 0) {
        std::cerr << "could not initialize rados handle" << std::endl;
        return 1;
    }

    r = rados1.connect();
    if (r < 0) {
        std::cerr << "error connecting to cluster" << std::endl;
        return 1;
    }

    librados::Rados rados2(new librados::Rados());

    r = rados2.init_with_context(g_ceph_context);
    if (r < 0) {
        std::cerr << "could not initialize rados handle" << std::endl;
        return 1;
    }

    r = rados2.connect();
    if (r < 0) {
        std::cerr << "error connecting to local cluster" << std::endl;
        return 1;
    }

    librados::IoCtx ioctx1;

    r = rados1.ioctx_create(pool1_name.c_str(), ioctx1);
    if (r < 0) {
        std::cerr << "error opening ioctx for local pool " << pool1_name
                  << ": " << cpp_strerror(r) << std::endl;
        return 1;
    }

    librados::IoCtx ioctx2;

    r = rados2.ioctx_create(pool2_name.c_str(), m_remote_ioctx);
    if (r < 0) {
        std::cerr << "error opening ioctx for remote pool " << pool2_name
                  << ": " << cpp_strerror(r) << std::endl;
        return 1;
    }

    librbd::RBD rbd1;
    librbd::Image image1;

    r = rbd1.open_read_only(ioctx, image1, image_name.c_str(), NULL);
    if (r < 0) {
        std::cerr << "error opening image " << image_name << ": "
                  << cpp_strerror(r) << std::endl;
        return 1;
    }

    librbd::RBD rbd2;
    librbd::Image image2;

    r = rbd2.open(ioctx, image2, image_name.c_str());
    if (r < 0) {
        std::cerr << "error opening image " << image_name << ": "
                  << cpp_strerror(r) << std::endl;
        return 1;
    }

    return 0;
}
