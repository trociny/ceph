// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RBD_MIRROR_IMAGE_MAPPER_H
#define RBD_MIRROR_IMAGE_MAPPER_H

#include <map>
#include <string>
#include <vector>

#include "common/Mutex.h"
#include "types.h"

namespace librbd { class ImageCtx; }

namespace rbd {
namespace mirror {

template <typename ImageCtxT = librbd::ImageCtx>
class ImageMapper {
public:
  static ImageMapper *create() {
    return new ImageMapper();
  }

  ImageMapper();

  void update(const std::string &instance_id, const ImageIds &image_ids,
              std::vector<std::string> *images_to_detach,
              std::vector<std::string> *images_to_attach);

  void attach(const std::string &instance_id,
              const std::string &global_image_id);
  void detach(const std::string &instance_id,
              const std::string &global_image_id);

private:
  enum State {
    ATTACHING,
    ATTACHED,
    DETACHING,
  };

  Mutex m_lock;
  std::map<std::string, std::map<std::string, State>> m_images;
};

} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::ImageMapper<librbd::ImageCtx>;

#endif // RBD_MIRROR_IMAGE_MAPPER_H
