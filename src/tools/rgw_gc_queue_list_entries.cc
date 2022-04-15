#include "include/types.h"

#include "cls/queue/cls_queue_types.h"
#include "cls/queue/cls_queue_ops.h"
#include "cls/rgw/cls_rgw_ops.h"
#include "cls/rgw_gc/cls_rgw_gc_types.h"
#include "cls/rgw_gc/cls_rgw_gc_ops.h"

#include <assert.h>
#include <err.h>
#include <stdio.h>
#include <stdarg.h>
#include <unistd.h>

#include <iostream>
#include <string>

#define GC_LIST_DEFAULT_MAX 128

using namespace std;

extern "C" {

#define CLS_LOG(level, fmt, ...)                                        \
  cls_log(level, "<cls> %s:%d: " fmt, __FILE__, __LINE__, ##__VA_ARGS__)

int cls_log(int level, const char *format, ...)
{
   int size = 256;
   va_list ap;
   while (1) {
     char buf[size];
     va_start(ap, format);
     int n = vsnprintf(buf, size, format, ap);
     va_end(ap);
#define MAX_SIZE 8196
     if ((n > -1 && n < size) || size > MAX_SIZE) {
       std::cerr << buf << std::endl;
       return n;
     }
     size *= 2;
   }
}

}

int cls_cxx_read2(int fd, uint64_t offset, uint64_t size, bufferlist *bl,
                  uint64_t flags)
{
    ceph_assert(flags == 0);

    int r = lseek(fd, offset, SEEK_SET);
    if (r < 0) {
        CLS_LOG(0, "ERROR: cls_cxx_read: lseek failed: %d", errno);
        return -errno;
    }

    return bl->read_fd(fd, size);
}


int cls_cxx_read(int fd, uint64_t offset, uint64_t size, bufferlist *bl)
{
    return cls_cxx_read2(fd, offset, size, bl, 0);
}

std::ostream& operator<<(std::ostream& out, cls_rgw_gc_obj_info info) {
    for (auto obj : info.chain.objs) {
        out << obj.pool << " " << obj.key.name << "\n";
    }

    return out;
}

int queue_read_head(int fd, cls_queue_head& head)
{
  uint64_t chunk_size = 1024 * 1024, start_offset = 0;

  bufferlist bl_head;
  const auto  ret = cls_cxx_read(fd, start_offset, chunk_size, &bl_head);
  if (ret < 0) {
    CLS_LOG(5, "ERROR: queue_read_head: failed to read head");
    return ret;
  }
  if (ret == 0) {
    CLS_LOG(20, "INFO: queue_read_head: empty head, not initialized yet");
    return -EINVAL;
  }

  //Process the chunk of data read
  auto it = bl_head.cbegin();
  // Queue head start
  uint16_t queue_head_start;
  try {
    decode(queue_head_start, it);
  } catch (const ceph::buffer::error& err) {
    CLS_LOG(0, "ERROR: queue_read_head: failed to decode queue start: %s", err.what());
    return -EINVAL;
  }
  if (queue_head_start != QUEUE_HEAD_START) {
    CLS_LOG(0, "ERROR: queue_read_head: invalid queue start");
    return -EINVAL;
  }

  uint64_t encoded_len;
  try {
    decode(encoded_len, it);
  } catch (const ceph::buffer::error& err) {
    CLS_LOG(0, "ERROR: queue_read_head: failed to decode encoded head size: %s", err.what());
    return -EINVAL;
  }

  if (encoded_len > (chunk_size - QUEUE_ENTRY_OVERHEAD)) {
    start_offset = chunk_size;
    chunk_size = (encoded_len - (chunk_size - QUEUE_ENTRY_OVERHEAD));
    bufferlist bl_remaining_head;
    const auto ret = cls_cxx_read2(fd, start_offset, chunk_size, &bl_remaining_head, CEPH_OSD_OP_FLAG_FADVISE_SEQUENTIAL);
    if (ret < 0) {
      CLS_LOG(5, "ERROR: queue_read_head: failed to read remaining part of head");
      return ret;
    }
    bl_head.claim_append(bl_remaining_head);
  }

  try {
    decode(head, it);
  } catch (const ceph::buffer::error& err) {
    CLS_LOG(0, "ERROR: queue_read_head: failed to decode head: %s", err.what());
    return -EINVAL;
  }

  return 0;
}

int queue_list_entries(int fd, const cls_queue_list_op& op, cls_queue_list_ret& op_ret, cls_queue_head& head)
{
  // If queue is empty, return from here
  if ((head.front.offset == head.tail.offset) && (head.front.gen == head.tail.gen)) {
    CLS_LOG(20, "INFO: queue_list_entries(): Next offset is %s", head.front.to_str().c_str());
    op_ret.next_marker = head.front.to_str();
    op_ret.is_truncated = false;
    return 0;
  }

  cls_queue_marker start_marker;
  start_marker.from_str(op.start_marker.c_str());
  cls_queue_marker next_marker = {0, 0};

  uint64_t start_offset = 0, gen = 0;
  if (start_marker.offset == 0) {
    start_offset = head.front.offset;
    gen = head.front.gen;
  } else {
    start_offset = start_marker.offset;
    gen = start_marker.gen;
  }

  op_ret.is_truncated = true;
  uint64_t chunk_size = 1024 * 1024;
  uint64_t contiguous_data_size = 0, size_to_read = 0;
  bool wrap_around = false;

  //Calculate length of contiguous data to be read depending on front, tail and start offset
  if (head.tail.offset > head.front.offset) {
    contiguous_data_size = head.tail.offset - start_offset;
  } else if (head.front.offset >= head.tail.offset) {
    if (start_offset >= head.front.offset) {
      contiguous_data_size = head.queue_size - start_offset;
      wrap_around = true;
    } else if (start_offset <= head.tail.offset) {
      contiguous_data_size = head.tail.offset - start_offset;
    }
  }

  CLS_LOG(10, "INFO: queue_list_entries(): front is: %s, tail is %s", head.front.to_str().c_str(), head.tail.to_str().c_str());

  bool offset_populated = false, entry_start_processed = false;
  uint64_t data_size = 0, num_ops = 0;
  uint16_t entry_start = 0;
  bufferlist bl;
  string last_marker;
  do
  {
    CLS_LOG(10, "INFO: queue_list_entries(): start_offset is %lu", start_offset);
  
    bufferlist bl_chunk;
    //Read chunk size at a time, if it is less than contiguous data size, else read contiguous data size
    if (contiguous_data_size > chunk_size) {
      size_to_read = chunk_size;
    } else {
      size_to_read = contiguous_data_size;
    }
    CLS_LOG(10, "INFO: queue_list_entries(): size_to_read is %lu", size_to_read);
    if (size_to_read == 0) {
      next_marker = head.tail;
      op_ret.is_truncated = false;
      CLS_LOG(20, "INFO: queue_list_entries(): size_to_read is 0, hence breaking out!\n");
      break;
    }

    auto ret = cls_cxx_read(fd, start_offset, size_to_read, &bl_chunk);
    if (ret < 0) {
      return ret;
    }

    //If there is leftover data from previous iteration, append new data to leftover data
    uint64_t entry_start_offset = start_offset - bl.length();
    CLS_LOG(20, "INFO: queue_list_entries(): Entry start offset accounting for leftover data is %lu", entry_start_offset);
    bl.claim_append(bl_chunk);
    bl_chunk = std::move(bl);

    CLS_LOG(20, "INFO: queue_list_entries(): size of chunk %u", bl_chunk.length());

    //Process the chunk of data read
    unsigned index = 0;
    auto it = bl_chunk.cbegin();
    uint64_t size_to_process = bl_chunk.length();
    do {
      CLS_LOG(10, "INFO: queue_list_entries(): index: %u, size_to_process: %lu", index, size_to_process);
      cls_queue_entry entry;
      ceph_assert(it.get_off() == index);
      //Use the last marker saved in previous iteration as the marker for this entry
      if (offset_populated) {
        entry.marker = last_marker;
      }
      //Populate offset if not done in previous iteration
      if (! offset_populated) {
        cls_queue_marker marker = {entry_start_offset + index, gen};
        CLS_LOG(5, "INFO: queue_list_entries(): offset: %s\n", marker.to_str().c_str());
        entry.marker = marker.to_str();
      }
      // Magic number + Data size - process if not done in previous iteration
      if (! entry_start_processed ) {
        if (size_to_process >= QUEUE_ENTRY_OVERHEAD) {
          // Decode magic number at start
          try {
            decode(entry_start, it);
          } catch (const ceph::buffer::error& err) {
            CLS_LOG(10, "ERROR: queue_list_entries: failed to decode entry start: %s", err.what());
            return -EINVAL;
          }
          if (entry_start != QUEUE_ENTRY_START) {
            CLS_LOG(5, "ERROR: queue_list_entries: invalid entry start %u", entry_start);
            return -EINVAL;
          }
          index += sizeof(uint16_t);
          size_to_process -= sizeof(uint16_t);
          // Decode data size
          try {
            decode(data_size, it);
          } catch (const ceph::buffer::error& err) {
            CLS_LOG(10, "ERROR: queue_list_entries: failed to decode data size: %s", err.what());
            return -EINVAL;
          }
        } else {
          // Copy unprocessed data to bl
          bl_chunk.splice(index, size_to_process, &bl);
          offset_populated = true;
          last_marker = entry.marker;
          CLS_LOG(10, "INFO: queue_list_entries: not enough data to read entry start and data size, breaking out!");
          break;
        }
        CLS_LOG(20, "INFO: queue_list_entries(): data size: %lu", data_size);
        index += sizeof(uint64_t);
        size_to_process -= sizeof(uint64_t);
      }
      // Data
      if (data_size <= size_to_process) {
        it.copy(data_size, entry.data);
        index += entry.data.length();
        size_to_process -= entry.data.length();
      } else {
        it.copy(size_to_process, bl);
        offset_populated = true;
        entry_start_processed = true;
        last_marker = entry.marker;
        CLS_LOG(10, "INFO: queue_list_entries(): not enough data to read data, breaking out!");
        break;
      }
      op_ret.entries.emplace_back(entry);
      // Resetting some values
      offset_populated = false;
      entry_start_processed = false;
      data_size = 0;
      entry_start = 0;
      num_ops++;
      last_marker.clear();
      if (num_ops == op.max) {
        CLS_LOG(10, "INFO: queue_list_entries(): num_ops is same as op.max, hence breaking out from inner loop!");
        break;
      }
    } while(index < bl_chunk.length());

    CLS_LOG(10, "INFO: num_ops: %lu and op.max is %lu\n", num_ops, op.max);

    if (num_ops == op.max) {
      next_marker = cls_queue_marker{(entry_start_offset + index), gen};
      CLS_LOG(10, "INFO: queue_list_entries(): num_ops is same as op.max, hence breaking out from outer loop with next offset: %lu", next_marker.offset);
      break;
    }

    //Calculate new start_offset and contiguous data size
    start_offset += size_to_read;
    contiguous_data_size -= size_to_read;
    if (contiguous_data_size == 0) {
      if (wrap_around) {
        start_offset = head.max_head_size;
        contiguous_data_size = head.tail.offset - head.max_head_size;
        gen += 1;
        wrap_around = false;
      } else {
        CLS_LOG(10, "INFO: queue_list_entries(): end of queue data is reached, hence breaking out from outer loop!");
        next_marker = head.tail;
        op_ret.is_truncated = false;
        break;
      }
    }
    
  } while(num_ops < op.max);

  //Wrap around next offset if it has reached end of queue
  if (next_marker.offset == head.queue_size) {
    next_marker.offset = head.max_head_size;
    next_marker.gen += 1;
  }
  if ((next_marker.offset == head.tail.offset) && (next_marker.gen == head.tail.gen)) {
    op_ret.is_truncated = false;
  }

  CLS_LOG(5, "INFO: queue_list_entries(): next offset: %s", next_marker.to_str().c_str());
  op_ret.next_marker = next_marker.to_str();

  return 0;
}


static int cls_rgw_gc_queue_list_entries(int fd)
{
  cls_queue_head head;
  auto ret = queue_read_head(fd, head);
  if (ret < 0) {
    return ret;
  }

  cls_rgw_gc_urgent_data urgent_data;
  if (head.bl_urgent_data.length() > 0) {
    auto iter_urgent_data = head.bl_urgent_data.cbegin();
    try {
      decode(urgent_data, iter_urgent_data);
    } catch (ceph::buffer::error& err) {
      CLS_LOG(5, "ERROR: cls_rgw_gc_queue_list_entries(): failed to decode urgent data\n");
      return -EINVAL;
    }
  }

  cls_queue_list_op list_op;  
  list_op.max = GC_LIST_DEFAULT_MAX;
  list_op.start_marker = "";

  cls_rgw_gc_list_ret list_ret;
  uint32_t num_entries = 0; //Entries excluding the deferred ones
  bool is_truncated = true;
  string next_marker;
  do {
    cls_queue_list_ret op_ret;
    int ret = queue_list_entries(fd, list_op, op_ret, head);
    if (ret < 0) {
      CLS_LOG(5, "ERROR: queue_list_entries(): returned error %d\n", ret);
      return ret;
    }
    is_truncated = op_ret.is_truncated;
    next_marker = op_ret.next_marker;
  
    if (op_ret.entries.size()) {
      for (auto it : op_ret.entries) {
        cls_rgw_gc_obj_info info;
        try {
          decode(info, it.data);
        } catch (ceph::buffer::error& err) {
          CLS_LOG(5, "ERROR: cls_rgw_gc_queue_list_entries(): failed to decode gc info\n");
          return -EINVAL;
        }
        std::cout << info << std::endl;
        list_ret.entries.emplace_back(info);
        num_entries++;
      }
      CLS_LOG(10, "INFO: cls_rgw_gc_queue_list_entries(): num_entries: %u and list_op.max: %u\n", num_entries, list_op.max);
      list_op.start_marker = op_ret.next_marker;
    } else {
      //We dont have data to process
      break;
    }
  } while(is_truncated);

  std::cerr << "XXXMG: is_truncated = " << is_truncated << std::endl;
  std::cerr << "XXXMG: next_marker = " << next_marker << std::endl;

  return 0;
}

int main(int argc, const char **argv)
{
    if (argc < 2) {
        std::cerr << "usage: " << argv[0] << " <gc_object> ..."  << std::endl;
        return 1;
    }

    for (int i = 1; i < argc; i++) {
        std::cerr << "processing " << argv[i] << "..." << std::endl;
        int fd = open(argv[i], O_RDONLY);
        if (fd < 0) {
            err(1, "failed to open %s", argv[i]);
        }
        int r = cls_rgw_gc_queue_list_entries(fd);
        if (r < 0) {
            return 1;
        }
        std::cerr << argv[i] << " done" << std::endl;
    }

    return 0;
}
