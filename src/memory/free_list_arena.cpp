#include "memory/free_list_arena.hpp"

namespace tundradb {

FreeListArena::FreeListArena(size_t initial_size, size_t min_fragment_size)
    : chunk_size_(initial_size),
      current_chunk_size_(0),
      min_fragment_size_(min_fragment_size) {
  allocate_new_chunk(chunk_size_);
}

void* FreeListArena::allocate(size_t size, size_t alignment) {
  size = align_up(size, alignment);

  void* reused_block = find_free_block(size);
  if (reused_block) {
    return reused_block;
  }

  return allocate_new_block(size, alignment);
}

void FreeListArena::deallocate(void* ptr) {
  if (!ptr) return;

  BlockHeader* header = get_block_header(ptr);

  assert(!header->is_free && "Double free detected");

  header->is_free = true;

  coalesce_blocks(ptr);

  freed_bytes_ += header->size;
  total_used_ -= header->size;
}

void FreeListArena::reset() {
  free_blocks_by_size_.clear();

  for (size_t i = 0; i < chunk_allocated_sizes_.size(); ++i) {
    chunk_allocated_sizes_[i] = 0;
  }

  current_offset_ = 0;
  if (!chunks_.empty()) {
    current_chunk_ = chunks_[0].get();
    current_chunk_size_ = chunk_sizes_[0];
  }

  total_used_ = 0;
  freed_bytes_ = 0;
}

void FreeListArena::clear() {
  chunks_.clear();
  chunk_sizes_.clear();
  chunk_allocated_sizes_.clear();
  free_blocks_by_size_.clear();
  current_chunk_ = nullptr;
  current_chunk_size_ = 0;
  current_offset_ = 0;
  total_allocated_ = 0;
  total_used_ = 0;
  freed_bytes_ = 0;
}

size_t FreeListArena::get_free_block_count() const {
  size_t count = 0;
  for (const auto& blocks : free_blocks_by_size_ | std::views::values) {
    count += blocks.size();
  }
  return count;
}

double FreeListArena::get_fragmentation_ratio() const {
  if (total_allocated_ == 0) return 0.0;
  return static_cast<double>(freed_bytes_) / total_allocated_;
}

void FreeListArena::allocate_new_chunk(size_t size) {
  auto new_chunk = std::make_unique<char[]>(size);
  current_chunk_ = new_chunk.get();
  current_chunk_size_ = size;
  chunks_.push_back(std::move(new_chunk));
  chunk_sizes_.push_back(size);
  chunk_allocated_sizes_.push_back(0);
  current_offset_ = 0;

  total_allocated_ += size;
}

char* FreeListArena::find_chunk_start(void* ptr) {
  char* char_ptr = static_cast<char*>(ptr);
  for (size_t i = 0; i < chunks_.size(); ++i) {
    char* chunk_start = chunks_[i].get();
    char* chunk_end = chunk_start + chunk_sizes_[i];
    if (char_ptr >= chunk_start && char_ptr < chunk_end) {
      return chunk_start;
    }
  }
  return nullptr;
}

BlockHeader* FreeListArena::find_prev_block(BlockHeader* target) {
  char* chunk_start = find_chunk_start(target);
  if (!chunk_start) return nullptr;

  char* target_ptr = reinterpret_cast<char*>(target);
  if (target_ptr == chunk_start) {
    return nullptr;
  }

  char* current_ptr = chunk_start;

  while (current_ptr < target_ptr) {
    BlockHeader* current = reinterpret_cast<BlockHeader*>(current_ptr);

    char* next_ptr = current_ptr + BlockHeader::HEADER_SIZE + current->size;

    if (next_ptr == target_ptr) {
      return current;
    }

    current_ptr = next_ptr;
  }

  return nullptr;
}

void* FreeListArena::allocate_new_block(size_t size, size_t alignment) {
  size_t aligned_size = align_up(size, alignment);

  size_t data_aligned_offset = calculate_aligned_offset(
      current_chunk_, current_offset_ + BlockHeader::HEADER_SIZE, alignment);

  size_t header_offset = data_aligned_offset - BlockHeader::HEADER_SIZE;
  size_t total_size = data_aligned_offset + aligned_size - current_offset_;

  if (current_chunk_ == nullptr ||
      current_offset_ + total_size > current_chunk_size_) {
    size_t needed_chunk_size =
        std::max(chunk_size_, total_size) + get_alignment_overhead(alignment);
    allocate_new_chunk(needed_chunk_size);

    data_aligned_offset = calculate_aligned_offset(
        current_chunk_, BlockHeader::HEADER_SIZE, alignment);
    header_offset = data_aligned_offset - BlockHeader::HEADER_SIZE;
    total_size = data_aligned_offset + aligned_size;
  }

  char* header_start = current_chunk_ + header_offset;
  BlockHeader* header = reinterpret_cast<BlockHeader*>(header_start);

  header->size = aligned_size;
  header->is_free = false;

  char* data_ptr = current_chunk_ + data_aligned_offset;

  current_offset_ = data_aligned_offset + aligned_size;
  total_used_ += aligned_size;

  chunk_allocated_sizes_.back() = current_offset_;

  return data_ptr;
}

void* FreeListArena::find_free_block(size_t size) {
  auto it = free_blocks_by_size_.lower_bound(size);
  if (it != free_blocks_by_size_.end()) {
    auto& blocks = it->second;
    if (!blocks.empty()) {
      BlockHeader* header = *blocks.begin();
      blocks.erase(blocks.begin());

      if (blocks.empty()) {
        free_blocks_by_size_.erase(it);
      }

      if (header->size > size + BlockHeader::HEADER_SIZE + min_fragment_size_) {
        split_block(header, size);
      }

      header->is_free = false;
      return reinterpret_cast<char*>(header) + BlockHeader::HEADER_SIZE;
    }
  }

  return nullptr;
}

void FreeListArena::split_block(BlockHeader* header, size_t needed_size) {
  size_t remaining_size = header->size - needed_size - BlockHeader::HEADER_SIZE;

  char* new_block_start =
      reinterpret_cast<char*>(header) + BlockHeader::HEADER_SIZE + needed_size;
  BlockHeader* new_header = reinterpret_cast<BlockHeader*>(new_block_start);

  new_header->size = remaining_size;
  new_header->is_free = true;

  header->size = needed_size;

  add_to_free_list(new_block_start + BlockHeader::HEADER_SIZE, remaining_size);
}

void FreeListArena::add_to_free_list(void* ptr, size_t size) {
  BlockHeader* header = get_block_header(ptr);
  free_blocks_by_size_[size].insert(header);
}

void FreeListArena::remove_block_from_free_list(BlockHeader* block) {
  auto it = free_blocks_by_size_.find(block->size);
  if (it != free_blocks_by_size_.end()) {
    it->second.erase(block);
    if (it->second.empty()) {
      free_blocks_by_size_.erase(it);
    }
  }
}

BlockHeader* FreeListArena::find_next_block(BlockHeader* header) {
  char* current_ptr = reinterpret_cast<char*>(header);
  char* next_ptr = current_ptr + BlockHeader::HEADER_SIZE + header->size;

  char* chunk_start = find_chunk_start(header);
  if (!chunk_start) {
    return nullptr;
  }

  size_t chunk_index = 0;
  for (size_t i = 0; i < chunks_.size(); ++i) {
    if (chunks_[i].get() == chunk_start) {
      chunk_index = i;
      break;
    }
  }

  char* chunk_allocated_end = chunk_start + chunk_allocated_sizes_[chunk_index];

  if (next_ptr + BlockHeader::HEADER_SIZE <= chunk_allocated_end) {
    return reinterpret_cast<BlockHeader*>(next_ptr);
  }

  return nullptr;
}

void FreeListArena::coalesce_blocks(void* ptr) {
  BlockHeader* header = get_block_header(ptr);

  BlockHeader* next = find_next_block(header);
  if (next && next->is_free) {
    remove_block_from_free_list(next);
    header->size += BlockHeader::HEADER_SIZE + next->size;
  }

  BlockHeader* prev = find_prev_block(header);
  if (prev && prev->is_free) {
    remove_block_from_free_list(prev);
    prev->size += BlockHeader::HEADER_SIZE + header->size;
    header = prev;
  }

  add_to_free_list(reinterpret_cast<char*>(header) + BlockHeader::HEADER_SIZE,
                   header->size);
}

}  // namespace tundradb
