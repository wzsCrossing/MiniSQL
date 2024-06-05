#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages){
	page_capacity_ = num_pages;
	page_list_.clear();
	page_iter_.resize(num_pages);
	for (int i = 0; i < num_pages; ++i) {
		page_iter_[i] = page_list_.end();
	}
}

LRUReplacer::~LRUReplacer() = default;

/**
 * TODO: Student Implement
 */
bool LRUReplacer::Victim(frame_id_t *frame_id) {
	if (page_list_.empty()) {
		*frame_id = INVALID_FRAME_ID;
		return false;
	}

	*frame_id = page_list_.front();
	page_list_.pop_front();
	page_iter_[*frame_id] = page_list_.end();
	return true;
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Pin(frame_id_t frame_id) {
	if (page_iter_[frame_id] != page_list_.end()) {
		page_list_.erase(page_iter_[frame_id]);
		page_iter_[frame_id] = page_list_.end();
	}
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Unpin(frame_id_t frame_id) {
	if (page_iter_[frame_id] == page_list_.end()) {
		page_list_.push_back(frame_id);
		auto it = page_list_.end();
		page_iter_[frame_id] = --it;
	}
}

/**
 * TODO: Student Implement
 */
size_t LRUReplacer::Size() {
	return (size_t)page_list_.size();
}