#include "buffer/clock_replacer.h"

CLOCKReplacer::CLOCKReplacer(size_t num_pages){
	capacity = num_pages;
    size = 0;
    clock_hand = 0;
    clock_status.resize(num_pages);
    for (int i = 0; i < num_pages; ++i) {
        clock_status[i] = 2;
    }
}

CLOCKReplacer::~CLOCKReplacer() = default;

bool CLOCKReplacer::Victim(frame_id_t *frame_id) {
	if (size == 0) {
		*frame_id = INVALID_FRAME_ID;
		return false;
	}

    while (true) {
        if (clock_hand == capacity) {
            clock_hand = 0;
        }
        if (clock_status[clock_hand] == 1) {
            clock_status[clock_hand] = 0;
        } else if (clock_status[clock_hand] == 0) {
            *frame_id = clock_hand;
            clock_status[clock_hand] = 2;
            --size;
            ++clock_hand;
            return true;
        }
        ++clock_hand;
    }
}

void CLOCKReplacer::Pin(frame_id_t frame_id) {
	if (clock_status[frame_id] != 2) {
        clock_status[frame_id] = 2;
        --size;
    }
}

void CLOCKReplacer::Unpin(frame_id_t frame_id) {
    if (clock_status[frame_id] == 2) {
        clock_status[frame_id] = 1;
        ++size;
    }
}

size_t CLOCKReplacer::Size() {
    return size;
}