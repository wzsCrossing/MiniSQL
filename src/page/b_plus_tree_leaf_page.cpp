#include "page/b_plus_tree_leaf_page.h"

#include <algorithm>

#include "index/generic_key.h"

#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(RowId))
#define key_off 0
#define val_off GetKeySize()
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * TODO: Student Implement
 */
/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 * 未初始化next_page_id
 */
void LeafPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
	SetPageType(IndexPageType::LEAF_PAGE);
	SetKeySize(key_size);
	SetSize(0);
	SetMaxSize(max_size);
	SetNextPageId(INVALID_PAGE_ID);
	SetParentPageId(parent_id);
	SetPageId(page_id);
}

/**
 * Helper methods to set/get next page id
 */
page_id_t LeafPage::GetNextPageId() const {
  	return next_page_id_;
}

void LeafPage::SetNextPageId(page_id_t next_page_id) {
	next_page_id_ = next_page_id;
	if (next_page_id == 0) {
		LOG(INFO) << "Fatal error";
	}
}

/**
 * TODO: Student Implement
 */
/**
 * Helper method to find the first index i so that pairs_[i].first >= key
 * NOTE: This method is only used when generating index iterator
 * 二分查找
 */
int LeafPage::KeyIndex(const GenericKey *key, const KeyManager &KM) {
	int l = 0, r = GetSize() - 1, index = GetSize();
	while (l <= r) {
		int mid = (l + r) >> 1;
		if (KM.CompareKeys(KeyAt(mid), key) >= 0) {
			index = mid;
			r = mid - 1;
		} else {
			l = mid + 1;
		}
	}
	return index;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *LeafPage::KeyAt(int index) {
  	return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void LeafPage::SetKeyAt(int index, GenericKey *key) {
  	memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

RowId LeafPage::ValueAt(int index) const {
  	return *reinterpret_cast<const RowId *>(pairs_off + index * pair_size + val_off);
}

void LeafPage::SetValueAt(int index, RowId value) {
  	*reinterpret_cast<RowId *>(pairs_off + index * pair_size + val_off) = value;
}

void *LeafPage::PairPtrAt(int index) {
  	return KeyAt(index);
}

void LeafPage::PairCopy(void *dest, void *src, int pair_num) {
  	memcpy(dest, src, pair_num * (GetKeySize() + sizeof(RowId)));
}
/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a. array offset)
 */
std::pair<GenericKey *, RowId> LeafPage::GetItem(int index) { return {KeyAt(index), ValueAt(index)}; }

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return page size after insertion
 */
int LeafPage::Insert(GenericKey *key, const RowId &value, const KeyManager &KM) {
	int index = KeyIndex(key, KM);
	if (index < GetSize()) {
		PairCopy(PairPtrAt(index + 1), PairPtrAt(index), GetSize() - index);
	}
	SetKeyAt(index, key);
	SetValueAt(index, value);
	IncreaseSize(1);
	return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
void LeafPage::MoveHalfTo(LeafPage *recipient) {
	ASSERT(recipient != nullptr, "recipient is nullptr.");
	recipient->CopyNFrom(PairPtrAt((GetSize() + 1) / 2), GetSize() / 2);
	SetSize((GetSize() + 1) / 2);
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
void LeafPage::CopyNFrom(void *src, int size) {
	PairCopy(PairPtrAt(GetSize()), src, size);
	IncreaseSize(size);
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
bool LeafPage::Lookup(const GenericKey *key, RowId &value, const KeyManager &KM) {
	int index = KeyIndex(key, KM);
	if (index < GetSize() && KM.CompareKeys(KeyAt(index), key) == 0) {
		value = ValueAt(index);
		return true;
	}
	return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * existed, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return  page size after deletion
 */
int LeafPage::RemoveAndDeleteRecord(const GenericKey *key, const KeyManager &KM) {
	int index = KeyIndex(key, KM);
	if (index < GetSize() && KM.CompareKeys(KeyAt(index), key) == 0) {
		if (index < GetSize() - 1) {
			PairCopy(PairPtrAt(index), PairPtrAt(index + 1), GetSize() - 1 - index);
		}
		IncreaseSize(-1);
		return GetSize();
	} else {
		return GetSize();
	}
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
void LeafPage::MoveAllTo(LeafPage *recipient) {
	ASSERT(recipient != nullptr, "recipient is nullptr.");
	recipient->CopyNFrom(PairPtrAt(0), GetSize());
	recipient->SetNextPageId(GetNextPageId());
	SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 *
 */
void LeafPage::MoveFirstToEndOf(LeafPage *recipient) {
	ASSERT(recipient != nullptr, "recipient is nullptr.");
	ASSERT(GetSize() > 0, "Page is empty.");
	recipient->CopyLastFrom(KeyAt(0), ValueAt(0));
	if (GetSize() > 1) {
		PairCopy(PairPtrAt(0), PairPtrAt(1), GetSize() - 1);
	}
	IncreaseSize(-1);
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
void LeafPage::CopyLastFrom(GenericKey *key, const RowId value) {
	SetKeyAt(GetSize(), key);
	SetValueAt(GetSize(), value);
	IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
void LeafPage::MoveLastToFrontOf(LeafPage *recipient) {
	ASSERT(recipient != nullptr, "recipient is nullptr.");
	ASSERT(GetSize() > 0, "Page is empty.");
	recipient->CopyFirstFrom(KeyAt(GetSize() - 1), ValueAt(GetSize() - 1));
	IncreaseSize(-1);
}

/*
 * Insert item at the front of my items. Move items accordingly.
 *
 */
void LeafPage::CopyFirstFrom(GenericKey *key, const RowId value) {
	if (GetSize() > 0) {
		PairCopy(PairPtrAt(1), PairPtrAt(0), GetSize());
	}
	SetKeyAt(0, key);
	SetValueAt(0, value);
	IncreaseSize(1);
}