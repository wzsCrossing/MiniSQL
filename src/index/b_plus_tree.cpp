#include "index/b_plus_tree.h"

#include <string>

#include "glog/logging.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"

/**
 * TODO: Student Implement
 */
BPlusTree::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyManager &KM,
                     int leaf_max_size, int internal_max_size)
    : index_id_(index_id),
      buffer_pool_manager_(buffer_pool_manager),
      processor_(KM),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {

	if (leaf_max_size == UNDEFINED_SIZE) {
		leaf_max_size_ = (PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) / (processor_.GetKeySize() + sizeof(RowId)) - 1;
	}
	if (internal_max_size == UNDEFINED_SIZE) {
		internal_max_size_ = (PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / (processor_.GetKeySize() + sizeof(page_id_t)) - 1;
	}

	auto page = buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID);
	ASSERT(page != nullptr, "Index root page is nullptr.");
	auto roots_page = reinterpret_cast<IndexRootsPage *>(page->GetData());
	roots_page->GetRootId(index_id_, &root_page_id_);
	buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, false);
}

void BPlusTree::Destroy(page_id_t current_page_id) {
	if (current_page_id == INVALID_PAGE_ID) {
		current_page_id = root_page_id_;
	}

	auto page = buffer_pool_manager_->FetchPage(current_page_id);
	if (page == nullptr) {
		return;
	}

	auto node = reinterpret_cast<BPlusTreePage *>(page->GetData());
	if (!node->IsLeafPage()) {
		auto internal_node = reinterpret_cast<InternalPage *>(node);
		int size = internal_node->GetSize();
		for (int index = 0; index < size; ++index) {
			Destroy(internal_node->ValueAt(index));
		}
	}
	buffer_pool_manager_->UnpinPage(current_page_id, false);
	buffer_pool_manager_->DeletePage(current_page_id);

	if (current_page_id == root_page_id_) {
		root_page_id_ = INVALID_PAGE_ID;
		UpdateRootPageId(-1);
	}
}

/*
 * Helper function to decide whether current b+tree is empty
 */
bool BPlusTree::IsEmpty() const {
	return root_page_id_ == INVALID_PAGE_ID;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
bool BPlusTree::GetValue(const GenericKey *key, std::vector<RowId> &result, Txn *transaction) {
	auto page = FindLeafPage(key);
	if (page == nullptr) {
		return false;
	}

	auto leaf_node = reinterpret_cast<LeafPage *>(page->GetData());
	RowId value;
	if (leaf_node->Lookup(key, value, processor_)) {
		result.push_back(value);
		buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
		return true;
	} else {
		buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
		return false;
	}
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::Insert(GenericKey *key, const RowId &value, Txn *transaction) {
	if (IsEmpty()) {
		StartNewTree(key, value);
		return true;
	}
	return InsertIntoLeaf(key, value, transaction);
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
void BPlusTree::StartNewTree(GenericKey *key, const RowId &value) {
	auto root_page = buffer_pool_manager_->NewPage(root_page_id_);
	if (root_page == nullptr) {
		throw "out of memory";
	}

	auto leaf_node = reinterpret_cast<LeafPage *>(root_page->GetData());
	leaf_node->Init(root_page_id_, INVALID_PAGE_ID, processor_.GetKeySize(), leaf_max_size_);
	ASSERT(leaf_node->Insert(key, value, processor_) == 1, "Insert failed.");

	UpdateRootPageId(1);
	buffer_pool_manager_->UnpinPage(root_page_id_, true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::InsertIntoLeaf(GenericKey *key, const RowId &value, Txn *transaction) {
	auto page = FindLeafPage(key);
	if (page == nullptr) {
		return false;
	}

	auto leaf_node = reinterpret_cast<LeafPage *>(page->GetData());
	RowId temp;
	if (leaf_node->Lookup(key, temp, processor_)) {
		buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), false);
		return false;
	} else {
		if (leaf_node->Insert(key, value, processor_) > leaf_max_size_) {
			auto new_leaf_node = Split(leaf_node, transaction);
			InsertIntoParent(leaf_node, new_leaf_node->KeyAt(0), new_leaf_node, transaction);
			buffer_pool_manager_->UnpinPage(new_leaf_node->GetPageId(), true);
		}
		buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), true);
		return true;
	}
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
BPlusTreeInternalPage *BPlusTree::Split(InternalPage *node, Txn *transaction) {
	page_id_t new_page_id;
	auto new_page = buffer_pool_manager_->NewPage(new_page_id);
	if (new_page == nullptr) {
		throw "out of memory";
	}

	auto new_node = reinterpret_cast<InternalPage *>(new_page->GetData());
	new_node->Init(new_page_id, node->GetParentPageId(), processor_.GetKeySize(), internal_max_size_);
	node->MoveHalfTo(new_node, buffer_pool_manager_);
	return new_node;
}

BPlusTreeLeafPage *BPlusTree::Split(LeafPage *node, Txn *transaction) {
	page_id_t new_page_id;
	auto new_page = buffer_pool_manager_->NewPage(new_page_id);
	if (new_page == nullptr) {
		throw "out of memory";
	}

	auto new_node = reinterpret_cast<LeafPage *>(new_page->GetData());
	new_node->Init(new_page_id, node->GetParentPageId(), processor_.GetKeySize(), leaf_max_size_);
	node->MoveHalfTo(new_node);
	new_node->SetNextPageId(node->GetNextPageId());
	node->SetNextPageId(new_page_id);
	return new_node;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
void BPlusTree::InsertIntoParent(BPlusTreePage *old_node, GenericKey *key, BPlusTreePage *new_node, Txn *transaction) {
	if (old_node->IsRootPage()) {
		auto root_page = buffer_pool_manager_->NewPage(root_page_id_);
		if (root_page == nullptr) {
			throw "out of memory";
		}

		auto root_node = reinterpret_cast<InternalPage *>(root_page->GetData());
		root_node->Init(root_page_id_, INVALID_PAGE_ID, processor_.GetKeySize(), internal_max_size_);
		root_node->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
		old_node->SetParentPageId(root_page_id_);
		new_node->SetParentPageId(root_page_id_);
		buffer_pool_manager_->UnpinPage(root_page_id_, true);
		UpdateRootPageId();
	} else {
		page_id_t parent_page_id = old_node->GetParentPageId();
		auto parent_node = reinterpret_cast<InternalPage *>((buffer_pool_manager_->FetchPage(parent_page_id))->GetData());

		if (parent_node->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId()) > internal_max_size_) {
			auto new_internal_node = Split(parent_node, transaction);
			InsertIntoParent(parent_node, new_internal_node->KeyAt(0), new_internal_node, transaction);
			buffer_pool_manager_->UnpinPage(new_internal_node->GetPageId(), true);
		}
		buffer_pool_manager_->UnpinPage(parent_page_id, true);
	}
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
void BPlusTree::Remove(const GenericKey *key, Txn *transaction) {
	if (IsEmpty()) {
		return;
	}

	auto page = FindLeafPage(key);
	auto leaf_node = reinterpret_cast<LeafPage *>(page->GetData());
	int old_size = leaf_node->GetSize();
	if (old_size == leaf_node->RemoveAndDeleteRecord(key, processor_)) {
		buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), false);
	} else {
		CoalesceOrRedistribute(leaf_node, transaction);
	}
}

/* todo
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
template <typename N>
bool BPlusTree::CoalesceOrRedistribute(N *&node, Txn *transaction) {
	if (node->GetSize() >= node->GetMinSize()) {
		buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
		return false;
	}
	if (node->IsRootPage()) {
		return AdjustRoot(node);
	}

	page_id_t parent_page_id = node->GetParentPageId();
	auto parent_node = reinterpret_cast<InternalPage *>((buffer_pool_manager_->FetchPage(parent_page_id))->GetData());
	int index = parent_node->ValueIndex(node->GetPageId());
	int neighbor_index = (index == 0) ? 1 : index - 1;
	page_id_t neighbor_page_id = parent_node->ValueAt(neighbor_index);
	auto neighbor_node = reinterpret_cast<N *>((buffer_pool_manager_->FetchPage(neighbor_page_id))->GetData());
	if (node->GetSize() + neighbor_node->GetSize() > node->GetMaxSize()) {
		buffer_pool_manager_->UnpinPage(parent_page_id, false);
		Redistribute(neighbor_node, node, index);
		return false;
	} else {
		Coalesce(neighbor_node, node, parent_node, index, transaction);
		return true;
	}
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion happened
 */
bool BPlusTree::Coalesce(LeafPage *&neighbor_node, LeafPage *&node, InternalPage *&parent, int index,
                         Txn *transaction) {
	if (index == 0) {
		neighbor_node->MoveAllTo(node);
		parent->Remove(1);
		buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), false);
		buffer_pool_manager_->DeletePage(neighbor_node->GetPageId());
		buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
	} else {
		node->MoveAllTo(neighbor_node);
		parent->Remove(index);
		buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
		buffer_pool_manager_->DeletePage(node->GetPageId());
		buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true);
	}

	return CoalesceOrRedistribute(parent, transaction);
}

bool BPlusTree::Coalesce(InternalPage *&neighbor_node, InternalPage *&node, InternalPage *&parent, int index,
                         Txn *transaction) {
	if (index == 0) {
		neighbor_node->MoveAllTo(node, parent->KeyAt(1), buffer_pool_manager_);
		parent->Remove(1);
		buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), false);
		buffer_pool_manager_->DeletePage(neighbor_node->GetPageId());
		buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
	} else {
		node->MoveAllTo(neighbor_node, parent->KeyAt(index), buffer_pool_manager_);
		parent->Remove(index);
		buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
		buffer_pool_manager_->DeletePage(node->GetPageId());
		buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true);
	}

	return CoalesceOrRedistribute(parent, transaction);
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
void BPlusTree::Redistribute(LeafPage *neighbor_node, LeafPage *node, int index) {
	page_id_t parent_page_id = node->GetParentPageId();
	auto parent_node = reinterpret_cast<InternalPage *>((buffer_pool_manager_->FetchPage(parent_page_id))->GetData());
	
	if (index == 0) {
		neighbor_node->MoveFirstToEndOf(node);
		parent_node->SetKeyAt(1, neighbor_node->KeyAt(0));
	} else {
		neighbor_node->MoveLastToFrontOf(node);
		parent_node->SetKeyAt(index, node->KeyAt(0));
	}

	buffer_pool_manager_->UnpinPage(parent_page_id, true);
	buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
	buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true);	
}
void BPlusTree::Redistribute(InternalPage *neighbor_node, InternalPage *node, int index) {
	page_id_t parent_page_id = node->GetParentPageId();
	auto parent_node = reinterpret_cast<InternalPage *>((buffer_pool_manager_->FetchPage(parent_page_id))->GetData());
	
	if (index == 0) {
		neighbor_node->MoveFirstToEndOf(node, parent_node->KeyAt(1), buffer_pool_manager_);
		parent_node->SetKeyAt(1, neighbor_node->KeyAt(0));
	} else {
		neighbor_node->MoveLastToFrontOf(node, parent_node->KeyAt(index), buffer_pool_manager_);
		parent_node->SetKeyAt(index, node->KeyAt(0));
	}

	buffer_pool_manager_->UnpinPage(parent_page_id, true);
	buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
	buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true);
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
bool BPlusTree::AdjustRoot(BPlusTreePage *old_root_node) {
	if (old_root_node->IsLeafPage()) {
		root_page_id_ = INVALID_PAGE_ID;
	} else {
		auto internal_node = reinterpret_cast<InternalPage *>(old_root_node);
		root_page_id_ = internal_node->RemoveAndReturnOnlyChild();
		auto new_root_node = reinterpret_cast<BPlusTreePage *>((buffer_pool_manager_->FetchPage(root_page_id_))->GetData());
		new_root_node->SetParentPageId(INVALID_PAGE_ID);
		buffer_pool_manager_->UnpinPage(root_page_id_, true);
	}
	buffer_pool_manager_->UnpinPage(old_root_node->GetPageId(), false);
	buffer_pool_manager_->DeletePage(old_root_node->GetPageId());
	UpdateRootPageId(0);
	return true;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin() {
	auto page = FindLeafPage(nullptr, root_page_id_, true);
	page_id_t page_id = page->GetPageId();
	buffer_pool_manager_->UnpinPage(page_id, false);
	return IndexIterator(page_id, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin(const GenericKey *key) {
	auto page = FindLeafPage(key);
	auto leaf_node = reinterpret_cast<LeafPage *>(page->GetData());
	int index = leaf_node->KeyIndex(key, processor_);
	if (index == leaf_node->GetSize()) {
		page_id_t page_id = leaf_node->GetNextPageId();
		buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
		if (page_id == INVALID_PAGE_ID) {
			return End();
		} else {
			return IndexIterator(page_id, buffer_pool_manager_);
		}
	} else {
		page_id_t page_id = page->GetPageId();
		buffer_pool_manager_->UnpinPage(page_id, false);
		return IndexIterator(page_id, buffer_pool_manager_, index);
	}
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
IndexIterator BPlusTree::End() {
	return IndexIterator();
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 */
Page *BPlusTree::FindLeafPage(const GenericKey *key, page_id_t page_id, bool leftMost) {
	if (page_id == INVALID_PAGE_ID) {
		page_id = root_page_id_;
	}

	auto page = buffer_pool_manager_->FetchPage(page_id);
	if (page == nullptr) {
		return nullptr;
	}

	auto node = reinterpret_cast<BPlusTreePage *>(page->GetData());
	if (node->IsLeafPage()) {
		return page;
	} else {
		auto internal_node = reinterpret_cast<InternalPage *>(node);
		page_id_t child_page_id = leftMost ? internal_node->ValueAt(0) : internal_node->Lookup(key, processor_);
		buffer_pool_manager_->UnpinPage(page_id, false);
		return FindLeafPage(key, child_page_id, leftMost);
	}
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, current_page_id> into header page instead of
 * updating it.
 */
void BPlusTree::UpdateRootPageId(int insert_record) {
	auto page = buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID);
	ASSERT(page != nullptr, "Index root page is nullptr.");

	auto roots_page = reinterpret_cast<IndexRootsPage *>(page->GetData());
	if (insert_record == 1) {
		ASSERT(roots_page->Insert(index_id_, root_page_id_), "Insert record failed.");
	} else if (insert_record == -1) {
		ASSERT(roots_page->Delete(index_id_), "Delete record failed.");
	} else {
		ASSERT(roots_page->Update(index_id_, root_page_id_), "Update record failed.");
	}
	buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
}

/**
 * This method is used for debug only, You don't need to modify
 */
void BPlusTree::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out, Schema *schema) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
        << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      Row ans;
      processor_.DeserializeToKey(leaf->KeyAt(i), ans, schema);
      out << "<TD>" << ans.GetField(0)->toString() << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
        << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        Row ans;
        processor_.DeserializeToKey(inner->KeyAt(i), ans, schema);
        out << ans.GetField(0)->toString();
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out, schema);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 */
void BPlusTree::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
      bpm->UnpinPage(internal->ValueAt(i), false);
    }
  }
}

bool BPlusTree::Check() {
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned) {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}