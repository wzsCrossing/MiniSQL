#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
TableIterator::TableIterator(TableHeap *table_heap, RowId rid, Txn *txn) {
	this->table_heap_ = table_heap;
	this->row_id_ = rid;
	this->txn_ = txn;
	this->row_ = new Row(rid);
	if (table_heap != nullptr) {
		table_heap->GetTuple(this->row_, txn);
	}
}

TableIterator::TableIterator(const TableIterator &other) {
	this->table_heap_ = other.table_heap_;
	this->row_id_ = other.row_id_;
	this->txn_ = other.txn_;
	*(this->row_) = *(other.row_);
}

TableIterator::~TableIterator() {
	delete this->row_;
}

bool TableIterator::operator==(const TableIterator &itr) const {
	return this->row_id_ == itr.row_id_;
}

bool TableIterator::operator!=(const TableIterator &itr) const {
	return !(this->row_id_ == itr.row_id_);
}

const Row &TableIterator::operator*() {
	return *(this->row_);
}

Row *TableIterator::operator->() {
	return this->row_;
}

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
	this->table_heap_ = itr.table_heap_;
	this->row_id_ = itr.row_id_;
	this->txn_ = itr.txn_;
	*(this->row_) = *(itr.row_);
	return *this;
}

// ++iter
TableIterator &TableIterator::operator++() {
	RowId next_rid_;
	page_id_t page_id_ = row_id_.GetPageId();
	ASSERT(page_id_ != INVALID_PAGE_ID, "Invalid operation.");
	auto page_ = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(page_id_));

	page_->RLatch();
	if (page_->GetNextTupleRid(row_id_, &next_rid_)) {
		page_->RUnlatch();
		row_id_ = next_rid_;
		row_->SetRowId(row_id_);
		table_heap_->GetTuple(row_, txn_);
		table_heap_->buffer_pool_manager_->UnpinPage(page_id_, false);
		return *this;
	}

	do {
		page_->RUnlatch();
		table_heap_->buffer_pool_manager_->UnpinPage(page_id_, false);
		page_id_ = page_->GetNextPageId();
		page_ = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(page_id_));
		if (page_ != nullptr) page_->RLatch();
	} while (page_ != nullptr && !page_->GetFirstTupleRid(&next_rid_));

	if (page_ != nullptr) {
		page_->RUnlatch();
		table_heap_->buffer_pool_manager_->UnpinPage(page_id_, false);
		row_id_ = next_rid_;
		row_->SetRowId(row_id_);
		table_heap_->GetTuple(row_, txn_);
		return *this;
	} else {
		row_id_.Set(INVALID_PAGE_ID, 0);
		row_->SetRowId(row_id_);
		return *this;
	}
}

// iter++
TableIterator TableIterator::operator++(int) {
	TableHeap *table_heap = this->table_heap_;
	RowId row_id = this->row_id_;
	Txn *txn = this->txn_;
	++(*this);
	return TableIterator(table_heap, row_id, txn);
}
