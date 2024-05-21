#include "record/schema.h"

/**
 * TODO: Student Implement
 */
uint32_t Schema::SerializeTo(char *buf) const {
	// replace with your code here
	uint32_t offset = 0;

	MACH_WRITE_UINT32(buf, SCHEMA_MAGIC_NUM);
	offset += sizeof(uint32_t);

	MACH_WRITE_TO(bool, buf + offset, is_manage_);
	offset += sizeof(bool);

	uint32_t ColumnCount = GetColumnCount();
	MACH_WRITE_UINT32(buf + offset, ColumnCount);
	offset += sizeof(uint32_t);

	for (int i = 0; i < ColumnCount; ++i) {
		offset += columns_[i]->SerializeTo(buf + offset);
	}

	return offset;
}

uint32_t Schema::GetSerializedSize() const {
	// replace with your code here
	uint32_t offset = 2 * sizeof(uint32_t) + sizeof(bool);

	uint32_t ColumnCount = GetColumnCount();
	for (int i = 0; i < ColumnCount; ++i) {
		offset += columns_[i]->GetSerializedSize();
	}

	return offset;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema) {
	// replace with your code here
	uint32_t offset = 0;

	uint32_t magic_num_ = MACH_READ_UINT32(buf);
	ASSERT(magic_num_ == SCHEMA_MAGIC_NUM, "Schema magic number mismatch");
	offset += sizeof(uint32_t);

	bool is_manage_ = MACH_READ_FROM(bool, buf + offset);
	offset += sizeof(bool);

	uint32_t ColumnCount = MACH_READ_UINT32(buf + offset);
	offset += sizeof(uint32_t);

	std::vector<Column *> columns_;
	for (int i = 0; i < ColumnCount; ++i) {
		Column *column;
		offset += Column::DeserializeFrom(buf + offset, column);
		columns_.push_back(column);
	}

	schema = new Schema(columns_, is_manage_);

	return offset;
}