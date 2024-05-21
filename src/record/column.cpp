#include "record/column.h"

#include "glog/logging.h"

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)), type_(type), table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt:
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat:
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)),
      type_(type),
      len_(length),
      table_ind_(index),
      nullable_(nullable),
      unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other)
    : name_(other->name_),
      type_(other->type_),
      len_(other->len_),
      table_ind_(other->table_ind_),
      nullable_(other->nullable_),
      unique_(other->unique_) {}

/**
* TODO: Student Implement
*/
uint32_t Column::SerializeTo(char *buf) const {
	// replace with your code here
	uint32_t offset = 0;

	MACH_WRITE_UINT32(buf, COLUMN_MAGIC_NUM);
	offset += sizeof(uint32_t);

	uint32_t name_len_ = name_.length();
	MACH_WRITE_UINT32(buf + offset, name_len_);
	offset += sizeof(uint32_t);
	MACH_WRITE_STRING(buf + offset, name_);
	offset += name_len_ * sizeof(char);

	MACH_WRITE_TO(TypeId, buf + offset, type_);
	offset += sizeof(TypeId);

	MACH_WRITE_UINT32(buf + offset, len_);
	offset += sizeof(uint32_t);

	MACH_WRITE_UINT32(buf + offset, table_ind_);
	offset += sizeof(uint32_t);

	MACH_WRITE_TO(bool, buf + offset, nullable_);
	offset += sizeof(bool);

	MACH_WRITE_TO(bool, buf + offset, unique_);
	offset += sizeof(bool);

	return offset;
}

/**
 * TODO: Student Implement
 */
uint32_t Column::GetSerializedSize() const {
	// replace with your code here
	uint32_t offset = 4 * sizeof(uint32_t) + name_.length() * sizeof(char) + 2 * sizeof(bool) + sizeof(TypeId);
	return offset;
}

/**
 * TODO: Student Implement
 */
uint32_t Column::DeserializeFrom(char *buf, Column *&column) {
	// replace with your code here
	uint32_t offset = 0;

	uint32_t magic_num_ = MACH_READ_UINT32(buf);
	ASSERT(magic_num_ == COLUMN_MAGIC_NUM, "Wrong magic number.");
	offset += sizeof(uint32_t);

	uint32_t name_len_ = MACH_READ_UINT32(buf + offset);
	offset += sizeof(uint32_t);
	std::string name_(name_len_, ' ');
	for (int i = 0; i < name_len_; ++i) {
		name_[i] = MACH_READ_FROM(char, buf + offset);
		offset += sizeof(char);
	}

	TypeId type_ = MACH_READ_FROM(TypeId, buf + offset);
	offset += sizeof(TypeId);

	uint32_t len_ = MACH_READ_UINT32(buf + offset);
	offset += sizeof(uint32_t);

	uint32_t table_ind_ = MACH_READ_UINT32(buf + offset);
	offset += sizeof(uint32_t);

	bool nullable_ = MACH_READ_FROM(bool, buf + offset);
	offset += sizeof(bool);

	bool unique_ = MACH_READ_FROM(bool, buf + offset);
	offset += sizeof(bool);

	if (type_ == TypeId::kTypeChar) {
		column = new Column(name_, type_, len_, table_ind_, nullable_, unique_);
	} else {
		column = new Column(name_, type_, table_ind_, nullable_, unique_);
	}

	return offset;
}
