#include "catalog/catalog.h"

void CatalogMeta::SerializeTo(char *buf) const {
  ASSERT(GetSerializedSize() <= PAGE_SIZE, "Failed to serialize catalog metadata to disk.");
  MACH_WRITE_UINT32(buf, CATALOG_METADATA_MAGIC_NUM);
  buf += 4;
  MACH_WRITE_UINT32(buf, table_meta_pages_.size());
  buf += 4;
  MACH_WRITE_UINT32(buf, index_meta_pages_.size());
  buf += 4;
  for (auto iter : table_meta_pages_) {
    MACH_WRITE_TO(table_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
  for (auto iter : index_meta_pages_) {
    MACH_WRITE_TO(index_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf) {
  // check valid
  uint32_t magic_num = MACH_READ_UINT32(buf);
  buf += 4;
  ASSERT(magic_num == CATALOG_METADATA_MAGIC_NUM, "Failed to deserialize catalog metadata from disk.");
  // get table and index nums
  uint32_t table_nums = MACH_READ_UINT32(buf);
  buf += 4;
  uint32_t index_nums = MACH_READ_UINT32(buf);
  buf += 4;
  // create metadata and read value
  CatalogMeta *meta = new CatalogMeta();
  for (uint32_t i = 0; i < table_nums; i++) {
    auto table_id = MACH_READ_FROM(table_id_t, buf);
    buf += 4;
    auto table_heap_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->table_meta_pages_.emplace(table_id, table_heap_page_id);
  }
  for (uint32_t i = 0; i < index_nums; i++) {
    auto index_id = MACH_READ_FROM(index_id_t, buf);
    buf += 4;
    auto index_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->index_meta_pages_.emplace(index_id, index_page_id);
  }
  return meta;
}

/**
 * TODO: Student Implement
 */
uint32_t CatalogMeta::GetSerializedSize() const {
    uint32_t offset=12+8*(table_meta_pages_.size()+index_meta_pages_.size());
    return offset;
}

CatalogMeta::CatalogMeta() {}

/**
 * TODO: Student Implement
 */
CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
    : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager), log_manager_(log_manager) {

    next_index_id_.store(0);
    next_table_id_.store(0);
    if(init==true){
      catalog_meta_=new CatalogMeta();
    }else {
      Page *metapage=buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
      char *buffer=metapage->GetData();
      catalog_meta_=CatalogMeta::DeserializeFrom(buffer);
      buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID,false);
      for(auto it:catalog_meta_->table_meta_pages_){
        page_id_t table_page_id=it.second;
        Page *table_page=buffer_pool_manager_->FetchPage(table_page_id);
        char *buffer1=table_page->GetData();
        TableMetadata *tablemetadata;
        TableMetadata::DeserializeFrom(buffer1,tablemetadata);
        TableHeap *heap=TableHeap::Create(buffer_pool_manager_,tablemetadata->GetFirstPageId(),tablemetadata->GetSchema(),log_manager_,lock_manager_);
        table_names_[tablemetadata->GetTableName()]=tablemetadata->GetTableId();
        TableInfo *info=TableInfo::Create();
        info->Init(tablemetadata,heap);
        tables_[tablemetadata->GetTableId()]=info;
        buffer_pool_manager_->UnpinPage(table_page_id,false);
      }
      for(auto it:catalog_meta_->index_meta_pages_){
        page_id_t index_page_id=it.second;
        Page *index_page=buffer_pool_manager_->FetchPage(index_page_id);
        char *buffer2=index_page->GetData();
        IndexMetadata *indexmeta=nullptr;
        
        IndexMetadata::DeserializeFrom(buffer2,indexmeta);
        std::string table_name=tables_[indexmeta->GetTableId()]->GetTableName();
        std::string index_name=indexmeta->GetIndexName();
        index_names_[table_name][index_name]=indexmeta->GetIndexId();

        IndexInfo *info=IndexInfo::Create();
        info->Init(indexmeta,tables_[indexmeta->GetTableId()],buffer_pool_manager_);
        indexes_[indexmeta->GetIndexId()]=info;
        buffer_pool_manager_->UnpinPage(index_page_id,false);
      }
    }
}

CatalogManager::~CatalogManager() {
  FlushCatalogMetaPage();
  delete catalog_meta_;
  /*
   for (auto iter : tables_) {
    delete iter.second;
  }
  for (auto iter : indexes_) {
    delete iter.second;
  }
  */
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema, Txn *txn, TableInfo *&table_info) {
  if(table_names_.count(table_name)!=0){
    return DB_TABLE_ALREADY_EXIST;
  }
  table_id_t new_table_id=catalog_meta_->GetNextTableId();
  table_names_[table_name]=new_table_id;

  Schema* table_schema=TableSchema::DeepCopySchema(schema);
  page_id_t new_page_id;
  Page *new_table_page_meta=buffer_pool_manager_->NewPage(new_page_id);
  catalog_meta_->table_meta_pages_.emplace(new_table_id,new_page_id);
  TableHeap *new_heap=TableHeap::Create(buffer_pool_manager_,table_schema,txn,log_manager_,lock_manager_);

  page_id_t first_page=new_heap->GetFirstPageId();
  TableMetadata *table_meta=TableMetadata::Create(new_table_id,table_name,first_page,table_schema);
  char *buffer=new_table_page_meta->GetData();
  table_meta->SerializeTo(buffer);
  buffer_pool_manager_->UnpinPage(new_page_id,true);
  
  table_info =TableInfo::Create();
  table_info->Init(table_meta,new_heap);
  tables_.emplace(new_table_id,table_info);
  // ASSERT(false, "Not Implemented yet");
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  auto it=table_names_.find(table_name);
  if(it==table_names_.end())return DB_TABLE_NOT_EXIST;
  table_info=tables_[it->second];
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  tables.reserve(tables_.size());
  for(auto it:tables_){
    tables.push_back(it.second);
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Txn *txn, IndexInfo *&index_info,
                                    const string &index_type) {
  if(table_names_.count(table_name)==0)return DB_TABLE_NOT_EXIST;
  if(index_names_[table_name].count(index_name)!=0)return DB_INDEX_ALREADY_EXIST;
  TableInfo *table_info=tables_[table_names_[table_name]];
  Schema *schema=table_info->GetSchema();
  std::vector<uint32_t>key_map;
  for(auto it:index_keys){
    uint32_t col_index;
    if(schema->GetColumnIndex(it,col_index)!=DB_COLUMN_NAME_NOT_EXIST)key_map.push_back(col_index);
    else return DB_COLUMN_NAME_NOT_EXIST;
  }
  
  index_id_t index_id=catalog_meta_->GetNextIndexId();
  index_names_[table_name][index_name]=index_id;

  page_id_t page_id;
  Page *new_page_meta=buffer_pool_manager_->NewPage(page_id);
  buffer_pool_manager_->UnpinPage(page_id,false);
  catalog_meta_->index_meta_pages_.emplace(index_id,page_id);

  table_id_t table_id=table_names_[table_name];
  IndexMetadata *index_meta=IndexMetadata::Create(index_id,index_name,table_id,key_map);
  char *buffer =new_page_meta->GetData();
  index_meta->SerializeTo(buffer);

  index_info=IndexInfo::Create();
  index_info->Init(index_meta,table_info,buffer_pool_manager_);
  indexes_.emplace(index_id,index_info);
  
  TableIterator table_it=table_info->GetTableHeap()->Begin(txn);
  while(table_it!=table_info->GetTableHeap()->End()){
    TableIterator it=table_it++;
    Row row_=*it;
    Row key_row;
    row_.GetKeyFromRow(table_info->GetSchema(),index_info->GetIndexKeySchema(),key_row);
    dberr_t message=index_info->GetIndex()->InsertEntry(key_row,row_.GetRowId(),txn);
    if(message==DB_FAILED)return DB_FAILED;
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  if(table_names_.find(table_name)==table_names_.end())return DB_TABLE_NOT_EXIST;
  auto it=index_names_.at(table_name).find(index_name);
  if(it==index_names_.at(table_name).end())return DB_INDEX_NOT_FOUND;
  index_info=indexes_.at(it->second);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  if(index_names_.find(table_name)==index_names_.end())return DB_TABLE_NOT_EXIST;
  for(auto it:index_names_.at(table_name)){
    indexes.push_back(indexes_.at(it.second));
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropTable(const string &table_name) {
  auto it=table_names_.find(table_name);
  if(it==table_names_.end())return DB_TABLE_NOT_EXIST;
  std::vector<IndexInfo*> index;
  if(GetTableIndexes(table_name,index)==DB_SUCCESS){
    for(auto x:index){
      DropIndex(table_name,x->GetIndexName());
    }
  }
  table_id_t table_id=it->second;
  page_id_t table_page=catalog_meta_->table_meta_pages_[table_id];
  buffer_pool_manager_->DeletePage(table_page);
  catalog_meta_->table_meta_pages_.erase(table_id);
  tables_.erase(table_id);
  table_names_.erase(table_name);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  if(table_names_.find(table_name)==table_names_.end())return DB_TABLE_NOT_EXIST;
  auto iter=index_names_.at(table_name).find(index_name);
  if(iter==index_names_.at(table_name).end())return DB_INDEX_NOT_FOUND;
  index_id_t index_id=iter->second;
  page_id_t page_id=catalog_meta_->index_meta_pages_[index_id];
  buffer_pool_manager_->DeletePage(page_id);
  catalog_meta_->index_meta_pages_.erase(index_id);
  indexes_.erase(index_id);
  index_names_.at(table_name).erase(index_name);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::FlushCatalogMetaPage() const {
  Page* meta_page=buffer_pool_manager_->FetchPage(META_PAGE_ID);
  if(meta_page==nullptr)return DB_FAILED;
  char *buffer =meta_page->GetData();
  catalog_meta_->SerializeTo(buffer);
  buffer_pool_manager_->FlushPage(META_PAGE_ID);
  buffer_pool_manager_->UnpinPage(META_PAGE_ID,false);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  if(tables_.find(table_id)!=tables_.end())return DB_TABLE_ALREADY_EXIST;
  catalog_meta_->table_meta_pages_.emplace(table_id,page_id);
  Page * meta_page=buffer_pool_manager_->FetchPage(page_id);
  char *buffer=meta_page->GetData();
  TableMetadata* temp_meta;
  TableMetadata::DeserializeFrom(buffer,temp_meta);

  std::string table_name=temp_meta->GetTableName(); 
  table_names_[table_name]=table_id;

  TableInfo * temp_info=TableInfo::Create();
  Schema* temp_sch=temp_meta->GetSchema();
  TableHeap* temp_heap=TableHeap::Create(buffer_pool_manager_,page_id,temp_sch,log_manager_,lock_manager_);
  temp_info->Init(temp_meta,temp_heap);
  tables_[table_id]=temp_info;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  if(indexes_.find(index_id)!=indexes_.end())return DB_INDEX_ALREADY_EXIST;
  catalog_meta_->index_meta_pages_.emplace(index_id,page_id);
  IndexMetadata * index_meta;
  Page *index_page=buffer_pool_manager_->FetchPage(page_id);
  char *buffer=index_page->GetData();
  IndexMetadata::DeserializeFrom(buffer,index_meta);
  table_id_t table_id=index_meta->GetTableId();
  TableInfo * table_info=tables_[table_id];
  string table_name=table_info->GetTableName();
  index_names_[table_name][index_meta->GetIndexName()]=index_id;
  
  IndexInfo *index_info;
  index_info->Init(index_meta,table_info,buffer_pool_manager_);
  indexes_.emplace(index_id,index_info);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  if(tables_.find(table_id)==tables_.end())return DB_TABLE_NOT_EXIST;
  table_info=tables_.find(table_id)->second;
  return DB_SUCCESS;
}