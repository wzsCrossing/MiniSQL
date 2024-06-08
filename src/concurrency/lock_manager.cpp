#include "concurrency/lock_manager.h"

#include <iostream>

#include "common/rowid.h"
#include "concurrency/txn.h"
#include "concurrency/txn_manager.h"

void LockManager::SetTxnMgr(TxnManager *txn_mgr) { txn_mgr_ = txn_mgr; }

/**
 * TODO: Student Implement
 */
bool LockManager::LockShared(Txn *txn, const RowId &rid) {
    std::unique_lock<std::mutex>lock(latch_);
    if(txn->GetIsolationLevel()==IsolationLevel::kReadUncommitted){
        txn->SetState(TxnState::kAborted);
        throw TxnAbortException(txn->GetTxnId(),AbortReason::kLockSharedOnReadUncommitted);
    }
    LockPrepare(txn,rid);
    LockRequestQueue &que=lock_table_[rid];
    que.EmplaceLockRequest(txn->GetTxnId(),LockMode::kShared);
    if(que.is_writing_){
       que.cv_.wait(lock,[&que,txn]{return (bool)(!que.is_writing_||txn->GetState()==TxnState::kAborted);});
    }
    CheckAbort(txn,que);
    txn->GetSharedLockSet().emplace(rid);
    que.sharing_cnt_++;
    que.req_list_iter_map_[txn->GetTxnId()]->granted_=LockMode::kShared;
    return true;
}

/**
 * TODO: Student Implement
 */
bool LockManager::LockExclusive(Txn *txn, const RowId &rid) {
    std::unique_lock<std::mutex>lock(latch_);
    LockPrepare(txn,rid);
    LockRequestQueue &que=lock_table_[rid];
    que.EmplaceLockRequest(txn->GetTxnId(),LockMode::kExclusive);
    if(que.is_writing_||que.sharing_cnt_>0){
        que.cv_.wait(lock,[&que,txn]{return (bool)((!que.is_writing_&&que.sharing_cnt_==0)||txn->GetState()==TxnState::kAborted);});
    }
    CheckAbort(txn,que);
    txn->GetExclusiveLockSet().emplace(rid);
    que.req_list_iter_map_[txn->GetTxnId()]->granted_=LockMode::kExclusive;
    que.is_writing_=true;
    return true;
}

/**
 * TODO: Student Implement
 */
bool LockManager::LockUpgrade(Txn *txn, const RowId &rid) {
    std::unique_lock<std::mutex>lock(latch_);
    LockPrepare(txn,rid);
    LockRequestQueue &que=lock_table_[rid];
    if(que.is_upgrading_){
        txn->SetState(TxnState::kAborted);
        throw TxnAbortException(txn->GetTxnId(),AbortReason::kUpgradeConflict);
    }
    auto it=que.GetLockRequestIter(txn->GetTxnId());
    if(it->lock_mode_==LockMode::kExclusive&&it->granted_==LockMode::kExclusive)return true;
    it->lock_mode_=LockMode::kExclusive;
    it->granted_=LockMode::kShared;
    if(que.is_writing_||que.sharing_cnt_>1){
        que.is_upgrading_=true;
        que.cv_.wait(lock,[&que,txn]{return (bool)((!que.is_writing_&&que.sharing_cnt_==1)||txn->GetState()==TxnState::kAborted);});
    }
    if(txn->GetState()==TxnState::kAborted)que.is_upgrading_=false;
    CheckAbort(txn,que);
    txn->GetSharedLockSet().erase(rid);
    txn->GetExclusiveLockSet().emplace(rid);
    que.sharing_cnt_--;
    it->granted_=LockMode::kExclusive;
    que.is_upgrading_=false;
    que.is_writing_=true;
    return true;
}

/**
 * TODO: Student Implement
 */
bool LockManager::Unlock(Txn *txn, const RowId &rid) {
    std::unique_lock<std::mutex>lock(latch_);
    LockRequestQueue &que=lock_table_[rid];
    if(txn->GetExclusiveLockSet().find(rid)!=txn->GetExclusiveLockSet().end())txn->GetExclusiveLockSet().erase(rid);
    if(txn->GetSharedLockSet().find(rid)!=txn->GetSharedLockSet().end())txn->GetSharedLockSet().erase(rid);
    auto it=que.GetLockRequestIter(txn->GetTxnId());
    bool res = que.EraseLockRequest(txn->GetTxnId());
    assert(res);
    if(it->lock_mode_==LockMode::kExclusive){
        que.is_writing_=0;
        que.cv_.notify_all();
    }
    else if(it->lock_mode_==LockMode::kShared){
        que.sharing_cnt_--;
        que.cv_.notify_all();
    }
    if(txn->GetState()==TxnState::kGrowing&&!(txn->GetIsolationLevel()==IsolationLevel::kReadCommitted&&it->lock_mode_==LockMode::kShared)){
        txn->SetState(TxnState::kShrinking);
    }

    return true;
}
/**
 * Transaction states for 2PL:
 *
 *     _________________________
 *    |                         v
 * GROWING -> SHRINKING -> COMMITTED   ABORTED
 *    |__________|________________________^
 *
 * Transaction states for Non-2PL:
 *     __________
 *    |          v
 * GROWING  -> COMMITTED     ABORTED
 *    |_________________________^
 *
 **/

/**
 * TODO: Student Implement
 */
void LockManager::LockPrepare(Txn *txn, const RowId &rid) {
    if(txn->GetState()==TxnState::kShrinking){
        txn->SetState(TxnState::kAborted);
        throw TxnAbortException(txn->GetTxnId(),AbortReason::kLockOnShrinking);
    }
    if(lock_table_.find(rid)!=lock_table_.end())lock_table_.emplace(std::piecewise_construct,std::forward_as_tuple(rid),std::forward_as_tuple());
}

/**
 * TODO: Student Implement
 */
void LockManager::CheckAbort(Txn *txn, LockManager::LockRequestQueue &req_queue) {
    if(txn->GetState()==TxnState::kAborted){
        req_queue.EraseLockRequest(txn->GetTxnId());
        throw TxnAbortException(txn->GetTxnId(),AbortReason::kDeadlock);
    }
}

/**
 * TODO: Student Implement
 */
void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
    waits_for_[t1].insert(t2);
}

/**
 * TODO: Student Implement
 */
void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
    waits_for_[t1].erase(t2);
}

/**
 * TODO: Student Implement
 */
bool LockManager::dfs(txn_id_t nw){
    if(visited_set_.find(nw)!=visited_set_.end()){
        revisited_node_=nw;
        return true;
    }
    visited_path_.push(nw);
    visited_set_.insert(nw);
    for(auto to:waits_for_[nw]){
        if(dfs(to))return true;
    }
    visited_path_.pop();
    visited_set_.erase(nw);
    return false;
}
bool LockManager::HasCycle(txn_id_t &newest_tid_in_cycle) {
    std::set<txn_id_t>all_pts;
    for(auto it:waits_for_){
        all_pts.insert(it.first);
        for(auto to:it.second)all_pts.insert(to);
    }
    while(!visited_path_.empty())visited_path_.pop();
    visited_set_.clear();
    for(auto st:all_pts){
        if(dfs(st)){
            newest_tid_in_cycle=revisited_node_;
            while(!visited_path_.empty()&&revisited_node_!=visited_path_.top()){
                newest_tid_in_cycle=std::max(newest_tid_in_cycle,visited_path_.top());
                visited_path_.pop();
            }
            return true;
        }
    }
    return false;

}

void LockManager::DeleteNode(txn_id_t txn_id) {
    waits_for_.erase(txn_id);

    auto *txn = txn_mgr_->GetTransaction(txn_id);

    for (const auto &row_id: txn->GetSharedLockSet()) {
        for (const auto &lock_req: lock_table_[row_id].req_list_) {
            if (lock_req.granted_ == LockMode::kNone) {
                RemoveEdge(lock_req.txn_id_, txn_id);
            }
        }
    }

    for (const auto &row_id: txn->GetExclusiveLockSet()) {
        for (const auto &lock_req: lock_table_[row_id].req_list_) {
            if (lock_req.granted_ == LockMode::kNone) {
                RemoveEdge(lock_req.txn_id_, txn_id);
            }
        }
    }
}

/**
 * TODO: Student Implement
 */
void LockManager::RunCycleDetection() {
    while(enable_cycle_detection_){
        std::this_thread::sleep_for(cycle_detection_interval_);
        std::unique_lock<std::mutex> lock(latch_);
        std::unordered_map<txn_id_t, RowId> que;
        for(auto &it:lock_table_){
            for(auto &req:it.second.req_list_){
                if(req.granted_!=LockMode::kNone)continue;
                que[req.txn_id_]=it.first;
                for(auto &from:it.second.req_list_){
                    if(from.granted_==LockMode::kNone)continue;
                    AddEdge(req.txn_id_,from.txn_id_);//req waits for from
                }
            }
        }
        txn_id_t del_pt=INVALID_TXN_ID;
        while(HasCycle(del_pt)){
            DeleteNode(del_pt);
            auto *txn=txn_mgr_->GetTransaction(del_pt);
            txn->SetState(TxnState::kAborted);
            lock_table_[que[del_pt]].cv_.notify_all();
        }
    }
}
/**
 * TODO: Student Implement
 */
std::vector<std::pair<txn_id_t, txn_id_t>> LockManager::GetEdgeList() {
    std::vector<std::pair<txn_id_t, txn_id_t>> result;
    for(auto &it:waits_for_){
        for(auto &to:it.second){
            result.push_back(std::make_pair(it.first,to));
        }
    }
    std::sort(result.begin(),result.end());
    return result;
}
