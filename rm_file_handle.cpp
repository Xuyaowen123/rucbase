/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "rm_file_handle.h"

/**
 * @description: 获取当前表中记录号为rid的记录
 * @param {Rid&} rid 记录号，指定记录的位置
 * @param {Context*} context
 * @return {unique_ptr<RmRecord>} rid对应的记录对象指针
 */
std::unique_ptr<RmRecord> RmFileHandle::get_record(const Rid& rid, Context* context) const {
    // Todo:
    // 1. 获取指定记录所在的page handle
    // 2. 初始化一个指向RmRecord的指针（赋值其内部的data和size）
    auto record = std::make_unique<RmRecord>(file_hdr_.record_size);
    auto pageHandler = fetch_page_handle(rid.page_no);//获取指定记录所在的page handle

    if( !Bitmap::is_set(pageHandler.bitmap, rid.slot_no) ) {
        throw RecordNotFoundError(rid.page_no, rid.slot_no);
    }
    //初始化一个指向RmRecord的指针（赋值其内部的data和size）
    record -> size = file_hdr_.record_size;
    //把位于指定slot的record拷贝一份，然后返回给上层。
    memcpy( record -> data, pageHandler.get_slot(rid.slot_no), file_hdr_.record_size );
    return record;
}

/**
 * @description: 在当前表中插入一条记录，不指定插入位置
 * @param {char*} buf 要插入的记录的数据
 * @param {Context*} context
 * @return {Rid} 插入的记录的记录号（位置）
 */
Rid RmFileHandle::insert_record(char* buf, Context* context) {
    // Todo:
    // 1. 获取当前未满的page handle
    // 2. 在page handle中找到空闲slot位置
    // 3. 将buf复制到空闲slot位置
    // 4. 更新page_handle.page_hdr中的数据结构
    // 注意考虑插入一条记录后页面已满的情况，需要更新file_hdr_.first_free_page_no
    auto pageHandler = create_page_handle();
    int slot_no = Bitmap::first_bit(false, pageHandler.bitmap, file_hdr_.num_records_per_page);//在page handle中找到空闲slot位置

    memcpy( pageHandler.get_slot(slot_no), buf, file_hdr_.record_size );//将buf（要插入数据的地址）复制到空闲slot位置
    Bitmap::set(pageHandler.bitmap, slot_no);//注意更新bitmap，它跟踪了每个slot是否存放了record；
    
    if( ++ pageHandler.page_hdr -> num_records == file_hdr_.num_records_per_page ) {//如果当前page handle中的page插入后已满，还需要更新file_hdr的第一个空闲页。
        file_hdr_.first_free_page_no = pageHandler.page_hdr -> next_free_page_no;
    }

    return Rid{pageHandler.page -> get_page_id().page_no, slot_no};
}

/**
 * @description: 在当前表中的指定位置插入一条记录
 * @param {Rid&} rid 要插入记录的位置
 * @param {char*} buf 要插入记录的数据
 */
void RmFileHandle::insert_record(const Rid& rid, char* buf) {
    if (rid.page_no < file_hdr_.num_pages) {
        create_new_page_handle();
    }
    RmPageHandle pageHandle = fetch_page_handle(rid.page_no);
    Bitmap::set(pageHandle.bitmap, rid.slot_no);
    pageHandle.page_hdr->num_records++;
    if (pageHandle.page_hdr->num_records == file_hdr_.num_records_per_page) {
        file_hdr_.first_free_page_no = pageHandle.page_hdr->next_free_page_no;
    }

    char *slot = pageHandle.get_slot(rid.slot_no);
    memcpy(slot, buf, file_hdr_.record_size);

    buffer_pool_manager_->unpin_page(pageHandle.page->get_page_id(), true);
}

/**
 * @description: 删除记录文件中记录号为rid的记录
 * @param {Rid&} rid 要删除的记录的记录号（位置）
 * @param {Context*} context
 */
void RmFileHandle::delete_record(const Rid& rid, Context* context) {
    // Todo:
    // 1. 获取指定记录所在的page handle
    // 2. 更新page_handle.page_hdr中的数据结构
    // 注意考虑删除一条记录后页面未满的情况，需要调用release_page_handle()
    auto pageHandler = fetch_page_handle(rid.page_no);//获取指定记录所在的page handle
    
    if( !Bitmap::is_set(pageHandler.bitmap, rid.slot_no) ) {//将page的bitmap中表示对应槽位的bit置0。
        throw RecordNotFoundError(rid.page_no, rid.slot_no);
    }

    Bitmap::reset(pageHandler.bitmap, rid.slot_no);//更新page_handle.page_hdr中的数据结构
    if( (pageHandler.page_hdr -> num_records ) -- == file_hdr_.num_records_per_page ) {
        release_page_handle(pageHandler);//如果删除操作导致该页面恰好从已满变为未满，那么需要调用release_page_handle()。
    }
}


/**
 * @description: 更新记录文件中记录号为rid的记录
 * @param {Rid&} rid 要更新的记录的记录号（位置）
 * @param {char*} buf 新记录的数据
 * @param {Context*} context
 */
void RmFileHandle::update_record(const Rid& rid, char* buf, Context* context) {
    // Todo:
    // 1. 获取指定记录所在的page handle
    // 2. 更新记录
    auto pageHandler = fetch_page_handle(rid.page_no);

    if( !Bitmap::is_set(pageHandler.bitmap, rid.slot_no) ) {
        throw RecordNotFoundError(rid.page_no, rid.slot_no);
    }
    memcpy( pageHandler.get_slot(rid.slot_no), buf, file_hdr_.record_size );
}

/**
 * 以下函数为辅助函数，仅提供参考，可以选择完成如下函数，也可以删除如下函数，在单元测试中不涉及如下函数接口的直接调用
*/
/**
 * @description: 获取指定页面的页面句柄
 * @param {int} page_no 页面号
 * @return {RmPageHandle} 指定页面的句柄
 */
RmPageHandle RmFileHandle::fetch_page_handle(int page_no) const {
    // Todo:
    // 使用缓冲池获取指定页面，并生成page_handle返回给上层
    // if page_no is invalid, throw PageNotExistError exception
    if( page_no >= file_hdr_.num_pages ) {//page_no无效
        throw PageNotExistError(disk_manager_ -> get_file_name(fd_), page_no);
    }
    return RmPageHandle(&file_hdr_, buffer_pool_manager_ -> fetch_page( {fd_, page_no} ));//获取RmPageHandle 返回给上层的page_handle
    // return RmPageHandle(&file_hdr_, nullptr);
}

/**
 * @description: 创建一个新的page handle
 * @return {RmPageHandle} 新的PageHandle
 */
RmPageHandle RmFileHandle::create_new_page_handle() {
     PageId pageid = {fd_, INVALID_FRAME_ID};
    Page* page = buffer_pool_manager_ -> new_page(&pageid);//使用缓冲池来创建一个新page

    auto pageHandler = RmPageHandle(&file_hdr_, page);

    if( page != nullptr ) {
        file_hdr_.num_pages ++;//更新file_hdr_
        file_hdr_.first_free_page_no = pageid.page_no;

        pageHandler.page_hdr -> next_free_page_no = RM_NO_PAGE;//更新page_hdr中的相关信息
        pageHandler.page_hdr -> num_records = 0;
        Bitmap::init(pageHandler.bitmap, file_hdr_.bitmap_size);//从地址pageHandler.bitmap开始的file_hdr_.bitmap_size个字节全部置0
    }
    
    return pageHandler;
}

/**
 * @brief 创建或获取一个空闲的page handle
 *
 * @return RmPageHandle 返回生成的空闲page handle
 * @note pin the page, remember to unpin it outside!
 */
RmPageHandle RmFileHandle::create_page_handle() {
    // Todo:
    // 1. 判断file_hdr_中是否还有空闲页
    //     1.1 没有空闲页：使用缓冲池来创建一个新page；可直接调用create_new_page_handle()
    //     1.2 有空闲页：直接获取第一个空闲页
    // 2. 生成page handle并返回给上层
    if(file_hdr_.first_free_page_no == RM_NO_PAGE) {
        return create_new_page_handle();//不存在空闲页，用create_new_page_handle()创建一个新的RmPageHandle
    }
    return fetch_page_handle(file_hdr_.first_free_page_no);//第一个空闲页存在，直接用fetch_page_handle()获取它；

}

/**
 * @description: 当一个页面从没有空闲空间的状态变为有空闲空间状态时，更新文件头和页头中空闲页面相关的元数据
 */
void RmFileHandle::release_page_handle(RmPageHandle&page_handle) {
    // Todo:
    // 当page从已满变成未满，考虑如何更新：
    // 1. page_handle.page_hdr->next_free_page_no
    // 2. file_hdr_.first_free_page_no
    page_handle.page_hdr -> next_free_page_no = file_hdr_.first_free_page_no;//更新page_hdr的下一个空闲页
    file_hdr_.first_free_page_no = page_handle.page -> get_page_id().page_no;//更新file_hdr的第一个空闲页    
}