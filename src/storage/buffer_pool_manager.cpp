/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "buffer_pool_manager.h"

/**
 * @description: 从free_list或replacer中得到可淘汰帧页的 *frame_id
 * @return {bool} true: 可替换帧查找成功 , false: 可替换帧查找失败
 * @param {frame_id_t*} frame_id 帧页id指针,返回成功找到的可替换帧id
 */
bool BufferPoolManager::find_victim_page(frame_id_t *frame_id) {
    // Todo:
    // 1 使用BufferPoolManager::free_list_判断缓冲池是否已满需要淘汰页面
    // 1.1 未满获得frame
    // 1.2 已满使用lru_replacer中的方法选择淘汰页面
    
    if( free_list_.size() != 0 ) {//判断缓冲池是否已满需要淘汰页面
        //没满
        (*frame_id) = free_list_.front();//从free_list或replacer中得到可淘汰帧页的 *frame_id
        free_list_.pop_front();
        return true;//未满获得frame
    }
    //页面替换类
    return replacer_ -> victim( frame_id );//已满使用lru_replacer中的方法选择淘汰页面    
}


/**
 * @description: 更新页面数据, 如果为脏页则需写入磁盘，再更新为新页面，更新page元数据(data, is_dirty, page_id)和page
 * table
 * @param {Page*} page 写回页指针
 * @param {PageId} new_page_id 新的page_id
 * @param {frame_id_t} new_frame_id 新的帧frame_id
 */
void BufferPoolManager::update_page(Page *page, PageId new_page_id, frame_id_t new_frame_id) {
    // Todo:
    // 1 如果是脏页，写回磁盘，并且把dirty置为false
    // 2 更新page table
    // 3 重置page的data，更新page id
    // std::lock_guard<std::mutex> guard(latch_);

    // // 1. 如果是脏页，写回磁盘，并且把dirty置为false
    // if (page->is_dirty()) {
    //     int fd = page->get_page_id().fd;  // 获取文件描述符
    //     if (fd != -1) {
    //         disk_manager_->write_page(fd, page->get_page_id().page_no, page->get_data(), PAGE_SIZE);  // 写回磁盘
    //     }
    //     page->is_dirty_ = false;
    // }

    // // 2. 更新page table
    // page_table_.erase(page->get_page_id());
    // page_table_[new_page_id] = new_frame_id;

    // // 3. 重置page的data，更新page id
    // page->reset_memory();
    // page->id_ = new_page_id;
    // page->pin_count_ ++; // 假设更新后页面被固定在内存中
    if( page -> is_dirty() ) {//是脏页写回磁盘
        disk_manager_ -> write_page(page->get_page_id().fd, page->get_page_id().page_no, page->get_data(), PAGE_SIZE);
        page->is_dirty_ = false;
    }
    //把脏页写回磁盘，要更新page元数据(data, is_dirty, page_id)和page table
    //更新页面
    page -> reset_memory();
    //page_table_用于根据PageId定位其在BufferPool中的frame_id_t
    //更新页表
    auto it = page_table_.find(page -> id_);
    if( it != page_table_.end() ) page_table_.erase(it);
    page_table_[page -> id_ = new_page_id] = new_frame_id;

    if( page -> id_ .page_no != INVALID_FRAME_ID ) {
        disk_manager_ -> read_page(page->get_page_id().fd, page->get_page_id().page_no, page->get_data(), PAGE_SIZE);
    }    
}

/**
 * @description: 从buffer pool获取需要的页。
 *              如果页表中存在page_id（说明该page在缓冲池中），并且pin_count++。
 *              如果页表不存在page_id（说明该page在磁盘中），则找缓冲池victim
 * page，将其替换为磁盘中读取的page，pin_count置1。
 * @return {Page*} 若获得了需要的页则将其返回，否则返回nullptr
 * @param {PageId} page_id 需要获取的页的PageId
 */
Page *BufferPoolManager::fetch_page(PageId page_id) {
    // Todo:
    //  1.     从page_table_中搜寻目标页
    //  1.1    若目标页有被page_table_记录，则将其所在frame固定(pin)，并返回目标页。
    //  1.2    否则，尝试调用find_victim_page获得一个可用的frame，若失败则返回nullptr
    //  2.     若获得的可用frame存储的为dirty page，则须调用updata_page将page写回到磁盘
    //  3.     调用disk_manager_的read_page读取目标页到frame
    //  4.     固定目标页，更新pin_count_
    //  5.     返回目标页
    // std::lock_guard<std::mutex> guard(latch_);

    // // 1. 从page_table_中搜寻目标页
    // auto it = page_table_.find(page_id);
    // if (it != page_table_.end()) {
    //     // 1.1 若目标页有被page_table_记录，则将其所在frame固定(pin)，并返回目标页
    //     frame_id_t frame_id = it->second;
    //     Page *page = &pages_[frame_id];
    //     page->pin_count_++;
    //     replacer_->pin(frame_id);
    //     return page;
    // }

    // // 1.2 否则，尝试调用find_victim_page获得一个可用的frame，若失败则返回nullptr
    // frame_id_t victim_frame_id;
    // if (!find_victim_page(&victim_frame_id)) {
    //     return nullptr;
    // }

    // // 2. 若获得的可用frame存储的为dirty page，则须调用update_page将page写回到磁盘
    // Page *victim_page = &pages_[victim_frame_id];
    // if (victim_page->is_dirty()) {
    //     // int fd = disk_manager_->get_file_fd(victim_page->get_page_id());
    //     // if (fd != -1) {
    //     //     disk_manager_->write_page(fd, victim_page->get_page_id(), victim_page->get_data(), PAGE_SIZE);
    //     // }
    //     // victim_page->is_dirty_ = false;
    //     update_page(victim_page, page_id, victim_frame_id);
    // }

    // // 3. 调用disk_manager_的read_page读取目标页到frame
    // page_table_.erase(victim_page->id_);
    // page_table_[page_id] = victim_frame_id;
    // Page *new_page = victim_page;
    // disk_manager_->read_page(page_id.fd, page_id.page_no, new_page->data_, PAGE_SIZE);
    // // 4. 固定目标页，更新pin_count_
    // new_page->id_ = page_id;
    // new_page->pin_count_ ++;
    // new_page->is_dirty_ = false;
    // replacer_->pin(victim_frame_id);

    // // 5. 更新page table
    // page_table_[new_page->id_] = victim_frame_id;

    // // 6. 返回目标页
    // return new_page;
    std::lock_guard<std::mutex> guard(latch_);

    frame_id_t frame_id;

    if( page_table_.find(page_id) != page_table_.end() ) {//该page在缓冲池中
        frame_id = page_table_[page_id];//更新页表
    } else { //该page不在缓冲池中
        if( !find_victim_page(&frame_id) ) return nullptr;//用DiskManager从磁盘中读取。
        update_page(&pages_[frame_id], page_id, frame_id);//找缓冲池的淘汰页，将其替换为磁盘中读取的page
        //即把page从磁盘写入内存
    }

    replacer_ -> pin(frame_id);//固定页面
    pages_[frame_id].pin_count_ ++;//在缓冲池中，pin_count++

    return &pages_[frame_id];
}

/**
 * @description: 取消固定pin_count>0的在缓冲池中的page
 * @return {bool} 如果目标页的pin_count<=0则返回false，否则返回true
 * @param {PageId} page_id 目标page的page_id
 * @param {bool} is_dirty 若目标page应该被标记为dirty则为true，否则为false
 */
bool BufferPoolManager::unpin_page(PageId page_id, bool is_dirty) {
    // Todo:
    // 0. lock latch
    // 1. 尝试在page_table_中搜寻page_id对应的页P
    // 1.1 P在页表中不存在 return false
    // 1.2 P在页表中存在，获取其pin_count_
    // 2.1 若pin_count_已经等于0，则返回false
    // 2.2 若pin_count_大于0，则pin_count_自减一
    // 2.2.1 若自减后等于0，则调用replacer_的Unpin
    // 3 根据参数is_dirty，更改P的is_dirty_
    // std::lock_guard<std::mutex> guard(latch_);

    // // 1. 尝试在page_table_中搜寻page_id对应的页P
    // auto it = page_table_.find(page_id);
    // if (it == page_table_.end()) {
    //     // 1.1 P在页表中不存在 return false
    //     return false;
    // }

    // // 1.2 P在页表中存在，获取其pin_count_
    // frame_id_t frame_id = it->second;
    // Page *page = &pages_[frame_id];
    // int pin_count = page->pin_count_;

    // // 2.1 若pin_count_已经等于0，则返回false
    // if (pin_count <= 0) {
    //     return false;
    // }

    // // 2.2 若pin_count_大于0，则pin_count_自减一
    // page->pin_count_--;

    // // 2.2.1 若自减后等于0，则调用replacer_的Unpin
    // if (page->pin_count_ == 0) {
    //     replacer_->unpin(frame_id);
    // }

    // // 3. 根据参数is_dirty，更改P的is_dirty_
    // if (is_dirty) {
    //     page->is_dirty_ = true;
    // }
    // return true;
    std::lock_guard<std::mutex> guard(latch_);
    //尝试在page_table_中搜索页P的page_id
    if( page_table_.find(page_id) == page_table_.end() ) return false;//没找到，P在页表中不存在 return false
    //P在页表中存在
    frame_id_t frame_id = page_table_[page_id];

    Page *page = &pages_[frame_id];

    if( page -> pin_count_ <= 0 ) return false;
    if( page -> pin_count_ > 0 ) replacer_ -> unpin(frame_id);//减少页面的一次引用次数
    pages_[frame_id].pin_count_ --;
    //参数is_dirty决定是否对页面置脏，如果上层修改了页面，就将该页面的脏标志置true。
    page -> is_dirty_ |= is_dirty;//页面是否需要置脏

    return true;
}

/**
 * @description: 将目标页写回磁盘，不考虑当前页面是否正在被使用
 * @return {bool} 成功则返回true，否则返回false(只有page_table_中没有目标页时)
 * @param {PageId} page_id 目标页的page_id，不能为INVALID_PAGE_ID
 */
bool BufferPoolManager::flush_page(PageId page_id) {
    // Todo:
    // 0. lock latch
    // 1. 查找页表,尝试获取目标页P
    // 1.1 目标页P没有被page_table_记录 ，返回false
    // 2. 无论P是否为脏都将其写回磁盘。
    // 3. 更新P的is_dirty_
    // std::lock_guard<std::mutex> guard(latch_);

    // // 1. 查找页表,尝试获取目标页P
    // auto it = page_table_.find(page_id);
    // if (it == page_table_.end()|| page_id.page_no == INVALID_PAGE_ID) {
    //     // 1.1 目标页P没有被page_table_记录 ，返回false
    //     return false;
    // }

    // // 获取目标页的frame_id和page指针
    // frame_id_t frame_id = it->second;
    // Page *page = &pages_[frame_id];

    // // 2. 无论P是否为脏都将其写回磁盘
    // disk_manager_->write_page(page_id.fd, page_id.page_no, page->data_, PAGE_SIZE);

    // // 3. 更新P的is_dirty_
    // page->is_dirty_ = false;
    // return true;
    std::lock_guard<std::mutex> guard(latch_);
    //从内存往磁盘上写

    if( page_table_.find(page_id) == page_table_.end() ) return false;//页表查找

    Page *page = &pages_[page_table_[page_id]];//存在时如何写回磁盘
    //调用类disk_manager
    disk_manager_ -> write_page(page_id.fd, page_id.page_no, page->data_, PAGE_SIZE);

    page -> is_dirty_ = false;//写回后页面的脏位

    return true;
}

/**
 * @description: 创建一个新的page，即从磁盘中移动一个新建的空page到缓冲池某个位置。
 * @return {Page*} 返回新创建的page，若创建失败则返回nullptr
 * @param {PageId*} page_id 当成功创建一个新的page时存储其page_id
 */
Page *BufferPoolManager::new_page(PageId *page_id) {
    // 1.   获得一个可用的frame，若无法获得则返回nullptr
    // 2.   在fd对应的文件分配一个新的page_id
    // 3.   将frame的数据写回磁盘
    // 4.   固定frame，更新pin_count_
    // 5.   返回获得的page
    // std::lock_guard<std::mutex> guard(latch_);

    // // 1. 获得一个可用的frame，若无法获得则返回nullptr
    // bool is_all = true;
    // for(int i = 0; i < static_cast<int>(pool_size_); i++)
    // {
    //     if(pages_[i].pin_count_ == 0)
    //     {
    //         is_all = false;
    //         break;
    //     }
    // }
    // if (is_all)
    // {
    //     return nullptr;
    // }
    
    // frame_id_t frame_id;
    // if (!find_victim_page(&frame_id)) {
    //     throw InternalError("Failed to find a victim page.");
    //     return nullptr;
    // }

    // // 2. 在fd对应的文件分配一个新的page_id
    // page_id_t new_page_id = disk_manager_->allocate_page(page_id->fd);
    // if (new_page_id == INVALID_PAGE_ID) {
    //     throw InternalError("Failed to allocate a new page ID.");
    //     return nullptr;
    // }

    // // 3. 将frame的数据写回磁盘
    // Page *victim_page = &pages_[frame_id];
    // victim_page->id_.page_no = new_page_id;
    // victim_page->pin_count_++;
    // replacer_->pin(frame_id);
    // page_table_[victim_page->id_] = frame_id;
    // victim_page->is_dirty_ = false;
    // page_id->page_no = new_page_id;
    // disk_manager_->write_page(victim_page->id_.fd, victim_page->id_.page_no, victim_page->data_, PAGE_SIZE);
    // return victim_page;
    std::lock_guard<std::mutex> guard(latch_);

    frame_id_t frame_id;
    if( !find_victim_page(&frame_id) ) return nullptr;//没有找到淘汰页（如果缓冲池中的所有页面都被固定，则返回nullptr）
    //找到淘汰页了
    //从空闲列表或替换页中选择一个淘汰页P
    page_id -> page_no = disk_manager_ -> allocate_page( page_id -> fd );//在磁盘上分配一个页面
    
    //需要用DiskManager分配页面编号
    update_page(&pages_[frame_id], *page_id, frame_id);//更新淘汰页的的元数据，清空内存并将其添加到页表中

    replacer_ -> pin(frame_id);//固定页面
    pages_[frame_id].pin_count_ = 1;//pin_count设置为1。

    return &pages_[frame_id];//设置页面ID输出参数。返回一个指向P的指针。
}

/**
 * @description: 从buffer_pool删除目标页
 * @return {bool} 如果目标页不存在于buffer_pool或者成功被删除则返回true，若其存在于buffer_pool但无法删除则返回false
 * @param {PageId} page_id 目标页
 */
bool BufferPoolManager::delete_page(PageId page_id) {
    // 1.   在page_table_中查找目标页，若不存在返回true
    // 2.   若目标页的pin_count不为0，则返回false
    // 3.   将目标页数据写回磁盘，从页表中删除目标页，重置其元数据，将其加入free_list_，返回true
    // std::lock_guard<std::mutex> guard(latch_);

    // // 1. 在page_table_中查找目标页，若不存在返回true
    // auto it = page_table_.find(page_id);
    // if (it == page_table_.end()) {
    //     return true;
    // }

    // frame_id_t frame_id = it->second;
    // Page *target_page = &pages_[frame_id];

    // // 2. 若目标页的pin_count不为0，则返回false
    // if (target_page->pin_count_ > 0) {
    //     return false;
    // }

    // // 3. 将目标页数据写回磁盘
    // if(target_page->is_dirty_)
    // {
    //     this->flush_page(page_id);
    // }

    // // 4. 从页表中删除目标页
    // disk_manager_->deallocate_page(page_id.page_no);
    // page_table_.erase(page_id);

    // // 5. 重置其元数据
    // target_page->id_.page_no = INVALID_PAGE_ID;
    // target_page->reset_memory();
    // target_page->pin_count_ = 0;
    // target_page->is_dirty_ = false;

    // // 6. 将其加入free_list_
    // free_list_.push_back(frame_id);

    // return true;
    std::lock_guard<std::mutex> guard(latch_);
    //在页表中搜索请求的页(P)。
    if( page_table_.find(page_id) == page_table_.end() ) return true;//如果P不存在，返回true。
    frame_id_t frame_id = page_table_[page_id];

    Page *page = &pages_[frame_id];

    if( page -> pin_count_ > 0 ) return false;//如果P存在，但引脚计数非零，返回false。有人正在使用这个页面。
    //即不能删除
    
    //否则可以被删除，释放磁盘上的页。
    disk_manager_->deallocate_page(page_id.page_no);
	//重置其元数据并将其返回给free
    page_id.page_no =  INVALID_PAGE_ID;
	update_page(page, page_id, frame_id);
	free_list_.push_back(frame_id);
	
    return true;
}

/**
 * @description: 将buffer_pool中的所有页写回到磁盘
 * @param {int} fd 文件句柄
 */
void BufferPoolManager::flush_all_pages(int fd) {
    std::lock_guard<std::mutex> guard(latch_);  // 确保线程安全

    // // 遍历 buffer pool 中的所有页面
    // for (size_t i = 0; i < pool_size_; ++i) {
    //     // 获取当前页面
    //     Page *page = &pages_[i];
    //     if(!(page->id_.page_no == INVALID_PAGE_ID))
    //     {
    //         disk_manager_->write_page(page->id_.fd, page->id_.page_no, page->data_, PAGE_SIZE);
    //         page->is_dirty_ = false;
    //     }
    // }
    for (size_t i = 0; i < pool_size_; i++) {//存在于缓冲池的所有页面都刷新到磁盘
        Page *page = &pages_[i];//在磁盘上存储数据的结构
        if (page->get_page_id().fd == fd && page->get_page_id().page_no != INVALID_PAGE_ID) {
            disk_manager_->write_page(page->get_page_id().fd, page->get_page_id().page_no, page->get_data(), PAGE_SIZE);
            page->is_dirty_ = false;//淘汰脏页之前，都要将脏页写入磁盘。
        }
    }
}