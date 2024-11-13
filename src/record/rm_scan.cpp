/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "rm_scan.h"
#include "rm_file_handle.h"

/**
 * @brief 初始化file_handle和rid
 * @param file_handle
 */
RmScan::RmScan(const RmFileHandle *file_handle) : file_handle_(file_handle) {
    // Todo:
    // 初始化file_handle和rid（指向第一个存放了记录的位置）
    rid_ = Rid{RM_FIRST_RECORD_PAGE, -1};//rid_指向第一个存放记录的位置
    next();
}

/**
 * @brief 找到文件中下一个存放了记录的位置
 */
void RmScan::next() {
    // Todo:
    // 找到文件中下一个存放了记录的非空闲位置，用rid_来指向这个位置
    while( rid_.page_no < file_handle_ -> file_hdr_.num_pages ) {//遍历所有页面
        auto page_handler = file_handle_ -> fetch_page_handle(rid_.page_no);//对于当前页面
        //用bitmap来找bit为1的slot_no即存放了记录的位置
        rid_.slot_no = Bitmap::next_bit( true, page_handler.bitmap, file_handle_->file_hdr_.num_records_per_page, rid_.slot_no );

        if( rid_.slot_no < file_handle_ -> file_hdr_.num_records_per_page) {
            return;
        }//当前页面的所有slot都没有存放record，也就是当前页面没有没找过的记录了
        rid_ = Rid{ rid_.page_no + 1, -1 };//找下一个页面从头开始遍历
        if( rid_.page_no >= file_handle_ -> file_hdr_.num_pages ) {
            rid_ = Rid{RM_NO_PAGE, -1};
            break;
        }//遍历完了
    }
}

/**
 * @brief ​ 判断是否到达文件末尾
 */
bool RmScan::is_end() const {
    // Todo: 修改返回值
    return rid_.page_no == RM_NO_PAGE;//到末尾就返回1
    return false;
}

/**
 * @brief RmScan内部存放的rid
 */
Rid RmScan::rid() const {
    return rid_;
}