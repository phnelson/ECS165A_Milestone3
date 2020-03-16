from os import path

from template.page import *
import pickle
import copy
import os.path
import threading
class BufferRange:

    def __init__(self):
        self.pageR = None
        self.dirty = 0
        self.pin = 0
        self.write_lock = 0
        self.range_data = None # THIS IS A PAGERANGE CLASS

    def requestWrite(self):
        while self.pin != 0:
            pass
        self.pin = -1
        return True

    def submitWrite(self):
        if self.pin == -1:
            self.pin = 0
            self.dirty = 1
            return True
        else:
            print("submit write without lock error")

    def incPin(self):
        self.pin += 1

    def unpin(self):
        self.pin -= 1

    def setDirty(self):
        self.dirty = 1

    def setNotDirty(self):
        self.dirty = 0

    def getPin(self):
        return self.pin

    def writeBack(self):
        if self.dirty == 1:
            #print("Writeback: True")
            return True
        else:
            #print("Writeback: False")
            return False

    def canEvict(self):
        if self.pin == 0:
            return True
        else:
            return False

    def delete(self):
        self.pageR = None
        self.dirty = 0
        self.pin = 0
        self.range_data = None

    def getRange(self):
        return self.range_data

    def setRange(self, prange):
        self.range_data = prange

    def getPageR(self):
        return self.pageR

    def setPageR(self, pageR):
        self.pageR = pageR

    def hasCapacityBase_Range(self):
        return self.range_data.hasCapacityBase()

    def hasCapacityTail_Range(self):
        return self.range_data.hasCapacityTail()

    def getIndirection_Range(self, page_block, offset):
        return self.range_data.getIndirection(page_block, offset)

    def nextBaseRid_Range(self):
        return self.range_data.nextBaseRid()

    def nextTailRid_Range(self):
        return self.range_data.nextTailRid()

    def readBlock_Range(self, page_block, offset):
        return self.range_data.readBlock(page_block, offset)

    def writeBaseBlock_Range(self, columns):
        return self.range_data.writeBaseBlock(columns)

    def writeTailBlock_Range(self, columns):
        return self.range_data.writeTailBlock(columns)

    def editBlock_Range(self, page_block, index, offset, value):
        return self.range_data.editBlock(page_block, index, offset, value)

    def deleteRecord_Range(self, page_block, offset):
        return self.range_data.deleteRecord(page_block, offset)

class BufferPoolRange:

    def __init__(self, buffer_size, num_columns):
        self.MRU = None # range from [0:buffer_size]
        self.num_columns = num_columns
        self.buffer_size = buffer_size
        self.buffer_dic = {}
        self.buffer_ranges = [] # range from [0:buffer_size]
        self.empty_ranges = []
        self.lock = threading.Lock()

        for i in range(0, buffer_size):
            self.buffer_ranges.append(BufferRange())

        self.next_available = 0 #[0, buffer_size - 1]

    def pageRToFileName(self, pageR):
        return '%d.prange' % pageR

        # Helper function for the translation of RID components to RID value
    def getRID(self, pageR, pageB, offset):
        return (pageR * (BASE_CONST + TAIL_CONST) * (PAGE_SIZE // COL_DATA_SIZE)) + (pageB * (PAGE_SIZE // COL_DATA_SIZE)) + offset

    def evictAll(self):
        # for all slots in buffer_pool
        # print("EvictAll: dic = ", self.buffer_dic)
        # print("dic size = ", len(self.buffer_dic))
        for curr_range in range(0, self.buffer_size):
            # if slot value is not empty
            if not self.buffer_ranges[curr_range].getPageR() is None:
                self.evictRange(curr_range)
            else:
                pass

    def evictRange(self, index):
        # loop until MRU is free to evict from pool
        loop = True
        curr_range = None

        while(loop):
            curr_range = self.buffer_ranges[index]
            loop = not curr_range.canEvict()

        if curr_range.writeBack():
            #print("WriteBack!")

            #with open(self.pageRToFileName(curr_range.getPageR()), 'wb') as output:
            # f = open('%d.prange' % curr_range.getPageR(), 'wb')
            # pickle.dump(curr_range.getRange(), f, pickle.HIGHEST_PROTOCOL)

            f = open('%d.prange' % curr_range.getPageR(), 'wb')
            # with open(self.pageRToFileName(curr_range.getPageR()), 'wb') as output:
            pickle.dump(curr_range.getRange(), f, pickle.HIGHEST_PROTOCOL)

            # Pickling contained within a with block, as it closes the file automatically after exiting the block.
            # # with open('%d.prange' % curr_range.getPageR(), 'wb') as f:
            # with open(self.pageRToFileName(curr_range.getPageR(), 'wb')) as f:
                # pickle.dump(f, pickle.HIGHEST_PROTOCOL)
                # # pickle.dump(f)
        # print(self.buffer_dic)
        # print("Evicting range:", self.buffer_ranges[index].getPageR())
        self.buffer_dic.pop(self.buffer_ranges[index].getPageR())
        curr_range.delete()
        return index

    def flushRange(self, index):
        curr_range = self.buffer_ranges[index]

        if curr_range.writeBack():
            f = open('%d.prange' % curr_range.getPageR(), 'wb')
            pickle.dump(curr_range.getRange(), f, pickle.HIGHEST_PROTOCOL)

        self.buffer_ranges[index].setNotDirty()
        return True

    def loadRange(self, pageR):
        #print("Range Loader")
        dirty = False
        if len(self.buffer_dic) >= self.buffer_size:
            index = self.evictRange(self.MRU)
        else:
            index = len(self.buffer_dic)

        curr_range = None
        if path.exists(self.pageRToFileName(pageR)):
            #with open(self.pageRToFileName(pageR), 'rb') as input:
            with open('%d.prange' % pageR, 'rb') as input:
                curr_range = pickle.load(input)
        else:
            curr_range = PageRange(self.num_columns)
            dirty = True

        self.buffer_ranges[index].setPageR(pageR)
        self.buffer_ranges[index].setRange(curr_range)
        if dirty:
            # self.buffer_ranges[index].writeBack()
            self.buffer_ranges[index].setDirty()

        #print("Loading range:", pageR)
        self.buffer_dic[pageR] = index
        #print(self.buffer_dic)
        return index

    def preloadRange(self, pageR):
        self.lock.acquire()
        dirty = False
        retval = False

        index = self.buffer_dic.get(pageR)
        if index is not None:
            self.buffer_ranges[index].incPin()
            self.lock.release()
            return True

        for i in range(self.buffer_size):
            if retval is False:

                if self.buffer_ranges[i].canEvict():
                    self.evictRange(i)
                
                    curr_range = None
                    if path.exists(self.pageRToFileName(pageR)):
                        #with open(self.pageRToFileName(pageR), 'rb') as input:
                        with open('%d.prange' % pageR, 'rb') as input:
                            curr_range = pickle.load(input)
                    else:
                        curr_range = PageRange(self.num_columns)
                        dirty = True

                    self.buffer_ranges[i].setPageR(pageR)
                    self.buffer_ranges[i].setRange(curr_range)
                    self.buffer_ranges[i].incPin()
                    if dirty:
                        self.buffer_ranges[i].setDirty()
                    self.buffer_dic[pageR] = i
                    retval = True
                else:
                    pass
            else:
                pass

        self.lock.release()
        return retval

    def getRange(self, pageR):
        self.lock.acquire()

        index = self.buffer_dic.get(pageR)
        if index is None:
            index = self.loadRange(pageR)

        self.buffer_ranges[index].incPin()

        self.lock.release()
        return index

    def setRangeDirty(self, pageR):
        self.lock.acquire()

        index = self.buffer_dic.get(pageR)
        if index is None:
            print("Error, pageR to be set to dirty not found in bufferpool")
            self.lock.release()
            return False
        self.buffer_ranges[index].setDirty()
        self.lock.release()
        return True

    def setRangeNotDirty(self, pageR):
        self.lock.acquire()
        index = self.buffer_dic.get(pageR)
        if index is None:
            print("Error, pageR to be set to dirty not found in bufferpool")
            self.lock.release()
            return False
        self.buffer_ranges[index].setNotDirty()
        self.lock.release()
        return True

    def directFlushRange(self, pageR):
        self.lock.acquire()

        index = self.buffer_dic.get(pageR)
        if index is None:
            print("Error, pageR to be evicted not found in bufferpool")
            return False
        retval = self.flushRange(index)
        self.buffer_ranges[index].unpin()

        self.lock.release()
        return retval

    def loadMerge(self, pageR):
        index = self.getRange(pageR)
        deep_copy = copy.deepcopy(self.buffer_ranges[index].range_data)
        self.buffer_ranges[index].unpin()
        return deep_copy

    def submitMerge(self, pageR, new_copy, tails):
        index = self.getRange(pageR)
        self.buffer_ranges[index].requestWrite()

        for pageB in range (0, self.buffer_ranges[index].range_data.max_base):
            for offset in range (0, self.buffer_ranges[index].range_data.page_blocks[pageB].pages[0].num_records):
                if self.buffer_ranges[index].range_data.getRID(pageB, offset) == 0:
                    # most recent record is 0, meaning either empty to start or deleted record from DB
                    write_block = [0] * self.buffer_ranges[index].range_data.page_blocks[pageB].total
                    # print("if write_block", write_block)
                else:
                    write_block = new_copy.readBlock(pageB, offset)
                    write_block[RID_COLUMN] = self.getRID(pageR, pageB, offset)
                    curr_indir = self.buffer_ranges[index].range_data.getIndirection(pageB, offset)
                    if new_copy.getIndirection(pageB, offset) == curr_indir:
                        write_block[INDIRECTION_COLUMN] = 0
                    else:
                        # indir values are not the same, so a new entry has been entered
                        curr_indir = self.buffer_ranges[index].range_data.getInder()
                        # shifts indir
                        write_block[INDIRECTION_COLUMN] = curr_indir - (tails * (PAGE_SIZE / COL_DATA_SIZE))
                # print("writeBlock: ", write_block)
                self.buffer_ranges[index].range_data.editWholeBlock(pageB, offset, write_block)

        for tailIndex in range(0, new_copy.max_tail - tails):
            # print("base + tailIndex:", new_copy.max_base+tailIndex, " base+index+tails:", new_copy.max_base+tailIndex+tails)
            self.buffer_ranges[index].range_data.page_blocks[new_copy.max_base + tailIndex] = new_copy.page_blocks[new_copy.max_base + tailIndex + tails]
            self.buffer_ranges[index].range_data.tail_count -= 1

        self.buffer_ranges[index].submitWrite()
        mru = self.evictRange(index)
        self.MRU = mru
        return

    def hasCapacityBase_Pool(self, pageR):
        index = self.getRange(pageR)
        ret_value = self.buffer_ranges[index].hasCapacityBase_Range()
        self.buffer_ranges[index].unpin()
        self.MRU = index
        return ret_value

    def hasCapacityTail_Pool(self, pageR):
        index = self.getRange(pageR)
        ret_value = self.buffer_ranges[index].hasCapacityTail_Range()
        self.buffer_ranges[index].unpin()
        self.MRU = index
        return ret_value

    def getIndirection_Pool(self, pageR, page_block, offset):
        index = self.getRange(pageR)
        ret_value = self.buffer_ranges[index].getIndirection_Range(page_block, offset)
        self.buffer_ranges[index].unpin()
        self.MRU = index
        return ret_value

    def nextBaseRid_Pool(self, pageR):
        index = self.getRange(pageR)
        ret_value = self.buffer_ranges[index].nextBaseRid_Range()
        self.buffer_ranges[index].unpin()
        self.MRU = index
        return ret_value

    def nextTailRid_Pool(self, pageR):
        index = self.getRange(pageR)
        ret_value = self.buffer_ranges[index].nextTailRid_Range()
        self.buffer_ranges[index].unpin()
        self.MRU = index
        return ret_value

    def readBlock_Pool(self, pageR, page_block, offset):
        index = self.getRange(pageR)
        ret_value = self.buffer_ranges[index].readBlock_Range(page_block, offset)
        self.buffer_ranges[index].unpin()
        self.MRU = index
        return ret_value

    def writeBaseBlock_Pool(self, pageR, columns):
        index = self.getRange(pageR)
        ret_value = self.buffer_ranges[index].writeBaseBlock_Range(columns)
        self.buffer_ranges[index].setDirty()
        self.buffer_ranges[index].unpin()
        self.MRU = index
        return ret_value

    def writeTailBlock_Pool(self, pageR, columns):
        index = self.getRange(pageR)
        ret_value = self.buffer_ranges[index].writeTailBlock_Range(columns)
        self.buffer_ranges[index].setDirty()
        self.buffer_ranges[index].unpin()
        self.MRU = index
        return ret_value

    def editBlock_Pool(self, pageR, page_block, arg_index, offset, value):
        index = self.getRange(pageR)
        ret_value = self.buffer_ranges[index].editBlock_Range(page_block, arg_index, offset, value)
        self.buffer_ranges[index].setDirty()
        self.buffer_ranges[index].unpin()
        self.MRU = index
        return ret_value

    def deleteRecord_Pool(self, pageR, page_block, offset):
        index = self.getRange(pageR)
        ret_value = self.buffer_ranges[index].deleteRecord_Range(page_block, offset)
        self.buffer_ranges[index].unpin()
        self.MRU = index
        return ret_value