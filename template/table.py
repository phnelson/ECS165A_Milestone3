from template.page import *
from template.buffer_range import *
from template.index import Index
from template.page import *
from queue import Queue

from time import time
# import thread
import sys
import threading


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

    # Getter for columns
    def getColumns(self):
        return self.columns

    # to_string() function that returns the data of Record
    def __str__(self):
        return str(self.columns)


class Table:
    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """

    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {} # Replace with index, and all references inside table and query with index API
        self.index = Index(self, self.num_columns)
        self.buffer_pool_range = BufferPoolRange(BUFFER_POOL_SIZE_RANGE, num_columns)
        # self.page_ranges = []
        # self.page_ranges.append(PageRange(self.num_columns))
        self.curr_page_range = 0
        self.insertRecord([0] * num_columns)
        #self.merge_queue = Queue()
        self.lock = threading.Lock()

        self.lock_manager_rids = {}
        self.lock_manager_pageRanges = {}

    def __str__(self):
            return self.name

    def lockReadRid(self, rid):
        self.lock.acquire()

        val = self.lock_manager_rids.get(rid)

        if val is None:
            self.lock_manager_rids[rid] = 0
            val = 0
        else:
            pass

        if val >= 0:
            self.lock_manager_rids[rid] = val + 1
            retval = True
        else:
            retval = False

        self.lock.release()

        return retval

    def unlockReadRid(self, rid):
        self.lock.acquire()

        val = self.lock_manager_rids.get(rid)

        if val is None:
            self.lock_manager_rids[rid] = 0
            val = 0
        else:
            pass

        if val > 0:
            self.lock_manager_rids[rid] = val - 1
            retval = True
        else:
            retval = False

        self.lock.release()
        
        return retval

    def lockWriteRid(self, rid):
        self.lock.acquire()

        val = self.lock_manager_rids.get(rid)

        if val is None:
            self.lock_manager_rids[rid] = 0
            val = 0
        else:
            pass

        if val == 0:
            self.lock_manager_rids[rid] = val - 1
            retval = True
        else:
            retval = False

        self.lock.release()
        
        return retval

    def unlockWriteRid(self, rid):
        self.lock.acquire()

        val = self.lock_manager_rids.get(rid)

        if val is None:
            self.lock_manager_rids[rid] = 0
            val = 0
        else:
            pass

        if val == -1:
            self.lock_manager_rids[rid] = val + 1
            retval = True
        else:
            retval = False

        self.lock.release()
        
        return retval

    def lockWriteRange(self, pageR):
        self.lock.acquire()

        val = self.lock_manager_pageRanges.get(pageR)

        if val is None:
            self.lock_manager_pageRanges[pageR] = 0
            val = 0
        else:
            pass

        if val == 0:
            self.lock_manager_pageRanges[pageR] = val - 1
            retval = True
        else:
            retval = False

        self.lock.release()
        
        return retval

    def unlockWriteRange(self, pageR):
        self.lock.acquire()

        val = self.lock_manager_pageRanges.get(pageR)

        if val is None:
            self.lock_manager_pageRanges[pageR] = 0
            val = 0
        else:
            pass

        if val == -1:
            self.lock_manager_pageRanges[pageR] = val + 1
            retval = True
        else:
            retval = False

        self.lock.release()
        
        return retval

    # Future function to merge tail records into base records
    def merge(self, pageR):
        # create deep copy of page range
        deep_copy = self.buffer_pool_range.loadMerge(pageR)
        tail_blocks = deep_copy.tail_count - 1 #indexes clear for removal, shift constant
        new_copy = PageRange(self.num_columns)
        read_block = None

        # for all base blocks in deep_copy page range
        for pageB in range(0, deep_copy.max_base):
            # for all base records in a base block
            for offset in range(0, deep_copy.page_blocks[pageB].pages[0].num_records):
                # check if records is invalid
                read_block = [0] * deep_copy.page_blocks[0].total
                # print("Empty read block:", read_block)
                if deep_copy.getRID(pageB, offset) == 0:
                    # empty record found, create empty record for writing
                    pass
                else:
                    # record found, check indir column for most up to date version
                    indir = deep_copy.getIndirection(pageB, offset)
                    if indir == 0:
                        # version is up to date, copy over from original base record
                        read_block = deep_copy.readBlock(pageB, offset)

                    else:
                        # version is not up to date, copy over from indir tail record
                        inder_pageB = self.getPageB(indir)
                        inder_offset = self.getOffset(indir)
                        read_block = deep_copy.readBlock(inder_pageB, inder_offset)
                        # set record indir column to the original indir value
                        read_block[INDIRECTION_COLUMN] = indir

                # print("read_block:", read_block)
                checker = new_copy.hasCapacityBase()
                if checker:
                    new_copy.writeBaseBlock(read_block)
                else:
                    print("Unexpected error, new_copy at capacity")

        self.buffer_pool_range.submitMerge(pageR, new_copy, tail_blocks)

    '''
    def enqueueMerge(self, pageR):
        self.merge_queue.enqueue

    def checkMerge(self):
        if self.merge_queue.empty():
            pass
        else:
            # run merge
            next = self.merge_queue.get()
            self.__merge(next)
            pass
    '''

    def createIndex(self, column_num):
        self.index.create_index(column_num)

    def getIndex(self, column_num):
        return self.index.getIndex(column_num)

    def close(self):
        self.buffer_pool_range.evictAll()

    def getName(self):
        return self.name

    # Creates a new PageRange if needed, and appends it to page_ranges
    def newPageRange(self):
        # self.page_ranges.append(PageRange(self.num_columns))
        # self.buffer_pool_range.
        self.curr_page_range = self.curr_page_range + 1

    # Helper function for the translation of RID value to RID components
    def getOffset(self, rid):
        return rid % (PAGE_SIZE // COL_DATA_SIZE)

    # Helper function for the translation of RID value to RID components
    def getPageR(self, rid):
        return rid // ((BASE_CONST + TAIL_CONST) * (PAGE_SIZE // COL_DATA_SIZE))

    # Helper function for the translation of RID value to RID components
    def getPageB(self, rid):
        return (rid // (PAGE_SIZE // COL_DATA_SIZE)) % (BASE_CONST + TAIL_CONST)

    # Helper function for the translation of RID components to RID value
    def getRID(self, pageR, pageB, offset):
        return (pageR * (BASE_CONST + TAIL_CONST) * (PAGE_SIZE // COL_DATA_SIZE)) + (
                    pageB * (PAGE_SIZE // COL_DATA_SIZE)) + offset

    # Helper function to find the value of the next RID before writing to basepages
    def nextBaseRid(self):
        # Calls for calculation of the first two RID components
        # #prerid = self.page_ranges[self.curr_page_range].nextBaseRid()
        prerid = self.buffer_pool_range.nextBaseRid_Pool(self.curr_page_range)
        # Calculates the last RID component and adds it together with the previous for the next base RID
        rid = self.curr_page_range * (BASE_CONST + TAIL_CONST) * (PAGE_SIZE // COL_DATA_SIZE) + prerid
        return rid

    # Helper function to find the value of the next tail RID before writing to tail pages
    def nextTailRid(self, pageR):
        # Calls for calculation of the first two RID components
        # #prerid = self.page_ranges[self.curr_page_range].nextTailRid()
        prerid = self.buffer_pool_range.nextTailRid_Pool(pageR)
        # print("prerid:", prerid)
        # Calculates the last RID component and adds it together with the previous for the next tail RID
        rid = pageR * (BASE_CONST + TAIL_CONST) * (PAGE_SIZE // COL_DATA_SIZE) + prerid
        return rid

    # Helper function unique for this metadata scheme
    def formatCols(self, indir, rid, timestamp, schema, columns):
        format_cols = []
        format_cols.append(indir)
        format_cols.append(rid)
        format_cols.append(timestamp)
        format_cols.append(schema)

        for index in range(self.num_columns):
            format_cols.append(columns[index])

        return format_cols

    # Function to set the RID of a record to the invalid value
    def deleteRecord(self, rid):
        pageR = self.getPageR(rid)
        pageB = self.getPageB(rid)
        offset = self.getOffset(rid)
        # #self.page_ranges[pageR].deleteRecord(pageB, offset)
        self.buffer_pool_range.deleteRecord_Pool(pageR, pageB, offset)

    # Function to check the indirection value of a record before doing a full read
    def checkIndirection(self, rid):
        pageR = self.getPageR(rid)
        pageB = self.getPageB(rid)
        offset = self.getOffset(rid)
        # print(pageR, pageB, offset)
        # #indir = self.page_ranges[pageR].getIndirection(pageB, offset)
        indir = self.buffer_pool_range.getIndirection_Pool(pageR, pageB, offset)
        # print("checked indir", indir)

        if indir == 0:
            return rid
        else:
            return indir

    def readFullRecord(self,rid):
        # Gets the true rid for the most recent version of the data
        trueRID = self.checkIndirection(rid)
        # Does the math to calculate pageR, pageB, and offset for record retrieval
        pageR = self.getPageR(trueRID)
        pageB = self.getPageB(trueRID)
        offset = self.getOffset(trueRID)

        # print("RID:",rid)
        # print("TrueRID:",trueRID)
        # print("Reading: Rid=",rid," pageR=",pageR," pageB=",pageB," offset=",offset)

        # Retrieves record
        # #full_record = self.page_ranges[pageR].readBlock(pageB, offset)
        full_record = self.buffer_pool_range.readBlock_Pool(pageR, pageB, offset)

        return full_record

    def readRecord(self, rid):
        # Gets the true rid for the most recent version of the data
        trueRID = self.checkIndirection(rid)
        # Does the math to calculate pageR, pageB, and offset for record retrieval
        pageR = self.getPageR(trueRID)
        pageB = self.getPageB(trueRID)
        offset = self.getOffset(trueRID)

        # print("RID:",rid)
        # print("TrueRID:",trueRID)
        # print("Reading: Rid=",rid," pageR=",pageR," pageB=",pageB," offset=",offset)

        # Retrieves record
        # #full_record = self.page_ranges[pageR].readBlock(pageB, offset)
        full_record = self.buffer_pool_range.readBlock_Pool(pageR, pageB, offset)
        # print("Full record:", full_record)
        if full_record[RID_COLUMN] == 0:
            return None

        data_record = full_record[len(full_record) - self.num_columns:]

        # print(full_record)

        ret_record = Record(rid, data_record[self.key], data_record)
        return ret_record

    def insertRecord(self, columns):
        # Check for room for base page, if not make more room
        # #if self.page_ranges[self.curr_page_range].hasCapacityBase() == False:
        if self.buffer_pool_range.hasCapacityBase_Pool(self.curr_page_range) == False:
            self.newPageRange()

        indir = 0
        schema_encoding = 0  # '0' * self.num_columns
        cur_Time = 0  # time()
        base_rid = self.nextBaseRid()
        format_columns = self.formatCols(indir, base_rid, cur_Time, schema_encoding, columns)
        # print(format_columns)
        # #self.page_ranges[self.curr_page_range].writeBaseBlock(format_columns)
        self.buffer_pool_range.writeBaseBlock_Pool(self.curr_page_range, format_columns)

        self.index.insertPair(self.key, columns[self.key], base_rid)

        # Redundant to make sure we always have a current working page range
        if self.buffer_pool_range.hasCapacityBase_Pool(self.curr_page_range) == False:
            self.newPageRange()

        return base_rid

    def updateRecord(self, rid, columns):
        # Check for room for tail page, if not make more room
        # #if self.page_ranges[self.curr_page_range].hasCapacityTail() == False:
        # print("update record space check", self.buffer_pool_range.hasCapacityTail_Pool(self.curr_page_range))
        if self.buffer_pool_range.hasCapacityTail_Pool(self.curr_page_range) == False:
            self.newPageRange()

        page_R = self.getPageR(rid)
        page_B = self.getPageB(rid)
        page_offset = self.getOffset(rid)

        # #prev_vers = self.page_ranges[page_R].getIndirection(page_B, page_offset)
        prev_vers = self.buffer_pool_range.getIndirection_Pool(page_R, page_B, page_offset)
        schema_encoding = 0  # '0' * self.num_columns
        currTime = 0  # time()
        tail_rid = self.nextTailRid(page_R)

        prev_record = self.readRecord(rid)
        if prev_record is None:
            return
        prev_columns = prev_record.getColumns()

        # print(prev_columns)
        new_columns = []

        for index in range(self.num_columns):
            if type(columns[index]) == type(None):
                new_columns.append(prev_columns[index])
            else:
                new_columns.append(columns[index])

        # print(new_columns)

        # print("tail rid:", tail_rid)
        format_columns = self.formatCols(prev_vers, tail_rid, currTime, schema_encoding, new_columns)

        # print(format_columns)
        # #self.page_ranges[self.curr_page_range].writeTailBlock(format_columns)
        # val = self.buffer_pool_range.writeTailBlock_Pool(self.curr_page_range, format_columns)
        val = self.buffer_pool_range.writeTailBlock_Pool(page_R, format_columns)
        tail_rid2 = int(val)
        # print("written tail rid:", tail_rid2)

        # #self.page_ranges[page_R].editBlock(page_B, INDIRECTION_COLUMN, page_offset, tail_rid)
        self.buffer_pool_range.editBlock_Pool(page_R, page_B, INDIRECTION_COLUMN, page_offset, tail_rid)
