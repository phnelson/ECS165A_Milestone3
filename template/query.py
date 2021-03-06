from template.table import Table, Record

# from template.index import Index

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table
    """

    def __init__(self, table):
        self.table = table

    """
    # internal Method
    # Read a record with specified RID
    """

    def getRids(self, query, *args):
        # if this doesnt work, try id(self.delete)
        retval = False
        q_func = query.__func__

        #print("These are the args", args)

        if q_func == self.delete.__func__:
            rids = self.getDeleteRid(args)
            retval = True

        if q_func == self.insert.__func__:
            rids = None
            retval = True

        if q_func == self.selectFull.__func__:
            rids = self.getSelectFullRids(args[0][0], args[0][1])
            retval = True

        if q_func == self.select.__func__:
            rid = self.getSelectRids(args[0][0], args[0][1], args[0][2])[0]
            rids = []
            rids.append(rid)
            retval = True

        if q_func == self.update.__func__:
            rids = self.getUpdateRid(args[0])
            retval = True

        if q_func == self.sum.__func__:
            rids = self.getSumRids(args[0][0], args[0][1], args[0][2])
            retval = True

        if q_func == self.increment.__func__:
            rid = self.getIncrementRids(args[0][0], args[0][1])
            rids = []
            rids.append(rid)
            retval = True

        if retval is False:
            return False
        else:
            pass
        
        # print("Rids:", rids)
        return rids
        

    def getPageRanges(self, query, *args):
        retval = False
        q_func = query.__func__

        #print("getPageRanges, q_func:", q_func)

        if q_func == self.delete.__func__:
            pageR = self.getDeletePageRange(args[0])
            retval = True

        if q_func == self.insert.__func__:
            pageR = self.getInsertPageRange(args[0])
            #print("inside insert function match, pageR:", pageR)
            retval = True

        if q_func == self.selectFull.__func__:
            pageR = None # new
            retval = True

        if q_func == self.select.__func__:
            pageR = [] # new
            retval = True

        if q_func == self.update.__func__:
            pageR = self.getUpdatePageRange(args[0])
            retval = True

        if q_func == self.sum.__func__:
            pageR = None # new
            retval = True

        if q_func == self.increment.__func__:
            page_R = self.getIncrementPageRange(args[0][0], args[0][1])
            pageR = []
            pageR.append(page_R)

            #print("inside increment function match")
            retval = True

        if retval is False:
            return False
        else:
            pass

        return pageR


    def getDeleteRid(self, key):
        rid = self.table.index.locate(self.table.key, key)[0]
        return rid

    def getDeletePageRange(self, key):
        rid = self.table.index.locate(self.table.key, key)[0]
        pageR = self.table.getPageR(rid)
        return pageR

    def delete(self, key):
        # print("Deleting key:", key)
        # rid = self.table.page_directory.get(key)
        rid = self.table.index.locate(self.table.key, key)[0]
        # Go into memory with the rid -> (PageRange, offset), change value of Record.rid to None
        retval = self.table.deleteRecord(rid)
        # Remove key from dictionary ,dict.pop(key) might be useful
        # self.table.page_directory.pop(key, None)
        self.table.index.deletePair(self.table.key, key, rid)

        return retval

    """
    # Insert a record with specified columns
    """
    def getInsertPageRange(self, *columns):
        pageR = self.table.curr_page_range
        return pageR

    def insert(self, *columns):
        key = columns[self.table.key]
        # Insert value into available base -> get rid->(page, offset)
        rid = self.table.insertRecord(columns)
        # print("Insert: rid = ", rid)
        # Add key,rid pair to dictionary
        # self.table.page_directory[key] = rid

        return True

    """
    # Read a record with specified key
    """
   
    def getSelectFullRids(self, key, column):
        rids = self.table.index.locate(column, key)
        if rids is None:
            return [None]
        else:
            pass
        return rids

    def selectFull(self, key, column):
        retList = []
        # print("test", key)
        rids = self.table.index.locate(column, key)
        # print(rids)

        # Validator for record presence in table
        if rids is None:
            retList.append(None)
            return [None]

        for rid in rids:
            record = self.table.readFullRecord(rid)
            # print("record:", record)

            if record is None:
                pass
            else:
                retList.append(record)

        return retList

    def getSelectRids(self, key, column, query_columns):
        rids = self.table.index.locate(column, key)
        if rids is None:
            return [None]
        else:
            pass
        return rids

    def select(self, key, column, query_columns):
        retList = []
        # print("test", key)
        rids = self.table.index.locate(column, key)
        #print("rids", rids)

        # Validator for record presence in table
        if rids is None:
            retList.append(None)
            return [None]

        for rid in rids:
            record = self.table.readRecord(rid)
            # print("record:", record)
            formatted_record = []

            if record is None:
                pass
            else:
                # print(record)
                for i in range(0, len(query_columns)):
                    # print("checkpoint: i = ", i)
                    if query_columns[i] == 1:
                        formatted_record.append(record.columns[i])
                    else:
                        formatted_record.append(0)


            #retList.append(formatted_record)

                created_record = Record(record.columns[self.table.key], self.table.key, formatted_record)
                retList.append(created_record)
            #retList.append(record)

        # print("retlist:", retList)
        return retList

    """
    # Update a record with specified key and columns
    """
    def getUpdateRid(self, key, *columns):
        rid = self.table.index.indices[self.table.key][key][0]
        return rid

    def getUpdatePageRange(self, key, *columns):
        rid = self.table.index.indices[self.table.key][key][0]
        pageR = self.table.getPageR(rid)
        return pageR

    def update(self, key, *columns):
        # rid = self.table.page_directory.get(key)
        rid = self.table.index.indices[self.table.key][key][0]
        #print("update test, rid:", rid)
        retval = self.table.updateRecord(rid, columns)
        return retval

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    """

    def getSumRids(self, start_range, end_range, aggregate_column_index):
        rids = []
        for x in range(start_range, end_range):
            rids.append(x)

        return rids


    def sum(self, start_range, end_range, aggregate_column_index):
        sum_val = 0
        selected_cols = []
        data = []

        num_cols = self.table.num_columns

        # Pre-processing to set up selected_cols for query.select() calls
        for i in range(0, num_cols):
            if i == aggregate_column_index:
                selected_cols.append(1)
            else:
                selected_cols.append(0)

        # rid_list = self.table.index.locate_range(start_range, end_range)

        # Loops through the specified range
        for i in range(start_range, end_range+1):
            data = self.select(i, 0, selected_cols)
            #print("data:", data)
            # Data validation to ensure it exists in table
            if data is not None:
                if len(data) == 0:
                    pass
                else:
                    if data[0] is None:
                        pass
                    else:
                        sum_val += data[0].getColumns()[aggregate_column_index]
            # print(sum_val)


        return sum_val

    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def getIncrementRids(self, key, column):
        rid = self.getSelectRids(key, self.table.key, [1] * self.table.num_columns)[0]
        return rid

    def getIncrementPageRange(self, key, column):
        rid = self.getSelectRids(key, self.table.key, [1] * self.table.num_columns)[0]
        pageR = self.table.getPageR(rid)
        return pageR

    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            r_cols = r.getColumns()
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r_cols[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False
