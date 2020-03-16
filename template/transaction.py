from template.table import Table, Record
from template.index import Index

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        self.rids = []
        self.ranges = []
        self.table = None
        pass

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, *args):
        self.queries.append((query, args))

    def preProcess(self):

        self.table = self.queries[0].table

        for query, args in self.queries:
            # get required rids
            # get required ranges
            rids = self.table.getRids(query, *args)
            ranges = self.table.getRanges(query, *args)

            # add rids that do not already exist in self.rids to self.rids
            for rid in rids:
                if rid not in self.rids:\
                    # acquire lock here
                    lock_success = self.table.lockWriteRid(rid)
                    if lock_success is True:
                        # add rid to collected locks
                        self.rids.append(rid)
                    else:
                        return False    
                else:
                    pass
            
            # add pageRs that do not already exist in self.ranges to self.ranges
            for pageR in ranges:
                if pageR not in self.ranges:
                    # acquire lock here
                    lock_success = self.table.lockWriteRange(pageR)
                    if lock_success is True:
                        # add pageR to collected locks
                        self.ranges.append(pageR)
                        # load range into bufferpool here
                        load_success = self.table.buffer_pool_range.preloadRange(pageR)
                        if load_success is False:
                            return False
                        else:
                            pass 
                else:
                    pass


        return True

    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        val = self.preProcess()

        if val is False:
            return self.abort()
        else:
            pass

        for query, args in self.queries:
            result = query(*args)
            # If the query has failed the transaction should abort
            if result == False:
                return self.abort()
        return self.commit()

    def abort(self):
        #TODO: do roll-back and any other necessary operations

        # set all dirty bits in ranges to 0
        for pageR in self.ranges:
            val = self.table.buffer_pool_range.setRangeNotDirty(pageR)
            if val is not True:
                pass
            else:
                pass

        # evict all ranges
        for pageR in self.ranges:
            val = self.table.buffer_pool_range.directFlushRange(pageR)

        # unlock required rids
        for rid in self.rids:
            lock_success = self.table.unlockWriteRid(rid)
            if lock_success is False:
                pass
            else:
                pass

        # unlock required ranges
        for pageR in self.ranges:
            lock_success = self.table.unlockWriteRange(pageR)
            if lock_success is False:
                pass
            else:
                pass

        return False

    def commit(self):
        # TODO: commit to database

        # set all dirty bits in ranges to 1
        for pageR in self.ranges:
            val = self.table.buffer_pool_range.setRangeDirty(pageR)
            if val is not True:
                pass
            else:
                pass

        # evict all ranges
        for pageR in self.ranges:
            val = self.table.buffer_pool_range.directFlushRange(pageR)

        # unlock required rids
        for rid in self.rids:
            lock_success = self.table.unlockWriteRid(rid)
            if lock_success is False:
                pass
            else:
                pass

        # unlock required ranges
        for pageR in self.ranges:
            lock_success = self.table.unlockWriteRange(pageR)
            if lock_success is False:
                pass
            else:
                pass

        return True

