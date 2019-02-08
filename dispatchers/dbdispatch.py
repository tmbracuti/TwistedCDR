
import psycopg2
import logging
import threading
import Queue

class DBDispatcher(threading.Thread):
    def __init__(self, cm_id, props):
        threading.Thread.__init__(self)  
        self.cm_id = cm_id     
        self.props = props        
        self.dqueue = Queue.Queue()
        self.conn = None
        self.log = logging.getLogger('TwistedCDR.DBDispatcher')
        
    def run(self):
        self.log.info("DBDispatcher starting for cm_id: %d" % self.cm_id)                                  
        count = 0
        while True:
            try:
                cdritem = self.dqueue.get(True)
                count += 1
                if cdritem == '__QUIT__':
                    if self.conn is not None:
                        self.conn.close()
                        self.conn = None                                                         
                    break
                dialed, calling = cdritem.split(',')                              
                self.publish(dialed)
                self.publish(calling)                    
            except:
                pass
        self.log.info("DBDispatcher closed normally for cm_id: " + str(self.cm_id))

    def publish(self, station):
        try:
            while not self.getConnection():
                time.sleep(1)
            cur = self.conn.cursor()                    
            cur.callproc('record_action_ex', (self.cm_id, station))               
        except:
            if self.conn is not None:
                self.conn.close()
                self.conn = None
              
    def getConnection(self):
        if self.conn is not None:
            return True
        connStr = self.props.get_value('sf_db', '') #already assured to be there        
        try:
            self.conn = psycopg2.connect(connStr)
            self.conn.autocommit = True
            return True
        except:
            if self.conn is not None:
                try:
                    self.conn.close()
                except:
                    pass
            self.conn = None
            return False

    def setItem(self, cdritem):
        self.dqueue.put(cdritem)