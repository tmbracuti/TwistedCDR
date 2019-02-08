from twisted.internet import protocol, reactor
import psycopg2
import properties
import logging
import logging.handlers
import threading

import dispatchers.dbdispatch

#init logging
myLogger = logging.getLogger('TwistedCDR')
myLogger.setLevel(logging.DEBUG)
handler = logging.handlers.RotatingFileHandler('./sf_twisted_cdr.log', maxBytes = 1000000, backupCount = 5)
format = "%(asctime)s : %(name)-12s: %(levelname)s | %(message)s"
handler.setFormatter(logging.Formatter(format))
myLogger.addHandler(handler)

#supported schemas with start/length indexes for dialed and called numbers respectively and the min length
schemaMap = { 'unformatted':(17,15,32,10,96), 'custom_aig':(18,18,44,15,102)}

class UnformattedReceiver(protocol.Protocol):
    def __init__(self, id, port, props):
        self.props = props        
        self.buffer = ''
        self.id = id
        self.port = port
        self.dispatcher = dispatchers.dbdispatch.DBDispatcher(self.id, self.props)
        self.dispatcher.daemon = True #die when process dies (don't block process exiting)
        self.dispatcher.start()
        self.dstart, self.dlen, self.calledstart, self.calledlen, self.minLen = schemaMap['unformatted']

    def connectionLost(self, reason):        
        print 'client connection has closed, flushing the buffer and closing receiver'
        while self.buffer != '':
            self.dataReceived('')
        self.dispatcher.setItem("__QUIT__")
        self.dispatcher.join()
        logStr = "closed unformatted dispatcher for cm_id: %d" % self.id
        myLogger.info(logStr)


    def dataReceived(self, data):
        self.buffer = self.buffer + data
        loc = self.buffer.find('\n')
        if loc == -1:
            pass
        else:
            nlen = len(self.buffer)
            record = self.buffer[:loc]
            record = record.replace('\0','')
            self.processRecord(record)
            i = nlen - loc - 1
            saved = self.buffer[-i:]
            if saved != self.buffer:               
                self.buffer = saved
            else:
                self.buffer = ''

    def processRecord(self, cdrRecord):
        if len(cdrRecord) < self.minLen:
            return
        dialed = cdrRecord[self.dstart:self.dstart+self.dlen].strip()
        calling = cdrRecord[self.calledstart:self.calledstart+self.calledlen].strip()        
        print "print id %d: calling: %s\tdialed: %s" % (self.id, calling, dialed)
        self.dispatcher.setItem("%s,%s" % (dialed,calling))

#---------------------------------------------------------------------------------------------
class AigCustomReceiver(protocol.Protocol):
    def __init__(self, id, port, props):
        self.buffer = ''
        self.id = id
        self.props = props
        self.dispatcher = dispatchers.dbdispatch.DBDispatcher(self.id, self.props)
        self.dispatcher.daemon = True #die when process dies (don't block process exiting)
        self.dispatcher.start()
        self.port = port
        self.dstart, self.dlen, self.calledstart, self.calledlen, self.minLen = schemaMap['custom_aig']

    def connectionLost(self, reason):
        print 'client connection has closed, flushing the buffer and closing receiver'
        while self.buffer != '':
            self.dataReceived('')
        self.dispatcher.setItem('__QUIT__')
        self.dispatcher.join()
        logStr = "closed custom_aig dispatcher for cm_id: %d" % self.id
        myLogger.info(logStr)

    def dataReceived(self, data):
        self.buffer = self.buffer + data
        loc = self.buffer.find('\n')
        if loc == -1:
            pass
        else:
            nlen = len(self.buffer)
            record = self.buffer[:loc]
            record = record.replace('\0','')
            self.processRecord(record)
            i = nlen - loc - 1
            saved = self.buffer[-i:]
            if saved != self.buffer:                
                self.buffer = saved
            else:                
                self.buffer = ''

    def processRecord(self, cdrRecord):  
        if len(cdrRecord) < self.minLen:
            return      
        dialed = cdrRecord[self.dstart:self.dstart+self.dlen].strip()
        calling = cdrRecord[self.calledstart:self.calledstart+self.calledlen].strip()        
        print "print id %d: calling: %s\tdialed: %s" % (self.id, calling, dialed)
        self.dispatcher.setItem("%s,%s" % (dialed,calling))
#---------------------------------------------------------------------------------------------                

class AigCustomFactory(protocol.Factory):
    def __init__(self, id, port, props):
        self.id = id
        self.port = port
        self.props = props

    def buildProtocol(self, addr):
        #print 'instantiating custom_aig protocol for ', addr
        myLogger.info("AIGCustomFactory instantiating custom_aig protocol for CM %d - from %s" % (self.id,str(addr)) )
        return AigCustomReceiver(self.id, self.port, self.props)
#---------------------------------------------------------------------------------------------
class UnformattedFactory(protocol.Factory):
    def __init__(self, id, port, props):
        self.id = id
        self.port = port
        self.props = props

    def buildProtocol(self, addr):
        #print 'instantiating unformatted protocol for ', addr
        myLogger.info("UnformattedFactory instantiating unformatted protocol for CM %d - from %s" % (self.id,str(addr)) )
        return UnformattedReceiver(self.id, self.port, self.props)
#---------------------------------------------------------------------------------------------

def getTargets(props):
    qry = """select cm.id,cm.cdr_port,m.value from sf_cm cm
        join sf_miscconfig m on cm.server_name=m.section
        where cm.enable_cdr=true and m.key='cdr_schema_name'"""

    target_list = []
    connStr = props.get_value("sf_db", "")
    if connStr == '':
        myLogger.error("sf_db (connection string parameter) is bad or missing")
        return None
    try:
        c = psycopg2.connect(connStr)
        cur = c.cursor()
        cur.execute(qry)
        rows = cur.fetchall()
        for row in rows:
            id = row[0]
            prt = row[1]
            format = row[2]
            t = (id, prt, format)
            target_list.append(t)
        c.close()
    except:
        return None
    return target_list

#---------------------------------------------------------------------------------------
if __name__== '__main__':    
    props = properties.Properties("./cdr.properties")
    if not props.isReady():
        print 'cdr.properties file failed to load...exiting'
        myLogger.error('cdr.properties file failed to load...exiting')
        os._exit(1)
    try:
        while True:
            target_list = getTargets(props)
            if target_list is None or len(target_list) == 0:
                print 'no suitable targets found'
                time.sleep(5)
            else:
                break
                
        for t in target_list:
            id, port, format = t            
            factory = None
            if format == 'custom_aig':
                factory = AigCustomFactory(id, port, props)            
            elif format == 'unformatted':
                factory = UnformattedFactory(id, port, props)
            else:
                factory = None
                print 'ignoring spec for CM %d, unknown format: %s' % (id, format)
                continue
            reactor.listenTCP(port, factory)
            print 'running ID %d on %d with format %s' % (id, port, format)

        print 'CDR receiver(s) activated, starting master event-loop'
        reactor.run()
    except Exception as e:
        myLogger.error(str(e) + ' = fatal error, program ending')
                      


