var _ = require('underscore');

function NosDesktopSync(configObj) {
    if (!(this instanceof NosDesktopSync)) {
        return new NosDesktopSync();
    }
    
    // this.solariaStore = configObj.solariaStore;
    // this.solariaModel = configObj.solariaModel;

    this.database = configObj.database || null;
    this.batchSize = configObj.batchSize || 5;
    this.debounceRate = configObj.debounceRate || 10 * 1000;
    this.syncRate = configObj.syncRate || 3 * 1000;
    this.ackRate = configObj.ackRate || 10 * 1000;
    
    this.desktopOfflineTimeout = 20 * 1000;

    this.msgQueue = [];
    this.sentPackets = [];
    this.receivedACKs = [];

    this.lastId = 0;
    this.lastPacket = null;
    this.lastPacketFirstSent = null;
    this.lastPacketResent = null;
    this.lastDebounce = Date.now();

    this.desktopStatus = null;
    this.engineStatus = 'initialized';
    this.packetStatus = 'INIT';

    this.syncIntervalId = null;
    this.ACK_OVERRIDE = false;
}

NosDesktopSync.prototype.overrideACKs = function(bool) {
    this.ACK_OVERRIDE = bool;
};

NosDesktopSync.prototype.status = function() {
    return this;
};

NosDesktopSync.prototype.addMsgToQueue = function(msg, objId) {
    var msgObj = {
        _id: this.lastId++,
        payload: msg,
        objId: objId,
        timestamp: Date.now()
    };

    this.msgQueue.push(msgObj);
    console.log('Queue:', this.msgQueue.map(function(msg) { return msg.payload }).join(' '));

    // Reset debounce timer for processing Queue
    this.lastDebounce = Date.now();
};

NosDesktopSync.prototype.dequeueBatch = function() {
    if (this.msgQueue.length === 0) {
        console.log('Queue empty.');
        return;
    }

    var msgBatch = [];
    var i = 0;
    var iMax = this.msgQueue.length < this.batchSize
        ? this.msgQueue.length
        : this.batchSize;

    for (; i < iMax; i++) {
        msgBatch.push(this.msgQueue.shift());
    }

    return msgBatch;
};

NosDesktopSync.prototype.createDataPacket = function(msgBatch) {
    return {
        _id: this.lastId++,
        timestamp: Date.now(),
        msgList: msgBatch
    };
};

NosDesktopSync.prototype.normalizeQueue = function() {
    var q = this.msgQueue;
    var idsToRemove = [];

    // Remove all but the most recent update for any given objectId
    for (var i = 0; i < q.length; i++) {
        for (var j = i + 1; j < q.length; j++) {
            if (q[i].objId === q[j].objId) {
                idsToRemove.push(q[i].timestamp < q[j].timestamp ? q[i]._id : q[j]._id);
            }
        }
    }

    // Remove duplicate ids
    idsToRemove = _.uniq(idsToRemove);

    if (idsToRemove.length > 0) {
        console.log('Normalizing Queue -- Ids to remove:', idsToRemove);
        // console.log('Queue before normalizing:', this.msgQueue);
        this.msgQueue = this.msgQueue.filter(function(item) { 
            return _.indexOf(idsToRemove, item._id) === -1; 
        });
        console.log('Normalized:', _.pluck(this.msgQueue, '_id'));
    }
};

NosDesktopSync.prototype.processQueueForSyncOp = function() {
    var self = this;
    console.log('Processing Queue for Sync');

    var timeSinceLastDebounce = Date.now() - this.lastDebounce;
    var debounceExpired = function() {
        return timeSinceLastDebounce > self.debounceRate;
    };

    // console.log('timeSinceLastDebounce:', timeSinceLastDebounce);

    if (!debounceExpired()) {
        console.log('Waiting for debounce...');
        
    } else {
        self.normalizeQueue();

        var packet = self.createDataPacket(self.dequeueBatch());
        console.log('Created Packet:', packet);

        // Save packet to registered database
        self.commitPacket(packet, function (err, success) {
            if (success) {
                // Send packet to registered endpoint
                self.sendPacket(packet, function () {
                    
                    self.packetStatus = 'ACK_PENDING';
                    self.lastPacket = packet;
                    self.lastPacketResent = packet.timestamp;
                    self.sentPackets.push(packet);

                    console.log('Remaining Queue:', _.pluck(self.msgQueue, '_id'));
                });

            } else {
                console.log('Error committing packet to database.');
            }
        });
    }
};

NosDesktopSync.prototype.commitPacket = function(packet, cb) {
    // Save packet to registered database
    // if (err) cb(err, false);

    // If successful
    cb(null, true);
};

NosDesktopSync.prototype.sendPacket = function(packet, cb) {
    var self = this;
    // TODO send POST to endpoint registered for Desktop App
    // this.postEndpoint = ?

    // If just starting or if the last packet has been received, reset first sent time
    console.log('Packet status', self.packetStatus);
    if (['ACK_OK', 'INIT'].indexOf(self.packetStatus) > -1) {
        self.lastPacketFirstSent = Date.now();
    }
    
    console.log('Packet sent');
    cb();
};

NosDesktopSync.prototype.resendLastPacket = function() {
    console.log('Resending last packet:', this.lastPacket);
    this.sendPacket(this.lastPacket, function(){});
    this.lastPacketResent = Date.now();
};

NosDesktopSync.prototype.registerACK = function(ACK) {
    console.log('Registered ACK:', ACK);
    this.receivedACKs.push(ACK);

    if (this.lastPacket) {
        if (this.lastPacket._id === ACK.packetId) {
            this.packetStatus = 'ACK_OK';
            this.desktopStatus = 'online';
            // TODO Remove packet from sent packet list
            // TODO Remove packet from database

        } else {
            if (_.indexOf(_.pluck(this.sentPackets, '_id'), ACK.packetId) > -1) {
                console.log('ACK id previously received. Possible error?');
            }

        }
    } else {
        console.log('ACK received before packet sent... whaaaat?');
    }

    // TODO flush ACK cache
};

NosDesktopSync.prototype.start = function() {
    var self = this;

    if (self.engineStatus === 'running') {
        console.log('Sync Engine already running');
        return;

    } else {
        console.log('Sync engine starting...');
        if (!self.database) {
            console.log('No database configured. Stopping engine.');
            return;
        }
    }

    // TODO If on Desktop, load saved queue from database
    // Note: When Desktop is offline, use separate method for saving new events to database for later sync?
    //    OR: use Journal processing to determine change set at time desktop comes online?

    //------------------------------------
    // Main Sync Engine
    //------------------------------------
    
    var iteration = 0;
    var _checkQueue = function() {
        iteration++;
        console.log('\nChecking queue, iteration:', iteration);
        
        // TODO Check desktop online status
        // If desktop was offline but is now online, reset lastPacketFirstSent time
        // if (self.desktopStatus === 'offline' && newStatus === 'online') {
        //     self.lastPacketFirstSent = Date.now();
        // }
        
        if (self.desktopStatus !== 'online') {
            console.log('Desktop is offline. Sync operations suspended.');
            return;
        }

        // Process queue
        if (self.msgQueue.length > 0) {
            // Test for whether ACK received for previous packet
            if (['ACK_OK', 'INIT'].indexOf(self.packetStatus) > -1 || self.ACK_OVERRIDE) {
                self.processQueueForSyncOp();

            } else if (self.packetStatus === 'ACK_PENDING') {
                // Test time elapsed since last packet transmission
                var timeSincePacket = Date.now() - self.lastPacketResent;

                console.log('Awaiting ACK for last packet');
                console.log('Time since packet:', timeSincePacket);

                var desktopLapsed = (Date.now() - self.lastPacketFirstSent) > self.desktopOfflineTimeout;
                
                // console.log('Last packet first sent:', self.lastPacketFirstSent);
                // console.log('Online status timeout limit:', self.desktopOfflineTimeout);
                
                if (desktopLapsed) {
                    // If it has been a while since packet was first sent, set desktopStatus to offline
                    self.desktopStatus = 'offline';

                } else if (timeSincePacket > self.ackRate) {
                    // If over certain limit... resend packet...
                    self.resendLastPacket();
                }
                
                
            }
        } else {
            console.log('Queue empty');
        }
    };

    // Start Engine Loop
    self.syncIntervalId = setInterval(_checkQueue, self.syncRate);
    self.engineStatus = 'running';
};

NosDesktopSync.prototype.stop = function() {
    var self = this;
    clearInterval(this.syncIntervalId);
    this.engineStatus = 'stopped';
    console.log('Sync Engine Stopped. Status:', self.status());
};

/*********************
 * Receiving packets
 * *******************/

NosDesktopSync.prototype.receivePacket = function(recdPacket) {
    // This method will be called by endpoint receiving the packet
    var msgList = recdPacket.msgList;

    msgList.forEach(function(msg) {
        // Apply event message to device / Aurora
    });
};

NosDesktopSync.prototype.configure = function(confObj) {
    this.database = confObj.db;
    this.postEndpoint = confObj.postEndpoint;
    // etc.
};


// Following method is only for use by Aurora
NosDesktopSync.prototype.checkDesktopStatus = function() {
    var apiStreamPersistence = require(currWorkingDir + '/lib/citta-cloud-fx/solaria/mongo/NosSolariaStore')();
    var apiStreamModel = require(currWorkingDir + '/lib/citta-cloud-fx/solaria/mongo/NosSolariaModel')();
    
    apiStreamPersistence.getPrimaryContentDbByUser(req.user.id, storeConfig, function(err, contentDb) {


        apiStreamModel.getCollectionByName('device', null, contentDb, user, app.db, function(err, deviceColl) {
            res.send(deviceColl);
        });


    });
    
};

// exports = module.exports = NosDesktopSync;


/*****************************************
 * SETUP FOR TESTING -- Not part of module
 *****************************************/

function addSampleData(api) {
    setTimeout(() => api.addMsgToQueue('Hello', 4), 0);
    setTimeout(() => api.addMsgToQueue('world.', 8), 100);
    setTimeout(() => api.addMsgToQueue('One', 8), 120);
    setTimeout(() => api.addMsgToQueue('two', 15), 140);
    setTimeout(() => api.addMsgToQueue('three', 16), 160);
    setTimeout(() => api.addMsgToQueue('four', 23), 180);
    setTimeout(() => api.addMsgToQueue('get', 15), 200);
    setTimeout(() => api.addMsgToQueue('your', 15), 220);
    setTimeout(() => api.addMsgToQueue('booty', 42), 240);
    setTimeout(() => api.addMsgToQueue('on the floor', 42), 260);
    setTimeout(() => api.addMsgToQueue('Gotta', 43), 280);
    setTimeout(() => api.addMsgToQueue('gotta', 44), 300);
    setTimeout(() => api.addMsgToQueue('get up', 45), 320);
    setTimeout(() => api.addMsgToQueue('to get', 46), 340);
    setTimeout(() => api.addMsgToQueue('down.'), 360);
    setTimeout(() => api.addMsgToQueue('Twas', 48), 380);
    setTimeout(() => api.addMsgToQueue('brillig', 49), 400);
    setTimeout(() => api.addMsgToQueue('and the', 50), 420);
    setTimeout(() => api.addMsgToQueue('slithy', 50), 440);
    setTimeout(() => api.addMsgToQueue('toves', 42), 460);
    setTimeout(() => api.addMsgToQueue('did', 42), 480);
    setTimeout(() => api.addMsgToQueue('gyre', 42), 500);
    setTimeout(() => api.addMsgToQueue('and gimble', 42), 520);
    setTimeout(() => api.addMsgToQueue('in the wabe.', 98), 540);
}

var sync = new NosDesktopSync({
    batchSize: 2,
    debounceRate: 10000,
    syncRate: 2000,
    ackRate: 5000,
    database: 'mine'
});



addSampleData(sync);

