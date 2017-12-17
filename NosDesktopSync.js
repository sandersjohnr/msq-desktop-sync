var _ = require('underscore');
var apiStreamModel = new cloudApp.cloudFx.NosSolariaModel();
 

function NosDesktopSync(configObj) {
    if (!(this instanceof NosDesktopSync)) {
        return new NosDesktopSync();
    }

    this.database = configObj.database || null;

    this.batchSize = configObj.batchSize || 5;
    this.debounceRate = configObj.debounceRate || 10 * 1000;
    this.syncRate = configObj.syncRate || 3 * 1000;
    this.ackRate = configObj.ackRate || 10 * 1000;

    this.msgQueue = [];
    this.sentPackets = [];
    this.receivedACKs = [];

    this.lastId = 0;
    this.lastPacket = null;
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


    // TODO Aurora perspective:
    // If Desktop status is online, keep processing
    // Else either this.stop(); or just skip iteration
    // Once Desktop status is online again, begin processing

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

    console.log('timeSinceLastDebounce:', timeSinceLastDebounce);

    if (!debounceExpired()) {
        // if (true) {
        console.log('Waiting for debounce...');
        return;
    }

    self.normalizeQueue();

    var packet = self.createDataPacket(self.dequeueBatch());
    console.log('Created Packet:', packet);

    // Save packet to registered database
    self.commitPacket(packet, function(err, success) {
        if (success) {
            // Send packet to registered endpoint
            self.sendPacket(packet, function() {

                self.lastPacket = packet;
                self.lastPacketResent = packet.timestamp;
                self.sentPackets.push(packet);
                self.packetStatus = 'ACK_PENDING';

                // TODO Wait for ACK before sending next packet
                console.log('Remaining Queue:', _.pluck(self.msgQueue, '_id'));

            });

        } else {
            console.log('Error committing packet to database.');
        }
    });
};

NosDesktopSync.prototype.commitPacket = function(packet, cb) {
    // Save packet to registered database
    // if (err) cb(err, false);

    // If successful
    cb(null, true);
};

NosDesktopSync.prototype.sendPacket = function(packet, cb) {
    // TODO send POST to endpoint registered for Desktop App
    // this.postEndpoint = ?

    console.log('Packet sent');
    // setTimeout(() => {
    // this.registerACK({packetId: packet._id});
    cb();
    // }, 5000);
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

    if (this.engineStatus === 'running') {
        console.log('Sync Engine already running');
        return;

    } else {
        console.log('Sync engine starting...');
        if (!this.database) {
            console.log('No database configured. Stopping engine.');
            return;
        }
    }

    // TODO If on Desktop, load saved queue from database
    // Note: When Desktop is offline, use separate method for saving new events to database for later sync?
    //    OR: use Journal processing to determine change set at time desktop comes online?

    
    var iteration = 0;
    var _checkQueue = function() {
        iteration++;
        console.log('\nChecking queue, iteration:', iteration);

        if (this.desktopStatus !== 'online') {
            console.log('Desktop is offline. Sync operations suspended.');
            return;
        }


        // Process queue
        if (self.msgQueue.length > 0) {
            // test for whether ACK received for previous packet
            if (['ACK_OK', 'INIT'].indexOf(self.packetStatus) > -1 || self.ACK_OVERRIDE) {
                self.processQueueForSyncOp();

            } else if (self.packetStatus === 'ACK_PENDING') {
                // test time elapsed since last packet transmission
                var timeSincePacket = Date.now() - self.lastPacketResent;

                console.log('Awaiting ACK for last packet');
                console.log('Time since packet:', timeSincePacket);

                if (timeSincePacket > self.ackRate) {
                    // If over certain limit... resend packet...
                    self.resendLastPacket();
                }


            }
        } else {
            console.log('Queue empty');
        }
    };

    // Start Engine Loop
    this.syncIntervalId = setInterval(_checkQueue, this.syncRate);
    this.engineStatus = 'running';
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

NosDesktopSync.prototype.checkDesktopStatus = function() {
    // var url = '/d/v1/desktopStatus/desktop';
    // var Nos = require(currWorkingDir + '/lib/citta-cloud-fx/events/NosEventSubClient.js');

    var apiStreamPersistence = require(currWorkingDir + '/lib/citta-cloud-fx/events/NosEventSubClient.js');


};

exports = module.exports = NosDesktopSync;


/*****************************************
 * SETUP FOR TESTING -- Not part of module
 *****************************************/
/*
function addSampleData(api) {
    api.addMsgToQueue('Hello', 4);
    setTimeout(() => api.addMsgToQueue('world', 8), 100);
    setTimeout(() => api.addMsgToQueue('get', 8), 120);
    setTimeout(() => api.addMsgToQueue('on', 15), 140);
    setTimeout(() => api.addMsgToQueue('down', 16), 160);
    setTimeout(() => api.addMsgToQueue('like', 23), 180);
    setTimeout(() => api.addMsgToQueue('another', 15), 200);
    setTimeout(() => api.addMsgToQueue('tasty', 15), 220);
    setTimeout(() => api.addMsgToQueue('dance', 42), 240);
    setTimeout(() => api.addMsgToQueue('craze?', 42), 260);
    setTimeout(() => api.addMsgToQueue('no?', 43), 280);
    setTimeout(() => api.addMsgToQueue('fine!', 44), 300);
    setTimeout(() => api.addMsgToQueue('adfgadf?', 45), 320);
    setTimeout(() => api.addMsgToQueue('sausage?', 46), 340);
    setTimeout(() => api.addMsgToQueue('dfbadfbadbfdabf'), 360);
    setTimeout(() => api.addMsgToQueue('heyooo!!', 48), 380);
    setTimeout(() => api.addMsgToQueue('asdfasdfasdf?', 49), 400);
    setTimeout(() => api.addMsgToQueue('flarf?', 50), 420);
    setTimeout(() => api.addMsgToQueue('narf?', 50), 440);
    setTimeout(() => api.addMsgToQueue('sausage?', 42), 460);
    setTimeout(() => api.addMsgToQueue('derp', 42), 480);
    setTimeout(() => api.addMsgToQueue('doo', 42), 500);
    setTimeout(() => api.addMsgToQueue('sausagasdfasdfasdfasdfasdfe?', 42), 520);
    setTimeout(() => api.addMsgToQueue({ what: 'what', the: 'hell', you: 'say' }, 98), 540);
}

var sync = new NosDesktopSync({
    batchSize: 2,
    debounceRate: 10000,
    syncRate: 2000,
    ackRate: 5000,
    database: 'mine'
});



addSampleData(sync);
*/
