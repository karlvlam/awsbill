#!/usr/bin/nodejs

var fs = require('fs');
var csv = require('csv');
var mongoClient = require('mongodb').MongoClient;
var collName = 'bill';

var bill = process.argv[2];
var c = csv();

var header = null;

var csvCount = 0; // CSV record count
var recCount = 0; // vaild record count
var storedCount = 0; // saved record count
var dupCount = 0; // duplicated record count, should be Zero
var zeroCount = 0; // record count of zero cost

var mongoConn = null;
mongoClient.connect('mongodb://127.0.0.1:27017/awsbill', function(err, db){
    if(err){
        console.log(err);
        process.exit(1);
    }
    mongoConn = db;
    readCSV();
})

function insert(o){
    var collName = 'bill';
    if (o._id === ''){
        console.log(o);
    }
    var coll = mongoConn.collection(collName);
    coll.insert(o, function(err, doc){
        if(err && err.code != 11000){
            console.log(err.code);
            process.exit(1);
        }
        if(err && err.code === 11000){
            dupCount++;
            console.log(o);
            return;
        }
        //console.log('debug:', doc);
        storedCount++;
    });
}

function readCSV(){
    c.from.path(bill)
    .on('record', function(row, index){
        if (!header){
            header = row;
            return;
        };

        var o = rec2json(row);
        if (o){
            recCount++;
            insert(o);
        }
    })
    .on('error', function(err){
        console.log(err);
    })
    .on('end', function(count){
        function checkConnection(){
            console.log('-------------------');
            console.log(new Date());
            console.log('CSV count:', csvCount);
            console.log('---');
            console.log('Valid count:', recCount);
            console.log('Stored Count:', storedCount);
            console.log('---');
            console.log('Dup Count:', dupCount);
            console.log('Zero Count:', zeroCount);
            if (recCount === storedCount){
                mongoConn.close();
            }
            mongoConn.close();
            //process.exit(0);
        }
        setTimeout(checkConnection, 5000);
    })
}


/*
 *
 * PayerAccountId
 * LinkedAccountId
 * RecordId
 * BlendedCost
 * UnBlendedCost
 * AvailabilityZone
 * ResourceId
 * UsageType
 * ProductName
 * UsageStartDate
 * UsageEndDate
 *
 * */
function rec2json(row){
    var o = {};
    for (var i=0; i < header.length; i++){
        var name = header[i];
        var value = row[i];
        if (name === 'RecordType' && value != 'LineItem'){
            return null;
        }

        if (name === 'RecordId'){
            o['_id'] = value; 
            continue;
        }
        if (name === 'LinkedAccountId' && value != ''){
            o[name] = value; 
            continue;
        }
        if (name === 'UsageType' && value != ''){
            o[name] = value; 
            continue;
        }
        if (name === 'ResourceId' && value != ''){
            o[name] = value; 
            continue;
        }
        if (name === 'ProductName' && value != ''){
            o[name] = value; 
            continue;
        }
        if (name === 'AvailabilityZone' && value != ''){
            o[name] = value; 
            continue;
        }
        if (name === 'UnBlendedCost'){
            o['cost'] = parseFloat(value); 
            continue;
        }
        if (name === 'UsageStartDate'){
            o['time'] = new Date(value + 'Z'); // UTC
            try{
                var prefix = o['time'].toISOString().split("-")[0];
                prefix += o['time'].toISOString().split("-")[1];
                o['_id'] = prefix + o['_id'];
            }catch(err){
                console.log(err);
            }
            continue;
        }
        // For AWS resource Tagging
        if (name.match(/^user:/) && value != '' ){
            o[name] = value; 
            continue;
        }
        //o[name] = row[i]; 
    }

    csvCount++;
    if (!o['cost'] || o['cost'] === 0){
        //console.log(row)
        zeroCount++;
        return null;
    }
    if (!o['_id'] || o['_id'] === ''){
        return null;
    }
    

    //console.log(o);
    return o;
}


