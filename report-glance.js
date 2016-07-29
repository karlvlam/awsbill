#!/usr/bin/nodejs
var argv = require('yargs').argv;
var fs = require('fs');
var json2csv = require('json2csv');
var mongoClient = require('mongodb').MongoClient;
var collName = 'bill';
var col = null;

function help(){
    var cmd = argv['$0'] + " --start [START DATE] --end [END DATE]";
    console.log(cmd);
    process.exit(1);
}

if (argv['help']) help();

var start = new Date(argv['start']);
var end = new Date(argv['end']);

if (isNaN(start)) {
    start = new Date('2000-01-01T00:00:00.000+00:00');
}

if (isNaN(end)) {
    end = new Date('2999-01-01T00:00:00.000+00:00');
}

if (start >= end){
    console.log('Date Error!');
    process.exit(1);
}


var mongoConn = null;
mongoClient.connect('mongodb://127.0.0.1:27017/awsbill', function(err, db){
    if(err){
        console.log(err);
        process.exit(1);
    }
    mongoConn = db;
    col = mongoConn.collection(collName);
    glance();
})

function sort(a,b){
    if (a["user:Name"] > b["user:Name"]) return 1;
    if (a["user:Name"] < b["user:Name"]) return -1;
    return 0;
}

// main function, use mongodb mapreduce
function glance(){

    var o =  {
        // keys to be included
        keys:{
            LinkedAccountId:1,
            ProductName:1,
            UsageType: 1,
            "user:Name" : 1, 
            "user:Role" : 1, 
            "ResourceId" : 1, 
        }, 
        condition:{
            time: { $gte: start, $lt: end},
        }, 
        initial: { cost:0 }, 
        reduce:function(curr, result){ 
            result.cost += curr.cost;
        }, 
        cb: function(err, data){
            //console.log(err);
            for (var i=0; i < data.length; i++){
                if (!data[i]['ResourceId']){
                    continue;
                }

                if (data[i]['user:Role']){
                    data[i]['user:Name'] = data[i]['user:Role'];
                }

                if (!data[i]['user:Name']){
                    data[i]['user:Name'] = '(OTHER)';
                }


                if (data[i]['ProductName'] === 'Amazon Elastic Compute Cloud'){


                    if (data[i]['UsageType'].match(/DataTransfer/)) {

                        if (data[i]['user:Name'] === '(OTHER)'){
                            data[i]['user:Name'] = data[i]['ResourceId'];
                        }
                        data[i]['user:Name'] = '(DataTransfer) ' + data[i]['user:Name'];
                        continue;
                    }
                    if (data[i]['UsageType'].match(/VolumeUsage/)) {
                        data[i]['user:Name'] = '(VolumeUsage) ' + data[i]['user:Name'];
                        continue;
                    }
                    if (data[i]['UsageType'].match(/VolumeIOUsage/)) {
                        data[i]['user:Name'] = '(VolumeIOUsage)';
                        continue;
                    }

                    if (data[i]['UsageType'].match(/ElasticIP/)) {
                        data[i]['user:Name'] = '(ElasticIP) ' + data[i]['user:Name'];
                        continue;
                    }

                    if (data[i]['user:Name'] === '(OTHER)'){
                        data[i]['user:Name'] = '(OTHER) ' + data[i]['UsageType']
                    }
                    continue;


                }
                if (data[i]['ProductName'] === 'Amazon CloudFront'){
                    if (data[i]['user:Name'] === '(OTHER)'){
                        data[i]['user:Name'] = data[i]['ResourceId'];
                    }

                    continue;
                }
                if (data[i]['ProductName'] === 'Amazon RDS Service'){
                    if (data[i]['user:Name'] === '(OTHER)'){
                        data[i]['user:Name'] = data[i]['ResourceId'];
                    }
                    continue;
                }
                if (data[i]['ProductName'] === 'Amazon ElastiCache'){
                    if (data[i]['user:Name'] === '(OTHER)'){
                        data[i]['user:Name'] = data[i]['ResourceId'];
                    }
                    continue;
                }

                if (data[i]['ProductName'] === 'Amazon Simple Storage Service'){
                    data[i]['user:Name'] = data[i]['ResourceId'];
                    continue;
                }

                if (!data[i]['user:Name']){
                    //data[i]['user:Name'] = '(OTHER)';
                    data[i]['user:Name'] = '(' + data[i]['UsageType'] + ')' + data[i]['ResourceId'];
                }



            }
            var finalObj = {};

            for (var i=0; i < data.length; i++){
                var d = data[i];
                var key = d['LinkedAccountId'] + '#' + d['user:Name'] + '#' + d['ProductName'];
                if (!finalObj[key]) {
                    finalObj[key] = d['cost'];
                    continue;
                }
                finalObj[key] += d['cost'];
            }

            var finalData = [];
            var keys = Object.keys(finalObj);

            for (var i=0; i < keys.length; i++){
                var cost = finalObj[keys[i]];
                var values = keys[i].split("#");
                var o = {
                    'LinkedAccountId': values[0],
                    'user:Name': values[1],
                    'ProductName': values[2],
                    'cost': cost,
                }
                finalData.push(o);

                
            }
            json2csv({data:finalData, 
                     fields:['LinkedAccountId','user:Name', 'ProductName','cost'], 
                     fieldNames:['Account', 'Name', 'Service', 'Cost']}, 
                     function(err,csv){
                         console.log(csv);
                         process.exit(0);
                     });
            //console.log(data.sort(sort));
        }
    };

    col.group(o.keys, o.condition, o.initial, o.reduce, o.cb);

}


