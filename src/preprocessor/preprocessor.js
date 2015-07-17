// preprocessor.js
// After the digester.js handler has verified that the new file
// is not a duplicate, it puts the file into the inprogress
// bucket. The inprogress bucket is configured to trigger
// the execution of this handler, the preprocessor, to
// parse the new file, breaking it into separate parts for
// each line item, and placing those parts into separate
// S3 objects in the inorderlines bucket.
console.log('Loading function');

var async = require('async');
var aws = require('aws-sdk');
var s3 = new aws.S3();

var orderId;

var upload = function(lineItem, callback) {
    lineItemKey = "orderLineItem_" + orderId + "_" + lineItem.lineItemId;
    orderLineBody = JSON.stringify(lineItem, null, 2);
    putParams = {
        Bucket: 'inorderlines',
        Key: lineItemKey,
        Body: orderLineBody
    };
    console.log(putParams);
    s3.putObject(putParams, function(err, data) {
        if (err) {
            console.log("error putting object: " + err, err.stack);
            callback(null, "success");
        } else {
            console.log("done!", data);
            callback(null, "success");
        }
    });
};

exports.handler = function(event, context) {
    console.log("Received event ", JSON.stringify(event, null, 2));

    var bucket = event.Records[0].s3.bucket.name;
    var key = event.Records[0].s3.object.key;
    var eTag = event.Records[0].s3.object.eTag;

    // Get the object
    s3.getObject({Bucket: bucket, Key: key}, function(err, data) {
        if (err) {
            console.log("error getting object: " + err);
        } else {
            var body = new Buffer(data.Body);
            var parsedData = JSON.parse(body);
            orderId = parsedData.orderId;
            async.map(parsedData.lineItems, upload, function(err, results) {
                if (err) {
                    console.log(err, err.stack);
                    context.fail();
                } else {
                    console.log("success");
                    context.done();
                }
            });
        }
    });
};
