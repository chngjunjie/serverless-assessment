const serverless = require('serverless-http');
const bodyParser = require('body-parser');
const express = require('express')
const app = express()
const AWS = require('aws-sdk');
const uuid = require('uuid');
const crypto = require("crypto");

const USERS_TABLE = process.env.USERS_TABLE;

const IS_OFFLINE = process.env.IS_OFFLINE;
let dynamoDb;
let dynamoFunction;

if (IS_OFFLINE === 'true') {
    dynamoDb = new AWS.DynamoDB.DocumentClient({
        region: 'localhost',
        endpoint: 'http://localhost:8000',
    })
    dynamoFunction = new AWS.DynamoDB({
        region: 'localhost',
        endpoint: 'http://localhost:8000',
    });
    console.log(dynamoDb);
} else {
  dynamoDb = new AWS.DynamoDB.DocumentClient();
  dynamoFunction = new AWS.DynamoDB();
};

app.use(bodyParser.json({ strict: false }));

app.get('/', function (req, res) {
  res.send('Hello World!')
})

// Get User endpoint
app.get('/users/:userId', function (req, res) {
  const params = {
    TableName: USERS_TABLE,
    Key: {
      userId: req.params.userId,
    },
  }

  dynamoDb.get(params, (error, result) => {
    if (error) {
      console.log(error);
      res.status(400).json({ error: 'Could not get user' });
    }
    if (result.Item) {
      const {userId, name} = result.Item;
      res.json({ userId, name });
    } else {
      res.status(404).json({ error: "User not found" });
    }
  });
})

app.get('/users/search/:criteria', async function (req, res) {
    try {
        var params = {
            KeyConditionExpression: 'name = :name',
            ExpressionAttributeValues: {
                ':name': {'S': req.params.criteria}
            },
            TableName: USERS_TABLE
        };
        var result = await dynamoFunction.query(params).promise()
        console.log(JSON.stringify(result))
    } catch (error) {
        console.error(error);
    }
});


// Create User endpoint
app.post('/users', function (req, res) {

    // async function writeUser(name, email, address, contact) {
    //     let error = false;
    //     let errorField = '';
    //     if (typeof name !== 'string') {
    //       error = true;
    //       errorField = 'name must be a string';
    //     } 
    //     if (typeof email !== 'string') {
    //       error = true;
    //       errorField = 'email must be a string';
    //     } 
    //     else if (typeof address !== 'string') {
    //       error = true;
    //       errorField = 'address must be a string';
    //     }
    //     else if (typeof contact !== 'number') {
    //       error = true;
    //       errorField = 'contact must be a number';
    //     }

    //     if (error)
    //     {
    //       res.status(400).json({ error: errorField });
    //       return false;
    //     }
    //     else
    //     {
    //         let userId = uuid.v1();
    //         const params = {
    //             TableName: USERS_TABLE,
    //             Item: {
    //                 userId: userId,
    //                 name: name,
    //                 contact: contact,
    //                 address: address,
    //                 email: email
    //             },
    //         };

    //         // putNewData(data, callback);
    //         dynamoDb.put(params, (error) => {
    //             if (error) {
    //                 console.log("error duing create of ", name);
    //                 return false;
    //             }
    //             else
    //             {
    //                 console.log("create ", name , " successfully.");
    //                 return true;
    //             }
    //         }).promise()
    //         .then(function(data) {
    //           console.log('data : ', data);
    //         })
    //         .catch(function(err) {
    //           console.log(err);
    //         });;    
    //     }
    // }

    async function writeData() {
        const { users } = req.body;

        if (typeof users === 'undefined')
        {
            res.status(400).json({ error: 'No users found in json.' });
        }
        else
        {
            let fail = [];
            let sucessful = [];

            // Build the batches
            var batches = [];
            var current_batch = [];
            var item_count = 0;

            for (var i = 0; i < users.length; i++) {
                const user = users[i];

                // Validations
                let errorField = '';
                if (typeof user.name !== 'string') {
                  errorField += 'name must be a string.';
                } 
                if (typeof user.email !== 'string') {
                  errorField += 'email must be a string.';
                } 
                if (typeof user.address !== 'string') {
                  errorField += 'address must be a string.';
                }
                if (typeof user.contact !== 'number') {
                  errorField += 'contact must be a number.';
                }

                if (errorField)
                {
                    f_users = {
                        "users": user,
                        "error": errorField
                    }
                    fail.push(f_users);
                    continue;
                }
                
                users[i]['userId'] = uuid.v4();

                // Add the item to the current batch
                item_count++
                current_batch.push({
                  PutRequest: {
                    Item: users[i],
                  },
                })
                // If we've added 25 items, add the current batch to the batches array
                // and reset it
                if (item_count % 25 === 0) {
                  batches.push(current_batch)
                  current_batch = []
                }
              }
            
            // Add the last batch if it has records and is not equal to 25
            if (current_batch.length > 0 && current_batch.length !== 25) {
                batches.push(current_batch)
            }

            // Handler for the database operations
            var completed_requests = 0
            var errors = false;

            function handler (req) {

                console.log('in the handler: ', req)

                return function (err, data) {
                    // Increment the completed requests
                    completed_requests++;

                    // Set the errors flag
                    errors = (errors) ? true : err;

                    // Log the error if we got one
                    if(err) {
                        console.error("11 " ,JSON.stringify(err, null, 2));
                        console.error("Request that caused database error:");
                        console.error("12 ", JSON.stringify(req, null, 2));

                        fail.push({ 
                            "error message": err,
                            "error item" : req 
                        });
                    }

                    // Make the callback if we've completed all the requests
                    if(completed_requests === batches.length) {
                        // callback(errors);
                        sucessful.push("Finish Run.");
                        res.json({ "fail_message" : fail, "message": sucessful });
                    }
                }
            }

            // Make the requests
            var params;
            for (var j = 0; j < batches.length; j++) {
                // Items go in params.RequestItems.id array
                // Format for the items is {PutRequest: {Item: ITEM_OBJECT}}
                params = '{"RequestItems": {"' + USERS_TABLE + '": []}}'
                params = JSON.parse(params)
                params.RequestItems[USERS_TABLE] = batches[j]

                console.log('before db.batchWrite: ', params)

                // Perform the batchWrite operation
                dynamoDb.batchWrite(params, handler(params))
            }
        }
    }

    // Delete table and recreate
    var deleteParams = {
        TableName : USERS_TABLE
    };

    var paramsWaitFor = {
        TableName : USERS_TABLE /* required */
    };

    async function createTable() {
        dynamoFunction.createTable({
            TableName: USERS_TABLE,
            KeySchema: [{
                AttributeName: "userId",
                KeyType: "HASH"
            }],
            AttributeDefinitions: [{
                AttributeName: "userId",
                AttributeType: "S"
            }],
            ProvisionedThroughput: {
                ReadCapacityUnits: 1,
                WriteCapacityUnits: 1
            }
        }, async function(err){
            if (err) {
                console.log(err);
            }
            else {
                await writeData();
            }
        })
    }

    async function waitForTableNotExists() {
        dynamoFunction.waitFor('tableNotExists', paramsWaitFor, async function(waitForErr,
                waitForData) {
            if (waitForErr) {
                console.log(waitForErr, waitForErr.stack); // an error occurred
            } else {
                console.log('Deleted ====>', JSON.stringify(waitForData, null, 2));
                await createTable();
            }
        });
    }

    dynamoFunction.deleteTable(deleteParams, async function(err, data) {
        if (err) {
            console.error("Unable to delete table. Error JSON:", JSON.stringify(
                    err, null, 2));
        } else {
            console.log("Deleted table. Table description JSON:", JSON.stringify(
                    data, null, 2));
            await waitForTableNotExists();

        }
    });
})


module.exports.handler = serverless(app);

