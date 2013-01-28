<?php
namespace DynamoQL;
/*
 * Copyright 2013 Konrad Kiss - konradkiss.com. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

/**
 * DynamoQL is a class library that brings a (very) simple query language (SQL-like) interface to the
 * no-sql Amazon DynamoDB database service. Amazon DynamoDB is copyright 2010-2013 Amazon.com Inc. or its affiliates.
 *
 * @version 2013.01.28
    * @license See the included NOTICE.md file for complete information.
 * @copyright See the included NOTICE.md file for complete information.
 * @link https://github.com/KonradKiss/dynamoql DynamoQL on GitHub
 */

use Aws\DynamoDb\DynamoDbClient;
use Aws\DynamoDb\Enum\Type;
use Aws\DynamoDb\Exception;
use Aws\DynamoDb\Model\BatchRequest\WriteRequestBatch;
use Aws\DynamoDb\Model\BatchRequest\PutRequest;
use Aws\DynamoDb\Model\BatchRequest\DeleteRequest;
use Aws\DynamoDb\Model\Item;
use Guzzle\Common\Collection;
use DynamoQL\Common\Helper;
use DynamoQL\Common\DKeyPair;
use DynamoQL\Common\TypeConverter;
use DynamoQL\Common\Enum;
use DynamoQL\Common\ConditionParser;
use DynamoQL\Common\Exception\ExceptionManager;
use DynamoQL\Common\Exception\DynamoQLException;
use DynamoQL\Common\UUIDManager;

use DynamoQL\RuntimeDebug;

class DynamoQL
// ----------------------------------------------------------------------------
{
    private $dynamodb                                   = null;
    private $batch                                      = null;
    private $tables                                     = [];
    private $debug                                      = false;

    private $maximum_try_count                          = 5;
    private $executionTime                              = 30;
    private $waitTimeoutSeconds                         = 300;

    /**
     * DynamoQL constructor.
     *
     * @param null $opts options array for passing AWS credentials
     * @param bool $debug controls whether the current instance allows debug messages
     */
    public function __construct($opts=null, $debug=false)
    {
        $this->debug = $debug;
        if ($opts == null) {
            ExceptionManager::raiseError(new DynamoQLException("DynamoQL: Either specify a DynamoDBClient object or an array of credentials."));
        } else {
            if (is_array($opts)) {
                $this->dynamodb = DynamoDBClient::factory($opts);
            } else {
                $this->dynamodb = $opts;
            }
        }
    }

    /**
     * DynamoQL destructor. Makes sure that the batch queue is empty when the object is destroyed.
     */
    public function __destruct()
    {
        if (!$this->getBatch()->isEmpty()) {
            // TODO: Should we automatically flush here?
            ExceptionManager::raiseError(new DynamoQLException("DynamoQL: The write request batch is not empty!"));
        }
    }

    /**
     * Prepares a unified response with return status information.
     *
     * @param $response - a response array with the resulting data
     *
     * @return array    an array of response data extended with status code, status message
     *                  and exception type information or the input if it was an unrecognized object
     */
    private function prepareResponse($response)
    {
        // get only the result data array
        // assuming we don't need anything from the parameter object
        if (gettype($response) == "object" && method_exists($response, "getAll")) {
            $response = $response->getAll();
        }
        // convert boolean return values (just in case)
        if ($response === true) {
            return ["status" => \DynamoQL\Common\Enum\Response::OK, "message" => "", "exception_type" => ""];
        } else if ($response === false) {
            return ["status" => \DynamoQL\Common\Enum\Response::UNKNOWN, "message" => "Unknown error.", "exception_type" => ""];
        } else if (gettype($response) == "array") {
            if (!isset($response['status']))
                $response['status'] = \DynamoQL\Common\Enum\Response::OK;
            if (!isset($response['message']))
                $response['message'] = '';
            if (!isset($response['exception_type']))
                $response['exception_type'] = '';
            return $response;
        }

        // uh.. better not touch it?
        return $response;
    }

    private function createOKResponse()
    {
        return [
            'status' => \DynamoQL\Common\Enum\Response::OK,
            'message' => '',
            'exception_type' => ''
        ];
    }

    private function createGenericErrorResponse($message)
    {
        return [
            'status' => \DynamoQL\Common\Enum\Response::UNKNOWN,
            'message' => $message,
            'exception_type' => 'Exception'
        ];
    }

    private function prepareReturnValue($response, $commandType="")
    {
        $r = [];

        $r["dql"] = true;
        $r["insertid"] = "";
        $r["cursor"] = 0;
        $r["rows"] = null;
        $r["errno"] = -1;
        $r["error"] = "Invalid response. Please review your dql query.";
        $r["errtype"] = "";
        $r["type"] = $commandType;

        if ($response === false) {
            //
        } else if ($response === true) {
            $r["errno"] = \DynamoQL\Common\Enum\Response::OK;
            $r["error"] = "";
        } else {
            if (isset($response['insertid']) && is_numeric($response['insertid'])) {
                $r["insertid"] = $response['insertid'];
            }
            $r["rows"] = isset($response['rows']) ? $response['rows'] : [];
            $r["errno"] = $response['status'];
            $r["error"] = $response['message'];
            $r["errtype"] = isset($response['exception_type']) ? $response['exception_type'] : '';
        }

        return $r;
    }

    /**
     * Executes an sql statement and returns the result object.
     *
     * @param {String} $dql The sql-like query markup to execute.
     * @param {Integer} (Optional) $try_count The number of retries already done.
     * @return {Array} The response array
     */
    public function dql($dql, $try_count=0)
    {
        if ($this->debug) {
            RuntimeDebug::message("DQL", $dql);
        }

        if (strtoupper(substr(trim($dql), 0, 4))=="DQL ") {
            $dql = substr(trim($dql), 4);
        }

        $params = $this->parseDQL($dql);

        if (!empty($params["TABLE"]))
            $this->cacheTableDescription($params["TABLE"]);

        $commandType = trim(strtolower(substr(trim($dql), 0, 6)));

        switch(trim(strtoupper(substr(trim($dql), 0, 6)))) // Note: this is uppercase, $commandType is lowercase!
        {
            case "FLUSH": // flush the WriteRequestBatch to make sure that all queued operations are sent
                $response = $this->flushBatch();
                break;
            case "Q SELE": // eventually consistent read
                $response = $this->selectFromTable($params["TABLE"], $params["FIELDS"], $params["WHERE"], $params["LIMIT"], $params["STARTING"], false);
                break;
            case "SELECT": // consistent read
                $response = $this->selectFromTable($params["TABLE"], $params["FIELDS"], $params["WHERE"], $params["LIMIT"], $params["STARTING"], true);
                break;
            case "Q INSE": // queued insert
                $response = $this->insertIntoTable($params["TABLE"], $params["VALUES"], true);
                break;
            case "INSERT": // not queued
                $response = $this->insertIntoTable($params["TABLE"], $params["VALUES"], false);
                break;
            case "Q UPDA": // (not implemented, equivalent to UPDATE) queued update, the value in the condition must exist
                $response = $this->updateInTable($params["TABLE"], $params["SET"], $params["WHERE"], false);
                break;
            case "UPDATE": // not queued, the value in the condition must exist
                $response = $this->updateInTable($params["TABLE"], $params["SET"], $params["WHERE"], true);
                break;
            case "Q DELE": // queued delete
                $response = $this->deleteFromTable($params["TABLE"], $params["WHERE"], true);
                break;
            case "DELETE": // not queued
                $response = $this->deleteFromTable($params["TABLE"], $params["WHERE"], false);
                break;
            case "Q CREA":
                $response = $this->createTable($params["TABLE"], $params["HASH"], $params["HASH_TYPE"], $params["RANGE"], $params["RANGE_TYPE"], false);
                break;
            case "CREATE":
                $response = $this->createTable($params["TABLE"], $params["HASH"], $params["HASH_TYPE"], $params["RANGE"], $params["RANGE_TYPE"], true);
                break;
            case "Q REMO":
                $response = $this->removeTable($params["TABLE"], false);
                break;
            case "REMOVE":
                $response = $this->removeTable($params["TABLE"], true);
                break;
            case "RESIZE":
                $response = $this->resizeThroughput($params["TABLE"], $params["READ"], $params["WRITE"]);
                break;
            case "GLOBAL":
                $response = $this->globalThroughput($params["READ"], $params["WRITE"]);
                break;
            // throughput change rules (per AWS):
            // increase to at most double
            // decrease (change?) only once a day
            default:
                $response = $this->createGenericErrorResponse("Syntax error in DQL: ".$dql);
        }

        $r = $this->prepareReturnValue($response, $commandType);

        // handle a few special errors that require retries
        if ($r["errno"] != \DynamoQL\Common\Enum\Response::OK) {
            $must_retry = false;
            $too_many_tries = false;
            if ($try_count>=$this->maximum_try_count)
                $too_many_tries = true;
            switch ($commandType) {
                case "q inse":
                case "insert":
                    switch ($r["errno"]) {
                        case \DynamoQL\Common\Enum\Response::CONDITIONAL_CHECK_FAILED:
                            // inserts are tried only a number of times.. it is possible that the record is an exact duplicate of another and it will then loop endlessly
                            if (!$too_many_tries)
                                $must_retry = true;
                            break;
                    }
                    break;
                case "q upda":
                case "update":
                    switch ($r["errno"]) {
                        case \DynamoQL\Common\Enum\Response::CONDITIONAL_CHECK_FAILED:
                            // updates fail when the key to update does not exist
                            if (!$too_many_tries)
                                $must_retry = false;
                            break;
                    }
                    break;
                case "q crea":
                case "create":
                case "q remo":
                case "remove":
                    switch ($r["error"]) {
                        // any kind of limits, throttling messages and resources can be tried again until they succeed
                        case \DynamoQL\Common\Enum\Response::LIMIT_EXCEEDED:
                        case \DynamoQL\Common\Enum\Response::PROVISIONED_THROUGHPUT_EXCEEDED:
                        case \DynamoQL\Common\Enum\Response::THROTTLING:
                            if (!$too_many_tries)
                                $must_retry = true;
                            break;
                    }
                    break;
            }

            if ($must_retry) {
                $try_count++;
                echo("\nRetrying (".($try_count<$this->maximum_try_count ? $try_count : "last try").") ".$dql."\nReason: ".$r["errno"].": ".$r["error"]."\n");
                set_time_limit($this->executionTime);
                sleep(1); // the query will wait 1 second before trying again
                $r = $this->dql($dql, $try_count);
                return $r;
            }

            ExceptionManager::raiseError(new DynamoQLException("DynamoQL: DQL ERROR ".($try_count>=$this->maximum_try_count ? "(maximum [".$this->maximum_try_count."] tries made)" : "")." " . $r["errno"]." : ".$r["error"]." > ".$dql));
        }

        if ($this->debug)
            RuntimeDebug::showQuery($r);

        return $r;
    }

    /*
     * Caches generic table information.
     * @returns boolean Returns true if there was an actual request made for table info,
     *                  false if the information is already available in the cache.
     */
    private function cacheTableDescription($table_name)
    {
        if (isset($table_name)) {
            if (!isset($this->tables[$table_name])) {
                $response = $this->describeTable($table_name);
                if ($response['status'] == \DynamoQL\Common\Enum\Response::OK) {
                    if (isset($response['Table'])) {
                        $this->tables[$table_name] = $response['Table'];
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private function selectFromTable($table_name, $fields="", $condition=[], $limit="", $starting="", $consistent=false)
    {
        // cache table data
        $this->cacheTableDescription($table_name);
        $keypair = new DKeyPair($this->tables[$table_name]);

        $hash_key_value = "";

        // $has_hash = false;
        $has_range = false;
        $hash_specific = false;

        $cmd_arr = [];
        $cmd_arr["TableName"] = $table_name;
        if (!empty($fields)) {
            $cmd_arr["AttributesToGet"] = $fields;
        }
        if (!empty($condition)) {
            $filter_list = [];
            ConditionParser\Select::parse($condition, $keypair, $filter_list, $hash_key_value, $hash_specific, $has_range);
            $cmd_arr["ScanFilter"] = $filter_list;
        }
        if (is_numeric($limit)) $cmd_arr["Limit"] = (int)$limit;
        if ($starting != "") {
            $starting = explode(",", $starting);
            if (count($starting) > 0) {
                $esk = [];
                $esk["HashKeyElement"] = [$keypair->hash->type => $starting[0]];
                if (count($starting) > 1) {
                    $esk["RangeKeyElement"] = [$keypair->range->type => $starting[1]];
                }
                $cmd_arr["ExclusiveStartKey"] = $esk;
            }
        }

        if ($hash_specific && $keypair->range != null) {
            $ca = $cmd_arr;
            // query by hash
            // will need to rearrange cmd_arr
            unset($cmd_arr["ScanFilter"]);
            $cmd_arr["HashKeyValue"] = [$keypair->hash->type => $hash_key_value];
            if ($has_range) {
                $cmd_arr["RangeKeyCondition"] = $ca["ScanFilter"][$keypair->range->name];
            }
            if ($consistent)
                $cmd_arr["ConsistentRead"] = true;
            try {
                $response = $this->dynamodb->query($cmd_arr);
                // make sure the response has status information
                $response = $this->prepareResponse($response);
            } catch (\Exception $e) {
                $response = ExceptionManager::process($e);
            }
        } else {
            // scan
            try {
                $response = $this->dynamodb->scan($cmd_arr);
                // make sure the response has status information
                $response = $this->prepareResponse($response);
            } catch (\Exception $e) {
                $response = ExceptionManager::process($e);
            }
        }

        if ($response['status'] != \DynamoQL\Common\Enum\Response::OK) {
            return $response;
        }

        $rows = $response['Items'];
        $returned_rows = [];
        foreach($rows as $row) {
            $returned_row = [];
            foreach($row as $field_name => $field_data) {
                foreach($field_data as $field_value) {
                    $returned_row[$field_name] = $field_value;
                    break;
                }
            }
            $returned_rows[] = $returned_row;
        }

        $response['rows'] = [];
        $response['rows'] = $returned_rows;

        return $response;
    }

    private function getBatch()
    {
        if ($this->batch == null) {
            $this->batch = WriteRequestBatch::factory($this->dynamodb);
        }
        return $this->batch;
    }

    private function flushBatch()
    {
        try {
            $response = $this->getBatch()->flush();
            $response = $this->prepareResponse($response);
        } catch (\Exception $e) {
            $response = ExceptionManager::process($e);
        }
        return $response;
    }

    private function addBatch($op, $table, $data)
    {
        switch ($op) {
            case "put":
                try {
                    $response = $this->getBatch()->add(new PutRequest(new Item($data), $table));
                    if (get_class($response) == "Guzzle\\Batch\\FlushingBatch") {
                        // this is the object we expect here
                        $response = $this->createOKResponse();
                    } else {
                        // this is something other than a batch object
                        $response = $this->createGenericErrorResponse("There was an error while trying to batch a put request in table ".$table." with the following data: ".print_r($data, true));
                    }
                } catch (\Exception $e) {
                    $response = ExceptionManager::process($e);
                }
                break;
            case "delete":
                try {
                    $response = $this->getBatch()->add(new DeleteRequest($data['Key'], $table));
                    if (get_class($response) == "Guzzle\\Batch\\FlushingBatch") {
                        // this is the object we expect here
                        $response = $this->createOKResponse();
                    } else {
                        // this is something other than a batch object
                        $response = $this->createGenericErrorResponse("There was an error while trying to batch a delete request in table ".$table." with the following data: ".print_r($data, true));
                    }
                } catch (\Exception $e) {
                    $response = ExceptionManager::process($e);
                }
                break;
            default:
                $response = $this->createGenericErrorResponse('Unknown batch operation ('.$op.'). Use put or delete (delete not implemented).');
        }
        return $response;
    }

    private function updateInTable($table_name, $values, $condition, $should_batch = false)
    {
        // cache table data
        $this->cacheTableDescription($table_name);
        $keypair = new DKeyPair($this->tables[$table_name]);

        $field_pairs = [];
        foreach($values as $attr_name => $value) {
            if ($attr_name != $keypair->hash->name && $attr_name != $keypair->range->name) {
                $field_pairs[$attr_name] = ["Action" => \Aws\DynamoDb\Enum\AttributeAction::PUT, "Value" => [(is_numeric($value) ? Type::NUMBER : Type::STRING) => $value]];
            }
        }

        $expected = [];
        $keys = [];

        // fetch the condition into the field_pairs and expected arrays
        ConditionParser\Update::parse($condition, $keypair, $keys, $expected);

        // $expected and $field_pairs should now be parsed out
        $update_data = [
            "TableName" => $table_name,
            "Key" => $keys,
            "AttributeUpdates" => $field_pairs
        ];
        if (!empty($expected)) {
            $update_data["Expected"] = $expected;
        }

        // can not be batched until Expected is supported
//        if ($should_batch) {
//            $response = $this->addBatch("put", $table_name, $field_pairs);
//        } else {
//            $this->flushBatch();
            try {
                $response = $this->dynamodb->updateItem($update_data);
                // make sure the response has status information
                $response = $this->prepareResponse($response);
            } catch (\Exception $e) {
                $response = ExceptionManager::process($e);
            }
//        }
        return $response;
    }

    private function insertIntoTable($table_name, $values, $should_batch = true)
    {
        // cache table data
        $this->cacheTableDescription($table_name);
        $keypair = new DKeyPair($this->tables[$table_name]);

        $field_pairs = [];
        $hash_key_value = "";
        $uuid_base = "";
        foreach($values as $attr_name => $value) {
            $uuid_base .= $attr_name.$value;
            if ($attr_name == $keypair->hash->name) {
                $field_pairs[$attr_name] = [$keypair->hash->type => (string)$value];
                $hash_key_value = $value;
            } else if ($keypair->range != null && $attr_name == $keypair->range->name) {
                $field_pairs[$attr_name] = [$keypair->range->type => $value];
            } else {
                $field_pairs[$attr_name] = is_numeric($value) ? [Type::NUMBER => (string)$value] : [Type::STRING => (string)$value];
            }
        }
        $expected = "";
        // omitting the hash key from the value list will make this automatically create a uuid number as the hash key value
        if ($hash_key_value == "") {
            $uuid = (string)UUIDManager::generate($uuid_base, 32);
            $field_pairs[$keypair->hash->name] = [$keypair->hash->type => (string)$uuid];
            $hash_key_value = $uuid;
            // if we're auto-generating a uuid, then we assume that it does not exist
            // this will make it fail with an EXCEPTION_VALIDATION if the id exists and not simply overwrite an existing record
            $expected = [$keypair->hash->name => ["Exists" => false]];
        }
        $insert_data = [
            "TableName" => $table_name,
            "Item" => $field_pairs ///*$this->dynamodb->attributes(*/$values/*)*/
        ];
        if ($expected != "")
            $insert_data["Expected"] = $expected;

        // INSERT does not batch, Q INSERT does
        if ($should_batch) {
            $response = $this->addBatch("put", $table_name, $field_pairs);
        } else {
            // flush the batch
            $this->flushBatch();
            try {
                $response = $this->dynamodb->putItem($insert_data);
                // make sure the response has status information
                $response = $this->prepareResponse($response);
                $response['insertid'] = $hash_key_value;
            } catch (\Exception $e) {
                $response = ExceptionManager::process($e);
            }
        }

        return $response;
    }

    private function deleteFromTable($table_name, $filter_condition, $should_batch)
    {
        if ($should_batch) {
            // assemble an array that works with batches (Expected is not supported in batches as of 2013-01-17)
            $delete_request = [
                'TableName' => $table_name,
                'Key' =>  [
                    'HashKeyElement' => $filter_condition['Key']['HashKeyElement']
                ]
            ];
            if (isset($filter_condition['Key']['RangeKeyElement'])) {
                $delete_request['Key']['RangeKeyElement'] = $filter_condition['Key']['RangeKeyElement'];
            }
            $response = $this->addBatch("delete", $table_name, $delete_request);
        } else {
            // flush the batch
            $this->flushBatch();
            $filter_condition['TableName'] = $table_name;
            try {
                $response = $this->dynamodb->deleteItem($filter_condition);
                // make sure the response has status information
                $response = $this->prepareResponse($response);
            } catch (\Exception $e) {
                $response = ExceptionManager::process($e);
            }
        }
        return $response;
    }

    private function createTable($table_name, $hash_key, $hash_type, $range_key, $range_type, $wait=true)
    {
        $param_array = [
            'TableName' => $table_name,
            'KeySchema' => [
                'HashKeyElement' => [
                    'AttributeName' => $hash_key,
                    'AttributeType' => TypeConverter::byName($hash_type)
                ]
            ],
            'ProvisionedThroughput' => [
                'ReadCapacityUnits' => 5,
                'WriteCapacityUnits' => 5
            ]
        ];
        if ($range_key != "") {
            $param_array['KeySchema']['RangeKeyElement'] = [
                'AttributeName' => $range_key,
                'AttributeType' => TypeConverter::byName($range_type)
            ];
        }
        try {
            $response = $this->dynamodb->createTable($param_array);
            // make sure the response has status information
            $response = $this->prepareResponse($response);
        } catch (\Exception $e) {
            $response = ExceptionManager::process($e);
            return $response;
        }

        if (!$wait)
            return $response;

        $count = $this->waitTimeoutSeconds;
        do {
            set_time_limit($this->executionTime);
            try {
                $response = $this->dynamodb->describeTable([
                    'TableName' => $table_name
                ]);
                // make sure the response has status information
                $response = $this->prepareResponse($response);
                $table_status = $response['Table']['TableStatus'];
            } catch (\Exception $e) {
                // this should wait on if there's a ResourceNotFound exception but bail for any other exception
                $response = ExceptionManager::process($e, "ResourceNotFound");
                if ($response['status'] != \DynamoQL\Common\Enum\Response::OK)
                    break;
                // try again later otherwise
                $table_status = "";
            }
            // check if the table is up
            if ($table_status == "ACTIVE")
                break;
            // try again in a sec
            sleep(1);
        } while ($count--);

        return $response;
    }

    private function removeTable($table_name, $wait=true)
    {
        try {
            $response = $this->dynamodb->deleteTable([
                'TableName' => $table_name
            ]);
            // make sure the response has status information
            $response = $this->prepareResponse($response);
        } catch (\Exception $e) {
            // it should not be an error when the table is not found
            $response = ExceptionManager::process($e, "Aws\\DynamoDb\\Exception\\ResourceNotFoundException");
            return $response;
        }

        if (!$wait)
            return $response;

        $count = $this->waitTimeoutSeconds;
        do {
            set_time_limit($this->executionTime);
            try {
                $response = $this->dynamodb->describeTable([
                    'TableName' => $table_name
                ]);
                // make sure the response has status information
                $response = $this->prepareResponse($response);
            } catch (\Exception $e) {
                // We do not want to treat a ResourceNotFoundException as an exception,
                // since this signals normal behavior (the resource has been deleted).
                // Anything else is scary!
                $response = ExceptionManager::process($e, "Aws\\DynamoDb\\Exception\\ResourceNotFoundException");
                return $response;
            }
            // we'll need to wait for the table to be deleted
            sleep(1);
        } while ($count--);

        return $response;
    }

    private function resizeThroughput($table_name, $read_capacity = 5, $write_capacity = 5)
    {
        // use a cache if at all possible
        $this->cacheTableDescription($table_name);

        $current_read_capacity = $this->tables[$table_name]['ProvisionedThroughput']['ReadCapacityUnits'];
        $current_write_capacity = $this->tables[$table_name]['ProvisionedThroughput']['WriteCapacityUnits'];

        $read_capacity = max(1, min($current_read_capacity * 2, $read_capacity));
        $write_capacity = max(1, min($current_write_capacity * 2, $write_capacity));

        if ($current_read_capacity==$read_capacity && $current_write_capacity==$write_capacity)
            return $this->createOKResponse();
        try {
            $response = $this->dynamodb->updateTable([
                'TableName' => $table_name,
                'ProvisionedThroughput' => array(
                    'ReadCapacityUnits' => (int)$read_capacity,
                    'WriteCapacityUnits' => (int)$write_capacity
                )
            ]);
            // make sure the response has status information
            $response = $this->prepareResponse($response);
        } catch (\Exception $e) {
            $response = ExceptionManager::process($e);
            return $response;
        }
        // success - remove the table cache so that it's refreshed next time
        unset($this->tables[$table_name]);
        return $response;
    }

    private function globalThroughput($read_capacity = 5, $write_capacity = 5)
    {
        try {
            $response = $this->dynamodb->listTables();
            // make sure the response has status information
            $response = $this->prepareResponse($response);
        } catch (\Exception $e) {
            $response = ExceptionManager::process($e);
            return $response;
        }
        $result = $response['status'] == \DynamoQL\Common\Enum\Response::OK;
        if ($result) {
            foreach($response['TableNames'] as $table_name) {
                $resp = $this->resizeThroughput($table_name, $read_capacity, $write_capacity);
                if ($resp['status'] != \DynamoQL\Common\Enum\Response::OK)
                    return $resp;
            }
        }
        return $response;
    }

    private function describeTable($table_name, $must_exist=false)
    {
        try {
            $response = $this->dynamodb->describeTable([
                'TableName' => $table_name
            ]);
        } catch (\Exception $e) {
            // if there is no need for the table to exist (ie. during pre-caching)
            // then not finding the table will be treated as a \DynamoQL\Common\Enum\Response::OK
            $response = ExceptionManager::process($e, ($must_exist ? "" : "Aws\\DynamoDb\\Exception\\ResourceNotFoundException"));
            return $response;
        }
        // make sure the response has status information
        $response = $this->prepareResponse($response);
        return $response;
    }

    public function parseDQL($dql)
    {
        $dql = trim($dql);
        if (empty($dql)) {
            return "";
        }

        $cmd = [];
        switch(strtoupper(trim(substr($dql, 0, 6))))
        {
            case "FLUSH":
                break;
            case "Q SELE":
            case "SELECT":
                $split = preg_split("/^(q select|select)[\s]+(.*?)(from)[\s]+([^\s]*?)[\s]*?(where){0,1}?[\s]*?(.*)?/ismU", $dql, -1, PREG_SPLIT_NO_EMPTY | PREG_SPLIT_DELIM_CAPTURE);
                $cmd["FIELDS"] = Helper::breakupList(trim($split[1]), false);
                if ($cmd["FIELDS"][0] == "*")
                    $cmd["FIELDS"] = [];
                $cmd["TABLE"] = trim($split[3]);
                $cmd["WHERE"] = "";
                $cmd["LIMIT"] = "";
                $cmd["STARTING"] = "";
                if (count($split)>5) {
                    // stuff trailing WHERE
                    $trailing = $split[5];
                    $ta = explode(" ", $trailing);
                    $mnemonic = 0; // 0- where, 1- limit, 2- starting
                    foreach($ta as $text) {
                        if (strtoupper($text) == "LIMIT") {
                            $mnemonic = 1;
                            continue;
                        }
                        if (strtoupper($text) == "STARTING") {
                            $mnemonic = 2;
                            continue;
                        }
                        switch($mnemonic) {
                            case 0:
                                $cmd["WHERE"] .= $text . " ";
                                break;
                            case 1:
                                $cmd["LIMIT"] .= $text . " ";
                                break;
                            case 2:
                                $cmd["STARTING"] .= $text . " ";
                                break;
                        }
                    }
                    $cmd["WHERE"] = trim($cmd["WHERE"]);
                    $cmd["LIMIT"] = trim($cmd["LIMIT"]);
                    $cmd["STARTING"] = trim($cmd["STARTING"]);

                    $cmd["WHERE"] = $this->processSelectCondition($cmd["WHERE"]);
                    if (empty($cmd["WHERE"])) $cmd["WHERE"] = [];
                }
                break;
            case "Q INSE":
            case "INSERT":
                $split = preg_split("/^(q insert[\s]+into|insert[\s]+into)[\s]+(.*?)(values)[\s]+(.*?)[\s]*?/ismU", $dql, -1, PREG_SPLIT_NO_EMPTY | PREG_SPLIT_DELIM_CAPTURE);
                $cmd["TABLE"] = trim($split[1]);
                try {
                    $cmd["VALUES"] = Helper::breakupKeyValuePairs(trim($split[3]));
                } catch (DynamoQLException $e) {
                    ExceptionManager::raiseError($e);
                }
                break;
            case "Q UPDA":
            case "UPDATE":
                $split = preg_split("/^(q update|update)[\s]+(.*?)[\s]*?(set)[\s]+(.*?)[\s]*?(where){0,1}?[\s]*?(.*)?/ismU", $dql, -1, PREG_SPLIT_NO_EMPTY | PREG_SPLIT_DELIM_CAPTURE);
                $cmd["TABLE"] = trim(trim($split[1]));
                $set_where = $split[3];
                $set_where = preg_replace("/[\s]+where[\s]+/i", " WHERE ", $set_where);
                $set_where = explode("WHERE", $set_where);
                try {
                    $cmd["SET"] =  Helper::breakupKeyValuePairs(trim($set_where[0]));
                } catch (DynamoQLException $e) {
                    ExceptionManager::raiseError($e);
                }
                $cmd["WHERE"] = (count($set_where)>1) ? $this->processUpdateCondition(trim($set_where[1])) : [];
                break;
            case "Q DELE":
            case "DELETE":
                $split = preg_split("/^(q delete[\s]+from|delete[\s]+from)[\s]+(.*?)(where)[\s]*?(.*)?/ismU", $dql, -1, PREG_SPLIT_NO_EMPTY | PREG_SPLIT_DELIM_CAPTURE);
                $cmd["TABLE"] = trim($split[1]);
                $cmd["WHERE"] = $this->processDeleteCondition(trim($split[1]), trim($split[3]));
                break;
            case "Q CREA":
            case "CREATE":
                $split = preg_split("/^(q create[\s]+table|create[\s]+table)[\s]+(.*?)(hash){1}(.*?)[\s]*/ismU", $dql, -1, PREG_SPLIT_NO_EMPTY | PREG_SPLIT_DELIM_CAPTURE);
                $cmd["TABLE"] = trim($split[1]);
                $split[3] = preg_replace("/[\s]+range[\s]+/i", " RANGE ", trim($split[3]));
                $key_definitions = explode(" RANGE ", $split[3]);
                $key_definitions[0] = preg_replace("/[\s]+as[\s]+/i", " AS ",trim($key_definitions[0]));
                $hash_part = explode(" AS ", $key_definitions[0]);
                $key_definitions[1] = preg_replace("/[\s]+as[\s]+/i", " AS ",trim($key_definitions[1]));
                $range_part = count($key_definitions)>1 ? explode(" AS ", $key_definitions[1]) : ["", ""];
                $cmd["HASH"] = Helper::breakupList(trim($hash_part[0]));
                $cmd["HASH_TYPE"] = trim($hash_part[1]);
                $cmd["RANGE"] = (!empty($range_part[0])) ? Helper::breakupList(trim($range_part[0])) : "";
                $cmd["RANGE_TYPE"] = trim($range_part[1]);
                break;
            case "Q REMO":
            case "REMOVE":
                $split = preg_split("/^(q remove[\s]+table|remove[\s]+table)[\s]+(.*?)[\s]*?/ismU", $dql, -1, PREG_SPLIT_NO_EMPTY | PREG_SPLIT_DELIM_CAPTURE);
                $cmd["TABLE"] = trim($split[1]);
                break;
            case "RESIZE":
                $split = preg_split("/^(resize[\s]+throughput)[\s]+(.*?)[\s]+(read)[\s]+([0-9]+?)[\s]+(write)[\s]+([0-9]+?)[\s]*/ismU", $dql, -1, PREG_SPLIT_NO_EMPTY | PREG_SPLIT_DELIM_CAPTURE);
                $cmd["TABLE"] = trim($split[1]);
                $cmd["READ"] = trim($split[3]);
                $cmd["WRITE"] = trim($split[5]);
                break;
            case "GLOBAL":
                $split = preg_split("/^(global[\s]+throughput)[\s]+(read)[\s]+([0-9]+?)[\s]+(write)[\s]+([0-9]+?)[\s]*/ismU", $dql, -1, PREG_SPLIT_NO_EMPTY | PREG_SPLIT_DELIM_CAPTURE);
                $cmd["READ"] = trim($split[2]);
                $cmd["WRITE"] = trim($split[4]);
                break;
            default:
                ExceptionManager::raiseError(new DynamoQLException("DynamoQL: Syntax error in query: ".$dql));
                break;
        }

        return $cmd;
    }

    private function processSelectCondition($raw_conditions)
    {
        try {
            $parsed_conditions = ConditionParser::parse($raw_conditions);
        } catch (\Exception $e) {
            ExceptionManager::raiseError($e);
            return [];
        }
        if (empty($parsed_conditions))
            return [];

        return $parsed_conditions;
    }

    private function processUpdateCondition($raw_conditions)
    {
        try {
            $parsed_conditions = ConditionParser::parse($raw_conditions);
        } catch (\Exception $e) {
            ExceptionManager::raiseError($e);
            return [];
        }
        if (empty($parsed_conditions))
            return [];

        return $parsed_conditions;
    }

    private function processDeleteCondition($table_name, $raw_conditions)
    {
        // cache table data
        $this->cacheTableDescription($table_name);
        $keypair = new DKeyPair($this->tables[$table_name]);

        try {
            $parsed_conditions = ConditionParser::parse($raw_conditions);
        } catch (\Exception $e) {
            ExceptionManager::raiseError($e);
            return [];
        }
        if (empty($parsed_conditions))
            return [];

        $result = [];
        $expected = [];
        $attributes = [];

        ConditionParser\Delete::parse($parsed_conditions, $keypair, $attributes, $this->tables[$table_name]);

        if (!empty($attributes))
            $result['Key'] = $attributes;
        if (!empty($expected))
            $result['Expected'] = $expected;

        return $result;
    }
}
