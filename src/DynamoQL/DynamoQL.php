<?php
namespace DynamoQL;
/*
 * Copyright 2012 Konrad Kiss - konradkiss.com. All Rights Reserved.
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
 * no-sql Amazon DynamoDB database service. Amazon DynamoDB is copyright 2010-2012 Amazon.com Inc. or its affiliates.
 *
* DynamoQL is a low cost simplified solution to making simple queries to the Amazon DynamoDB service.
 *
 * @version 2012.01.26
    * @license See the included NOTICE.md file for complete information.
 * @copyright See the included NOTICE.md file for complete information.
 * @link http://www.konradkiss.com/p/dynamoql.html DynamoQL
 * @link http://www.konradkiss.com/p/documentation/ DynamoQL documentation
 */

use Aws\DynamoDb\DynamoDbClient;
use Aws\DynamoDb\Enum\Type;
use Aws\DynamoDb\Exception;
use Aws\DynamoDb\Model\BatchRequest\WriteRequestBatch;
use Aws\DynamoDb\Model\BatchRequest\PutRequest;
use Aws\DynamoDb\Model\Item;
use Guzzle\Common\Collection;
use DynamoQL\Common\Enum;

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
     * Constructs a new instance of <DynamoQL>.
     *
     * @param bool $debug
     * @param array|object $opts (Optional) If an existing AmazonDynamoDB is to be used for DynamoDB connections, pass that object, otherwise pass the options required to create a new AmazonDynamoDB object.
     * @return DynamoQL
     */
    public function __construct($opts=null, $debug=false)
    {
        $this->debug = $debug;
        if ($opts == null) {
            $this->raiseError("DynamoQL: Either specify a DynamoDBClient object or an array of credentials.");
        } else {
            if (is_array($opts)) {
                $this->dynamodb = DynamoDBClient::factory($opts);
            } else {
                $this->dynamodb = $opts;
            }
        }
    }

    public function __destruct()
    {
        if (!$this->getBatch()->isEmpty()) {
            $this->raiseError("DynamoQL: The write request batch is not empty!");
        }
    }

    private function prepareResponse($response)
    {
        // get only the result data array
        // assuming we don't need anything from the parameter object
        if (gettype($response) == "object" && method_exists($response, "getAll")) {
            $response = $response->getAll();
        }
        // convert boolean return values (just in case)
        if ($response === true) {
            return ["status" => \DynamoQL\Common\Enum\Response::OK, "message" => ""];
        } else if ($response === false) {
            return ["status" => \DynamoQL\Common\Enum\Response::UNKNOWN, "message" => "Unknown error."];
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

    private function handleException($e, $exceptions_ok = "")
    {
        $resObject = ["status" => \DynamoQL\Common\Enum\Response::OK, "message" => "", "exception" => $e];

        if (!is_subclass_of($e, "Exception"))
            return $e;

        $className = get_class($e);

        if (!empty($exceptions_ok)) {
            if (gettype($exceptions_ok)=="string") {
                $exceptions_ok = [$exceptions_ok];
            }
            if (gettype($exceptions_ok)=="array") {
                if (in_array($className, $exceptions_ok))
                    return $resObject; // RESULT_OK, since the error is accepted / expected
            }
        }

        // this is now definitely an exception we don't like
        //$this->debugWrite("EXCEPTION (".get_class($e).") in ".$e->getFile()." at line ".$e->getLine(), $e->getMessage()."\n".$e->getTraceAsString(), false, "#f00", "#fcc");

        $resObject['message'] = $e->getMessage();
        $resObject['exception_type'] = $className;

        switch ($className) {
            case "Aws\\DynamoDb\\Exception\\AccessDeniedException":
                $resObject['status'] = \DynamoQL\Common\Enum\Response::ACCESS_DENIED;
                break;
            case "Aws\\DynamoDb\\Exception\\ConditionalCheckFailedException":
                $resObject['status'] = \DynamoQL\Common\Enum\Response::CONDITIONAL_CHECK_FAILED;
                break;
            case "Aws\\DynamoDb\\Exception\\DynamoDbException":
                $resObject['status'] = \DynamoQL\Common\Enum\Response::DYNAMODB;
                break;
            case "Aws\\DynamoDb\\Exception\\IncompleteSignatureException":
                $resObject['status'] = \DynamoQL\Common\Enum\Response::INCOMPLETE_SIGNATURE;
                break;
            case "Aws\\DynamoDb\\Exception\\InternalFailureException":
                $resObject['status'] = \DynamoQL\Common\Enum\Response::INTERNAL_FAILURE;
                break;
            case "Aws\\DynamoDb\\Exception\\InternalServerErrorException":
                $resObject['status'] = \DynamoQL\Common\Enum\Response::INTERNAL_SERVER_ERROR;
                break;
            case "Aws\\DynamoDb\\Exception\\LimitExceededException":
                $resObject['status'] = \DynamoQL\Common\Enum\Response::LIMIT_EXCEEDED;
                break;
            case "MissingAuthenticationTokenException":
                $resObject['status'] = \DynamoQL\Common\Enum\Response::MISSING_AUTHENTICATION_TOKEN;
                break;
            case "Aws\\DynamoDb\\Exception\\ProvisionedThroughputExceededException":
                $resObject['status'] = \DynamoQL\Common\Enum\Response::PROVISIONED_THROUGHPUT_EXCEEDED;
                break;
            case "Aws\\DynamoDb\\Exception\\ResourceInUseException":
                $resObject['status'] = \DynamoQL\Common\Enum\Response::RESOURCE_IN_USE;
                break;
            case "Aws\\DynamoDb\\Exception\\ResourceNotFoundException":
                $resObject['status'] = \DynamoQL\Common\Enum\Response::RESOURCE_NOT_FOUND;
                break;
            case "Aws\\DynamoDb\\Exception\\ServiceUnavailableException":
                $resObject['status'] = \DynamoQL\Common\Enum\Response::SERVICE_UNAVAILABLE;
                break;
            case "Aws\\DynamoDb\\Exception\\ThrottlingException":
                $resObject['status'] = \DynamoQL\Common\Enum\Response::THROTTLING;
                break;
            case "Aws\\DynamoDb\\Exception\\UnprocessedWriteRequestsException":
                $resObject['status'] = \DynamoQL\Common\Enum\Response::UNPROCESSED_WRITE_REQUESTS;
                break;
            case "Aws\\DynamoDb\\Exception\\UnrecognizedClientException":
                $resObject['status'] = \DynamoQL\Common\Enum\Response::UNRECOGNIZED_CLIENT;
                break;
            case "Aws\\DynamoDb\\Exception\\ValidationException":
                $resObject['status'] = \DynamoQL\Common\Enum\Response::VALIDATION;
                break;
            default:
                $resObject['status'] = \DynamoQL\Common\Enum\Response::UNKNOWN;
        }
        return $resObject;
    }

    private function prepareReturnValue($response, $commandType="")
    {
        //$this->debugWrite("RESPONSE", $response);

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
            $r["errtype"] = $response['exception_type'];
        }

        return $r;
    }

    /**
     * Executes an sql statement and returns the result object. See http://www.konradkiss.com/p/dynamoql.html
     *
     * @param string $dql The sql-like query markup to execute.
     * @param boolean $wait (Optional) Creating a table and making it active takes a while. Set this to true to make the method wait until the table is active, or set to false (default) to return regardless of the created table's status.
     * @param int $try_count
     * @return object The resulting XML object
     */
    public function dql($dql, $wait=true, $try_count=0)
    {
        $this->debugWrite("DQL (wait=".($wait?"true":"false").")", $dql);

        if (strtoupper(substr(trim($dql), 0, 4))=="DQL ") {
            $dql = substr(trim($dql), 4);
        }

        $params = $this->parseDQL($dql);
        //$this->debugWrite("PARAMS", $params);

        if (!empty($params["TABLE"]))
            $this->cacheTableDescription($params["TABLE"]);

        $commandType = trim(strtolower(substr(trim($dql), 0, 6)));

        switch(trim(strtoupper(substr(trim($dql), 0, 6)))) // Note: this is uppercase, $commandType is lowercase!
        {
            case "FLUSH":
                $response = $this->flushBatch(); // must be called at the end when either BATCHQ or MODIFY is used, all parameters are discarded
                break;
            case "CHOOSE": // consistent read
                $response = $this->selectFromTable($params["TABLE"], $params["FIELDS"], $params["WHERE"], $params["LIMIT"], $params["STARTING"], true);
                break;
            case "SELECT": // eventually consistent read
                $response = $this->selectFromTable($params["TABLE"], $params["FIELDS"], $params["WHERE"], $params["LIMIT"], $params["STARTING"], false);
                break;
            case "BATCH":
            case "BATCHQ":
                $response = $this->insertIntoTable($params["TABLE"], $params["VALUES"], true); // same as INSERT, batched
                break;
            case "INSERT":
                $response = $this->insertIntoTable($params["TABLE"], $params["VALUES"], false); // not batched
                break;
            case "MODIFY":
                $response = $this->updateInTable($params["TABLE"], $params["SET"], $params["WHERE"], false); // insert even if the item does not exist, batched
                break;
            case "UPDATE":
                $response = $this->updateInTable($params["TABLE"], $params["SET"], $params["WHERE"], true); // bail if the item does not exist, not batched
                break;
            case "DELETE":
                $response = $this->deleteFromTable($params["TABLE"], $params["WHERE"]);
                break;
            case "CREATE":
                $response = $this->createTable($params["TABLE"], $params["HASH"], $params["HASH_TYPE"], $params["RANGE"], $params["RANGE_TYPE"], $wait);
                break;
            case "REMOVE":
                $response = $this->removeTable($params["TABLE"], $wait);
                break;
            case "RESIZE":
                $response = $this->resizeThroughput($params["TABLE"], $params["READ"], $params["WRITE"]);
                break;
            case "EXTEND":
                $response = $this->extendThroughput($params["READ"], $params["WRITE"]);
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
                case "batch":
                case "batchq":
                case "insert":
                    switch ($r["errno"]) {
                        case \DynamoQL\Common\Enum\Response::CONDITIONAL_CHECK_FAILED:
                            // inserts are tried only a number of times.. it is possible that the record is an exact duplicate of another and it will then loop endlessly
                            if (!$too_many_tries)
                                $must_retry = true;
                            break;
                    }
                    break;
                case "create":
                case "remove":
                    switch ($r["error"]) {
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
                $r = $this->dql($dql, $wait, $try_count);
                return $r;
            }

            $this->raiseError("DynamoQL: DQL ERROR ".($try_count>=$this->maximum_try_count ? "(maximum [".$this->maximum_try_count."] tries made)" : "")." " . $r["errno"]." : ".$r["error"]." > ".$dql);
        }

        if ($this->debug) {
            if ($r["errno"] == \DynamoQL\Common\Enum\Response::OK) {
                if (is_array($r["rows"]) && count($r["rows"])>0) {
                    $rows = $r["rows"];
                    $table_source = "<div style='padding:4px;margin:4px;'>".count($rows)." rows returned:</div><table style='color:white;border:1px solid silver;margin:2px;'>\n";
                    // table header
                    $table_source .= "<tr style='padding:0;margin:0;'>\n";
                    $table_source .= "   <td style='padding:4px;margin:4px;border:1px solid gray;color:yellow;text-align:center;'>#</td>\n";
                    foreach($rows[0] as $field => $value) {
                        $table_source .= "   <td style='padding:4px;margin:4px;border:1px solid gray;color:yellow;text-align:center;'>".$field."</td>\n";
                    }
                    $table_source .= "</tr>\n";
                    // table rows
                    foreach ($rows as $row_id => $row_data) {
                        $table_source .= "<tr style='padding:0;margin:0;'>\n";
                        $table_source .= "   <td style='padding:4px;margin:4px;border:1px solid gray;color:yellow;text-align:right'>".$row_id."</td>\n";
                        foreach($row_data as $value) {
                            $table_source .= "   <td style='padding:4px;margin:4px;border:1px solid gray;color:white;".(is_numeric($value) ? "text-align:right;" : "")."'>".$value."</td>\n";
                        }
                        $table_source .= "</tr>\n";
                    }
                    $table_source .= "</table>";
                    $this->debugWriteHTML("RESPONSE HTML OK [".$r["errno"]."]", $table_source);
                } else if (is_numeric($r["insertid"])) {
                    $this->debugWriteOK("RESPONSE INSERT OK [".$r["errno"]."] INSERTID: ".$r["insertid"]);
                } else {
                    $this->debugWriteOK("RESPONSE OK [".$r["errno"]."]");
                }
            } else {
                //$this->debugWriteErrorVerbose("RESPONSE ERROR [".$r["errno"]."]: ".$r["error"], $r);
                $this->debugWriteError("RESPONSE ERROR [".$r["errno"]."]: ".$r["error"]." {".$r['errtype']."}");
            }
        }
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

        // cast hash to number if we must
        $hash_key_name = $this->tables[$table_name]["KeySchema"]["HashKeyElement"]["AttributeName"];
        $hash_key_type = $this->tables[$table_name]["KeySchema"]["HashKeyElement"]["AttributeType"];
        if (isset($this->tables[$table_name]["KeySchema"]["RangeKeyElement"])) {
            $range_key_name = $this->tables[$table_name]["KeySchema"]["RangeKeyElement"]["AttributeName"];
            $range_key_type = $this->tables[$table_name]["KeySchema"]["RangeKeyElement"]["AttributeType"];
        } else {
            $range_key_name = "";
            $range_key_type = "";
        }
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
            foreach($condition as $condition_array) {
                $target = $condition_array["TARGET"];
                $op = $condition_array["OP"];
                $params = $condition_array["PARAMS"];

                $attributes = [];
                if (strtolower($target) == "hash" || strtolower($target) == "hash key" || $target == $hash_key_name) {
                    $target = $hash_key_name;
                    $attributes = array($hash_key_type => $params[0]);
                    // $has_hash = true;
                    if ($op == "EQ") {
                        $hash_specific = true;
                        $hash_key_value = $params[0];
                    }
                }
                if ($range_key_name != "" && (strtolower($target) == "range" || strtolower($target) == "range key" || $target == $range_key_name)) {
                    $target = $range_key_name;
                    $attributes = array($range_key_type => $params[0]);
                    $has_range = true;
                } else {
                    if (count($params)==1) {
                        $attributes = is_numeric($params[0]) ? array(Type::NUMBER => $params[0]) : array(Type::STRING => $params[0]);
                    } else if (count($params) > 1) {
                        $is_numeric = true;
                        foreach($params as $pi => $param) {
                            if (!is_numeric($params[$pi])) {
                                $is_numeric = false;
                                break;
                            }
                        }
                        $attributes = $is_numeric ? array(Type::NUMBER_SET => $params) : array(Type::STRING_SET => $params);
                    }
                }

                $filter = [];
                $filter["ComparisonOperator"] = $op;
                $filter["AttributeValueList"] = array( $attributes );
                $filter_list[$target] = $filter;
            }
            $cmd_arr["ScanFilter"] = $filter_list;
        }
        if (is_numeric($limit)) $cmd_arr["Limit"] = (int)$limit;
        if ($starting != "") {
            $starting = explode(",", $starting);
            if (count($starting) > 0) {
                $esk = [];
                $esk["HashKeyElement"] = array($hash_key_type => $starting[0]);
                if (count($starting) > 1) {
                    $esk["RangeKeyElement"] = array($range_key_type => $starting[1]);
                }
                $cmd_arr["ExclusiveStartKey"] = $esk;
            }
        }

        if ($hash_specific && $range_key_name != "") {
            $ca = $cmd_arr;
            // query by hash
            // will need to rearrange cmd_arr
            unset($cmd_arr["ScanFilter"]);
            $cmd_arr["HashKeyValue"] = array($hash_key_type => $hash_key_value);
            if ($has_range) {
                $cmd_arr["RangeKeyCondition"] = $ca["ScanFilter"][$range_key_name];
            }
            if ($consistent)
                $cmd_arr["ConsistentRead"] = true;
            try {
                $response = $this->dynamodb->query($cmd_arr);
                // make sure the response has status information
                $response = $this->prepareResponse($response);
            } catch (\Exception $e) {
                $response = $this->handleException($e);
            }
        } else {
            // scan
            try {
                $response = $this->dynamodb->scan($cmd_arr);
                // make sure the response has status information
                $response = $this->prepareResponse($response);
            } catch (\Exception $e) {
                $response = $this->handleException($e);
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
            $response = $this->handleException($e);
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
                    $response = $this->handleException($e);
                }
                break;
            default:
                $response = $this->createGenericErrorResponse('Unknown batch operation ('.$op.'). Use put or delete (delete not implemented).'); // TODO: implement delete batching
        }
        return $response;
    }

    private function updateInTable($table_name, $values, $condition, $check_if_exists = false)
    {
        // cache table data
        $this->cacheTableDescription($table_name);

        // cast hash to number if we must
        $hash_key_name = $this->tables[$table_name]["KeySchema"]["HashKeyElement"]["AttributeName"];
        $hash_key_type = $this->tables[$table_name]["KeySchema"]["HashKeyElement"]["AttributeType"];
        if (isset($this->tables[$table_name]["KeySchema"]["RangeKeyElement"])) {
            $range_key_name = $this->tables[$table_name]["KeySchema"]["RangeKeyElement"]["AttributeName"];
            $range_key_type = $this->tables[$table_name]["KeySchema"]["RangeKeyElement"]["AttributeType"];
        } else {
            $range_key_name = "";
            $range_key_type = "";
        }

        $field_pairs = [];
        foreach($values as $attr_name => $value) {
            if ($attr_name == $hash_key_name) {
                $field_pairs[$attr_name] = array($hash_key_type => $value);
            } else if ($attr_name == $range_key_name && $range_key_name != "") {
                $field_pairs[$attr_name] = array($range_key_type => $value);
            } else {
                $field_pairs[$attr_name] = is_numeric($value) ? array(Type::NUMBER => $value) : array(Type::STRING => $value);
            }
        }

        $can_batch = true;
        if ($check_if_exists) {
            // we must have a hash / range key & value, this operation updates one specific record
            $hash_key_value = "";
            $range_key_value = "";
            foreach($condition as $condition_array) {
                $target = $condition_array["TARGET"];
                $op = $condition_array["OP"];
                $params = $condition_array["PARAMS"];
                if (strtolower($target) == "hash" || strtolower($target) == "hash key" || $target == $hash_key_name) {
                    if ($op == "EQ")
                        $hash_key_value = $params[0];
                } else if (strtolower($target) == "range" || strtolower($target) == "range key" || $target == $range_key_name) {
                    if ($op == "EQ")
                        $range_key_value = $params[0];
                }
            }

            // if there's no such record, bail immediately with an error
            if ($range_key_value == "") {
                $response = $this->dql("CHOOSE ".$hash_key_name." FROM ".$table_name." WHERE Hash Key = '".$hash_key_value."'");
            } else {
                $response = $this->dql("CHOOSE ".$hash_key_name." FROM ".$table_name." WHERE Hash Key = '".$hash_key_value."' AND Range Key = '".$range_key_value."'");
            }

            if (!isset($response["rows"]) || count($response['rows'])!=1) {
                return $this->createGenericErrorResponse("Not an existing record or not unique. UPDATE modifies a single record filtered by hash and (optional) range keys. Use a MODIFY operation if you first want to create the record if it does not exist. You can not bulk update with either operations. UPDATE is slower as it breaks batch operations. Use MODIFY where possible.");
            }

            // this can not be batched
            $can_batch = false;
        }

        // this currently only supports EQ and only hash and range key filtering
        foreach($condition as $condition_array) {
            $target = $condition_array["TARGET"];
            $op = $condition_array["OP"];
            $params = $condition_array["PARAMS"];
            if (strtolower($target) == "hash" || strtolower($target) == "hash key" || $target == $hash_key_name) {
                // hash condition
                if ($op == "EQ") {
                    $field_pairs[$hash_key_name] = array($hash_key_type => $params[0]);
                }
            } else if (strtolower($target) == "range" || strtolower($target) == "range key" || $target == $range_key_name) {
                // range condition
                if ($op == "EQ") {
                    $field_pairs[$range_key_name] = array($range_key_type => $params[0]);
                }
            }
        }
        $insert_data = array(
            "TableName" => $table_name,
            "Item" => $field_pairs ///*$this->dynamodb->attributes(*/$values/*)*/
        );

        if ($can_batch) {
            $response = $this->addBatch("put", $table_name, $field_pairs);
        } else {
            $this->flushBatch();
            try {
                $response = $this->dynamodb->putItem($insert_data);
                // make sure the response has status information
                $response = $this->prepareResponse($response);
            } catch (\Exception $e) {
                $response = $this->handleException($e);
            }
        }
        return $response;
    }

    private function insertIntoTable($table_name, $values, $should_batch = true)
    {
        // cache table data
        $this->cacheTableDescription($table_name);

        // cast hash to number if we must
        $hash_key_name = $this->tables[$table_name]["KeySchema"]["HashKeyElement"]["AttributeName"];
        $hash_key_type = $this->tables[$table_name]["KeySchema"]["HashKeyElement"]["AttributeType"];
        if (isset($this->tables[$table_name]["KeySchema"]["RangeKeyElement"])) {
            $range_key_name = $this->tables[$table_name]["KeySchema"]["RangeKeyElement"]["AttributeName"];
            $range_key_type = $this->tables[$table_name]["KeySchema"]["RangeKeyElement"]["AttributeType"];
        } else {
            $range_key_name = "";
            $range_key_type = "";
        }

        $field_pairs = [];
        $hash_key_value = "";
        $uuid_base = "";
        foreach($values as $attr_name => $value) {
            $uuid_base .= $attr_name.$value;
            if ($attr_name == $hash_key_name) {
                $field_pairs[$attr_name] = array($hash_key_type => (string)$value);
                $hash_key_value = $value;
            } else if ($attr_name == $range_key_name && $range_key_name != "") {
                $field_pairs[$attr_name] = array($range_key_type => $value);
            } else {
                $field_pairs[$attr_name] = is_numeric($value) ? array(Type::NUMBER => (string)$value) : array(Type::STRING => (string)$value);
            }
        }
        $expected = "";
        // omitting the hash key from the value list will make this automatically create a uuid number as the hash key value
        if ($hash_key_value == "") {
            $uuid = (string)$this->generateUUID($uuid_base, 32);
            $field_pairs[$hash_key_name] = array($hash_key_type => (string)$uuid);
            $hash_key_value = $uuid;
            // if we're auto-generating a uuid, then we assume that it does not exist
            // this will make it fail with an EXCEPTION_VALIDATION if the id exists and not simply overwrite an existing record
            $expected = array($hash_key_name => array( "Exists" => false ));
        }
        $insert_data = array(
            "TableName" => $table_name,
            "Item" => $field_pairs ///*$this->dynamodb->attributes(*/$values/*)*/
        );
        if ($expected != "")
            $insert_data["Expected"] = $expected;
        //$this->debugWrite("INSERT DATA", $insert_data);

        // INSERTS do not batch, BATCHQ or BATCH do
        if ($should_batch) {
            $response = $this->addBatch("put", $table_name, $insert_data);
        } else {
            // flush the batch
            $this->flushBatch();
            // old code before batching...
            try {
                $response = $this->dynamodb->putItem($insert_data);
                // make sure the response has status information
                $response = $this->prepareResponse($response);
                $response['insertid'] = $hash_key_value;
            } catch (\Exception $e) {
                $response = $this->handleException($e);
            }
        }

        return $response;
    }

    private function deleteFromTable($table_name, $filter_condition)
    {
        $filter_condition['TableName'] = $table_name;
        try {
            $response = $this->dynamodb->deleteItem($filter_condition);
            // make sure the response has status information
            $response = $this->prepareResponse($response);
        } catch (\Exception $e) {
            $response = $this->handleException($e);
        }
        return $response;
    }

    private function createTable($table_name, $hash_key, $hash_type, $range_key, $range_type, $wait=true)
    {
        $param_array = array(
            'TableName' => $table_name,
            'KeySchema' => array(
                'HashKeyElement' => array(
                    'AttributeName' => $hash_key,
                    'AttributeType' => $this->getTypeByName($hash_type)
                )
            ),
            'ProvisionedThroughput' => array(
                'ReadCapacityUnits' => 5,
                'WriteCapacityUnits' => 5
            )
        );
        if ($range_key != "") {
            $param_array['KeySchema']['RangeKeyElement'] = array(
                'AttributeName' => $range_key,
                'AttributeType' => $this->getTypeByName($range_type)
            );
        }
        try {
            $response = $this->dynamodb->createTable($param_array);
            // make sure the response has status information
            $response = $this->prepareResponse($response);
        } catch (\Exception $e) {
            $response = $this->handleException($e);
            return $response;
        }

        if (!$wait)
            return $response;

        $count = 0;
        do {
            set_time_limit($this->executionTime);
            sleep(1);
            $count++;
            try {
                $response = $this->dynamodb->describeTable(array(
                    'TableName' => $table_name
                ));
                // make sure the response has status information
                $response = $this->prepareResponse($response);
                $table_status = $response['Table']['TableStatus'];
            } catch (\Exception $e) {
                // this should wait on if there's a ResourceNotFound exception but bail for any other exception
                $response = $this->handleException($e, "ResourceNotFound");
                if ($response['status'] != \DynamoQL\Common\Enum\Response::OK)
                    break;
                // try again later otherwise
                $table_status = "";
            }
        } while ($table_status !== 'ACTIVE' && $count < $this->waitTimeoutSeconds);

        return $response;
    }

    private function removeTable($table_name, $wait=true)
    {
        try {
            $response = $this->dynamodb->deleteTable(array(
                'TableName' => $table_name
            ));
            // make sure the response has status information
            $response = $this->prepareResponse($response);
        } catch (\Exception $e) {
            // it should not be an error when the table is not found
            $response = $this->handleException($e, "ResourceNotFoundException");
            return $response;
        }

        if (!$wait)
            return $response;

        $count = 0;
        do {
            set_time_limit($this->executionTime);
            sleep(1);
            $count++;
            try {
                $response = $this->dynamodb->describeTable(array(
                    'TableName' => $table_name
                ));
                // make sure the response has status information
                $response = $this->prepareResponse($response);
            } catch (\Exception $e) {
                // We do not want to treat a ResourceNotFoundException as an exception,
                // since this signals normal behavior (the resource has been deleted).
                // Anything else is scary!
                $response = $this->handleException($e, "ResourceNotFoundException");
                return $response;
            }
        } while ($count < $this->waitTimeoutSeconds);

        return $response;
    }

    private function resizeThroughput($table_name, $read_capacity = 5, $write_capacity = 5)
    {
        // use a cache if at all possible
        $this->cacheTableDescription($table_name);

        $current_read_capacity = $this->tables[$table_name]['ProvisionedThroughput']['ReadCapacityUnits'];
        $current_write_capacity = $this->tables[$table_name]['ProvisionedThroughput']['WriteCapacityUnits'];
        if ($read_capacity < 1) $read_capacity = 1;
        if ($write_capacity < 1) $write_capacity = 1;
        if ($read_capacity > $current_read_capacity * 2) $read_capacity = $current_read_capacity * 2;
        if ($write_capacity > $current_write_capacity * 2) $write_capacity = $current_write_capacity * 2;
        if ($current_read_capacity==$read_capacity && $current_write_capacity==$write_capacity)
            return $this->createOKResponse();
        try {
            $response = $this->dynamodb->updateTable(array(
                'TableName' => $table_name,
                'ProvisionedThroughput' => array(
                    'ReadCapacityUnits' => (int)$read_capacity,
                    'WriteCapacityUnits' => (int)$write_capacity
                )
            ));
            // make sure the response has status information
            $response = $this->prepareResponse($response);
        } catch (\Exception $e) {
            $response = $this->handleException($e);
            return $response;
        }
        // success - remove the table cache so that it's refreshed next time
        unset($this->tables[$table_name]);
        return $response;
    }

    private function extendThroughput($read_capacity = 5, $write_capacity = 5)
    {
        try {
            $response = $this->dynamodb->listTables();
            // make sure the response has status information
            $response = $this->prepareResponse($response);
        } catch (\Exception $e) {
            $response = $this->handleException($e);
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
            $response = $this->dynamodb->describeTable(array(
                'TableName' => $table_name
            ));
        } catch (\Exception $e) {
            // if there is no need for the table to exist (ie. during pre-caching)
            // then not finding the table will be treated as a \DynamoQL\Common\Enum\Response::OK
            $response = $this->handleException($e, ($must_exist ? "" : "ResourceNotFoundException"));
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
        switch(trim(substr($dql, 0, 6)))
        {
            case "FLUSH":
                break;
            case "CHOOSE":
            case "SELECT":
                $split = preg_split("/^(SELECT|Select|select|CHOOSE|Choose|choose)[\s]+(.*?)(FROM|From|from)[\s]+([^\s]*?)[\s]*?(WHERE|Where|where){0,1}?[\s]*?(.*)?/smU", $dql, -1, PREG_SPLIT_NO_EMPTY | PREG_SPLIT_DELIM_CAPTURE);
                $cmd["FIELDS"] = $this->breakupList(trim($split[1]), false);
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
                        if ($text == "LIMIT" || $text == "Limit" || $text == "limit") {
                            $mnemonic = 1;
                            continue;
                        }
                        if ($text == "STARTING" || $text == "Starting" || $text == "starting") {
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
            case "BATCH":
            case "BATCHQ":
            case "INSERT":
                $split = preg_split("/^(INSERT[\s]+INTO|Insert[\s]+Into|Insert[\s]+into|insert[\s]+into|BATCHQUEUE|BatchQueue|Batchqueue|batchqueue|BATCH[\s]+QUEUE|Batch[\s]+Queue|Batch[\s]+queue|batch[\s]+queue)[\s]+(.*?)(VALUES|Values|values)[\s]+(.*?)[\s]*?/smU", $dql, -1, PREG_SPLIT_NO_EMPTY | PREG_SPLIT_DELIM_CAPTURE);
                $cmd["TABLE"] = trim($split[1]);
                $cmd["VALUES"] = $this->breakupKeyValuePairs(trim($split[3]));
                break;
            case "MODIFY":
            case "UPDATE":
                $split = preg_split("/^(UPDATE|Update|update|MODIFY|Modify|modify)[\s]+(.*?)[\s]*?(SET|Set|set)[\s]+(.*?)[\s]*?(WHERE|Where|where){0,1}?[\s]*?(.*)?/smU", $dql, -1, PREG_SPLIT_NO_EMPTY | PREG_SPLIT_DELIM_CAPTURE);
                $cmd["TABLE"] = trim(trim($split[1]));
                $set_where = $split[3];
                $set_where = str_replace(" where ", " WHERE ", $set_where);
                $set_where = str_replace(" Where ", " WHERE ", $set_where);
                $set_where = explode("WHERE", $set_where);
                $cmd["SET"] = $this->breakupKeyValuePairs(trim($set_where[0]));
                $cmd["WHERE"] = (count($set_where)>1) ? $this->processUpdateCondition(trim($set_where[1])) : [];
                break;
            case "DELETE":
                $split = preg_split("/^(DELETE[\s]+FROM|Delete[\s]+From|Delete[\s]+from|delete[\s]+from)[\s]+(.*?)(WHERE|Where|where)[\s]*?(.*)?/smU", $dql, -1, PREG_SPLIT_NO_EMPTY | PREG_SPLIT_DELIM_CAPTURE);
                $cmd["TABLE"] = trim($split[1]);
                $cmd["WHERE"] = $this->processDeleteCondition(trim($split[1]), trim($split[3]));
                break;
            case "CREATE":
                $split = preg_split("/^(CREATE[\s]+TABLE|Create[\s]+Table|Create[\s]+table|create[\s]+table)[\s]+(.*?)(HASH|Hash|hash){1}(.*?)[\s]*/smU", $dql, -1, PREG_SPLIT_NO_EMPTY | PREG_SPLIT_DELIM_CAPTURE);
                $cmd["TABLE"] = trim($split[1]);
                $key_definitions = explode(" RANGE ", trim($split[3]));
                $hash_part = explode(" AS ", trim($key_definitions[0]));
                $range_part = count($key_definitions)>1 ? explode(" AS ", trim($key_definitions[1])) : array("", "");
                $cmd["HASH"] = $this->breakupList(trim($hash_part[0]));
                $cmd["HASH_TYPE"] = trim($hash_part[1]);
                $cmd["RANGE"] = (!empty($range_part[0])) ? $this->breakupList(trim($range_part[0])) : "";
                $cmd["RANGE_TYPE"] = trim($range_part[1]);
                break;
            case "REMOVE":
                $split = preg_split("/^(REMOVE[\s]+TABLE|Remove[\s]+Table|Remove[\s]+table|remove[\s]+table)[\s]+(.*?)[\s]*?/smU", $dql, -1, PREG_SPLIT_NO_EMPTY | PREG_SPLIT_DELIM_CAPTURE);
                $cmd["TABLE"] = trim($split[1]);
                break;
            case "RESIZE":
                $split = preg_split("/^(RESIZE[\s]+THROUGHPUT|Resize[\s]+Throughput|Resize[\s]+throughput|resize[\s]+throughput)[\s]+(.*?)[\s]+(READ|Read|read)[\s]+([0-9]+?)[\s]+(WRITE|Write|write)[\s]+([0-9]+?)[\s]*/smU", $dql, -1, PREG_SPLIT_NO_EMPTY | PREG_SPLIT_DELIM_CAPTURE);
                $cmd["TABLE"] = trim($split[1]);
                $cmd["READ"] = trim($split[3]);
                $cmd["WRITE"] = trim($split[5]);
                break;
            case "EXTEND":
                $split = preg_split("/^(EXTEND[\s]+THROUGHPUT|Extend[\s]+Throughput|Extend[\s]+throughput|extend[\s]+throughput)[\s]+(READ|Read|read)[\s]+([0-9]+?)[\s]+(WRITE|Write|write)[\s]+([0-9]+?)[\s]*/smU", $dql, -1, PREG_SPLIT_NO_EMPTY | PREG_SPLIT_DELIM_CAPTURE);
                $cmd["READ"] = trim($split[2]);
                $cmd["WRITE"] = trim($split[4]);
                break;
            default:
                $this->raiseError("DynamoQL: Syntax error in query: ".$dql);
                break;
        }

        return $cmd;
    }

    private function raiseError($msg)
    {
        error_log($msg);
        trigger_error($msg, E_USER_ERROR);
    }

    private static function getWords($str, $max_parameter_count=-1)
    {
        $split = preg_split("/[\s,]*\\\"([^\\\"]+)\\\"[\s,]*|" . "[\s,]*'([^']+)'[\s,]*|" . "[\s,]+/", $str, -1, PREG_SPLIT_NO_EMPTY | PREG_SPLIT_DELIM_CAPTURE);

        if ($max_parameter_count>-1) {
            $res = array_slice($split, 0, $max_parameter_count);
        } else {
            $res = $split;
        }
        return $res;
    }


    private static function breakupKeyValuePairs($txt)
    {
        $split = explode(",", $txt);
        $ret = [];
        foreach($split as $spl) {
            $kv = explode("=", trim($spl));
            if (!is_array($kv)) {
                DynamoQL::raiseError("DynamoQL: Illegal key-value pair. / DynamoQL::breakupKeyValuePairs('".$txt."')");
                exit;
            }
            $ret[trim($kv[0])] = trim($kv[1], " '\"");
        }
        return $ret;
    }


    private static function breakupList($txt, $simplify=true)
    {
        $split = explode(",", $txt);
        foreach($split as $sid => $spl) {
            $split[$sid] = trim($spl);
        }
        return (count($split) > 1 || !$simplify) ? $split : (count($split)>0 ? $split[0] : "");
    }

    private function processSelectCondition($raw_conditions)
    {
        $parsed_conditions = DynamoQL::parseCondition($raw_conditions);
        if (empty($parsed_conditions))
            return [];
        return $parsed_conditions;
    }

    private function processUpdateCondition($raw_conditions)
    {
        $parsed_conditions = DynamoQL::parseCondition($raw_conditions);
        if (empty($parsed_conditions))
            return [];
        return $parsed_conditions;
    }

    private function processDeleteCondition($table_name, $raw_conditions)
    {
        // cache table data
        $this->cacheTableDescription($table_name);

        // cast hash to number if we must
        $hash_key_name = $this->tables[$table_name]["KeySchema"]["HashKeyElement"]["AttributeName"];
        $hash_key_type = $this->tables[$table_name]["KeySchema"]["HashKeyElement"]["AttributeType"];
        if (isset($this->tables[$table_name]["KeySchema"]["RangeKeyElement"])) {
            $range_key_name = $this->tables[$table_name]["KeySchema"]["RangeKeyElement"]["AttributeName"];
            $range_key_type = $this->tables[$table_name]["KeySchema"]["RangeKeyElement"]["AttributeType"];
        } else {
            $range_key_name = "";
            $range_key_type = "";
        }

        $parsed_conditions = DynamoQL::parseCondition($raw_conditions);
        if (empty($parsed_conditions))
            return [];
        $result = [];
        $expected = [];
        $attributes = [];
        foreach($parsed_conditions as $condition_array) {
            $target = $condition_array["TARGET"];
            $op = $condition_array["OP"];
            $params = $condition_array["PARAMS"];

            $attributes["Table"] = $this->tables[$table_name];

            if (strtolower($target) == "hash" || strtolower($target) == "hash key" || $target == $hash_key_name) {
                // hash condition
                if ($op == "EQ") {
                    $attributes['HashKeyElement'] = array($hash_key_type => count($params)==1 ? $params[0] : $params);
                }
            } else if (strtolower($target) == "range" || strtolower($target) == "range key" || $target == $range_key_name) {
                // range condition
                if ($op == "EQ") {
                    $attributes['RangeKeyElement'] = array($range_key_type => count($params)==1 ? $params[0] : $params);
                }
            } else {
                // expected field condition
                if ($op == "EQ") {
                    if (count($params)==1) {
                        $attributes = is_numeric($params[0]) ? array(Type::NUMBER => $params[0]) : array(Type::STRING => $params[0]);
                    }
                    $expected[$target] = [];
                    $expected[$target]['Value'] = $attributes;
                }
            }
        }
        if (!empty($attributes))
            $result['Key'] = $attributes;
        if (!empty($expected))
            $result['Expected'] = $expected;

        return $result;
    }

    private function parseCondition($textual_range_key_condition="")
    {
        if (empty($textual_range_key_condition))
            return [];

        $operator_aliases = array(
            'NE' => array('!=', '<>'),
            'LE' => array('<='),
            'GE' => array('>='),
            'EQ' => array('==', '='),
            'IN' => [],
            'LT' => array('<'),
            'GT' => array('>'),
            'BETWEEN' => [],
            'NOT_' => array('! ', '!', 'NOT '),
            'NULL' => [],
            'CONTAINS' => array('HAS'),
            'BEGINS_WITH' => array('BEGINS WITH')
        );

        // replace aliases with real operators
        foreach($operator_aliases as $operator => $aliases) {
            foreach($aliases as $alias) {
                $textual_range_key_condition = str_replace($alias, " ".$operator." ", $textual_range_key_condition);
            }
        }

        $textual_range_key_condition = str_replace(" AND ", ", ", $textual_range_key_condition);
        $textual_range_key_condition = str_replace(" and ", ", ", $textual_range_key_condition);
        $condition_array = explode(",", $textual_range_key_condition);

        $condition_list = [];

        foreach($condition_array as $condition) {
            $op = "";
            //$real_operator = "";
            foreach($operator_aliases as $operator => $aliases) {
                //$real_operator = $operator;
                $operator_position = strpos($condition, ($operator=="NOT_NULL" || $operator=="NULL") ? $operator : $operator." ");
                if ($operator_position !== false) {
                    $op = $operator;
                }
                if (!empty($op))
                    break;
            }
            if (empty($op)) {
                $this->raiseError("DynamoQL: No operators found in range key condition. Execution halted. / DynamoQL::parseCondition('".$textual_range_key_condition."' in '".$condition."')");
                exit;
            }

            unset($params);
            $params = DynamoQL::getWords($condition);

            // recreate params separated by real_operator
            $operator_position = -1;
            $new_params = array(0 => "");
            $end_params = [];
            foreach($params as $pid => $param) {
                if (array_key_exists($param, $operator_aliases)) {
                    $operator_position = $pid;
                    $new_params[] = $param;
                } else if ($operator_position == -1) {
                    $new_params[0] .= " " . $param;
                    $new_params[0] = trim($new_params[0]);
                } else {
                    $new_params[] = $param;
                    $end_params[] = $param;
                }
            }
            $params = $new_params;

            $res = [];
            $res["TARGET"] = $params[0];
            $res["OP"] = $params[1];
            $res["PARAMS"] = $end_params;

            $condition_list[] = $res;
        }

        return $condition_list;
    }

    private function getNameByType($type)
    {
        $name_to_type = array(
            Type::STRING => "STRING",
            Type::NUMBER => "NUMBER",
            Type::STRING_SET => "STRINGSET",
            Type::NUMBER_SET => "NUMBERSET"
        );

        return $name_to_type[$type];
    }

    private function getTypeByName($name)
    {
        $type_to_name = array(
            "STRING" => Type::STRING,
            "NUMBER" => Type::NUMBER,
            "STRINGSET" => Type::STRING_SET,
            "NUMBERSET" => Type::NUMBER_SET
        );

        return $type_to_name[$name];
    }

    // 32 bit: 4294967295
    // 64 bit: 18446744073709551615
    //           379811802031484680
    // format: <hash[len-(bit_count/8)]><time[(bit_count/16)]><rand[(bit_count/16)]>
    private function generateUUID($data, $bit_count=32)
    {
        $hex_char_count = $bit_count/4;
        $exw = round($hex_char_count/8); // random width
        $data = sha1($data);
        $data = substr($data, 0, $hex_char_count-$exw); // hex
        $random_part = substr(sha1(microtime(true)*1000000+rand()*10000000), -$exw); // dec
        $final = $data.$random_part;
        $final = base_convert($final, 16, 10);
        //return $data."-".$random_part."(".$final.")";
        return $final;
    }

    private function extractUUID($data)
    {
        $data = base_convert($data, 10, 16);
        $bit_count = strlen($data) * 4;
        $hex_char_count = $bit_count/4;
        $exw = round($hex_char_count/8);
        return substr($data, 0, -$exw);
    }

    private function compareUUIDs($uuid1, $uuid2)
    {
        return DynamoQL::extractUUID($uuid1) == DynamoQL::extractUUID($uuid2);
    }

    private function debugWriteHTML($title, $variable, $header_only=false, $color_title="#fc6")
    {
        if ($this->debug)
            echo("<div style='border:2px solid white;font-family:monospace;background-color:darkslategray;color:white;margin:0px;'><div style='padding:3px;color:".$color_title.";font-weight:bold;'>".$title."</div>".($header_only?"":"<hr style='height:1px;color:#ccc;background-color:white;border:0px;' />\n".$variable)."</div>");
    }

    private function debugWrite($title, $variable, $header_only=false, $color_title="#fc6", $color_variable="#cfc")
    {
        $this->debugWriteHTML($title, "<pre style='padding:3px;color:".$color_variable.";'>".print_r($variable, true)."\n</pre>", $header_only, $color_title);
    }

    private function debugWriteOK($title)
    {
        $this->debugWrite($title, "", true);
    }

    private function debugWriteErrorVerbose($title, $variable)
    {
        $this->debugWrite($title, $variable, false, "#f44", "#f44");
    }

    private function debugWriteError($title)
    {
        $this->debugWrite($title, "", true, "#f44", "#f44");
    }
}
