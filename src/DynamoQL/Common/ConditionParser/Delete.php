<?php
namespace DynamoQL\Common\ConditionParser;

/**
 * Created by: Konrad Kiss
 * Date: 1/17/13
 * Time: 5:40 PM
 */

use Aws\DynamoDb\Enum\Type;

class Delete
{
    public static function parse( &$parsed_conditions, &$keypair, &$attributes, $table_data )
    {
        foreach ( $parsed_conditions as $condition_array ) {
            $target = $condition_array["TARGET"];
            $op     = $condition_array["OP"];
            $params = $condition_array["PARAMS"];

            $attributes["Table"] = $table_data;

            // for now only EQ conditions are supported
            if ( $op == "EQ" ) {
                if ( strtolower($target) == "hash" || strtolower($target) == "hash key" || $target == $keypair->hash->name ) {
                    // hash condition
                    $attributes['HashKeyElement'] = array($keypair->hash->type => count($params) == 1 ? $params[0] : $params);
                } else if ( strtolower($target) == "range" || strtolower($target) == "range key" || $target == $keypair->range->name ) {
                    // range condition
                    $attributes['RangeKeyElement'] = array($keypair->range->type => count($params) == 1 ? $params[0] : $params);
                } else {
                    // expected field condition
                    if ( count($params) == 1 ) {
                        $attributes = is_numeric($params[0]) ? array(Type::NUMBER => $params[0]) : array(Type::STRING => $params[0]);
                    }
                    $expected[$target]          = [];
                    $expected[$target]['Value'] = $attributes;
                }
            }
        }
    }
}
