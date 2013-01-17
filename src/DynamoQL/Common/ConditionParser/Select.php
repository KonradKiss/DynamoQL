<?php
namespace DynamoQL\Common\ConditionParser;

/**
 * Created by: Konrad Kiss
 * Date: 1/17/13
 * Time: 5:40 PM
 */

use Aws\DynamoDb\Enum\Type;

class Select
{
    public static function parse(&$condition, &$keypair, &$filter_list, &$hash_key_value, &$hash_specific, &$has_range)
    {
        foreach($condition as $condition_array) {
            $target = $condition_array["TARGET"];
            $op = $condition_array["OP"];
            $params = $condition_array["PARAMS"];

            $attributes = [];
            if (strtolower($target) == "hash" || strtolower($target) == "hash key" || $target == $keypair->hash->name) {
                $target = $keypair->hash->name;
                $attributes = array($keypair->hash->type => $params[0]);
                // $has_hash = true;
                if ($op == "EQ") {
                    $hash_specific = true;
                    $hash_key_value = $params[0];
                }
            }
            if ($keypair->range != null && (strtolower($target) == "range" || strtolower($target) == "range key" || $target == $keypair->range->name)) {
                $target = $keypair->range->name;
                $attributes = array($keypair->range->type => $params[0]);
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
    }
}
