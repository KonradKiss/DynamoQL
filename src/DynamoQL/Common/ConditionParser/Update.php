<?php
namespace DynamoQL\Common\ConditionParser;

/**
 * Created by: Konrad Kiss
 * Date: 1/17/13
 * Time: 5:40 PM
 */

use Aws\DynamoDb\Enum\Type;

class Update
{
    public static function parse(&$condition, &$keypair, &$field_pairs, &$expected)
    {
        // this currently only supports EQ and only hash and range key filtering
        foreach($condition as $condition_array) {
            $target = $condition_array["TARGET"];
            $op = $condition_array["OP"];
            $params = $condition_array["PARAMS"];
            if ($op == "EQ") {
                if (strtolower($target) == "hash" || strtolower($target) == "hash key" || $target == $keypair->hash->name) {
                    // hash condition
                    $expected[$keypair->hash->name] = array("Value" => array($keypair->hash->type => $params[0]), "Exists" => true);
                    $field_pairs[$keypair->hash->name] = array($keypair->hash->type => $params[0]);
                } elseif (strtolower($target) == "range" || strtolower($target) == "range key" || $target == $keypair->range->name) {
                    // range condition
                    $expected[$keypair->range->name] = array("Value" => array($keypair->range->type => $params[0]), "Exists" => true);
                    $field_pairs[$keypair->range->name] = array($keypair->range->type => $params[0]);
                } else {
                    $expected[$target] = array("Value" => is_numeric($params[0]) ? array(Type::NUMBER => $params[0]) : array(Type::STRING => $params[0]), "Exists" => true);
                    $field_pairs[$target] = is_numeric($params[0]) ? array(Type::NUMBER => $params[0]) : array(Type::STRING => $params[0]);
                }
            }
        }
    }
}
