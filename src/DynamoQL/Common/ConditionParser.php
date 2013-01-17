<?php
namespace DynamoQL\Common;

/**
 * Created by: Konrad Kiss
 * Date: 1/17/13
 * Time: 6:10 PM
 */
class ConditionParser
{
    public static function parse($textual_range_key_condition = "")
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
                throw new \DynamoQL\Common\ConditionParser\Exception\MissingOperatorException("DynamoQL: No operators found in range key condition. >>> DynamoQL::parseCondition('".$textual_range_key_condition."' contains '".$condition."')");
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
}
